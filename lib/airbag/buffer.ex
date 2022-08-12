defmodule Airbag.Buffer do
  require Record
  alias __MODULE__

  # TODO: option for periodic sampling
  # TODO: telemetry integration for watermarks
  # TODO: test distributed_counters impact on ets:size

  defstruct [
    :name,
    partitions: 0,
    total_memory_threshold: :infinity,
    hash_by: &Function.identity/1,
    private: %{
      shards: %{}
    }
  ]

  @type t :: %Buffer{}

  Record.defrecord(
    :buffer_meta,
    [
      :buffer_name,
      partitions: 1,
      total_memory_threshold: :infinity,
      hash_by: &Function.identity/1
    ]
  )

  Record.defrecord(
    :shard_meta_entry,
    [
      :key,
      :ref,
      reserve_loc: 0,
      write_loc: 0,
      read_loc: 0
    ]
  )

  @type buffer_name() :: atom()
  @type partition() :: non_neg_integer()

  @opaque shard_meta_entry() ::
            record(:shard_meta_entry,
              key: {buffer_name(), partition()},
              ref: atom(),
              reserve_loc: non_neg_integer(),
              write_loc: non_neg_integer(),
              read_loc: non_neg_integer()
            )

  @type shard_meta() :: list(shard_meta_entry())

  @opaque buffer_meta() ::
            record(:buffer_meta,
              buffer_name: buffer_name(),
              partitions: partition(),
              total_memory_threshold: non_neg_integer() | :infinity,
              hash_by: (term() -> term())
            )

  @type opt :: [
          {:partitions, non_neg_integer()}
          | {:total_memory_threshold, non_neg_integer() | :infinity}
          | {:hash_by, (term() -> term())}
          | {:shard_ets_opts, list()}
        ]
  @type opts :: [opt()]

  @default_shard_table_opts [
    :public,
    :named_table,
    :ordered_set,
    write_concurrency: true
  ]

  @meta_table_name __MODULE__
  @meta_table_opts [:public, :ordered_set, :named_table, keypos: 2, read_concurrency: true]

  @spec new(buffer_name(), opts()) :: t()
  def new(buffer_name, opts \\ []) do
    true =
      :ets.info(@meta_table_name, :named_table) != :undefined or
        :ets.new(@meta_table_name, @meta_table_opts) == @meta_table_name

    shard_ets_opts = Keyword.get(opts, :ets_opts, @default_shard_table_opts)
    partitions = Keyword.get(opts, :partitions, System.schedulers_online())
    total_memory_threshold = Keyword.get(opts, :total_memory_threshold, :infinity)

    hash_by = Keyword.get(opts, :hash_by, &Function.identity/1)
    true = is_function(hash_by, 1)

    buffer_meta =
      buffer_meta(
        buffer_name: buffer_name,
        partitions: partitions,
        total_memory_threshold: total_memory_threshold,
        hash_by: hash_by
      )

    :ets.insert_new(@meta_table_name, buffer_meta) ||
      raise "Buffer #{inspect(buffer_name)} already exists"

    shard_meta =
      write_shard_meta(
        buffer_name,
        partitions,
        shard_ets_opts
      )

    to_info(buffer_meta, shard_meta)
  end

  @spec put(t(), term()) ::
          {:ok, partition()}
          | {:error, :threshold_reached}
  def put(buffer = %Buffer{}, term) do
    dest_partition =
      term
      |> buffer.hash_by.()
      |> :erlang.phash2(buffer.partitions)
      |> Kernel.+(1)

    shard_table_name = shard_table_name(buffer.name, dest_partition)
    meta_table_key = {buffer.name, dest_partition}

    if threshold_reached?(shard_table_name, buffer) do
      {:error, :threshold_reached}
    else
      reserve_loc = update(meta_table_key, reserve_write_cmd())
      :ets.insert(shard_table_name, {reserve_loc, term})
      update(meta_table_key, publish_write_cmd())

      {:ok, dest_partition}
    end
  end

  @spec pop(t(), non_neg_integer(), [{:limit, non_neg_integer()}]) :: nil | list(any())
  def pop(buffer = %Buffer{}, partition, opts \\ []) do
    limit = Keyword.get(opts, :limit, 1)
    buffer_name = buffer.name
    key = {buffer_name, partition}
    shard_table_name = shard_table_name(buffer_name, partition)

    write_loc = update(key, get_write_cmd())
    [start_read_loc, end_read_loc] = update(key, reserve_read_cmd(write_loc, limit))

    match = {:"$1", :"$2"}
    guard = [{:andalso, {:>, :"$1", start_read_loc}, {:"=<", :"$1", end_read_loc}}]
    match_specs_read = [{match, guard, [:"$2"]}]
    match_specs_delete = [{match, guard, [true]}]

    case :ets.select(shard_table_name, match_specs_read) do
      [] ->
        nil

      data ->
        :ets.select_delete(shard_table_name, match_specs_delete)
        data
    end
  end

  @spec info!(buffer_name()) :: t()
  def info!(buffer_name) do
    match_specs = [
      {{:buffer_meta, buffer_name, :_, :_, :_}, [], [:"$_"]},
      {{:shard_meta_entry, {buffer_name, :_}, :_, :_, :_, :_}, [], [:"$_"]}
    ]

    case :ets.select(@meta_table_name, match_specs) do
      [] -> raise "Invalid buffer #{buffer_name}"
      meta -> to_info(meta)
    end
  end

  defp threshold_reached?(_shard_table, %Buffer{total_memory_threshold: :infinity}) do
    false
  end

  defp threshold_reached?(shard_table, %Buffer{
         total_memory_threshold: total_memory_threshold,
         partitions: partitions
       }) do
    single_capacity_in_bytes = floor(total_memory_threshold / partitions)
    size_in_bytes = :ets.info(shard_table, :memory) * :erlang.system_info(:wordsize)
    size_in_bytes > single_capacity_in_bytes
  end

  defp shard_table_name(buffer_name, partition) do
    Module.concat(buffer_name, "P#{partition}")
  end

  defp write_shard_meta(buffer_name, partitions, ets_opts)
       when is_integer(partitions) and partitions > 0 and is_list(ets_opts) do
    1..partitions
    |> Enum.map(fn partition ->
      shard_table_name = shard_table_name(buffer_name, partition)
      ^shard_table_name = :ets.new(shard_table_name, ets_opts)

      shard_meta_entry =
        shard_meta_entry(
          key: {buffer_name, partition},
          ref: shard_table_name
        )

      true = :ets.insert_new(__MODULE__, shard_meta_entry)
      shard_meta_entry
    end)
  end

  defp to_info(buffer_meta, shard_meta) when is_tuple(buffer_meta) and is_list(shard_meta) do
    to_info([buffer_meta | shard_meta])
  end

  defp to_info(meta_records) when is_list(meta_records) do
    Enum.reduce(meta_records, %Buffer{}, fn meta_record, info ->
      to_info_reducer(info, meta_record)
    end)
  end

  defp to_info_reducer(%Buffer{} = info, meta_record) when is_tuple(meta_record) do
    case elem(meta_record, 0) do
      :buffer_meta ->
        %{
          info
          | name: buffer_meta(meta_record, :buffer_name),
            partitions: buffer_meta(meta_record, :partitions),
            total_memory_threshold: buffer_meta(meta_record, :total_memory_threshold),
            hash_by: buffer_meta(meta_record, :hash_by)
        }

      :shard_meta_entry ->
        {_, partition} = shard_meta_entry(meta_record, :key)

        shards =
          Map.put(info.private.shards, partition, %{
            reserve_loc: shard_meta_entry(meta_record, :reserve_loc),
            read_loc: shard_meta_entry(meta_record, :read_loc),
            write_loc: shard_meta_entry(meta_record, :write_loc),
            ref: shard_meta_entry(meta_record, :ref)
          })

        %{info | private: %{shards: shards}}
    end
  end

  defp update(key, update_command) do
    :ets.update_counter(
      @meta_table_name,
      key,
      update_command
    )
  end

  defp reserve_write_cmd(), do: {shard_meta_entry(:reserve_loc) + 1, 1}

  defp publish_write_cmd(),
    do: [{shard_meta_entry(:write_loc) + 1, 1}, {shard_meta_entry(:read_loc) + 1, 0}]

  defp get_write_cmd(),
    do: {shard_meta_entry(:write_loc) + 1, 0}

  defp reserve_read_cmd(write_loc, num_items),
    do: [
      {shard_meta_entry(:read_loc) + 1, 0},
      {shard_meta_entry(:read_loc) + 1, num_items, write_loc - 1, write_loc}
    ]
end
