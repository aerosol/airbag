defmodule Airbag.Buffer do
  @moduledoc false

  require Record
  alias __MODULE__

  # TODO: option for periodic sampling
  # TODO: telemetry integration for watermarks

  @default_hash_by &Function.identity/1

  defstruct [
    :name,
    :partition_count,
    total_memory_threshold: :infinity,
    hash_by: @default_hash_by,
    private: %{
      partitions: %{}
    }
  ]

  @type t :: %Buffer{}
  @type buffer_name() :: atom()
  @type partition_index() :: pos_integer()
  @type opt() ::
          {:partition_count, pos_integer()}
          | {:total_memory_threshold, pos_integer() | :infinity}
          | {:hash_by, (term() -> term())}
          | {:partition_ets_opts, list()}
  @type opts() :: [opt()]

  @default_partition_table_opts [
    :public,
    :named_table,
    :ordered_set,
    write_concurrency: true
  ]

  @meta_table_name __MODULE__
  @meta_table_opts [:public, :ordered_set, :named_table, keypos: 2, read_concurrency: true]

  Record.defrecordp(
    :buffer_meta,
    [
      :buffer_name,
      :partition_count,
      total_memory_threshold: :infinity,
      hash_by: @default_hash_by
    ]
  )

  Record.defrecordp(
    :partition_meta_entry,
    [
      :key,
      :ref,
      reserve_loc: 0,
      write_loc: 0,
      read_loc: 0
    ]
  )

  @spec new(buffer_name(), opts()) :: t()
  def new(buffer_name, opts \\ []) do
    true =
      :ets.info(@meta_table_name, :named_table) != :undefined or
        :ets.new(@meta_table_name, @meta_table_opts) == @meta_table_name

    partition_ets_opts = Keyword.get(opts, :ets_opts, @default_partition_table_opts)
    partition_count = Keyword.get(opts, :partition_count, System.schedulers_online())
    total_memory_threshold = Keyword.get(opts, :total_memory_threshold, :infinity)

    hash_by = Keyword.get(opts, :hash_by, @default_hash_by)
    is_function(hash_by, 1) || raise ":hash_by must be a function of arity 1"

    buffer_meta =
      buffer_meta(
        buffer_name: buffer_name,
        partition_count: partition_count,
        total_memory_threshold: total_memory_threshold,
        hash_by: hash_by
      )

    :ets.insert_new(@meta_table_name, buffer_meta) ||
      raise "Buffer #{inspect(buffer_name)} already exists"

    partitions_meta =
      write_partition_meta(
        buffer_name,
        partition_count,
        partition_ets_opts,
        total_memory_threshold
      )

    to_info(buffer_meta, partitions_meta)
  end

  @spec enqueue(t() | buffer_name(), term()) ::
          {:ok, partition_index()}
          | {:error, :threshold_reached}
  def enqueue(buffer_name, term) when is_atom(buffer_name) do
    buffer = info!(buffer_name, only: :buffer_meta)
    enqueue(buffer, term)
  end

  def enqueue(%Buffer{} = buffer, term) do
    start = System.monotonic_time()

    dest_partition_index =
      if buffer.partition_count == 1 do
        1
      else
        term
        |> buffer.hash_by.()
        |> :erlang.phash2(buffer.partition_count)
        |> Kernel.+(1)
      end

    result =
      if threshold_reached?(buffer, dest_partition_index) do
        {:error, :threshold_reached}
      else
        meta_table_key = {buffer.name, dest_partition_index}
        reserve_loc = update(meta_table_key, reserve_write_cmd())
        :ets.insert(partition_table_name(buffer.name, dest_partition_index), {reserve_loc, term})
        update(meta_table_key, publish_write_cmd())

        {:ok, dest_partition_index}
      end

    stop = System.monotonic_time()

    :telemetry.execute(
      [:airbag, :buffer, :enqueue, :stop],
      %{
        duration: stop - start,
        monotonic_time: stop
      },
      %{
        buffer_name: buffer.name,
        partition_index: dest_partition_index
      }
    )

    result
  end

  @spec dequeue(t() | buffer_name(), partition_index(), [{:limit, pos_integer()}]) :: list(any())
  def dequeue(buffer_or_buffer_name, partition_index, opts \\ [])

  def dequeue(%Buffer{} = buffer, partition_index, opts) do
    dequeue(buffer.name, partition_index, opts)
  end

  def dequeue(buffer_name, partition_index, opts) when is_atom(buffer_name) do
    start = System.monotonic_time()
    limit = Keyword.get(opts, :limit, 1)
    key = {buffer_name, partition_index}
    partition_table_name = partition_table_name(buffer_name, partition_index)

    write_loc = update(key, get_write_cmd())
    [start_read_loc, end_read_loc] = update(key, reserve_read_cmd(write_loc, limit))

    match = {:"$1", :"$2"}
    guard = [{:andalso, {:>, :"$1", start_read_loc}, {:"=<", :"$1", end_read_loc}}]
    match_specs_read = [{match, guard, [:"$2"]}]
    match_specs_delete = [{match, guard, [true]}]

    case :ets.select(partition_table_name, match_specs_read) do
      [] ->
        []

      data ->
        :ets.select_delete(partition_table_name, match_specs_delete)
        stop = System.monotonic_time()

        :telemetry.execute(
          [:airbag, :buffer, :dequeue, :stop],
          %{
            duration: stop - start,
            monotonic_time: stop,
            data_items: Enum.count(data)
          },
          %{buffer_name: buffer_name, partition_index: partition_index, limit: limit}
        )

        data
    end
  end

  @spec info!(buffer_name(), [{:only, :buffer_meta}]) :: t()
  def info!(buffer_name, opts \\ []) do
    :telemetry.span([:airbag, :buffer, :info], %{buffer_name: buffer_name}, fn ->
      match_specs_buffer_meta = {{:buffer_meta, buffer_name, :_, :_, :_}, [], [:"$_"]}

      match_specs_partition_meta =
        {{:partition_meta_entry, {buffer_name, :_}, :_, :_, :_, :_}, [], [:"$_"]}

      match_specs =
        case Keyword.get(opts, :only) do
          nil -> [match_specs_buffer_meta, match_specs_partition_meta]
          :buffer_meta -> [match_specs_buffer_meta]
        end

      case :ets.select(@meta_table_name, match_specs) do
        [] -> raise "Invalid buffer #{inspect(buffer_name)}"
        meta -> {to_info(meta), %{buffer_name: buffer_name}}
      end
    end)
  end

  defp threshold_reached?(%Buffer{total_memory_threshold: :infinity}, _) do
    false
  end

  defp threshold_reached?(
         %Buffer{
           name: buffer_name,
           total_memory_threshold: total_memory_threshold,
           partition_count: partition_count
         },
         partition_index
       ) do
    start = System.monotonic_time()
    partition_table = partition_table_name(buffer_name, partition_index)
    single_capacity_in_bytes = floor(total_memory_threshold / partition_count)
    size_in_bytes = :ets.info(partition_table, :memory) * :erlang.system_info(:wordsize)
    result = size_in_bytes > single_capacity_in_bytes
    stop = System.monotonic_time()

    :telemetry.execute(
      [:airbag, :buffer, :threshold_check, :stop],
      %{
        monotonic_time: stop,
        duration: stop - start,
        size_in_bytes: size_in_bytes
      },
      %{
        buffer_name: buffer_name,
        partition_index: partition_index
      }
    )

    result
  end

  defp partition_table_name(buffer_name, partition_index) do
    Module.concat(buffer_name, "P#{partition_index}")
  end

  defp write_partition_meta(buffer_name, partition_count, ets_opts, total_memory_threshold)
       when is_integer(partition_count) and partition_count > 0 and is_list(ets_opts) do
    1..partition_count
    |> Enum.map(fn partition_index ->
      partition_table_name = partition_table_name(buffer_name, partition_index)
      ^partition_table_name = :ets.new(partition_table_name, ets_opts)

      if is_integer(total_memory_threshold) do
        empty_partition_table_size =
          :ets.info(partition_table_name, :memory) * :erlang.system_info(:wordsize)

        partition_memory_threshold = floor(total_memory_threshold / partition_count)

        if empty_partition_table_size >= partition_memory_threshold do
          raise """
          Failed to create usable buffer partition with total_memory_threshold=#{total_memory_threshold} bytes.
          An empty partition table size is #{empty_partition_table_size} bytes.
          """
        end
      end

      partition_meta_entry =
        partition_meta_entry(
          key: {buffer_name, partition_index},
          ref: partition_table_name
        )

      true = :ets.insert_new(__MODULE__, partition_meta_entry)
      partition_meta_entry
    end)
  end

  defp to_info(buffer_meta, partitions_meta)
       when is_tuple(buffer_meta) and is_list(partitions_meta) do
    to_info([buffer_meta | partitions_meta])
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
            partition_count: buffer_meta(meta_record, :partition_count),
            total_memory_threshold: buffer_meta(meta_record, :total_memory_threshold),
            hash_by: buffer_meta(meta_record, :hash_by)
        }

      :partition_meta_entry ->
        {_, partition_index} = partition_meta_entry(meta_record, :key)
        ref = partition_meta_entry(meta_record, :ref)
        size = :ets.info(ref, :size)

        partitions =
          Map.put(info.private.partitions, partition_index, %{
            reserve_loc: partition_meta_entry(meta_record, :reserve_loc),
            read_loc: partition_meta_entry(meta_record, :read_loc),
            write_loc: partition_meta_entry(meta_record, :write_loc),
            ref: ref,
            size: size
          })

        %{info | private: %{partitions: partitions}}
    end
  end

  defp update(key, update_command) do
    :ets.update_counter(
      @meta_table_name,
      key,
      update_command
    )
  end

  defp reserve_write_cmd(), do: {partition_meta_entry(:reserve_loc) + 1, 1}

  defp publish_write_cmd(),
    do: [{partition_meta_entry(:write_loc) + 1, 1}, {partition_meta_entry(:read_loc) + 1, 0}]

  defp get_write_cmd(),
    do: {partition_meta_entry(:write_loc) + 1, 0}

  defp reserve_read_cmd(write_loc, num_items),
    do: [
      {partition_meta_entry(:read_loc) + 1, 0},
      {partition_meta_entry(:read_loc) + 1, num_items, write_loc - 1, write_loc}
    ]
end
