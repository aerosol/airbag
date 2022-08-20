defmodule Airbag.Buffer do
  @moduledoc """
  A FIFO ets buffer implementation based on [`ets_buffer`][1].

  Buffer is a storage abstraction that resides in ets tables (RAM)
  under the hood. Therfore, buffers can store arbitrary terms
  (e.g. maps, structs, functions - literally `any()`)
  and are designed for concurrent access with a simple locking
  mechanism ensuring serializability.

  Because the buffer implements FIFO characteristics,
  read/write operations are called enqueue/dequeue
  respectively.

  Buffer entries are reserved for reading when dequeuing and
  deleted immediately afterwards, becoming permanently unavailable.

  Main differences to the [reference implementation][1] are:

    * only FIFO buffer type is exposed
    * buffers can be partitioned (see: "Partitioning" section)
    * customizable ets initialization options
    * total maximum memory threshold in bytes can be set to
      prevent overflow (although with caveats, see: "Memory
      Limits" section)
    * instrumentation with telemetry events (see: "Instrumentation"
      section)

  # Partitioning

  A buffer can be split into user defined number of partitions
  (`:partition_count`). Each partition is implemented as
  a separate ets table to increase read/write throughput
  in highly concurrent scenarios.

  Each partition is identified by a positive integer called
  `:partition_index`.

  Each partition entry contains one term passed to the `enqueue`
  operation. Multiple entries can be dequeued from a specific 
  given partition in one shot.

  In other words, clients performing reads must be aware
  of `:partition_index` they are accessing. Clients performing
  writes, on the other hand, are unaware of the target
  partition, up until the write operation is completed.

  In order to route an object to its target partition,
  a consistent hashing function is applied either on the whole
  term or on a result of user defined function applied on that term.

  e.g. users can enqueue `Plug.Conn` structs in a partitioned buffer,
  but in order to ensure all connection objects from a given
  HTTP client end up in the same partition, a custom `:hash_by`
  function must be provided that only returns the value of client's
  IP address for which the actual routing hash is calculated then.

  By default, the number of partitions is equal to the number of
  available schedulers online, but it is possible to configure
  a buffer with only one partition - in this case that buffer can be,
  in some scenarios, a better alternative to a singleton GenServer
  processing its own message queue.

  # Memory Limits

  By default, buffer storage size is limited by the RAM available.
  In this scenario however the VM node can crash when too much 
  memory is consumed.

  Users can configure their buffers with `:total_memory_threshold`
  option, expressed in bytes and calculated dynamically using 
  the word size of the host architecture on each write.

  The total memory limit is divided by the number of buffer 
  partitions and checked individually against it, before 
  the enqueue operation can proceed.

  When user defined memory threshold is reached, an error
  is returned to all `enqueue` requests for the affected
  partition, until the terms are dequeued from it. 

  To compute each partition memory consumption dynamically, 
  `:ets.info(t, :memory)` is called. This design decision 
  comes with at a performance penalty in case of
  `decentralized_counters` [enabled][2].

  Depending on your use case, it might be still beneficial
  to keep that cost, as the overall throughput may be still
  much better than with `decentralized_counters` set to `false`.

  Telemetry events (see below) emitted by the library 
  should be a helpful starting point in evaluating 
  specific setup decisions.

  # Instrumentation

  Airbag uses the `:telemetry` library for instrumentation. 
  The following events are published by `Airbag.Buffer` with 
  the following measurements and metadata:

    * `[:airbag, :buffer, :enqueue, :stop]` - dispatched 
      whenever a term has been stored in a buffer partition.
      * Measurement: `%{monotonic_time: monotonic_time, duration: native}`
      * Metadata: `%{buffer_name: atom, partition_index: pos_integer}`

    * `[:airbag, :buffer, :dequeue, :stop]` - dispatched 
      whenever a list of terms has been retrieved and deleted 
      from a buffer partition.
      * Measurement: `%{monotonic_time: monotonic_time, duration: native, data_items: non_neg_integer}`
      * Metadata: `%{buffer_name: atom, partition_index: pos_integer, limit: pos_integer}`

    * `[:airbag, :buffer, :info, :start]` - dispatched 
      whenever `Buffer.info!/2` was called.
      * Measurement: `%{monotonic_time: monotonic_time}`
      * Metadata: none

    * `[:airbag, :buffer, :info, :stop]` - dispatched 
      whenever `Buffer.info!/2` has finished.
      * Measurement: `%{monotonic_time: monotonic_time, duration: native}`
      * Metadata: none

    * `[:airbag, :buffer, :threshold_check, :start]` - dispatched 
      before checking for partition memory size, when `:total_memory_threshold`
      is an integer and not `:infinity`
      * Measurement: `%{monotonic_time: monotonic_time}`
      * Metadata: none

    * `[:airbag, :buffer, :threshold_check, :stop]` - dispatched 
      after memory partition memory size check has completed.
      * Measurement: `%{monotonic_time: monotonic_time, duration: native}`
      * Metadata: none

  [1]: https://github.com/duomark/epocxy/blob/affd1c41aeae256050e2b2f11f2feb3532df8ebd/src/ets_buffer.erl

  [2]: https://www.erlang.org/blog/scalable-ets-counters/
  """
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
        partition_ets_opts
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

  def enqueue(buffer = %Buffer{}, term) do
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

    partition_table_name = partition_table_name(buffer.name, dest_partition_index)
    meta_table_key = {buffer.name, dest_partition_index}

    result =
      if threshold_reached?(partition_table_name, buffer) do
        {:error, :threshold_reached}
      else
        reserve_loc = update(meta_table_key, reserve_write_cmd())
        :ets.insert(partition_table_name, {reserve_loc, term})
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

  def dequeue(buffer = %Buffer{}, partition_index, opts) do
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
        meta -> {to_info(meta), %{}}
      end
    end)
  end

  defp threshold_reached?(_partition_table, %Buffer{total_memory_threshold: :infinity}) do
    false
  end

  defp threshold_reached?(partition_table, %Buffer{
         name: buffer_name,
         total_memory_threshold: total_memory_threshold,
         partition_count: partition_count
       }) do
    :telemetry.span([:airbag, :buffer, :threshold_check], %{buffer_name: buffer_name}, fn ->
      single_capacity_in_bytes = floor(total_memory_threshold / partition_count)
      size_in_bytes = :ets.info(partition_table, :memory) * :erlang.system_info(:wordsize)
      {size_in_bytes > single_capacity_in_bytes, %{size_in_bytes: size_in_bytes}}
    end)
  end

  defp partition_table_name(buffer_name, partition_index) do
    Module.concat(buffer_name, "P#{partition_index}")
  end

  defp write_partition_meta(buffer_name, partitions, ets_opts)
       when is_integer(partitions) and partitions > 0 and is_list(ets_opts) do
    1..partitions
    |> Enum.map(fn partition_index ->
      partition_table_name = partition_table_name(buffer_name, partition_index)
      ^partition_table_name = :ets.new(partition_table_name, ets_opts)

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
