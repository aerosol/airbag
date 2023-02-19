# airbag


## Up and running quickly

**For the impatient:** <details>

![](https://media.giphy.com/media/UX3xnMG6pXEs9Njkl5/giphy.gif)

Add buffer/consumer definition to your application supervisor tree:

```elixir
 children = [
      {Airbag,
       [
         buffer_name: SomeBufferName,
         partition_count: 1,
         consumers_per_partition: 1,
         dequeue_limit: 10_000,
         total_memory_threshold: 1024 * 1024 * 1024, # default is :infinity
         processor: fn messages -> 
           IO.puts("Processing: #{inspect(messages)}") 
           :timer.sleep(500) 
         end,
         interval: :timer.seconds(10)
       ]},
       ...
```

And enqueue items as you please:

```elixir
Airbag.Buffer.enqueue(SomeBufferName, "some term")
```

The items will be processed in chunks of max 10 000 every 10 seconds as per the child specs above.

</details> 
 
--- 

# Synopsis
 
<!-- MDOC !-->

A FIFO ets buffer implementation based on [`ets_buffer`][1].

Buffer is a storage abstraction that resides in ets tables (RAM)
under the hood. Therefore, buffers can store arbitrary terms
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

Note that it's possible to exceed the threshold: if current
partition size is `Threshold - 1` and a 10 MB term is written,
the partition size is now `Threshold - 1 + 10 MB` before any
subsequent write is rejected. It is user's responsibility to
set the thresholds to a value small enough to be still able
to accept last-minute writes of maximum size.

The total memory limit is divided by the number of buffer
partitions and checked individually against it, before
the enqueue operation can proceed.

When user defined memory threshold is reached, an error
is returned to all `enqueue` requests for the affected
partition, until the terms are dequeued from it.

An empty ets table alone can allocate an arbitrary amount
of initial memory -- this is platform specific. To avoid buffer
lock-out, the threshold values must be always greater
than the size of an empty buffer. The buffer initialization
interface will try to prevent that from happening by raising
a runtime exception on memory threshold supplied too small.

Threshold values up to a machine-specific number are practically unusable,
because an empty ets table can take that space alone.

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
    * Metadata: `%{buffer_name: atom}`

  * `[:airbag, :buffer, :info, :stop]` - dispatched
    whenever `Buffer.info!/2` has finished.
    * Measurement: `%{monotonic_time: monotonic_time, duration: native}`
    * Metadata: none

  * `[:airbag, :buffer, :threshold_check, :stop]` - dispatched
    after memory partition memory size check has completed.
    * Measurement: `%{monotonic_time: monotonic_time, duration: native, size_in_bytes: pos_integer}`
    * Metadata: none

[1]: https://github.com/duomark/epocxy/blob/affd1c41aeae256050e2b2f11f2feb3532df8ebd/src/ets_buffer.erl

[2]: https://www.erlang.org/blog/scalable-ets-counters/
