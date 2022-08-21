defmodule Airbag.BufferTest do
  use ExUnit.Case, async: true

  alias Airbag.Buffer
  require Airbag.Buffer
  import ExUnit.CaptureLog

  test "initialises the buffer with default opts", %{test: test} do
    buffer = Buffer.new(test)

    schedulers = System.schedulers_online()
    assert map_size(buffer.private.partitions) == schedulers
    assert buffer.partition_count == schedulers
    identity_fn = &Function.identity/1

    assert buffer.hash_by == identity_fn
    assert buffer.name

    assert Enum.all?(buffer.private.partitions, fn {partition_index, partition} ->
             assert is_integer(partition_index)
             assert partition.reserve_loc == 0
             assert partition.read_loc == 0
             assert partition.write_loc == 0
           end)
  end

  test "initialises the buffer with custom opts", %{test: test} do
    buffer =
      Buffer.new(test,
        partition_count: 2,
        total_memory_threshold: 100,
        hash_by: &IO.inspect/1
      )

    assert buffer.hash_by == (&IO.inspect/1)
    assert buffer.partition_count == 2
    # FIXME: values less than 1500 are practically unusable
    assert buffer.total_memory_threshold == 100
  end

  test "fails to initialise with improper hash_by function" do
    assert_raise RuntimeError,
                 ":hash_by must be a function of arity 1",
                 fn ->
                   Buffer.new(TestBuffer, hash_by: :moo)
                 end

    assert_raise RuntimeError,
                 ":hash_by must be a function of arity 1",
                 fn ->
                   Buffer.new(TestBuffer, hash_by: fn _, _ -> :moo end)
                 end
  end

  test "allows to initialise >1 buffers" do
    assert Buffer.new(TestBuffer1, partition_count: 1)
    assert Buffer.new(TestBuffer2, partition_count: 1)
  end

  test "fails to initialise the same buffer name twice" do
    Buffer.new(TestBuffer, partition_count: 1)

    assert_raise RuntimeError,
                 "Buffer TestBuffer already exists",
                 fn ->
                   Buffer.new(TestBuffer, partition_count: 2)
                 end
  end

  test "info!/1 retrieves buffer", %{test: test} do
    assert buffer = Buffer.new(test)
    assert ^buffer = Buffer.info!(test)
  end

  test "info/2 optionally retrieves only buffer meta", %{test: test} do
    Buffer.new(test)
    assert %Buffer{} = buffer = Buffer.info!(test, only: :buffer_meta)
    assert buffer.partition_count == System.schedulers_online()
    refute map_size(buffer.private) == 0
  end

  test "enqueue/2 bumps read/reserve locations", %{test: test} do
    buffer = Buffer.new(test, partition_count: 1)

    name = buffer.name

    buffer = Buffer.info!(name)
    assert buffer.private.partitions[1].read_loc == 0
    assert buffer.private.partitions[1].write_loc == 0
    assert buffer.private.partitions[1].reserve_loc == 0
    assert buffer.private.partitions[1].size == 0

    assert {:ok, 1} = Buffer.enqueue(buffer, %{object: :alice})

    buffer = Buffer.info!(name)
    assert buffer.private.partitions[1].read_loc == 0
    assert buffer.private.partitions[1].write_loc == 1
    assert buffer.private.partitions[1].reserve_loc == 1
  end

  test "enqueue/2 routes to partitions", %{test: test} do
    buffer = Buffer.new(test, partition_count: 2)

    assert {:ok, 1} = Buffer.enqueue(buffer, %{object: :alice})
    assert {:ok, 2} = Buffer.enqueue(buffer, %{object: :bob})
    assert {:ok, 1} = Buffer.enqueue(buffer, %{object: :alice})
  end

  test "a custom hash_by/1 function can be supplied for routing", %{test: test} do
    partition_count = 2
    dummy_hash_by = fn _ -> 1 end
    # phash always returns 0 for dummy hash return, so always first partition is chosen
    assert :erlang.phash2(1, partition_count) == 0

    buffer = Buffer.new(test, partition_count: partition_count, hash_by: dummy_hash_by)

    for _ <- 1..10 do
      assert {:ok, 1} = Buffer.enqueue(buffer, :crypto.strong_rand_bytes(10))
    end
  end

  test "enqueue/2 works when only buffer_name is provided", %{test: test} do
    buffer = Buffer.new(test, partition_count: 1)
    assert {:ok, 1} = Buffer.enqueue(buffer.name, %{object: :alice})
  end

  test "enqueue/2 fails when memory threshold is reached", %{test: test} do
    threshold = 1500

    buffer = Buffer.new(test, partition_count: 1, total_memory_threshold: threshold)

    assert {:ok, _} = Buffer.enqueue(buffer, %{object: :smol1})

    initial_size =
      :ets.info(buffer.private.partitions[1].ref, :memory) * :erlang.system_info(:wordsize)

    filler = generate_term(threshold - initial_size)
    flat_term_size = :erts_debug.flat_size(filler)

    assert initial_size + flat_term_size == 1500
    assert {:ok, _} = Buffer.enqueue(buffer, filler)

    assert {:error, :threshold_reached} = Buffer.enqueue(buffer, %{object: :smol2})
  end

  test "dequeue/2 gets first item(s) and deletes them", %{test: test} do
    buffer = Buffer.new(test, partition_count: 1)
    {:ok, _} = Buffer.enqueue(buffer, :foo)
    {:ok, _} = Buffer.enqueue(buffer, :foobar)
    {:ok, _} = Buffer.enqueue(buffer, :foobaz)

    assert [:foo] = Buffer.dequeue(buffer, 1)
    assert length(:ets.tab2list(buffer.private.partitions[1].ref)) == 2
    assert [:foobar, :foobaz] = Buffer.dequeue(buffer, 1, limit: 3)
    assert length(:ets.tab2list(buffer.private.partitions[1].ref)) == 0
  end

  test "dequeue/2 returns empty list if no entries written", %{test: test} do
    buffer = Buffer.new(test, partition_count: 1)
    assert Buffer.dequeue(buffer, 1) == []
  end

  test "dequeue/2 works when only buffer_name is provided", %{test: test} do
    buffer = Buffer.new(test, partition_count: 1)
    assert {:ok, 1} = Buffer.enqueue(buffer.name, %{object: :alice})
    assert [%{object: :alice}] = Buffer.dequeue(buffer.name, 1)
  end

  describe "telemetry events" do
    test "enqueue/2 emits enqueue.stop", %{test: test} do
      Buffer.new(test, partition_count: 1)
      attach_event_handler(test, [:airbag, :buffer, :enqueue, :stop])
      Buffer.enqueue(test, :foo)

      assert_received {:telemetry_event, [:airbag, :buffer, :enqueue, :stop], measurements,
                       metadata}

      assert is_integer(measurements.duration)
      assert is_integer(measurements.monotonic_time)
      assert metadata.buffer_name == test
      assert metadata.partition_index == 1
    end

    test "dequeue/2 emits dequeue.stop", %{test: test} do
      buffer = Buffer.new(test, partition_count: 1)
      Buffer.enqueue(buffer.name, :foo)

      attach_event_handler(test, [:airbag, :buffer, :dequeue, :stop])

      Buffer.dequeue(buffer.name, 1)

      assert_received {:telemetry_event, [:airbag, :buffer, :dequeue, :stop], measurements,
                       metadata}

      assert is_integer(measurements.duration)
      assert is_integer(measurements.monotonic_time)
      assert measurements.data_items == 1
      assert metadata.buffer_name == test
      assert metadata.partition_index == 1
    end

    test "info!/2 emits info.start", %{test: test} do
      Buffer.new(test, partition_count: 1)

      attach_event_handler(test, [:airbag, :buffer, :info, :start])

      Buffer.info!(test)

      assert_received {:telemetry_event, [:airbag, :buffer, :info, :start], measurements,
                       metadata}

      assert is_integer(measurements.monotonic_time)
      assert metadata.buffer_name == test
    end

    test "info!/2 emits info.stop", %{test: test} do
      Buffer.new(test, partition_count: 1)

      attach_event_handler(test, [:airbag, :buffer, :info, :stop])

      Buffer.info!(test)

      assert_received {:telemetry_event, [:airbag, :buffer, :info, :stop], measurements, metadata}
      assert is_integer(measurements.monotonic_time)
      assert is_integer(measurements.duration)
      assert metadata.buffer_name == test
    end

    test "enqueue/2 emits threshold_check.stop", %{test: test} do
      b = Buffer.new(test, partition_count: 1, total_memory_threshold: 1_000_000)

      attach_event_handler(test, [:airbag, :buffer, :threshold_check, :stop])

      Buffer.enqueue(b, :foo)

      assert_received {:telemetry_event, [:airbag, :buffer, :threshold_check, :stop],
                       measurements, metadata}

      assert is_integer(measurements.monotonic_time)
      assert is_integer(measurements.duration)
      assert metadata.buffer_name == test
      assert metadata.partition_index == 1
    end
  end

  defp generate_term(max_size) do
    generate_term([], :erts_debug.flat_size([]), max_size)
  end

  defp generate_term(list, current_size, max_size) when current_size >= max_size do
    list
  end

  defp generate_term(list, _current_size, max_size) do
    list = [?x | list]
    current_size = :erts_debug.flat_size(list)
    generate_term(list, current_size, max_size)
  end

  defp attach_event_handler(handler_id, event) do
    test_pid = self()

    event_handler = fn ^event, measurements, metadata, _ ->
      send(test_pid, {:telemetry_event, event, measurements, metadata})
    end

    capture_log(fn ->
      :ok = :telemetry.attach(handler_id, event, event_handler, nil)
    end)
  end
end
