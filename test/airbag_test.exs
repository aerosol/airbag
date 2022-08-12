defmodule AirbagTest do
  use ExUnit.Case

  alias Airbag.Buffer
  require Airbag.Buffer

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
end
