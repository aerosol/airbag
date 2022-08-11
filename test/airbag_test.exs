defmodule AirbagTest do
  use ExUnit.Case

  alias Airbag.Buffer
  require Airbag.Buffer

  test "initialises the buffer with default opts" do
    {:ok, buffer} = Buffer.new(TestBuffer)

    schedulers = System.schedulers_online()
    assert map_size(buffer.private.shards) == schedulers
    identity_fn = &Function.identity/1

    assert buffer.partitions == schedulers
    assert buffer.hash_by == identity_fn
    assert buffer.name

    assert Enum.all?(buffer.private.shards, fn {partition, shard} ->
             assert is_integer(partition)
             assert shard.reserve_loc == 0
             assert shard.read_loc == 0
             assert shard.write_loc == 0
           end)
  end

  test "initialises the buffer with custom opts" do
    {:ok, buffer} =
      Buffer.new(TestBuffer,
        partitions: 2,
        total_memory_threshold: 100,
        hash_by: &IO.inspect/1
      )

    assert buffer.hash_by == (&IO.inspect/1)
  end

  test "allows to initialise >1 buffers" do
    assert {:ok, _} = Buffer.new(TestBuffer1, partitions: 1)
    assert {:ok, _} = Buffer.new(TestBuffer2, partitions: 1)
  end

  test "fails to initialise the same buffer name twice" do
    {:ok, _} = Buffer.new(TestBuffer, partitions: 1)

    assert_raise RuntimeError,
                 "Buffer TestBuffer already exists",
                 fn ->
                   Buffer.new(TestBuffer, partitions: 2)
                 end
  end

  test "buffer/1 retrieves buffer buffer" do
    assert {:ok, buffer} = Buffer.new(TestBuffer)
    assert ^buffer = Buffer.info!(TestBuffer)
  end

  test "put/2 bumps read/reserve locations" do
    {:ok, buffer} = Buffer.new(TestBuffer, partitions: 1)

    name = buffer.name

    buffer = Buffer.info!(name)
    assert buffer.private.shards[1].read_loc == 0
    assert buffer.private.shards[1].write_loc == 0
    assert buffer.private.shards[1].reserve_loc == 0

    assert {:ok, 1} = Buffer.put(buffer, %{object: :alice})

    buffer = Buffer.info!(name)
    assert buffer.private.shards[1].read_loc == 0
    assert buffer.private.shards[1].write_loc == 1
    assert buffer.private.shards[1].reserve_loc == 1
  end

  test "put/2 routes to shards" do
    {:ok, buffer} = Buffer.new(TestBuffer, partitions: 2)

    assert {:ok, 1} = Buffer.put(buffer, %{object: :alice})
    assert {:ok, 2} = Buffer.put(buffer, %{object: :bob})
    assert {:ok, 1} = Buffer.put(buffer, %{object: :alice})
  end

  test "a custom hash_by/1 function can be supplied for routing" do
    partitions = 2
    dummy_hash_by = fn _ -> 1 end
    # phash always returns 0 for dummy hash return, so always first partition is chosen
    assert :erlang.phash2(1, partitions) == 0

    {:ok, buffer} = Buffer.new(TestBuffer, partitions: partitions, hash_by: dummy_hash_by)

    for _ <- 1..10 do
      assert {:ok, 1} = Buffer.put(buffer, :crypto.strong_rand_bytes(10))
    end
  end

  test "put/2 fails when memory threshold is reached" do
    threshold = 1500

    {:ok, buffer} = Buffer.new(TestBuffer, partitions: 1, total_memory_threshold: threshold)

    assert {:ok, _} = Buffer.put(buffer, %{object: :smol1})

    initial_size =
      :ets.info(buffer.private.shards[1].ref, :memory) * :erlang.system_info(:wordsize)

    filler = generate_term(threshold - initial_size)
    flat_term_size = :erts_debug.flat_size(filler)

    assert initial_size + flat_term_size == 1500
    assert {:ok, _} = Buffer.put(buffer, filler)

    assert {:error, :threshold_reached} = Buffer.put(buffer, %{object: :smol2})
  end

  test "pop/2 gets first item(s) and deletes them" do
    {:ok, buffer} = Buffer.new(TestBuffer, partitions: 1)
    {:ok, _} = Buffer.put(buffer, :foo)
    {:ok, _} = Buffer.put(buffer, :foobar)
    {:ok, _} = Buffer.put(buffer, :foobaz)

    assert [:foo] = Buffer.pop(buffer, 1)
    assert length(:ets.tab2list(buffer.private.shards[1].ref)) == 2
    assert [:foobar, :foobaz] = Buffer.pop(buffer, 1, limit: 3)
    assert length(:ets.tab2list(buffer.private.shards[1].ref)) == 0
  end

  test "pop/2 returns nil if no entries written" do
    {:ok, buffer} = Buffer.new(TestBuffer, partitions: 1)
    refute Buffer.pop(buffer, 1)
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
