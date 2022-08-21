defmodule Airbag.IntegrationTest do
  use ExUnit.Case

  alias Airbag.Buffer

  test "supervisor spec" do
    assert spec =
             Airbag.child_spec(
               buffer_name: TestBuffer,
               partition_count: 1,
               consumers_per_partition: 1,
               dequeue_limit: 10_000,
               total_memory_threshold: 1024 * 1024 * 1024,
               processor: fn _messages -> :timer.sleep(500) end
             )

    assert spec.id == TestBuffer
    assert {Airbag.Supervisor, :start_link, [init_arg]} = spec.start
    assert Keyword.get(init_arg, :buffer_name) == TestBuffer
    assert Keyword.get(init_arg, :consumers_per_partition) == 1
    assert Keyword.get(init_arg, :dequeue_limit) == 10_000
    assert Keyword.get(init_arg, :partition_count) == 1
    assert is_function(Keyword.get(init_arg, :processor), 1)
  end

  test "supervisor starts buffers and consumers" do
    test_pid = self()

    init_arg = [
      buffer_name: TestBuffer,
      partition_count: 2,
      total_memory_threshold: 10_000,
      processor: fn got -> send(test_pid, {:processed, got}) end
    ]

    {:ok, _supervisor} = Airbag.Supervisor.start_link(init_arg)
    Buffer.enqueue(TestBuffer, :first_message)
    assert_receive {:processed, [:first_message]}
  end
end
