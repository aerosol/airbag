defmodule Airbag.Consumer do
  use GenServer
  alias Airbag.Buffer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    _ = Buffer.info!(opts.buffer_name)
    IO.puts("Consumer #{inspect(opts)} is up")
    {:ok, opts, {:continue, :start_reading}}
  end

  @impl true
  def handle_call(_info, _, state) do
    {:reply, :got, state}
  end

  @impl true
  def handle_continue(:start_reading, state) do
    send(self(), :read)
    {:noreply, state}
  end

  @impl true
  def handle_info(:read, state) do
    case Buffer.dequeue(state.buffer_name, state.partition_index, limit: state.dequeue_limit) do
      [] ->
        :timer.sleep(1)
        send(self(), :read)

      objects ->
        Enum.each(objects, state.processor)
        send(self(), :read)
    end

    {:noreply, state}
  end
end
