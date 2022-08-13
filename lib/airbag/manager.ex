defmodule AirBag.Manager do
  use Agent

  alias Airbag.Buffer

  def start_link(opts) do
    {:ok, pid} =
      Agent.start_link(fn ->
        buffer_opts =
          Map.take(opts, [
            :partition_count,
            :total_memory_threshold,
            :hash_by,
            :partition_ets_opts
          ])
          |> Enum.into([])

        Buffer.new(opts.buffer_name, buffer_opts)
        |> IO.inspect(label: :buffer_initialized)
      end)

    for partition_index <- 1..Map.get(opts, :partition_count, System.schedulers_online()) do
      for _ <- 1..opts.consumers_per_partition do
        {:ok, _} =
          GenServer.start_link(Airbag.Consumer, %{
            buffer_name: opts.buffer_name,
            partition_index: partition_index,
            dequeue_limit: opts.dequeue_limit,
            processor: opts.processor
          })
      end
    end

    IO.puts("Workers initialized.")

    {:ok, pid}
  end
end
