defmodule DemoWeb.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    :ok = Airbag.LogTelemetry.init()

    children = [
      {Airbag,
       [
         buffer_name: DemoBufferSingleton,
         partition_count: 1,
         consumers_per_partition: 1,
         dequeue_limit: 10_000,
         total_memory_threshold: 1024 * 1024 * 1024,
         processor: fn _messages -> :timer.sleep(500) end,
         interval: 500
       ]},
      {Airbag,
       [
         buffer_name: DemoBufferPartitioned,
         partition_count: System.schedulers_online(),
         consumers_per_partition: System.schedulers_online(),
         dequeue_limit: 10_000,
         total_memory_threshold: 1024 * 1024 * 1024,
         processor: fn _messages -> :timer.sleep(500) end,
         interval: 500
       ]},
      {Plug.Cowboy, scheme: :http, plug: DemoWeb.Router, options: [port: 8099]}
    ]

    opts = [strategy: :one_for_one, name: DemoWeb.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
