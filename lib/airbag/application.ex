defmodule Airbag.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      %{
        id: ExampleBuffer1,
        start:
          {AirBag.Manager, :start_link,
           [
             %{
               buffer_name: ExampleBuffer1,
               partition_count: Application.fetch_env!(:airbag, :partition_count),
               total_memory_threshold: Application.fetch_env!(:airbag, :total_memory_threshold),
               processor: fn _ -> :ok end,
               consumers_per_partition: Application.fetch_env!(:airbag, :consumers_per_partition),
               dequeue_limit: Application.fetch_env!(:airbag, :dequeue_limit)
             }
           ]}
      },
      {Plug.Cowboy, scheme: :http, plug: Airbag.ExampleRouter, options: [port: 8099]}
      # Starts a worker by calling: Airbag.Worker.start_link(arg)
      # {Airbag.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Airbag.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
