defmodule Airbag.Supervisor do
  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl true
  def init(opts) do
    opts = Enum.into(opts, [])

    manager_opts =
      Keyword.take(opts, [
        :buffer_name,
        :partition_count,
        :total_memory_threshold,
        :hash_by,
        :partition_ets_opts
      ])

    buffer_consumers =
      for partition_index <-
            1..Keyword.get(manager_opts, :partition_count, System.schedulers_online()) do
        for consumer_index <- 1..Keyword.get(opts, :consumers_per_partition, 1) do
          %{
            id: {Airbag.Consumer, partition_index, consumer_index},
            start: {
              Airbag.Consumer,
              :start_link,
              [
                [
                  buffer_name: Keyword.fetch!(opts, :buffer_name),
                  partition_index: partition_index,
                  dequeue_limit: Keyword.get(opts, :dequeue_limit, 1),
                  processor: Keyword.fetch!(opts, :processor)
                ]
              ]
            }
          }
        end
      end
      |> Enum.flat_map(& &1)

    children = [
      {Airbag.Manager, manager_opts} | buffer_consumers
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
