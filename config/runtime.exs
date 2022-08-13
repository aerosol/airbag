import Config

config :airbag,
  partition_count: System.get_env("PARTITION_COUNT") |> String.to_integer(),
  total_memory_threshold: System.get_env("TOTAL_MEMORY_THRESHOLD") |> String.to_integer(),
  consumers_per_partition: System.get_env("CONSUMERS_PER_PARTITION") |> String.to_integer(),
  dequeue_limit: System.get_env("DEQUEUE_LIMIT") |> String.to_integer()
