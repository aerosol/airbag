defmodule Airbag.Consumer do
  @behaviour :gen_cycle
  alias __MODULE__
  alias Airbag.Buffer
  require Logger

  defstruct [:buffer_name, :partition_index, :dequeue_limit, :processor, :interval]

  @default_interval 1

  def start_link(opts) do
    :gen_cycle.start_link(__MODULE__, opts)
  end

  @impl true
  def init_cycle(opts) do
    opts = opts |> Enum.into([])

    buffer_name = Keyword.fetch!(opts, :buffer_name)
    partition_index = Keyword.fetch!(opts, :partition_index)
    processor = Keyword.fetch!(opts, :processor)
    interval = Keyword.get(opts, :interval, @default_interval)

    state = %Consumer{
      buffer_name: buffer_name,
      partition_index: partition_index,
      dequeue_limit: Keyword.get(opts, :dequeue_limit, 1),
      processor: processor,
      interval: interval
    }

    true = is_function(processor, 1)
    %{partition_count: partition_count} = Buffer.info!(buffer_name)
    true = partition_index <= partition_count
    Logger.debug("Initalized Airbag Consumer: #{inspect(state)}")
    {:ok, {interval, state}}
  end

  @impl true
  def handle_cycle(
        %{
          buffer_name: buffer_name,
          partition_index: partition_index,
          dequeue_limit: dequeue_limit
        } = state
      ) do
    case Buffer.dequeue(buffer_name, partition_index, limit: dequeue_limit) do
      [] ->
        {:continue, state}

      objects ->
        telemetry_metadata = %{
          buffer_name: buffer_name,
          partition_index: partition_index,
          limit: dequeue_limit
        }

        :telemetry.span(
          [:airbag, :consumer, :processor],
          telemetry_metadata,
          fn ->
            {process(objects, state.processor), telemetry_metadata}
          end
        )

        {:continue, state}
    end
  end

  @impl true
  def handle_info(_message, state) do
    {:continue, state}
  end

  defp process(term, processor) do
    try do
      processor.(term)
    catch
      _, caught ->
        Logger.error("Lost message #{inspect(term)} due to processing error: #{caught}")
    end
  end
end
