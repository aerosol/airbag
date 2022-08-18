defmodule Airbag.LogTelemetry do
  require Logger

  def init() do
    :ok =
      :telemetry.attach_many(
        "log-airbag-buffer-telemetry",
        [
          [:airbag, :buffer, :enqueue, :stop],
          [:airbag, :buffer, :dequeue, :stop],
          [:airbag, :buffer, :info, :stop],
          [:airbag, :buffer, :threshold_check, :stop],
          [:airbag, :consumer, :processor, :stop]
        ],
        &handle_event/4,
        nil
      )
  end

  def handle_event([:airbag | rest], %{duration: duration}, metadata, _config) do
    Logger.debug(
      "[Airbag] #{Enum.join(rest, ".")}: #{duration(duration)} for #{inspect(metadata)}"
    )
  end

  def duration(duration) do
    duration = System.convert_time_unit(duration, :native, :microsecond)

    if duration > 1000 do
      [duration |> div(1000) |> Integer.to_string(), "ms"]
    else
      [Integer.to_string(duration), "µs"]
    end
  end
end
