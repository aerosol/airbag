defmodule Airbag do
  def child_spec(opts) do
    %{
      id: Keyword.fetch!(opts, :buffer_name),
      start: {Airbag.Supervisor, :start_link, [opts]}
    }
  end
end
