defmodule Airbag do
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  def child_spec(opts) do
    %{
      id: Keyword.fetch!(opts, :buffer_name),
      start: {Airbag.Supervisor, :start_link, [opts]}
    }
  end
end
