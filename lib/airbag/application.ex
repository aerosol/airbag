defmodule Airbag.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    opts = [strategy: :one_for_one, name: Airbag.Supervisor]
    Supervisor.start_link([], opts)
  end
end
