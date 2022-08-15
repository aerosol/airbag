defmodule Airbag.Manager do
  use GenServer

  alias Airbag.Buffer

  def child_spec(opts) do
    {buffer_name, buffer_opts} = Keyword.pop!(opts, :buffer_name)

    %{
      id: buffer_name,
      start: {__MODULE__, :start_link, [{buffer_name, buffer_opts}]}
    }
  end

  def start_link({_buffer_name, _buffer_opts} = init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  @impl true
  def init({buffer_name, buffer_opts}) do
    buffer = Buffer.new(buffer_name, buffer_opts)
    {:ok, buffer}
  end
end
