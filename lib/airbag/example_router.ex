defmodule Airbag.ExampleRouter do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  plug(Plug.Parsers,
    parsers: [:json],
    json_decoder: Jason
  )

  forward("/buffer_1", to: Airbag.Plug, init_opts: %{buffer: ExampleBuffer1})

  match _ do
    send_resp(conn, 404, "not found")
  end
end
