defmodule DemoWeb.Router do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  plug(Plug.Parsers,
    parsers: [:json],
    json_decoder: Jason
  )

  forward("/buffer_singleton", to: DemoWeb.Plug, init_opts: %{buffer: DemoBufferSingleton})
  forward("/buffer_schedulers", to: DemoWeb.Plug, init_opts: %{buffer: DemoBufferSchedulers})

  match _ do
    send_resp(conn, 404, "not found")
  end
end
