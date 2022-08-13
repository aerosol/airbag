defmodule Airbag.Plug do
  import Plug.Conn

  def init(opts) do
    opts
  end

  def call(conn, %{buffer: buffer}) do
    case Airbag.Buffer.enqueue(buffer, conn) do
      {:ok, _} ->
        send_resp(conn, 200, "ok")

      {:error, :threshold_reached} ->
        send_resp(conn, 500, "fail")
    end
  end
end
