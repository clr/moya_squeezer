defmodule MoyaSqueezer.MetricsRouter do
  use Plug.Router

  alias MoyaSqueezer.MetricsApi

  plug :match
  plug :dispatch

  get "/worker/v0.1/metrics" do
    payload = MetricsApi.worker_payload()
    send_json(conn, 200, payload)
  end

  get "/manager/v0.1/metrics" do
    payload = MetricsApi.manager_payload()
    send_json(conn, 200, payload)
  end

  match _ do
    send_resp(conn, 404, "not_found")
  end

  defp send_json(conn, status, payload) do
    body = Jason.encode!(payload)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, body)
  end
end
