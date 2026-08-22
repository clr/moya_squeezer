defmodule MoyaSqueezer.MetricsRouterTest do
  use ExUnit.Case, async: false
  import Plug.Test

  alias MoyaSqueezer.MetricsRouter
  alias MoyaSqueezer.RuntimeState

  test "GET /worker/v0.1/metrics returns json" do
    RuntimeState.reset()
    RuntimeState.record_worker_response(200)

    conn = conn(:get, "/worker/v0.1/metrics") |> MetricsRouter.call([])

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["role"] == "worker"
    assert body["outbound"]["to"] == "moya_db_balancer"
  end

  test "GET /manager/v0.1/metrics returns json" do
    RuntimeState.reset()
    RuntimeState.set_measured_segments([])

    conn = conn(:get, "/manager/v0.1/metrics") |> MetricsRouter.call([])

    assert conn.status == 200
    body = Jason.decode!(conn.resp_body)
    assert body["role"] == "manager"
    assert body["dispatch"]["total_dispatched"] == 0
  end
end
