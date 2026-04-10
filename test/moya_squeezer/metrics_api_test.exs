defmodule MoyaSqueezer.MetricsApiTest do
  use ExUnit.Case, async: false

  alias MoyaSqueezer.MetricsApi
  alias MoyaSqueezer.RuntimeState

  setup do
    RuntimeState.reset()
    :ok
  end

  test "worker payload includes windowed outbound metrics" do
    RuntimeState.record_worker_response(200)
    RuntimeState.record_worker_response(404)
    RuntimeState.record_worker_response(503)

    payload = MetricsApi.worker_payload()

    assert payload.role == "worker"
    assert payload.window_ms == 1_000
    assert payload.outbound.request_count == 3
    assert payload.outbound.responses == %{"2xx" => 1, "4xx" => 1, "5xx" => 1}
    assert payload.outbound.last_status in [200, 404, 503]
  end

  test "manager payload includes total and per-worker dispatch" do
    {:ok, sup1} = Task.Supervisor.start_link()
    {:ok, sup2} = Task.Supervisor.start_link()

    task1 = Task.Supervisor.async_nolink(sup1, fn -> Process.sleep(:infinity) end)
    task2 = Task.Supervisor.async_nolink(sup2, fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Process.exit(task1.pid, :kill)
      Process.exit(task2.pid, :kill)
      Process.exit(sup1, :kill)
      Process.exit(sup2, :kill)
    end)

    segments = [
      %{node: :worker1, supervisor: sup1},
      %{node: :worker2, supervisor: sup2}
    ]

    RuntimeState.set_measured_segments(segments)

    payload = MetricsApi.manager_payload()

    assert payload.role == "manager"
    assert payload.window_ms == 1_000
    assert payload.dispatch.total_dispatched == 0
    assert Enum.sort_by(payload.dispatch.to_workers, & &1.worker_id) == [
             %{worker_id: "worker1", count: 0},
             %{worker_id: "worker2", count: 0}
           ]
  end
end
