defmodule MoyaSqueezer.MetricsApiTest do
  use ExUnit.Case, async: false

  alias MoyaSqueezer.MetricsApi
  alias MoyaSqueezer.RuntimeState

  setup_all do
    case Process.whereis(RuntimeState) do
      nil -> start_supervised!({RuntimeState, []})
      _pid -> :ok
    end

    :ok
  end

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

  test "manager payload includes cached total and per-worker dispatch" do
    segments = [
      %{node: :worker1, supervisor: nil},
      %{node: :worker2, supervisor: nil}
    ]

    RuntimeState.set_measured_segments(segments)
    RuntimeState.cache_manager_dispatch([
      %{worker_id: "worker1", count: 11},
      %{worker_id: "worker2", count: 7}
    ])

    payload = MetricsApi.manager_payload()

    assert payload.role == "manager"
    assert payload.window_ms == 1_000
    assert payload.dispatch.total_dispatched == 18
    assert Enum.sort_by(payload.dispatch.to_workers, & &1.worker_id) == [
             %{worker_id: "worker1", count: 11},
             %{worker_id: "worker2", count: 7}
           ]
  end
end
