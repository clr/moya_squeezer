defmodule MoyaSqueezer.MetricsApi do
  @moduledoc false

  alias MoyaSqueezer.RuntimeState
  @window_ms 1_000

  def worker_payload do
    snapshot = RuntimeState.worker_window(@window_ms)

    %{
      window_ms: @window_ms,
      timestamp: System.system_time(:millisecond),
      role: "worker",
      worker_id: "#{node()}",
      outbound: %{
        to: "moya_db_balancer",
        request_count: snapshot.request_count,
        responses: snapshot.responses,
        last_status: snapshot.last_status
      }
    }
  end

  def manager_payload do
    to_workers = RuntimeState.manager_dispatch_snapshot()

    %{
      window_ms: @window_ms,
      timestamp: System.system_time(:millisecond),
      role: "manager",
      manager_id: "#{node()}",
      dispatch: %{
        total_dispatched: Enum.reduce(to_workers, 0, fn row, acc -> acc + row.count end),
        to_workers: to_workers
      }
    }
  end
end
