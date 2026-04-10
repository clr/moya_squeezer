defmodule MoyaSqueezer.MetricsApi do
  @moduledoc false

  alias MoyaSqueezer.RuntimeState
  alias MoyaSqueezer.Runner

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
    segments = RuntimeState.measured_segments()

    to_workers =
      Enum.map(segments, fn seg ->
        count =
          segment_summaries(seg)
          |> Enum.reduce(0, fn row, acc -> acc + Map.get(row, :measured_requests, 0) end)

        %{worker_id: "#{seg.node}", count: count}
      end)

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

  defp segment_summaries(%{node: target_node, supervisor: supervisor}) when is_pid(supervisor) do
    if target_node == node() do
      Runner.worker_segment_summaries(supervisor)
    else
      case :rpc.call(target_node, Runner, :worker_segment_summaries, [supervisor]) do
        list when is_list(list) -> list
        _ -> []
      end
    end
  end

  defp segment_summaries(_), do: []
end
