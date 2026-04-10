defmodule MoyaSqueezer.RuntimeState do
  @moduledoc false

  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{role: nil, measured_segments: [], worker_events: []} end, name: __MODULE__)
  end

  def set_role(role) when role in [:manager, :worker], do: Agent.update(__MODULE__, &Map.put(&1, :role, role))
  def role, do: Agent.get(__MODULE__, &Map.get(&1, :role))

  def set_measured_segments(segments) when is_list(segments),
    do: Agent.update(__MODULE__, &Map.put(&1, :measured_segments, segments))

  def measured_segments, do: Agent.get(__MODULE__, &Map.get(&1, :measured_segments, []))

  def reset do
    Agent.update(__MODULE__, fn _ -> %{role: nil, measured_segments: [], worker_events: []} end)
  end

  def record_worker_response(status) when is_integer(status) do
    now = System.system_time(:millisecond)

    Agent.update(__MODULE__, fn state ->
      events = [%{ts: now, status: status} | Map.get(state, :worker_events, [])] |> Enum.take(50_000)
      Map.put(state, :worker_events, events)
    end)
  end

  def worker_window(window_ms) when is_integer(window_ms) and window_ms > 0 do
    now = System.system_time(:millisecond)
    cutoff = now - window_ms

    Agent.get_and_update(__MODULE__, fn state ->
      events = Map.get(state, :worker_events, [])
      pruned = Enum.filter(events, &(&1.ts >= cutoff))

      snapshot = %{
        request_count: length(pruned),
        responses: %{
          "2xx" => Enum.count(pruned, &(&1.status >= 200 and &1.status < 300)),
          "4xx" => Enum.count(pruned, &(&1.status >= 400 and &1.status < 500)),
          "5xx" => Enum.count(pruned, &(&1.status >= 500 and &1.status < 600))
        },
        last_status: (case pruned do
          [%{status: status} | _] -> status
          [] -> 0
        end)
      }

      {snapshot, Map.put(state, :worker_events, pruned)}
    end)
  end
end
