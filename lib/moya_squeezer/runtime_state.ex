defmodule MoyaSqueezer.RuntimeState do
  @moduledoc false

  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> initial_state() end, name: __MODULE__)
  end

  def set_role(role) when role in [:manager, :worker], do: Agent.update(__MODULE__, &Map.put(&1, :role, role))
  def role, do: Agent.get(__MODULE__, &Map.get(&1, :role))

  def set_measured_segments(segments) when is_list(segments) do
    Agent.update(__MODULE__, fn state ->
      known =
        Enum.into(segments, %{}, fn seg ->
          node_name = Map.get(seg, :node)
          current = get_in(state, [:manager_dispatch, node_name]) || 0
          {node_name, current}
        end)

      state
      |> Map.put(:measured_segments, segments)
      |> Map.put(:manager_dispatch, known)
    end)
  end

  def measured_segments, do: Agent.get(__MODULE__, &Map.get(&1, :measured_segments, []))

  def cache_manager_dispatch(rows) when is_list(rows) do
    Agent.update(__MODULE__, fn state ->
      dispatch =
        Enum.into(rows, %{}, fn row ->
          {Map.fetch!(row, :worker_id), Map.get(row, :count, 0)}
        end)

      Map.put(state, :manager_dispatch, dispatch)
    end)
  end

  def manager_dispatch_snapshot do
    Agent.get(__MODULE__, fn state ->
      segments = Map.get(state, :measured_segments, [])
      dispatch = Map.get(state, :manager_dispatch, %{})

      Enum.map(segments, fn seg ->
        worker_id = "#{Map.get(seg, :node)}"
        %{worker_id: worker_id, count: Map.get(dispatch, worker_id, 0)}
      end)
    end)
  end

  def reset do
    Agent.update(__MODULE__, fn _ -> initial_state() end)
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

  defp initial_state do
    %{role: nil, measured_segments: [], worker_events: [], manager_dispatch: %{}}
  end
end
