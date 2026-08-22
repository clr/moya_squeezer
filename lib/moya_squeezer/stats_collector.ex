defmodule MoyaSqueezer.StatsCollector do
  @moduledoc """
  Aggregates in-memory runtime statistics and emits per-second summaries.
  """

  use GenServer

  @tick_interval_ms 1_000

  @type metric :: %{
          request_type: :read | :write | :delete,
          started_at_ms: integer(),
          db_latency_us: integer(),
          response_code: integer()
        }

  @type metric_batch :: %{
          required(:count) => non_neg_integer(),
          required(:errors) => non_neg_integer(),
          required(:durations_us) => [non_neg_integer()],
          required(:total_duration_us) => non_neg_integer(),
          required(:histogram_100us) => %{optional(non_neg_integer()) => pos_integer()},
          optional(:worker_tick_count) => non_neg_integer(),
          optional(:worker_tick_work_us) => non_neg_integer(),
          optional(:worker_tick_overrun_count) => non_neg_integer(),
          optional(:worker_max_requests_in_tick) => non_neg_integer()
        }

  @type report :: %{
          total_requests: non_neg_integer(),
          total_errors: non_neg_integer(),
          error_rate_pct: float(),
          avg_latency_ms: float(),
          p50_latency_ms: float(),
          p90_latency_ms: float(),
          p95_latency_ms: float()
        }

  @type window_snapshot :: %{
          count: non_neg_integer(),
          errors: non_neg_integer(),
          error_rate_pct: float(),
          p50_latency_ms: float(),
          p90_latency_ms: float(),
          p95_latency_ms: float()
        }

  defstruct [
    :label,
    :window_count,
    :window_errors,
    :window_durations_us,
    :window_worker_tick_count,
    :window_worker_tick_work_us,
    :window_worker_tick_overrun_count,
    :window_worker_max_requests_in_tick,
    :last_emitted_window_count,
    :last_emitted_window_errors,
    :last_emitted_window_error_rate_pct,
    :last_emitted_window_p50_latency_ms,
    :last_emitted_window_p90_latency_ms,
    :last_emitted_window_p95_latency_ms,
    :total_count,
    :total_errors,
    :total_duration_us,
    :latency_histogram_100us
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @spec record(pid() | atom(), metric()) :: :ok
  def record(server, metric), do: GenServer.cast(server, {:record, metric})

  @spec record_batch(pid() | atom(), metric_batch()) :: :ok
  def record_batch(server, batch), do: GenServer.cast(server, {:record_batch, batch})

  @spec final_report(pid() | atom()) :: report()
  def final_report(server), do: GenServer.call(server, :final_report, 10_000)

  @spec reset(pid() | atom()) :: :ok
  def reset(server), do: GenServer.call(server, :reset, 10_000)

  @spec window_snapshot(pid() | atom()) :: window_snapshot()
  def window_snapshot(server), do: GenServer.call(server, :window_snapshot, 10_000)

  @spec last_emitted_window_snapshot(pid() | atom()) :: window_snapshot()
  def last_emitted_window_snapshot(server), do: GenServer.call(server, :last_emitted_window_snapshot, 10_000)

  @spec percentile_ms(pid() | atom(), float()) :: float()
  def percentile_ms(server, p), do: GenServer.call(server, {:percentile_ms, p}, 10_000)

  @impl true
  def init(opts) do
    label = Keyword.get(opts, :label, "manager")

    Process.send_after(self(), :tick, @tick_interval_ms)

    {:ok,
     %__MODULE__{
       label: label,
       window_count: 0,
       window_errors: 0,
       window_durations_us: [],
       window_worker_tick_count: 0,
       window_worker_tick_work_us: 0,
       window_worker_tick_overrun_count: 0,
       window_worker_max_requests_in_tick: 0,
       last_emitted_window_count: 0,
       last_emitted_window_errors: 0,
       last_emitted_window_error_rate_pct: 0.0,
       last_emitted_window_p50_latency_ms: 0.0,
       last_emitted_window_p90_latency_ms: 0.0,
       last_emitted_window_p95_latency_ms: 0.0,
       total_count: 0,
       total_errors: 0,
       total_duration_us: 0,
       latency_histogram_100us: %{}
     }}
  end

  @impl true
  def handle_cast({:record, metric}, state) do
    db_latency_us = max(metric.db_latency_us, 0)
    response_code = metric.response_code
    is_error = response_code == 0 or response_code >= 400
    bucket_100us = max(div(db_latency_us, 100), 0)

    batch = %{
      count: 1,
      errors: if(is_error, do: 1, else: 0),
      durations_us: [db_latency_us],
      total_duration_us: db_latency_us,
      histogram_100us: %{bucket_100us => 1}
    }

    {:noreply, merge_batch(state, batch)}
  end

  @impl true
  def handle_cast({:record_batch, batch}, state) do
    {:noreply, merge_batch(state, batch)}
  end

  @impl true
  def handle_call(:final_report, _from, state) do
    {:reply, to_report(state), state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    {:reply, :ok,
     %__MODULE__{
       label: state.label,
       window_count: 0,
       window_errors: 0,
       window_durations_us: [],
       window_worker_tick_count: 0,
       window_worker_tick_work_us: 0,
       window_worker_tick_overrun_count: 0,
       window_worker_max_requests_in_tick: 0,
       last_emitted_window_count: 0,
       last_emitted_window_errors: 0,
       last_emitted_window_error_rate_pct: 0.0,
       last_emitted_window_p50_latency_ms: 0.0,
       last_emitted_window_p90_latency_ms: 0.0,
       last_emitted_window_p95_latency_ms: 0.0,
       total_count: 0,
       total_errors: 0,
       total_duration_us: 0,
       latency_histogram_100us: %{}
     }}
  end

  @impl true
  def handle_call(:window_snapshot, _from, state) do
    sorted = Enum.sort(state.window_durations_us)

    snapshot = %{
      count: state.window_count,
      errors: state.window_errors,
      error_rate_pct: percentage(state.window_errors, state.window_count),
      p50_latency_ms: percentile_ms_from_sorted(sorted, 0.50),
      p90_latency_ms: percentile_ms_from_sorted(sorted, 0.90),
      p95_latency_ms: percentile_ms_from_sorted(sorted, 0.95)
    }

    {:reply, snapshot, state}
  end

  @impl true
  def handle_call(:last_emitted_window_snapshot, _from, state) do
    snapshot = %{
      count: state.last_emitted_window_count,
      errors: state.last_emitted_window_errors,
      error_rate_pct: state.last_emitted_window_error_rate_pct,
      p50_latency_ms: state.last_emitted_window_p50_latency_ms,
      p90_latency_ms: state.last_emitted_window_p90_latency_ms,
      p95_latency_ms: state.last_emitted_window_p95_latency_ms
    }

    {:reply, snapshot, state}
  end

  @impl true
  def handle_call({:percentile_ms, p}, _from, state) do
    {:reply, percentile_ms_from_histogram_100us(state.latency_histogram_100us, state.total_count, p), state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = maybe_print_window_summary(state)
    Process.send_after(self(), :tick, @tick_interval_ms)

    {:noreply,
     %{state |
       window_count: 0,
       window_errors: 0,
       window_durations_us: [],
       window_worker_tick_count: 0,
       window_worker_tick_work_us: 0,
       window_worker_tick_overrun_count: 0,
       window_worker_max_requests_in_tick: 0}}
  end

  defp maybe_print_window_summary(%{window_count: 0} = state), do: state

  defp maybe_print_window_summary(state) do
    sorted = Enum.sort(state.window_durations_us)

    p50 = percentile_ms_from_sorted(sorted, 0.50)
    p90 = percentile_ms_from_sorted(sorted, 0.90)
    p95 = percentile_ms_from_sorted(sorted, 0.95)
    error_rate = percentage(state.window_errors, state.window_count)

    diagnostics_suffix =
      if state.window_worker_tick_count > 0 do
        avg_tick_work_us = state.window_worker_tick_work_us / state.window_worker_tick_count

        " tick_avg_work=#{Float.round(avg_tick_work_us, 1)}us" <>
          " tick_overruns=#{state.window_worker_tick_overrun_count}" <>
          " tick_max_reqs=#{state.window_worker_max_requests_in_tick}"
      else
        ""
      end

    IO.puts(
      "[#{state.label}][sec] rps=#{state.window_count} " <>
        "errors=#{state.window_errors} error_rate=#{Float.round(error_rate, 2)}% " <>
        "p50=#{Float.round(p50, 2)}ms p90=#{Float.round(p90, 2)}ms p95=#{Float.round(p95, 2)}ms" <>
        diagnostics_suffix
    )

    %{
      state
      | last_emitted_window_count: state.window_count,
        last_emitted_window_errors: state.window_errors,
        last_emitted_window_error_rate_pct: error_rate,
        last_emitted_window_p50_latency_ms: p50,
        last_emitted_window_p90_latency_ms: p90,
        last_emitted_window_p95_latency_ms: p95
    }
  end

  defp to_report(state) do
    %{
      total_requests: state.total_count,
      total_errors: state.total_errors,
      error_rate_pct: percentage(state.total_errors, state.total_count),
      avg_latency_ms: avg_ms(state.total_duration_us, state.total_count),
      p50_latency_ms:
        percentile_ms_from_histogram_100us(state.latency_histogram_100us, state.total_count, 0.50),
      p90_latency_ms:
        percentile_ms_from_histogram_100us(state.latency_histogram_100us, state.total_count, 0.90),
      p95_latency_ms:
        percentile_ms_from_histogram_100us(state.latency_histogram_100us, state.total_count, 0.95)
    }
  end

  defp merge_batch(state, %{count: count}) when is_integer(count) and count <= 0, do: state

  defp merge_batch(state, batch) do
    window_durations = Enum.reverse(batch.durations_us || []) ++ state.window_durations_us
    window_errors = state.window_errors + max(batch.errors || 0, 0)
    total_duration_us = state.total_duration_us + max(batch.total_duration_us || 0, 0)

    histogram =
      Enum.reduce(batch.histogram_100us || %{}, state.latency_histogram_100us, fn {bucket, cnt}, acc ->
        bucket_100us = max(bucket, 0)
        count = max(cnt, 0)
        if count > 0, do: Map.update(acc, bucket_100us, count, &(&1 + count)), else: acc
      end)

    %{
      state
      | window_count: state.window_count + max(batch.count || 0, 0),
        window_errors: window_errors,
        window_durations_us: window_durations,
        window_worker_tick_count:
          state.window_worker_tick_count + max(batch[:worker_tick_count] || 0, 0),
        window_worker_tick_work_us:
          state.window_worker_tick_work_us + max(batch[:worker_tick_work_us] || 0, 0),
        window_worker_tick_overrun_count:
          state.window_worker_tick_overrun_count + max(batch[:worker_tick_overrun_count] || 0, 0),
        window_worker_max_requests_in_tick:
          max(state.window_worker_max_requests_in_tick, max(batch[:worker_max_requests_in_tick] || 0, 0)),
        total_count: state.total_count + max(batch.count || 0, 0),
        total_errors: state.total_errors + max(batch.errors || 0, 0),
        total_duration_us: total_duration_us,
        latency_histogram_100us: histogram
    }
  end

  defp percentage(_num, 0), do: 0.0
  defp percentage(num, denom), do: num * 100.0 / denom

  defp avg_ms(_total_duration_us, 0), do: 0.0
  defp avg_ms(total_duration_us, count), do: total_duration_us / count / 1_000

  defp percentile_ms_from_sorted([], _p), do: 0.0

  defp percentile_ms_from_sorted(sorted, p) do
    idx =
      sorted
      |> length()
      |> Kernel.*(p)
      |> Float.ceil()
      |> trunc()
      |> Kernel.-(1)
      |> max(0)

    Enum.at(sorted, idx, 0) / 1_000
  end

  defp percentile_ms_from_histogram_100us(_hist, 0, _p), do: 0.0

  defp percentile_ms_from_histogram_100us(hist, total_count, p) do
    threshold = total_count * p

    {bucket_100us, _seen} =
      hist
      |> Enum.sort_by(fn {bucket, _count} -> bucket end)
      |> Enum.reduce_while({0, 0}, fn {bucket, count}, {_bucket_acc, seen} ->
        new_seen = seen + count

        if new_seen >= threshold do
          {:halt, {bucket, new_seen}}
        else
          {:cont, {bucket, new_seen}}
        end
      end)

    bucket_100us / 10
  end
end
