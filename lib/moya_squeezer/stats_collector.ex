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

  @spec final_report(pid() | atom()) :: report()
  def final_report(server), do: GenServer.call(server, :final_report, 10_000)

  @spec reset(pid() | atom()) :: :ok
  def reset(server), do: GenServer.call(server, :reset, 10_000)

  @spec window_snapshot(pid() | atom()) :: window_snapshot()
  def window_snapshot(server), do: GenServer.call(server, :window_snapshot, 10_000)

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
       total_count: 0,
       total_errors: 0,
       total_duration_us: 0,
       latency_histogram_100us: %{}
     }}
  end

  @impl true
  def handle_cast({:record, metric}, state) do
    db_latency_us = metric.db_latency_us
    response_code = metric.response_code
    is_error = response_code == 0 or response_code >= 400
    bucket_100us = max(div(db_latency_us, 100), 0)

    {:noreply,
     %{state | window_count: state.window_count + 1}
     |> maybe_inc_window_errors(is_error)
     |> Map.update!(:window_durations_us, &[db_latency_us | &1])
     |> Map.update!(:total_count, &(&1 + 1))
     |> maybe_inc_total_errors(is_error)
     |> Map.update!(:total_duration_us, &(&1 + db_latency_us))
     |> put_histogram_bucket(bucket_100us)}
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
  def handle_call({:percentile_ms, p}, _from, state) do
    {:reply, percentile_ms_from_histogram_100us(state.latency_histogram_100us, state.total_count, p), state}
  end

  @impl true
  def handle_info(:tick, state) do
    maybe_print_window_summary(state)
    Process.send_after(self(), :tick, @tick_interval_ms)

    {:noreply, %{state | window_count: 0, window_errors: 0, window_durations_us: []}}
  end

  defp maybe_print_window_summary(%{window_count: 0}), do: :ok

  defp maybe_print_window_summary(state) do
    sorted = Enum.sort(state.window_durations_us)

    p50 = percentile_ms_from_sorted(sorted, 0.50)
    p90 = percentile_ms_from_sorted(sorted, 0.90)
    p95 = percentile_ms_from_sorted(sorted, 0.95)
    error_rate = percentage(state.window_errors, state.window_count)

    IO.puts(
      "[#{state.label}][sec] rps=#{state.window_count} " <>
        "errors=#{state.window_errors} error_rate=#{Float.round(error_rate, 2)}% " <>
        "p50=#{Float.round(p50, 2)}ms p90=#{Float.round(p90, 2)}ms p95=#{Float.round(p95, 2)}ms"
    )
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

  defp maybe_inc_window_errors(state, true), do: Map.update!(state, :window_errors, &(&1 + 1))
  defp maybe_inc_window_errors(state, false), do: state

  defp maybe_inc_total_errors(state, true), do: Map.update!(state, :total_errors, &(&1 + 1))
  defp maybe_inc_total_errors(state, false), do: state

  defp put_histogram_bucket(state, bucket_100us) do
    histogram = Map.update(state.latency_histogram_100us, bucket_100us, 1, &(&1 + 1))
    %{state | latency_histogram_100us: histogram}
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
