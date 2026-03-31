defmodule MoyaSqueezer.StatsCollectorTest do
  use ExUnit.Case, async: true

  alias MoyaSqueezer.StatsCollector

  test "computes percentiles and average from db_latency_us" do
    {:ok, pid} = StatsCollector.start_link([])

    for us <- [1_000, 2_000, 3_000, 4_000] do
      :ok =
        StatsCollector.record(pid, %{
          request_type: :read,
          started_at_ms: System.system_time(:millisecond),
          db_latency_us: us,
          response_code: 200
        })
    end

    Process.sleep(20)

    report = StatsCollector.final_report(pid)
    assert_in_delta report.avg_latency_ms, 2.5, 0.001
    assert_in_delta report.p50_latency_ms, 2.0, 0.2
    assert_in_delta report.p90_latency_ms, 4.0, 0.2
    assert_in_delta report.p95_latency_ms, 4.0, 0.2
  end

  test "merges batched worker stats" do
    {:ok, pid} = StatsCollector.start_link([])

    :ok =
      StatsCollector.record_batch(pid, %{
        count: 3,
        errors: 1,
        durations_us: [1_000, 2_000, 3_000],
        total_duration_us: 6_000,
        histogram_100us: %{10 => 1, 20 => 1, 30 => 1}
      })

    Process.sleep(20)

    snapshot = StatsCollector.window_snapshot(pid)
    assert snapshot.count == 3
    assert snapshot.errors == 1

    report = StatsCollector.final_report(pid)
    assert report.total_requests == 3
    assert report.total_errors == 1
    assert_in_delta report.avg_latency_ms, 2.0, 0.001
    assert_in_delta report.p50_latency_ms, 2.0, 0.2
    assert_in_delta report.p90_latency_ms, 3.0, 0.2
    assert_in_delta report.p95_latency_ms, 3.0, 0.2
  end
end
