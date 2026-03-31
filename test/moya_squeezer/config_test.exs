defmodule MoyaSqueezer.ConfigTest do
  use ExUnit.Case, async: true

  alias MoyaSqueezer.Config

  test "from_map applies defaults for squeeze fields" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5
    }

    assert {:ok, config} = Config.from_map(map)
    assert config.start_requests_per_second == 100
    assert config.rps_step == 0
    assert config.step_interval_seconds == 5
    assert config.baseline_window_seconds == 10
    assert config.max_error_rate_pct == 1.0
    assert config.error_breach_consecutive_windows == 1
    assert config.stop_latency_percentile == 0.9
    assert config.latency_breach_consecutive_windows == 1
    assert config.worker_tick_ms == 10
    assert config.worker_inflight_limit == 1
    assert config.metrics_flush_interval_ms == 10
    assert config.metrics_compact == true
    assert config.stats_flush_interval_ms == 100
    assert config.ramp_mode == :rps
    assert config.total_target_rps == 100
    assert config.initial_active_workers == 1
    assert config.worker_step == 1
    assert config.worker_step_interval_seconds == 5
    assert config.max_active_workers == 2
  end

  test "from_map accepts explicit squeeze fields" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      start_requests_per_second: 50,
      rps_step: 10,
      step_interval_seconds: 2,
      baseline_window_seconds: 3,
      max_error_rate_pct: 0.5,
      worker_tick_ms: 40,
      metrics_flush_interval_ms: 20,
      metrics_compact: false,
      stats_flush_interval_ms: 50,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5
    }

    assert {:ok, config} = Config.from_map(map)
    assert config.start_requests_per_second == 50
    assert config.rps_step == 10
    assert config.step_interval_seconds == 2
    assert config.baseline_window_seconds == 3
    assert config.max_error_rate_pct == 0.5
    assert config.worker_tick_ms == 40
    assert config.metrics_flush_interval_ms == 20
    assert config.metrics_compact == false
    assert config.stats_flush_interval_ms == 50
  end

  test "from_map rejects non-positive metrics_flush_interval_ms" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5,
      metrics_flush_interval_ms: 0
    }

    assert {:error, "optional field must be > 0: metrics_flush_interval_ms"} = Config.from_map(map)
  end

  test "from_map rejects non-positive stats_flush_interval_ms" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5,
      stats_flush_interval_ms: 0
    }

    assert {:error, "optional field must be > 0: stats_flush_interval_ms"} = Config.from_map(map)
  end

  test "from_map rejects non-boolean metrics_compact" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5,
      metrics_compact: "yes"
    }

    assert {:error, "optional field must be a boolean: metrics_compact"} = Config.from_map(map)
  end

  test "from_map rejects invalid stop_latency_percentile" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5,
      stop_latency_percentile: 1.5
    }

    assert {:error, "optional field must be > 0.0 and <= 1.0: stop_latency_percentile"} =
             Config.from_map(map)
  end

  test "from_map rejects non-positive latency_breach_consecutive_windows" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5,
      latency_breach_consecutive_windows: 0
    }

    assert {:error, "optional field must be > 0: latency_breach_consecutive_windows"} =
             Config.from_map(map)
  end

  test "from_map rejects non-positive worker_inflight_limit" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5,
      worker_inflight_limit: 0
    }

    assert {:error, "optional field must be > 0: worker_inflight_limit"} =
             Config.from_map(map)
  end

  test "from_map rejects non-positive error_breach_consecutive_windows" do
    map = %{
      connections_per_worker: 2,
      requests_per_second: 100,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5,
      error_breach_consecutive_windows: 0
    }

    assert {:error, "optional field must be > 0: error_breach_consecutive_windows"} =
             Config.from_map(map)
  end

  test "from_map accepts concurrency ramp settings" do
    map = %{
      connections_per_worker: 4,
      requests_per_second: 100,
      ramp_mode: "concurrency",
      total_target_rps: 200,
      initial_active_workers: 2,
      worker_step: 1,
      worker_step_interval_seconds: 3,
      max_active_workers: 4,
      read_ratio: 0.7,
      write_ratio: 0.2,
      delete_ratio: 0.1,
      payload_size: 128,
      duration_seconds: 5
    }

    assert {:ok, config} = Config.from_map(map)
    assert config.ramp_mode == :concurrency
    assert config.total_target_rps == 200
    assert config.initial_active_workers == 2
    assert config.worker_step == 1
    assert config.worker_step_interval_seconds == 3
    assert config.max_active_workers == 4
  end
end
