defmodule MoyaSqueezer.ConfigTest do
  use ExUnit.Case, async: true

  import MoyaSqueezer.ConfigFixtures

  alias MoyaSqueezer.Config

  describe "from_map/1" do
    test "applies defaults for squeeze fields" do
      assert {:ok, config} = Config.from_map(valid_squeeze_attrs())
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
      assert config.worker_container_pool == 2
    end

    test "accepts explicit squeeze fields" do
      attrs =
        valid_squeeze_attrs(%{
          start_requests_per_second: 50,
          rps_step: 10,
          step_interval_seconds: 2,
          baseline_window_seconds: 3,
          max_error_rate_pct: 0.5,
          worker_tick_ms: 40,
          metrics_flush_interval_ms: 20,
          metrics_compact: false,
          stats_flush_interval_ms: 50
        })

      assert {:ok, config} = Config.from_map(attrs)
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

    test "accepts concurrency ramp settings" do
      attrs =
        valid_squeeze_attrs(%{
          connections_per_worker: 4,
          ramp_mode: "concurrency",
          total_target_rps: 200,
          initial_active_workers: 2,
          worker_step: 1,
          worker_step_interval_seconds: 3,
          worker_container_pool: 4
        })

      assert {:ok, config} = Config.from_map(attrs)
      assert config.ramp_mode == :concurrency
      assert config.total_target_rps == 200
      assert config.initial_active_workers == 2
      assert config.worker_step == 1
      assert config.worker_step_interval_seconds == 3
      assert config.worker_container_pool == 4
    end

    test "rejects non-positive metrics_flush_interval_ms" do
      attrs = valid_squeeze_attrs(%{metrics_flush_interval_ms: 0})

      assert {:error, "optional field must be > 0: metrics_flush_interval_ms"} =
               Config.from_map(attrs)
    end

    test "rejects non-positive stats_flush_interval_ms" do
      attrs = valid_squeeze_attrs(%{stats_flush_interval_ms: 0})

      assert {:error, "optional field must be > 0: stats_flush_interval_ms"} =
               Config.from_map(attrs)
    end

    test "rejects non-boolean metrics_compact" do
      attrs = valid_squeeze_attrs(%{metrics_compact: "yes"})

      assert {:error, "optional field must be a boolean: metrics_compact"} =
               Config.from_map(attrs)
    end

    test "rejects invalid stop_latency_percentile" do
      attrs = valid_squeeze_attrs(%{stop_latency_percentile: 1.5})

      assert {:error, "optional field must be > 0.0 and <= 1.0: stop_latency_percentile"} =
               Config.from_map(attrs)
    end

    test "rejects non-positive latency_breach_consecutive_windows" do
      attrs = valid_squeeze_attrs(%{latency_breach_consecutive_windows: 0})

      assert {:error, "optional field must be > 0: latency_breach_consecutive_windows"} =
               Config.from_map(attrs)
    end

    test "rejects non-positive worker_inflight_limit" do
      attrs = valid_squeeze_attrs(%{worker_inflight_limit: 0})

      assert {:error, "optional field must be > 0: worker_inflight_limit"} =
               Config.from_map(attrs)
    end

    test "rejects non-positive error_breach_consecutive_windows" do
      attrs = valid_squeeze_attrs(%{error_breach_consecutive_windows: 0})

      assert {:error, "optional field must be > 0: error_breach_consecutive_windows"} =
               Config.from_map(attrs)
    end
  end
end
