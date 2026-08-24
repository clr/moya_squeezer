defmodule MoyaSqueezer.RunnerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import MoyaSqueezer.ConfigFixtures

  alias MoyaSqueezer.Config
  alias MoyaSqueezer.Runner

  defmodule FakeAdapter do
    @behaviour MoyaSqueezer.LoadAdapter

    @impl true
    def request(_type, _payload_size, _adapter_opts, _key) do
      {:ok, 200, 1_000}
    end
  end

  setup do
    Application.put_env(:moya_squeezer, :load_adapter, FakeAdapter)
    on_exit(fn -> Application.delete_env(:moya_squeezer, :load_adapter) end)
    :ok
  end

  describe "run/2" do
    test "completes and prints final report with duration stop" do
      attrs =
        valid_squeeze_attrs(%{
          start_requests_per_second: 10,
          rps_step: 0,
          step_interval_seconds: 1,
          baseline_window_seconds: 1,
          payload_size: 8,
          duration_seconds: 2,
          warmup_seconds: 0,
          log_path: "logs/test_runner_metrics.csv"
        })

      assert {:ok, config} = Config.from_map(attrs)

      output = capture_io(fn -> assert :ok = Runner.run(config, worker_nodes: [node()]) end)

      assert output =~ "[final]"
      assert output =~ "stop_reason=duration_elapsed"
    end

    test "returns error when manager is not distributed but worker nodes are configured" do
      attrs =
        valid_squeeze_attrs(%{
          start_requests_per_second: 10,
          rps_step: 0,
          step_interval_seconds: 1,
          baseline_window_seconds: 1,
          payload_size: 8,
          duration_seconds: 1,
          warmup_seconds: 0,
          log_path: "logs/test_runner_metrics_cluster.csv"
        })

      assert {:ok, config} = Config.from_map(attrs)
      assert {:error, _reason} = Runner.run(config, worker_nodes: [:"worker1@localhost"])
    end

    test "returns error when no worker nodes are configured" do
      attrs =
        valid_squeeze_attrs(%{
          duration_seconds: 1,
          warmup_seconds: 0,
          log_path: "logs/test_runner_metrics_no_workers.csv"
        })

      assert {:ok, config} = Config.from_map(attrs)
      assert {:error, reason} = Runner.run(config, worker_nodes: [])
      assert reason =~ "at least one worker node is required"
    end
  end
end
