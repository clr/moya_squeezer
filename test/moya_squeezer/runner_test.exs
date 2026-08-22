defmodule MoyaSqueezer.RunnerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias MoyaSqueezer.Config
  alias MoyaSqueezer.Runner

  defmodule FakeAdapter do
    @behaviour MoyaSqueezer.LoadAdapter

    @impl true
    def request(_type, _payload_size, _adapter_opts, _key) do
      {:ok, 200, 1_000}
    end
  end

  test "runner completes and prints final report with duration stop" do
    Application.put_env(:moya_squeezer, :load_adapter, FakeAdapter)

    on_exit(fn ->
      Application.delete_env(:moya_squeezer, :load_adapter)
    end)

    assert {:ok, config} =
             Config.from_map(%{
               connections_per_worker: 1,
               requests_per_second: 10,
               start_requests_per_second: 10,
               rps_step: 0,
               step_interval_seconds: 1,
               baseline_window_seconds: 1,
               read_ratio: 0.7,
               write_ratio: 0.2,
               delete_ratio: 0.1,
               payload_size: 8,
               duration_seconds: 2,
               warmup_seconds: 0,
               log_path: "logs/test_runner_metrics.csv"
             })

    output = capture_io(fn -> assert :ok = Runner.run(config, worker_nodes: [node()]) end)

    assert output =~ "[final]"
    assert output =~ "stop_reason=duration_elapsed"
  end

  test "runner returns error when manager is not distributed but worker nodes are configured" do
    Application.put_env(:moya_squeezer, :load_adapter, FakeAdapter)

    on_exit(fn ->
      Application.delete_env(:moya_squeezer, :load_adapter)
    end)

    assert {:ok, config} =
             Config.from_map(%{
               connections_per_worker: 1,
               requests_per_second: 10,
               start_requests_per_second: 10,
               rps_step: 0,
               step_interval_seconds: 1,
               baseline_window_seconds: 1,
               read_ratio: 0.7,
               write_ratio: 0.2,
               delete_ratio: 0.1,
               payload_size: 8,
               duration_seconds: 1,
               warmup_seconds: 0,
               log_path: "logs/test_runner_metrics_cluster.csv"
             })

    assert {:error, _reason} = Runner.run(config, worker_nodes: [:"worker1@localhost"])
  end
end
