defmodule MoyaSqueezer.ConnectionWorkerTest do
  use ExUnit.Case, async: false

  alias MoyaSqueezer.ConnectionWorker
  alias MoyaSqueezer.MetricsLogger
  alias MoyaSqueezer.StatsCollector

  defmodule CrashingAdapter do
    @behaviour MoyaSqueezer.LoadAdapter

    @impl true
    def request(_type, _payload_size, _adapter_opts, _key) do
      raise "boom"
    end
  end

  test "reclaims inflight capacity when a request task crashes" do
    log_dir = Path.expand("../tmp", __DIR__)
    File.mkdir_p!(log_dir)
    log_path = Path.join(log_dir, "moya_squeezer_connection_worker_test.csv")
    File.rm(log_path)

    {:ok, logger} =
      MetricsLogger.start_link(
        name: :"metrics_logger_test_#{System.unique_integer([:positive])}",
        log_path: log_path,
        flush_interval_ms: 50
      )

    {:ok, stats} = StatsCollector.start_link(name: :"stats_collector_test_#{System.unique_integer([:positive])}")
    {:ok, task_supervisor} = Task.Supervisor.start_link()

    {:ok, worker} =
      ConnectionWorker.start_link(
        id: 1,
        adapter: CrashingAdapter,
        adapter_opts: %{},
        logger: logger,
        task_supervisor: task_supervisor,
        stats_collector: stats,
        payload_size: 8,
        reqs_per_sec: 100.0,
        read_ratio: 1.0,
        write_ratio: 0.0,
        delete_ratio: 0.0,
        tick_ms: 10,
        worker_inflight_limit: 1,
        stats_flush_interval_ms: 50,
        mode: :measured
      )

    send(worker, :tick)
    Process.sleep(50)

    state = :sys.get_state(worker)

    assert state.inflight_count == 0
    assert state.task_refs == %{}
  end
end
