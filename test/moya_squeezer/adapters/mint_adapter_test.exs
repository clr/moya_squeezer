defmodule MoyaSqueezer.Adapters.MintAdapterTest do
  use ExUnit.Case, async: false

  alias MoyaSqueezer.Adapters.MintAdapter
  alias MoyaSqueezer.MetricsLogger
  alias MoyaSqueezer.StatsCollector

  defmodule EchoPlug do
    import Plug.Conn

    def init(opts), do: opts
    def call(conn, _opts), do: send_resp(conn, 200, "ok")
  end

  setup do
    ref = :"mint_adapter_test_#{System.unique_integer([:positive])}"
    port = 20_000 + :rand.uniform(20_000)
    {:ok, _pid} = Plug.Cowboy.http(EchoPlug, [], port: port, ref: ref)
    on_exit(fn -> Plug.Cowboy.shutdown(ref) end)
    %{port: port}
  end

  test "dispatches requests over a persistent Mint connection and records results", %{port: port} do
    log_dir = Path.expand("../../tmp", __DIR__)
    File.mkdir_p!(log_dir)
    log_path = Path.join(log_dir, "moya_squeezer_mint_adapter_test.csv")
    File.rm(log_path)

    {:ok, logger} =
      MetricsLogger.start_link(
        name: :"metrics_logger_mint_test_#{System.unique_integer([:positive])}",
        log_path: log_path,
        flush_interval_ms: 20
      )

    {:ok, stats} =
      StatsCollector.start_link(name: :"stats_collector_mint_test_#{System.unique_integer([:positive])}")

    {:ok, worker} =
      MintAdapter.start_link(
        id: 1,
        adapter_opts: %{base_url: "http://localhost:#{port}", request_timeout_ms: 2_000},
        logger: logger,
        stats_collector: stats,
        payload_size: 8,
        reqs_per_sec: 100.0,
        read_ratio: 1.0,
        write_ratio: 0.0,
        delete_ratio: 0.0,
        tick_ms: 10,
        stats_flush_interval_ms: 20,
        mode: :measured
      )

    Process.sleep(200)

    summary = MintAdapter.summary(worker)
    assert summary.measured_requests > 0

    report = StatsCollector.final_report(stats)
    assert report.total_requests > 0
    assert report.total_errors == 0
  end
end
