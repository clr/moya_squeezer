defmodule MoyaSqueezer.MetricsLoggerTest do
  use ExUnit.Case, async: false

  alias MoyaSqueezer.MetricsLogger

  test "uses configured flush interval for bucket rounding" do
    log_path = "logs/test_metrics_logger_custom_interval.csv"
    File.rm(log_path)

    {:ok, logger} =
      MetricsLogger.start_link(
        name: :metrics_logger_test_custom_interval,
        log_path: log_path,
        flush_interval_ms: 20,
        compact: false
      )

    MetricsLogger.log(logger, %{
      source_node: "nonode@nohost",
      request_type: :read,
      started_at_ms: 123,
      db_latency_us: 1000,
      response_code: 200
    })

    Process.sleep(50)
    GenServer.stop(logger, :normal, 5_000)

    content = File.read!(log_path)
    assert content =~ "bucket_ms,source_node,request_type,started_at_ms,db_latency_us,response_code"
    assert content =~ "120,nonode@nohost,read,123,1000,200"
  end

  test "uses default flush interval when not provided" do
    log_path = "logs/test_metrics_logger_default_interval.csv"
    File.rm(log_path)

    {:ok, logger} =
      MetricsLogger.start_link(
        name: :metrics_logger_test_default_interval,
        log_path: log_path
      )

    MetricsLogger.log(logger, %{
      source_node: "nonode@nohost",
      request_type: :write,
      started_at_ms: 123,
      db_latency_us: 1500,
      response_code: 201
    })

    Process.sleep(20)
    GenServer.stop(logger, :normal, 5_000)

    content = File.read!(log_path)
    assert content =~ "bucket_ms,source_node,request_type,response_code,count,sum_db_latency_us"
    assert content =~ "120,nonode@nohost,write,201,1,1500"
  end
end
