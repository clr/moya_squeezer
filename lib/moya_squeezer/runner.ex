defmodule MoyaSqueezer.Runner do
  @moduledoc """
  Main routine that runs a squeeze test from TOML configuration.

  Supports manager/worker distribution using standard Erlang node connectivity.
  """

  alias MoyaSqueezer.Config
  alias MoyaSqueezer.ConnectionWorker
  alias MoyaSqueezer.KeyPool
  alias MoyaSqueezer.MetricsLogger
  alias MoyaSqueezer.StatsCollector

  @spec run_from_file(String.t(), keyword()) :: :ok | {:error, term()}
  def run_from_file(path, opts \\ []) do
    with {:ok, config} <- Config.from_toml_file(path) do
      run(config, opts)
    end
  end

  @spec run(Config.t(), keyword()) :: :ok | {:error, term()}
  def run(config, opts \\ []) do
    worker_nodes = Keyword.get(opts, :worker_nodes, [])

    with :ok <- ensure_cluster(worker_nodes) do
      run_manager(config, worker_nodes)
    end
  end

  @spec start_worker_segment(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_worker_segment(opts) do
    children =
      worker_children(
        Keyword.fetch!(opts, :connections),
        Keyword.fetch!(opts, :adapter),
        Keyword.fetch!(opts, :adapter_opts),
        Keyword.fetch!(opts, :logger),
        Keyword.fetch!(opts, :stats_collector),
        Keyword.fetch!(opts, :payload_size),
        Keyword.fetch!(opts, :read_ratio),
        Keyword.fetch!(opts, :write_ratio),
        Keyword.fetch!(opts, :delete_ratio),
        Keyword.fetch!(opts, :key_pool),
        Keyword.fetch!(opts, :requests_per_worker),
        Keyword.fetch!(opts, :mode)
      )

    with {:ok, supervisor} <-
           Supervisor.start_link(children,
             strategy: :one_for_one,
             name: :"worker_sup_#{System.unique_integer()}"
           ) do
      Process.unlink(supervisor)
      {:ok, supervisor}
    end
  end

  @spec stop_worker_segment(pid()) :: :ok
  def stop_worker_segment(supervisor), do: Supervisor.stop(supervisor, :normal, 10_000)

  @spec set_worker_segment_rate(pid(), float()) :: :ok
  def set_worker_segment_rate(supervisor, reqs_per_worker) do
    supervisor
    |> worker_pids()
    |> Enum.each(&ConnectionWorker.set_reqs_per_sec(&1, reqs_per_worker))

    :ok
  end

  defp run_manager(config, worker_nodes) do
    adapter = Application.get_env(:moya_squeezer, :load_adapter, MoyaSqueezer.Adapters.HttpAdapter)
    start_rps = config.start_requests_per_second
    nodes = Enum.uniq([node() | worker_nodes])

    logger_name = {:global, :"metrics_logger_#{System.unique_integer([:positive])}"}
    stats_name = {:global, :"stats_collector_#{System.unique_integer([:positive])}"}
    key_pool_name = {:global, :"key_pool_#{System.unique_integer([:positive])}"}

    per_node_connections = distribute_connections(config.connections, nodes)

    {:ok, stats_collector} = StatsCollector.start_link(name: stats_name, label: "manager")
    {:ok, _key_pool} = KeyPool.start_link(name: key_pool_name)
    {:ok, supervisor} = Supervisor.start_link([{MetricsLogger, name: logger_name, log_path: config.log_path}], strategy: :one_for_one)

    warmup_segments =
      start_segments(
        nodes,
        per_node_connections,
        adapter,
        logger_name,
        stats_name,
        key_pool_name,
        start_rps / config.connections,
        :warmup,
        config
      )

    warmup_stop_reason = maybe_run_warmup(config, warmup_segments)

    if warmup_stop_reason == :warmup_interrupted do
      :ok = Supervisor.stop(supervisor, :normal, 10_000)
      GenServer.stop(stats_collector, :normal, 5_000)
      :ok
    else
      StatsCollector.reset(stats_collector)

      measured_segments =
        start_segments(
          nodes,
          per_node_connections,
          adapter,
          logger_name,
          stats_name,
          key_pool_name,
          start_rps / config.connections,
          :measured,
          config
        )

      signal_setup = install_signal_handlers()
      stop_reason = run_squeeze_control_loop(config, stats_collector, measured_segments, start_rps, config.duration_seconds)

      stop_segments(measured_segments)
      :ok = Supervisor.stop(supervisor, :normal, 10_000)
      restore_signal_handlers(signal_setup)

      report = StatsCollector.final_report(stats_collector)
      print_final_report(report, stop_reason)
      GenServer.stop(stats_collector, :normal, 5_000)

      :ok
    end
  end

  defp run_squeeze_control_loop(config, stats_collector, worker_segments, start_rps, duration_seconds) do
    started_at_ms = System.monotonic_time(:millisecond)
    deadline_ms = started_at_ms + duration_seconds * 1_000

    baseline_stop = wait_for_duration_or_signal(min(config.baseline_window_seconds * 1_000, deadline_ms - started_at_ms))

    case baseline_stop do
      {:signal, sig} -> {:signal, sig}
      :duration_elapsed ->
        now_ms = System.monotonic_time(:millisecond)

        if now_ms >= deadline_ms do
          :duration_elapsed
        else
          baseline_p90_ms = StatsCollector.percentile_ms(stats_collector, 0.90)
          IO.puts("[manager][baseline] p90=#{Float.round(baseline_p90_ms, 2)}ms")

          ramp_loop(%{
            config: config,
            stats_collector: stats_collector,
            worker_segments: worker_segments,
            current_rps: start_rps,
            baseline_p90_ms: baseline_p90_ms,
            last_step_at_ms: now_ms,
            deadline_ms: deadline_ms
          })
        end
    end
  end

  defp ramp_loop(state) do
    remaining_ms = max(state.deadline_ms - System.monotonic_time(:millisecond), 0)

    receive do
      {:signal, sig} -> {:signal, sig}
    after
      min(1_000, remaining_ms) ->
        now_ms = System.monotonic_time(:millisecond)

        cond do
          now_ms >= state.deadline_ms ->
            :duration_elapsed

          true ->
            snapshot = StatsCollector.window_snapshot(state.stats_collector)

            cond do
              snapshot.error_rate_pct > state.config.max_error_rate_pct -> :error_rate_exceeded
              snapshot.count > 0 and snapshot.p50_latency_ms > state.baseline_p90_ms -> :p50_exceeded_baseline_p90
              true ->
                {next_rps, next_step_ms} = maybe_step_rps(state, now_ms)
                ramp_loop(%{state | current_rps: next_rps, last_step_at_ms: next_step_ms})
            end
        end
    end
  end

  defp maybe_step_rps(state, now_ms) do
    step_interval_ms = state.config.step_interval_seconds * 1_000

    if state.config.rps_step > 0 and now_ms - state.last_step_at_ms >= step_interval_ms do
      next_rps = state.current_rps + state.config.rps_step
      set_worker_rates(state.worker_segments, next_rps / state.config.connections)
      IO.puts("[manager][ramp] target_rps=#{next_rps}")
      {next_rps, now_ms}
    else
      {state.current_rps, state.last_step_at_ms}
    end
  end

  defp worker_pids(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.map(fn {_id, pid, _type, _modules} -> pid end)
  end

  defp maybe_run_warmup(config, _segments) when config.warmup_seconds <= 0, do: :no_warmup

  defp maybe_run_warmup(config, segments) do
    IO.puts("[manager][warmup] seeding keys for #{config.warmup_seconds}s using write-only traffic...")
    warmup_stop_reason = wait_for_duration_or_signal(config.warmup_seconds * 1_000)
    stop_segments(segments)

    case warmup_stop_reason do
      {:signal, _sig} ->
        IO.puts("[manager] signal received during warmup, stopping run.")
        :warmup_interrupted

      :duration_elapsed ->
        IO.puts("[manager][warmup] completed. starting measured phase...")
        :warmup_complete
    end
  end

  defp worker_children(connection_count, _adapter, _adapter_opts, _logger_name, _stats_name, _payload_size, _read_ratio, _write_ratio, _delete_ratio, _key_pool_name, _requests_per_worker, _mode)
       when connection_count <= 0,
       do: []

  defp worker_children(
         connection_count,
         adapter,
         adapter_opts,
         logger_name,
         stats_name,
         payload_size,
         read_ratio,
         write_ratio,
         delete_ratio,
         key_pool_name,
         requests_per_worker,
         mode
       ) do
    Enum.map(1..connection_count, fn id ->
      %{
        id: {:connection_worker, id},
        start:
          {ConnectionWorker, :start_link,
           [[
             id: id,
             adapter: adapter,
             adapter_opts: adapter_opts,
             logger: logger_name,
             stats_collector: stats_name,
             payload_size: payload_size,
             reqs_per_sec: requests_per_worker,
             read_ratio: read_ratio,
             write_ratio: write_ratio,
             delete_ratio: delete_ratio,
             key_pool: key_pool_name,
             mode: mode
           ]]}
      }
    end)
  end

  defp wait_for_duration_or_signal(duration_ms) do
    receive do
      {:signal, :sigint} -> {:signal, :sigint}
      {:signal, :sigterm} -> {:signal, :sigterm}
    after
      duration_ms -> :duration_elapsed
    end
  end

  defp install_signal_handlers do
    try do
      %{sigint: :os.set_signal(:sigint, :handle), sigterm: :os.set_signal(:sigterm, :handle)}
    rescue
      _ -> nil
    end
  end

  defp restore_signal_handlers(nil), do: :ok

  defp restore_signal_handlers(signal_setup) do
    :os.set_signal(:sigint, signal_setup.sigint)
    :os.set_signal(:sigterm, signal_setup.sigterm)
    :ok
  end

  defp print_final_report(report, stop_reason) do
    stop_label =
      case stop_reason do
        :duration_elapsed -> "duration_elapsed"
        :error_rate_exceeded -> "error_rate_exceeded"
        :p50_exceeded_baseline_p90 -> "p50_exceeded_baseline_p90"
        {:signal, sig} -> Atom.to_string(sig)
      end

    IO.puts(
      "[manager][final] stop_reason=#{stop_label} " <>
        "total=#{report.total_requests} errors=#{report.total_errors} " <>
        "error_rate=#{Float.round(report.error_rate_pct, 2)}% " <>
        "avg=#{Float.round(report.avg_latency_ms, 2)}ms " <>
        "p50=#{Float.round(report.p50_latency_ms, 2)}ms " <>
        "p90=#{Float.round(report.p90_latency_ms, 2)}ms " <>
        "p95=#{Float.round(report.p95_latency_ms, 2)}ms"
    )
  end

  defp ensure_cluster([]), do: :ok

  defp ensure_cluster(worker_nodes) do
    if Node.alive?() do
      Enum.reduce_while(worker_nodes, :ok, fn worker_node, _acc ->
        if Node.connect(worker_node) and Node.ping(worker_node) == :pong do
          {:cont, :ok}
        else
          {:halt, {:error, "unable to connect to worker node #{worker_node}"}}
        end
      end)
    else
      {:error, "manager node is not distributed; start with --sname/--name and --cookie"}
    end
  end

  defp distribute_connections(total_connections, nodes) do
    base = div(total_connections, length(nodes))
    remainder = rem(total_connections, length(nodes))

    nodes
    |> Enum.with_index()
    |> Enum.map(fn {node_name, idx} ->
      extra = if idx < remainder, do: 1, else: 0
      {node_name, base + extra}
    end)
    |> Enum.into(%{})
  end

  defp start_segments(nodes, per_node_connections, adapter, logger_name, stats_name, key_pool_name, requests_per_worker, mode, config) do
    adapter_opts = %{
      base_url: config.base_url,
      request_timeout_ms: config.request_timeout_ms,
      max_retries: config.max_retries,
      retry_backoff_ms: config.retry_backoff_ms,
      read_path: config.read_path,
      write_path: config.write_path,
      delete_path: config.delete_path
    }

    Enum.map(nodes, fn target_node ->
      start_segment_on_node(
        target_node,
        per_node_connections[target_node],
        adapter,
        adapter_opts,
        logger_name,
        stats_name,
        key_pool_name,
        requests_per_worker,
        mode,
        config
      )
    end)
  end

  defp start_segment_on_node(target_node, node_connections, adapter, adapter_opts, logger_name, stats_name, key_pool_name, requests_per_worker, mode, config) do
    if node_connections <= 0 do
      %{node: target_node, supervisor: nil}
    else
      segment_opts = [
        connections: node_connections,
        adapter: adapter,
        adapter_opts: adapter_opts,
        logger: logger_name,
        stats_collector: stats_name,
        payload_size: config.payload_size,
        read_ratio: config.read_ratio,
        write_ratio: config.write_ratio,
        delete_ratio: config.delete_ratio,
        requests_per_worker: requests_per_worker,
        key_pool: key_pool_name,
        mode: mode
      ]

      result =
        if target_node == node() do
          start_worker_segment(segment_opts)
        else
          :rpc.call(target_node, __MODULE__, :start_worker_segment, [segment_opts])
        end

      case result do
        {:ok, pid} -> %{node: target_node, supervisor: pid}
        {:badrpc, reason} -> raise "failed to start worker segment on #{target_node}: #{inspect(reason)}"
        {:error, reason} -> raise "failed to start worker segment on #{target_node}: #{inspect(reason)}"
      end
    end
  end

  defp stop_segments(segments) do
    Enum.each(segments, fn %{node: target_node, supervisor: supervisor} ->
      if supervisor do
        if target_node == node() do
          stop_worker_segment(supervisor)
        else
          :rpc.call(target_node, __MODULE__, :stop_worker_segment, [supervisor])
        end
      end
    end)
  end

  defp set_worker_rates(segments, reqs_per_worker) do
    Enum.each(segments, fn %{node: target_node, supervisor: supervisor} ->
      if supervisor do
        if target_node == node() do
          set_worker_segment_rate(supervisor, reqs_per_worker)
        else
          :rpc.call(target_node, __MODULE__, :set_worker_segment_rate, [supervisor, reqs_per_worker])
        end
      end
    end)
  end
end