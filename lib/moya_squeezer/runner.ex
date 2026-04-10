defmodule MoyaSqueezer.Runner do
  @moduledoc """
  Main routine that runs a squeeze test from TOML configuration.

  Supports manager/worker distribution using standard Erlang node connectivity.
  """

  alias MoyaSqueezer.Config
  alias MoyaSqueezer.ConnectionWorker
  alias MoyaSqueezer.MetricsLogger
  alias MoyaSqueezer.RuntimeState
  alias MoyaSqueezer.StatsCollector

  @spec run_from_file(String.t(), keyword()) :: :ok | {:error, term()}
  def run_from_file(path, opts \\ []) do
    with {:ok, config} <- Config.from_toml_file(path) do
      run(config, opts)
    end
  end

  @spec run(Config.t(), keyword()) :: :ok | {:error, term()}
  def run(config, opts \\ []) do
    RuntimeState.set_role(:manager)

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
        Keyword.fetch!(opts, :stats_collector),
        Keyword.fetch!(opts, :tick_ms),
        Keyword.fetch!(opts, :worker_inflight_limit),
        Keyword.fetch!(opts, :stats_flush_interval_ms),
        Keyword.fetch!(opts, :payload_size),
        Keyword.fetch!(opts, :read_ratio),
        Keyword.fetch!(opts, :write_ratio),
        Keyword.fetch!(opts, :delete_ratio),
        Keyword.fetch!(opts, :requests_per_worker),
        Keyword.fetch!(opts, :mode),
        Keyword.fetch!(opts, :metrics_log_path),
        Keyword.fetch!(opts, :metrics_flush_interval_ms),
        Keyword.fetch!(opts, :metrics_compact)
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

  @spec worker_segment_pids(pid()) :: [pid()]
  def worker_segment_pids(supervisor), do: worker_pids(supervisor)

  @spec worker_segment_summaries(pid()) :: [map()]
  def worker_segment_summaries(supervisor) do
    supervisor
    |> worker_pids()
    |> Enum.map(&ConnectionWorker.summary/1)
  end

  @spec worker_segment_export_keyspace(pid()) :: [String.t()]
  def worker_segment_export_keyspace(supervisor) do
    supervisor
    |> worker_pids()
    |> Enum.flat_map(&ConnectionWorker.export_keyspace/1)
    |> Enum.uniq()
  end

  @spec worker_segment_import_keyspace(pid(), [String.t()]) :: :ok
  def worker_segment_import_keyspace(supervisor, keys) do
    supervisor
    |> worker_pids()
    |> Enum.each(&ConnectionWorker.import_keyspace(&1, keys))

    :ok
  end

  @spec read_metrics_file(String.t()) :: {:ok, binary()} | {:error, term()}
  def read_metrics_file(path), do: File.read(path)

  @spec set_worker_segment_mode(pid(), :warmup | :measured) :: :ok
  def set_worker_segment_mode(supervisor, mode) when mode in [:warmup, :measured] do
    supervisor
    |> worker_pids()
    |> Enum.each(&ConnectionWorker.set_mode(&1, mode))

    :ok
  end

  defp run_manager(config, worker_nodes) do
    adapter = Application.get_env(:moya_squeezer, :load_adapter, MoyaSqueezer.Adapters.HttpAdapter)
    start_rps = config.start_requests_per_second
    nodes = Enum.uniq(worker_nodes)

    if nodes == [] do
      {:error, "at least one worker node is required; manager is control-plane only"}
    else

    logger_name = :"metrics_logger_#{System.unique_integer([:positive])}"
    per_node_connections = Enum.into(nodes, %{}, fn n -> {n, config.connections_per_worker} end)
    total_connections = config.connections_per_worker * length(nodes)

    {:ok, stats_collector} = StatsCollector.start_link(label: "manager")
    {:ok, supervisor} =
      Supervisor.start_link(
        [
          {MetricsLogger,
           name: logger_name,
           log_path: config.log_path,
           flush_interval_ms: config.metrics_flush_interval_ms,
           compact: config.metrics_compact}
        ],
        strategy: :one_for_one
      )

      case config.ramp_mode do
        :concurrency ->
        run_manager_concurrency_mode(
            config,
            nodes,
            per_node_connections,
          total_connections,
            adapter,
            logger_name,
            stats_collector,
            start_rps,
            supervisor,
            stats_collector
          )

        :rps ->
          run_manager_rps_mode(
            config,
            nodes,
            per_node_connections,
            total_connections,
            adapter,
            logger_name,
            stats_collector,
            start_rps,
            supervisor,
            stats_collector
          )

        :payload ->
          run_manager_rps_mode(
            config,
            nodes,
            per_node_connections,
            total_connections,
            adapter,
            logger_name,
            stats_collector,
            start_rps,
            supervisor,
            stats_collector
          )
      end
    end
  end

  defp run_manager_rps_mode(config, nodes, per_node_connections, total_connections, adapter, logger_name, stats_collector_name, start_rps, supervisor, stats_collector) do
    warmup_segments =
      start_segments(
        nodes,
        per_node_connections,
        adapter,
        logger_name,
        stats_collector_name,
        start_rps / total_connections,
        :warmup,
        config
      )

    warmup_stop_reason = maybe_run_warmup(config, warmup_segments, false)

    if warmup_stop_reason == :warmup_interrupted do
      stop_segments(warmup_segments)
      :ok = Supervisor.stop(supervisor, :normal, 10_000)
      GenServer.stop(stats_collector, :normal, 5_000)
      :ok
    else
      warmup_keyspace = pull_combined_keyspace(warmup_segments)
      print_keyspace_acquired(warmup_keyspace)
      stop_segments(warmup_segments)
      StatsCollector.reset(stats_collector)

      measured_segments =
        start_segments(
          nodes,
          per_node_connections,
          adapter,
          logger_name,
          stats_collector_name,
          initial_measured_requests_per_worker(total_connections, config, start_rps),
          :measured,
          config
        )

      run_measured_phase(config, stats_collector, measured_segments, start_rps, supervisor,
        manager_keyspace: warmup_keyspace
      )
    end
  end

  defp run_manager_concurrency_mode(config, nodes, per_node_connections, total_connections, adapter, logger_name, stats_collector_name, start_rps, supervisor, stats_collector) do
    segments =
      start_segments(
        nodes,
        per_node_connections,
        adapter,
        logger_name,
        stats_collector_name,
        start_rps / total_connections,
        :warmup,
        config
      )

    warmup_stop_reason = maybe_run_warmup(config, segments, false)

    if warmup_stop_reason == :warmup_interrupted do
      safe_stop_segments(segments)
      safe_supervisor_stop(supervisor)
      safe_genserver_stop(stats_collector)
      :ok
    else
      manager_keyspace = pull_combined_keyspace(segments)
      print_keyspace_acquired(manager_keyspace)
      set_segments_mode(segments, :measured)
      StatsCollector.reset(stats_collector)

      try do
        with :ok <- validate_ramp_settings(config, segments) do
          log_concurrency_worker_capacity(config, segments)
          maybe_initialize_concurrency_ramp(config, segments, start_rps)
          seed_keyspace_to_inactive_workers(segments, config.initial_active_workers, manager_keyspace)
          run_measured_phase(config, stats_collector, segments, start_rps, supervisor,
            adapter: adapter,
            logger_name: logger_name,
            stats_name: stats_collector_name,
            manager_keyspace: manager_keyspace
          )
        else
          {:error, reason} -> {:error, reason}
        end
      after
        # Defensive cleanup: prevent orphaned worker segments from outliving key_pool/logger
        # when manager exits early and retries.
        safe_stop_segments(segments)
        safe_supervisor_stop(supervisor)
        safe_genserver_stop(stats_collector)
      end
    end
  end

  defp run_measured_phase(config, stats_collector, measured_segments, start_rps, supervisor, opts \\ []) do
    RuntimeState.set_measured_segments(measured_segments)
    signal_setup = install_signal_handlers()

    {stop_reason, worker_summaries} =
      try do
        stop_reason =
          run_squeeze_control_loop(
            config,
            stats_collector,
            measured_segments,
            start_rps,
            config.duration_seconds,
            opts
          )

        worker_summaries = gather_worker_summaries(measured_segments)
        {stop_reason, worker_summaries}
      after
        RuntimeState.set_measured_segments([])
        safe_stop_segments(measured_segments)
        safe_supervisor_stop(supervisor)
        restore_signal_handlers(signal_setup)
      end

    report = StatsCollector.final_report(stats_collector)
    print_final_report(report, stop_reason)
    print_worker_summary_table(worker_summaries)
    print_halt_summary(config, stats_collector, worker_summaries)
    merge_worker_metric_logs(config.log_path, measured_segments)
    safe_genserver_stop(stats_collector)

    :ok
  end

  defp run_squeeze_control_loop(config, stats_collector, worker_segments, start_rps, duration_seconds, opts) do
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
          baseline_percentile = config.stop_latency_percentile
          baseline_latency_ms = StatsCollector.percentile_ms(stats_collector, baseline_percentile)
          baseline_label = percentile_label(baseline_percentile)
          IO.puts("[manager][baseline] #{baseline_label}=#{format_2dp(baseline_latency_ms)}ms")

          stop_reason = ramp_loop(%{
            config: config,
            stats_collector: stats_collector,
            worker_segments: worker_segments,
            current_rps: start_rps,
            active_workers: initial_active_workers(config, worker_segments),
            current_payload_size: config.payload_size,
            total_target_rps: config.total_target_rps,
            adapter: Keyword.get(opts, :adapter),
            logger_name: Keyword.get(opts, :logger_name),
            stats_name: Keyword.get(opts, :stats_name),
            manager_keyspace: Keyword.get(opts, :manager_keyspace, []),
            baseline_latency_ms: baseline_latency_ms,
            error_breach_streak: 0,
            latency_breach_streak: 0,
            last_step_at_ms: now_ms,
            next_worker_step_due_ms: now_ms + config.worker_step_interval_seconds * 1_000,
            deadline_ms: deadline_ms
          })

          maybe_run_feel_the_burn(config, stop_reason)
        end
    end
  end

  defp maybe_run_feel_the_burn(%{feel_the_burn_seconds: burn_seconds}, stop_reason)
       when burn_seconds > 0 and stop_reason in [:error_rate_exceeded, :p50_exceeded_baseline_threshold] do
    IO.puts(
      "[manager][burn] stop criteria reached (#{stop_reason}); holding load steady for #{burn_seconds}s"
    )

    case wait_for_duration_or_signal(burn_seconds * 1_000) do
      {:signal, sig} -> {:signal, sig}
      :duration_elapsed -> stop_reason
    end
  end

  defp maybe_run_feel_the_burn(_config, stop_reason), do: stop_reason

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

            next_error_streak =
              if snapshot.error_rate_pct > state.config.max_error_rate_pct,
                do: state.error_breach_streak + 1,
                else: 0

            cond do
              next_error_streak >= state.config.error_breach_consecutive_windows ->
                :error_rate_exceeded

              snapshot.count > 0 ->
                next_latency_streak =
                  if snapshot.p50_latency_ms > state.baseline_latency_ms,
                    do: state.latency_breach_streak + 1,
                    else: 0

                if next_latency_streak >= state.config.latency_breach_consecutive_windows do
                  :p50_exceeded_baseline_threshold
                else
                  {next_state, next_step_ms} =
                    maybe_step_load(
                      %{state | error_breach_streak: next_error_streak, latency_breach_streak: next_latency_streak},
                      now_ms
                    )

                  ramp_loop(%{next_state | last_step_at_ms: next_step_ms})
                end

              true ->
                {next_state, next_step_ms} = maybe_step_load(%{state | error_breach_streak: next_error_streak}, now_ms)
                ramp_loop(%{next_state | last_step_at_ms: next_step_ms})
            end
        end
    end
  end

  defp maybe_step_load(%{config: %{ramp_mode: :concurrency}} = state, now_ms),
    do: maybe_step_workers(state, now_ms)

  defp maybe_step_load(%{config: %{ramp_mode: :payload}} = state, now_ms),
    do: maybe_step_payload(state, now_ms)

  defp maybe_step_load(state, now_ms), do: maybe_step_rps(state, now_ms)

  defp maybe_step_payload(state, now_ms) do
    step_interval_ms = state.config.step_interval_seconds * 1_000

    if state.config.payload_step_bytes > 0 and now_ms - state.last_step_at_ms >= step_interval_ms do
      next_payload_size = state.current_payload_size + state.config.payload_step_bytes
      set_worker_payload_sizes(state.worker_segments, next_payload_size)
      IO.puts("[manager][ramp] payload_size=#{next_payload_size}")
      {%{state | current_payload_size: next_payload_size}, now_ms}
    else
      {state, state.last_step_at_ms}
    end
  end

  defp maybe_step_rps(state, now_ms) do
    step_interval_ms = state.config.step_interval_seconds * 1_000

    if state.config.rps_step > 0 and now_ms - state.last_step_at_ms >= step_interval_ms do
      next_rps = state.current_rps + state.config.rps_step
      total_connections = state.config.connections_per_worker * length(state.worker_segments)
      set_worker_rates(state.worker_segments, next_rps / total_connections)
      IO.puts("[manager][ramp] target_rps=#{next_rps}")
      {%{state | current_rps: next_rps}, now_ms}
    else
      {state, state.last_step_at_ms}
    end
  end

  defp maybe_step_workers(state, now_ms) do
    step_interval_ms = state.config.worker_step_interval_seconds * 1_000
    next_due_ms = Map.get(state, :next_worker_step_due_ms, state.last_step_at_ms + step_interval_ms)

    if state.config.worker_step > 0 and now_ms >= next_due_ms do
      snapshot = StatsCollector.window_snapshot(state.stats_collector)

      if snapshot.error_rate_pct > state.config.max_error_rate_pct do
        IO.puts(
          "[manager][ramp] ts_ms=#{System.system_time(:millisecond)} " <>
            "join_deferred error_rate=#{Float.round(snapshot.error_rate_pct, 2)}% " <>
            "threshold=#{Float.round(state.config.max_error_rate_pct, 2)}%"
        )

        {%{state | next_worker_step_due_ms: next_due_ms + step_interval_ms}, state.last_step_at_ms}
      else
        max_active = min(state.config.worker_container_pool, available_worker_count(state.worker_segments))
        next_active = min(state.active_workers + state.config.worker_step, max_active)
        advanced_due_ms = next_due_ms + step_interval_ms

        if next_active > state.active_workers do
          set_active_worker_count(
            state.worker_segments,
            next_active,
            state.total_target_rps,
            state.config.connections_per_worker
          )
          per_worker = state.total_target_rps / max(next_active, 1)

          IO.puts(
            "[manager][ramp] ts_ms=#{System.system_time(:millisecond)} active_workers=#{next_active} " <>
              "target_total_rps=#{state.total_target_rps} " <>
              "target_rps_per_worker=#{Float.round(per_worker, 2)}"
          )

          {%{state | active_workers: next_active, next_worker_step_due_ms: advanced_due_ms}, state.last_step_at_ms}
        else
          {%{state | next_worker_step_due_ms: advanced_due_ms}, state.last_step_at_ms}
        end
      end
    else
      {state, state.last_step_at_ms}
    end
  end

  defp worker_pids(supervisor) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.filter(fn {id, _pid, _type, _modules} -> match?({:connection_worker, _, _}, id) end)
    |> Enum.map(fn {_id, pid, _type, _modules} -> pid end)
  end

  defp maybe_run_warmup(config, _segments, _stop_after_warmup \\ true)
  defp maybe_run_warmup(config, _segments, _stop_after_warmup) when config.warmup_seconds <= 0, do: :no_warmup

  defp maybe_run_warmup(config, segments, stop_after_warmup) do
    IO.puts("[manager][warmup] seeding keys for #{config.warmup_seconds}s using write-only traffic...")
    warmup_stop_reason = wait_for_duration_or_signal(config.warmup_seconds * 1_000)

    if stop_after_warmup do
      stop_segments(segments)
    end

    case warmup_stop_reason do
      {:signal, _sig} ->
        IO.puts("[manager] signal received during warmup, stopping run.")
        :warmup_interrupted

      :duration_elapsed ->
        IO.puts("[manager][warmup] completed. starting measured phase...")
        :warmup_complete
    end
  end

  defp worker_children(connection_count, _adapter, _adapter_opts, _stats_name, _tick_ms, _worker_inflight_limit, _stats_flush_interval_ms, _payload_size, _read_ratio, _write_ratio, _delete_ratio, _requests_per_worker, _mode, _metrics_log_path, _metrics_flush_interval_ms, _metrics_compact)
       when connection_count <= 0,
       do: []

  defp worker_children(
         connection_count,
         adapter,
         adapter_opts,
         stats_name,
         tick_ms,
         worker_inflight_limit,
         stats_flush_interval_ms,
         payload_size,
         read_ratio,
         write_ratio,
         delete_ratio,
         requests_per_worker,
         mode,
         metrics_log_path,
         metrics_flush_interval_ms,
         metrics_compact
       ) do
    local_logger_name = :"metrics_logger_local_#{System.unique_integer([:positive])}"
    local_task_supervisor_name = :"task_sup_local_#{System.unique_integer([:positive])}"

    logger_child =
      {MetricsLogger,
       name: local_logger_name,
       log_path: metrics_log_path,
       flush_interval_ms: metrics_flush_interval_ms,
       compact: metrics_compact}

    task_supervisor_child =
      {Task.Supervisor,
       name: local_task_supervisor_name}

    worker_children = Enum.map(1..connection_count, fn id ->
      %{
        id: {:connection_worker, local_logger_name, id},
        start:
          {ConnectionWorker, :start_link,
           [[
             id: id,
             adapter: adapter,
             adapter_opts: adapter_opts,
             logger: local_logger_name,
             task_supervisor: local_task_supervisor_name,
             stats_collector: stats_name,
             tick_ms: tick_ms,
             worker_inflight_limit: worker_inflight_limit,
             stats_flush_interval_ms: stats_flush_interval_ms,
             payload_size: payload_size,
             reqs_per_sec: requests_per_worker,
             read_ratio: read_ratio,
             write_ratio: write_ratio,
             delete_ratio: delete_ratio,
             mode: mode
           ]]}
      }
    end)

    [logger_child, task_supervisor_child | worker_children]
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
        :p50_exceeded_baseline_threshold -> "p50_exceeded_baseline_threshold"
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
    if Enum.all?(worker_nodes, &(&1 == node())) do
      :ok
    else
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
  end

  defp start_segments(nodes, per_node_connections, adapter, logger_name, stats_name, requests_per_worker, mode, config) do
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
        requests_per_worker,
        mode,
        config
      )
    end)
  end

  defp start_segment_on_node(target_node, node_connections, adapter, adapter_opts, logger_name, stats_name, requests_per_worker, mode, config) do
    if node_connections <= 0 do
      %{node: target_node, supervisor: nil, metrics_log_path: nil}
    else
      metrics_log_path = node_metrics_log_path(config.log_path, target_node)

      segment_opts = [
        connections: node_connections,
        adapter: adapter,
        adapter_opts: adapter_opts,
        logger: logger_name,
        stats_collector: stats_name,
        tick_ms: config.worker_tick_ms,
        worker_inflight_limit: config.worker_inflight_limit,
        stats_flush_interval_ms: config.stats_flush_interval_ms,
        payload_size: config.payload_size,
        read_ratio: config.read_ratio,
        write_ratio: config.write_ratio,
        delete_ratio: config.delete_ratio,
        requests_per_worker: requests_per_worker,
        mode: mode,
        metrics_log_path: metrics_log_path,
        metrics_flush_interval_ms: config.metrics_flush_interval_ms,
        metrics_compact: config.metrics_compact
      ]

      result =
        if target_node == node() do
          start_worker_segment(segment_opts)
        else
          :rpc.call(target_node, __MODULE__, :start_worker_segment, [segment_opts])
        end

      case result do
        {:ok, pid} ->
          %{node: target_node, supervisor: pid, connections: node_connections, metrics_log_path: metrics_log_path}
        {:badrpc, reason} -> raise "failed to start worker segment on #{target_node}: #{inspect(reason)}"
        {:error, reason} -> raise "failed to start worker segment on #{target_node}: #{inspect(reason)}"
      end
    end
  end

  defp stop_segments(segments) do
    Enum.each(segments, fn %{node: target_node, supervisor: supervisor} ->
      if supervisor do
        if target_node == node() do
          safe_stop_worker_segment(supervisor)
        else
          case :rpc.call(target_node, __MODULE__, :stop_worker_segment, [supervisor]) do
            :ok -> :ok
            {:badrpc, _} -> :ok
            _ -> :ok
          end
        end
      end
    end)
  end

  defp safe_stop_segments(segments), do: stop_segments(segments)

  defp safe_stop_worker_segment(supervisor) do
    try do
      stop_worker_segment(supervisor)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp safe_supervisor_stop(supervisor) do
    try do
      Supervisor.stop(supervisor, :normal, 10_000)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp safe_genserver_stop(server) do
    try do
      GenServer.stop(server, :normal, 5_000)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
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

  defp set_worker_payload_sizes(segments, payload_size) do
    Enum.each(segments, fn %{node: target_node, supervisor: supervisor} ->
      if supervisor do
        if target_node == node() do
          set_worker_segment_payload_size(supervisor, payload_size)
        else
          :rpc.call(target_node, __MODULE__, :set_worker_segment_payload_size, [supervisor, payload_size])
        end
      end
    end)
  end

  @spec set_worker_segment_payload_size(pid(), pos_integer()) :: :ok
  def set_worker_segment_payload_size(supervisor, payload_size) do
    supervisor
    |> worker_pids()
    |> Enum.each(&ConnectionWorker.set_payload_size(&1, payload_size))

    :ok
  end

  defp set_segments_mode(segments, mode) when mode in [:warmup, :measured] do
    Enum.each(segments, fn %{node: target_node, supervisor: supervisor} ->
      if supervisor do
        if target_node == node() do
          set_worker_segment_mode(supervisor, mode)
        else
          :rpc.call(target_node, __MODULE__, :set_worker_segment_mode, [supervisor, mode])
        end
      end
    end)
  end

  defp initial_measured_requests_per_worker(_total_connections, %{ramp_mode: :concurrency}, _start_rps), do: 0.0
  defp initial_measured_requests_per_worker(total_connections, _config, start_rps), do: start_rps / total_connections

  defp validate_ramp_settings(%{ramp_mode: :concurrency} = config, segments) do
    total_workers = length(segments)
    total_connections = Enum.reduce(segments, 0, fn seg, acc -> acc + (seg[:connections] || 0) end)
    available_workers = available_worker_count(segments)

    cond do
      config.initial_active_workers > available_workers ->
        {:error,
         "initial_active_workers (#{config.initial_active_workers}) exceeds available worker containers (#{available_workers}/#{total_workers}, connections=#{total_connections})"}

      config.worker_container_pool != available_workers ->
        {:error,
         "worker_container_pool (#{config.worker_container_pool}) must equal discovered worker containers (#{available_workers}/#{total_workers}, connections=#{total_connections}). " <>
           "All worker containers must be connected before run start."}

      config.worker_container_pool < config.initial_active_workers ->
        {:error,
         "worker_container_pool must be >= initial_active_workers (#{config.initial_active_workers})"}

      true ->
        :ok
    end
  end

  defp validate_ramp_settings(_config, _segments), do: :ok

  defp maybe_initialize_concurrency_ramp(%{ramp_mode: :concurrency} = config, segments, _start_rps) do
    set_active_worker_count(
      segments,
      config.initial_active_workers,
      config.total_target_rps,
      config.connections_per_worker
    )

    per_worker = config.total_target_rps / max(config.initial_active_workers, 1)

    IO.puts(
      "[manager][ramp] active_workers=#{config.initial_active_workers} " <>
        "target_total_rps=#{config.total_target_rps} " <>
        "target_rps_per_worker=#{Float.round(per_worker, 2)}"
    )

    :ok
  end

  defp maybe_initialize_concurrency_ramp(_config, _segments, _start_rps), do: :ok

  defp log_concurrency_worker_capacity(config, segments) do
    available = available_worker_count(segments)
    effective_initial = min(config.initial_active_workers, available)
    effective_max = min(config.worker_container_pool, available)

    IO.puts(
      "[manager][workers] discovered_nodes=#{available} " <>
        "requested_initial=#{config.initial_active_workers} effective_initial=#{effective_initial} " <>
        "requested_pool=#{config.worker_container_pool} effective_pool=#{effective_max}"
    )
  end

  defp initial_active_workers(%{ramp_mode: :concurrency} = config, _segments), do: config.initial_active_workers
  defp initial_active_workers(_config, segments), do: Enum.count(segments, & &1.supervisor)

  defp set_active_worker_count(segments, active_workers, total_target_rps, connections_per_worker) do
    workers = available_worker_segments(segments)
    active_count = min(active_workers, length(workers))

    reqs_per_container = if active_count > 0, do: total_target_rps / active_count, else: 0.0
    reqs_per_process = reqs_per_container / max(connections_per_worker, 1)

    workers
    |> Enum.with_index()
    |> Enum.each(fn {worker_segment, idx} ->
      rate = if idx < active_count, do: reqs_per_process, else: 0.0
      set_segment_rate(worker_segment, rate)
    end)
  end

  defp available_worker_count(segments) do
    segments
    |> available_worker_segments()
    |> length()
  end

  defp available_worker_segments(segments) do
    segments
    |> Enum.filter(& &1.supervisor)
    |> Enum.sort_by(fn %{node: node_name} -> to_string(node_name) end)
  end

  defp set_segment_rate(%{node: target_node, supervisor: supervisor}, reqs_per_worker) when is_pid(supervisor) do
    if target_node == node() do
      set_worker_segment_rate(supervisor, reqs_per_worker)
    else
      :rpc.call(target_node, __MODULE__, :set_worker_segment_rate, [supervisor, reqs_per_worker])
    end
  end

  defp set_segment_rate(_segment, _reqs_per_worker), do: :ok

  defp node_metrics_log_path(base_log_path, node_name) do
    ext = Path.extname(base_log_path)
    root = String.trim_trailing(base_log_path, ext)
    "#{root}.#{node_name}#{ext}"
  end

  defp gather_worker_summaries(segments) do
    segments
    |> Enum.flat_map(fn
      %{supervisor: nil} -> []
      %{node: target_node, supervisor: supervisor} ->
        summaries =
          if target_node == node() do
            worker_segment_summaries(supervisor)
          else
            case :rpc.call(target_node, __MODULE__, :worker_segment_summaries, [supervisor]) do
              list when is_list(list) -> list
              _ -> []
            end
          end

        Enum.map(summaries, fn s -> Map.put(s, :node, target_node) end)
    end)
  end

  defp pull_combined_keyspace(segments) do
    segments
    |> Enum.flat_map(fn
      %{supervisor: nil} ->
        []

      %{node: target_node, supervisor: supervisor} ->
        result =
          if target_node == node() do
            worker_segment_export_keyspace(supervisor)
          else
            :rpc.call(target_node, __MODULE__, :worker_segment_export_keyspace, [supervisor])
          end

        if is_list(result), do: result, else: []
    end)
    |> Enum.uniq()
  end

  defp import_keyspace_to_segment(%{supervisor: nil}, _keyspace), do: :ok

  defp import_keyspace_to_segment(%{node: target_node, supervisor: supervisor}, keyspace) do
    if target_node == node() do
      worker_segment_import_keyspace(supervisor, keyspace)
    else
      :rpc.call(target_node, __MODULE__, :worker_segment_import_keyspace, [supervisor, keyspace])
    end
  end

  defp seed_keyspace_to_inactive_workers(segments, active_workers, keyspace) do
    segments
    |> inactive_worker_segments(active_workers)
    |> Enum.each(&import_keyspace_to_segment(&1, keyspace))

    IO.puts(
      "[manager][keyspace] seeded_inactive_workers=#{length(inactive_worker_segments(segments, active_workers))} keys=#{length(keyspace)}"
    )

    :ok
  end

  defp inactive_worker_segments(segments, active_workers) do
    segments
    |> available_worker_segments()
    |> Enum.drop(max(active_workers, 0))
  end

  defp print_keyspace_acquired(keyspace) when is_list(keyspace) do
    IO.puts("[manager][keyspace] acquired #{length(keyspace)}")
  end

  defp print_worker_summary_table([]) do
    IO.puts("[manager][workers] workers\tconcurrency\trequests\tavg_rps")
    IO.puts("[manager][workers] (no worker summaries collected)")
  end

  defp print_worker_summary_table(rows) do
    IO.puts("[manager][workers] workers\tconcurrency\trequests\tavg_rps")

    rows
    |> Enum.group_by(& &1.node)
    |> Enum.sort_by(fn {node_name, _rows} -> to_string(node_name) end)
    |> Enum.each(fn {node_name, node_rows} ->
      total_requests = Enum.reduce(node_rows, 0, fn row, acc -> acc + row.measured_requests end)
      worker_count = length(node_rows)
      avg_rps = Enum.reduce(node_rows, 0.0, fn row, acc -> acc + row.avg_rps end)

      IO.puts(
        "[manager][workers] #{node_name}\t#{worker_count}\t#{total_requests}\t#{Float.round(avg_rps, 2)}"
      )
    end)
  end

  defp print_halt_summary(config, stats_collector, worker_summaries) do
    snapshot = StatsCollector.last_emitted_window_snapshot(stats_collector)
    aggregate_rps = snapshot.count
    avg_payload_size = config.payload_size
    concurrent_connections = length(worker_summaries)

    IO.puts(
      "[manager] This squeeze test ran until aggregate_rps=#{aggregate_rps} " <>
        "avg_payload_size=#{avg_payload_size}B " <>
        "concurrent_db_connections=#{concurrent_connections}"
    )
  end

  defp merge_worker_metric_logs(_manager_log_path, []), do: :ok

  defp merge_worker_metric_logs(manager_log_path, segments) do
    contents =
      Enum.map(segments, fn %{node: target_node, metrics_log_path: log_path} ->
        if is_nil(log_path) do
          ""
        else
        result =
          if target_node == node() do
            read_metrics_file(log_path)
          else
            :rpc.call(target_node, __MODULE__, :read_metrics_file, [log_path])
          end

        case result do
          {:ok, body} -> body
          _ -> ""
        end
        end
      end)

    merged =
      contents
      |> Enum.with_index()
      |> Enum.map(fn {body, idx} ->
        if idx == 0 do
          body
        else
          body
          |> String.split("\n", parts: 2)
          |> case do
            [_header, rest] -> rest
            [_only] -> ""
            _ -> ""
          end
        end
      end)
      |> Enum.join("")

    File.mkdir_p!(Path.dirname(manager_log_path))
    File.write!(manager_log_path, merged)
    :ok
  end

  defp format_2dp(value) when is_integer(value), do: format_2dp(value / 1)
  defp format_2dp(value) when is_float(value), do: :erlang.float_to_binary(value, decimals: 2)

  defp percentile_label(p) when is_float(p) do
    "p#{trunc(p * 100)}"
  end
end