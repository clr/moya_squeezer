defmodule MoyaSqueezer.ConnectionWorker do
  @moduledoc """
  Represents one logical client connection that continuously emits load.
  """

  use GenServer

  @default_tick_ms 10
  @default_stats_flush_interval_ms 100

  defstruct [
    :id,
    :adapter,
    :adapter_opts,
    :logger,
    :task_supervisor,
    :stats_collector,
    :payload_size,
    :reqs_per_sec,
    :read_ratio,
    :write_ratio,
    :delete_ratio,
    :mode,
    :tick_ms,
    :worker_inflight_limit,
    :stats_flush_interval_ms,
    token_balance: 0.0,
    inflight_count: 0,
    local_count: 0,
    local_errors: 0,
    local_durations_us: [],
    local_total_duration_us: 0,
    local_histogram_100us: %{},
    local_metrics: [],
    local_tick_count: 0,
    local_tick_work_us: 0,
    local_tick_overrun_count: 0,
    local_max_requests_in_tick: 0,
    local_next_id: 1,
    local_keys: MapSet.new(),
    local_key_list: [],
    measured_requests: 0,
    measured_started_at_ms: nil
  ]

  @type options :: [
          id: pos_integer(),
          adapter: module(),
          adapter_opts: map(),
          logger: pid() | atom(),
          task_supervisor: pid() | atom(),
          stats_collector: pid() | atom(),
          payload_size: pos_integer(),
          reqs_per_sec: float(),
          read_ratio: float(),
          write_ratio: float(),
          delete_ratio: float(),
          tick_ms: pos_integer(),
          worker_inflight_limit: pos_integer(),
          stats_flush_interval_ms: pos_integer(),
          mode: :warmup | :measured
        ]

  @spec start_link(options()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec set_reqs_per_sec(pid() | atom(), float()) :: :ok
  def set_reqs_per_sec(worker, reqs_per_sec), do: GenServer.cast(worker, {:set_reqs_per_sec, reqs_per_sec})

  @spec set_mode(pid() | atom(), :warmup | :measured) :: :ok
  def set_mode(worker, mode) when mode in [:warmup, :measured], do: GenServer.cast(worker, {:set_mode, mode})

  @spec summary(pid() | atom()) :: map()
  def summary(worker), do: GenServer.call(worker, :summary, 10_000)

  @impl true
  def init(opts) do
    mode = Keyword.get(opts, :mode, :measured)

    state = %__MODULE__{
      id: Keyword.fetch!(opts, :id),
      adapter: Keyword.fetch!(opts, :adapter),
      adapter_opts: Keyword.fetch!(opts, :adapter_opts),
      logger: Keyword.fetch!(opts, :logger),
      task_supervisor: Keyword.fetch!(opts, :task_supervisor),
      stats_collector: Keyword.fetch!(opts, :stats_collector),
      payload_size: Keyword.fetch!(opts, :payload_size),
      reqs_per_sec: Keyword.fetch!(opts, :reqs_per_sec),
      read_ratio: Keyword.fetch!(opts, :read_ratio),
      write_ratio: Keyword.fetch!(opts, :write_ratio),
      delete_ratio: Keyword.fetch!(opts, :delete_ratio),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      worker_inflight_limit: Keyword.get(opts, :worker_inflight_limit, 1),
      stats_flush_interval_ms:
        Keyword.get(opts, :stats_flush_interval_ms, @default_stats_flush_interval_ms),
      mode: mode,
      measured_started_at_ms: if(mode == :measured, do: System.monotonic_time(:millisecond), else: nil)
    }

    Process.send_after(self(), :tick, state.tick_ms)
    Process.send_after(self(), :flush_stats, state.stats_flush_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    tick_started_us = System.monotonic_time(:microsecond)
    token_balance = state.token_balance + state.reqs_per_sec * (state.tick_ms / 1000)
    requests_ready = trunc(token_balance)
    available_slots = max(state.worker_inflight_limit - state.inflight_count, 0)
    requests_to_dispatch = min(requests_ready, available_slots)

    next_state = dispatch_requests(state, requests_to_dispatch)
    remaining = token_balance - requests_to_dispatch

    tick_work_us = max(System.monotonic_time(:microsecond) - tick_started_us, 0)

    updated_state = %{
      next_state
      | token_balance: remaining,
        local_tick_count: next_state.local_tick_count + 1,
        local_tick_work_us: next_state.local_tick_work_us + tick_work_us,
        local_tick_overrun_count:
          next_state.local_tick_overrun_count + if(tick_work_us > state.tick_ms * 1_000, do: 1, else: 0),
        local_max_requests_in_tick: max(next_state.local_max_requests_in_tick, requests_to_dispatch)
    }

    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, updated_state}
  end

  @impl true
  def handle_info(:flush_stats, state) do
    flushed_state = flush_local_buffers(state)
    Process.send_after(self(), :flush_stats, state.stats_flush_interval_ms)
    {:noreply, flushed_state}
  end

  @impl true
  def handle_info({:request_result, request_type, key, started_at_ms, response_code, db_latency_us, dispatched_mode}, state) do
    state_after_pool = maybe_update_local_key_pool(request_type, key, response_code, state)

    metric = %{
      source_node: Atom.to_string(node()),
      request_type: request_type,
      started_at_ms: started_at_ms,
      db_latency_us: db_latency_us,
      response_code: response_code
    }

    next_state =
      state_after_pool
      |> local_record(metric, db_latency_us, response_code, dispatched_mode)
      |> Map.update!(:inflight_count, &max(&1 - 1, 0))

    {:noreply, next_state}
  end

  @impl true
  def handle_cast({:set_reqs_per_sec, reqs_per_sec}, state) when is_number(reqs_per_sec) do
    {:noreply, %{state | reqs_per_sec: reqs_per_sec / 1}}
  end

  @impl true
  def handle_cast({:set_mode, mode}, state) when mode in [:warmup, :measured] do
    measured_started_at_ms =
      case {mode, state.measured_started_at_ms} do
        {:measured, nil} -> System.monotonic_time(:millisecond)
        _ -> state.measured_started_at_ms
      end

    {:noreply, %{state | mode: mode, measured_started_at_ms: measured_started_at_ms}}
  end

  @impl true
  def handle_call(:summary, _from, state) do
    now_ms = System.monotonic_time(:millisecond)

    elapsed_ms =
      case state.measured_started_at_ms do
        nil -> 0
        started -> max(now_ms - started, 0)
      end

    avg_rps = if elapsed_ms > 0, do: state.measured_requests * 1_000 / elapsed_ms, else: 0.0

    {:reply,
     %{id: state.id, node: node(), measured_requests: state.measured_requests, avg_rps: avg_rps}, state}
  end

  @impl true
  def terminate(_reason, state) do
    _ = flush_local_buffers(state)
    :ok
  end

  defp dispatch_requests(state, count) when count <= 0, do: state

  defp dispatch_requests(state, count) do
    Enum.reduce(1..count, state, fn _, acc -> dispatch_one_request(acc) end)
  end

  defp dispatch_one_request(state) do
    request_type = choose_request_type(state)
    {key, state_with_key} = choose_key_for_request(request_type, state)
    started_at_ms = System.system_time(:millisecond)
    parent = self()
    dispatched_mode = state.mode

    case Task.Supervisor.start_child(state.task_supervisor, fn ->
           {response_code, db_latency_us} =
             case state.adapter.request(request_type, state.payload_size, state.adapter_opts, key) do
               {:ok, status, latency_us} -> {status, latency_us}
               {:error, _reason, latency_us} -> {0, latency_us}
             end

           send(parent, {:request_result, request_type, key, started_at_ms, response_code, db_latency_us, dispatched_mode})
         end) do
      {:ok, _pid} ->
        %{state_with_key | inflight_count: state_with_key.inflight_count + 1}

      {:error, _reason} ->
        state_with_key
    end
  end

  defp local_record(state, metric, db_latency_us, response_code, dispatched_mode) do
    latency_us = max(db_latency_us, 0)
    bucket_100us = max(div(latency_us, 100), 0)
    is_error = response_code == 0 or response_code >= 400

    %{
      state
      | local_count: state.local_count + 1,
        local_errors: state.local_errors + if(is_error, do: 1, else: 0),
        local_durations_us: [latency_us | state.local_durations_us],
        local_total_duration_us: state.local_total_duration_us + latency_us,
        local_metrics: [metric | state.local_metrics],
        measured_requests:
          state.measured_requests + if(dispatched_mode == :measured, do: 1, else: 0),
        local_histogram_100us:
          Map.update(state.local_histogram_100us, bucket_100us, 1, &(&1 + 1))
    }
  end

  defp flush_local_buffers(state) do
    state
    |> flush_local_stats()
    |> flush_local_metrics()
  end

  defp flush_local_stats(%{local_count: 0} = state), do: state

  defp flush_local_stats(state) do
    MoyaSqueezer.StatsCollector.record_batch(state.stats_collector, %{
      count: state.local_count,
      errors: state.local_errors,
      durations_us: Enum.reverse(state.local_durations_us),
      total_duration_us: state.local_total_duration_us,
      histogram_100us: state.local_histogram_100us,
      worker_tick_count: state.local_tick_count,
      worker_tick_work_us: state.local_tick_work_us,
      worker_tick_overrun_count: state.local_tick_overrun_count,
      worker_max_requests_in_tick: state.local_max_requests_in_tick
    })

    %{
      state
      | local_count: 0,
        local_errors: 0,
        local_durations_us: [],
        local_total_duration_us: 0,
        local_histogram_100us: %{},
        local_tick_count: 0,
        local_tick_work_us: 0,
        local_tick_overrun_count: 0,
        local_max_requests_in_tick: 0
    }
  end

  defp flush_local_metrics(%{local_metrics: []} = state), do: state

  defp flush_local_metrics(state) do
    MoyaSqueezer.MetricsLogger.log_batch(state.logger, Enum.reverse(state.local_metrics))
    %{state | local_metrics: []}
  end

  defp choose_request_type(state) do
    if state.mode == :warmup do
      :write
    else
      choose_request_type_measured(state)
    end
  end

  defp choose_request_type_measured(state) do
    p = :rand.uniform()

    cond do
      p <= state.read_ratio -> :read
      p <= state.read_ratio + state.write_ratio -> :write
      true -> :delete
    end
  end

  defp choose_key_for_request(:write, state), do: next_local_new_key(state)

  defp choose_key_for_request(_type, state) do
    case random_local_existing_key(state) do
      {:ok, key} -> {key, state}
      :empty -> next_local_new_key(state)
    end
  end

  defp maybe_update_local_key_pool(:write, key, status, state) when status >= 200 and status < 300,
    do: note_local_write_success(state, key)

  defp maybe_update_local_key_pool(:delete, key, status, state) when status >= 200 and status < 300,
    do: note_local_delete_success(state, key)

  defp maybe_update_local_key_pool(_type, _key, _status, state), do: state

  defp random_local_existing_key(state) do
    case state.local_key_list do
      [] -> :empty
      list -> {:ok, Enum.at(list, :rand.uniform(length(list)) - 1)}
    end
  end

  defp next_local_new_key(state) do
    key = "k#{node()}_#{state.id}_#{state.local_next_id}"
    {key, %{state | local_next_id: state.local_next_id + 1}}
  end

  defp note_local_write_success(state, key) do
    if MapSet.member?(state.local_keys, key) do
      state
    else
      %{state | local_keys: MapSet.put(state.local_keys, key), local_key_list: [key | state.local_key_list]}
    end
  end

  defp note_local_delete_success(state, key) do
    if MapSet.member?(state.local_keys, key) do
      %{state | local_keys: MapSet.delete(state.local_keys, key), local_key_list: Enum.reject(state.local_key_list, &(&1 == key))}
    else
      state
    end
  end
end