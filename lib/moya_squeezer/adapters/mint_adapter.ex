defmodule MoyaSqueezer.Adapters.MintAdapter do
  @moduledoc """
  Load-generating connection worker that owns a single persistent Mint HTTP
  connection for its whole lifetime, instead of issuing requests through a
  pooled client like Finch.

  Unlike `MoyaSqueezer.LoadAdapter` implementations (`HttpAdapter`,
  `HttpcAdapter`), this module is not a stateless per-request callback — Mint
  connections are owned by a single process and cannot be shared, so this
  process *is* the logical connection. It is started directly in place of
  `MoyaSqueezer.Adapters.ConnectionWorker` (see `MoyaSqueezer.Runner`) and
  exposes the same public API so the manager can control it identically.

  HTTP/1.1 has no safe pipelining in Mint, so at most one request is ever
  in flight on the connection at a time; concurrency comes from running more
  of these processes (i.e. more real connections), not more in-flight
  requests per connection.
  """

  use GenServer

  alias MoyaSqueezer.MetricsLogger
  alias MoyaSqueezer.RuntimeState
  alias MoyaSqueezer.StatsCollector

  @default_tick_ms 10
  @default_stats_flush_interval_ms 100
  @default_request_timeout_ms 5_000
  @default_retry_backoff_ms 25

  defstruct [
    :id,
    :adapter_opts,
    :logger,
    :stats_collector,
    :payload_size,
    :reqs_per_sec,
    :read_ratio,
    :write_ratio,
    :delete_ratio,
    :mode,
    :tick_ms,
    :stats_flush_interval_ms,
    :scheme,
    :host,
    :port,
    conn: nil,
    conn_status: :disconnected,
    in_flight: %{},
    token_balance: 0.0,
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
          adapter_opts: map(),
          logger: pid() | atom(),
          stats_collector: pid() | atom(),
          payload_size: pos_integer(),
          reqs_per_sec: float(),
          read_ratio: float(),
          write_ratio: float(),
          delete_ratio: float(),
          tick_ms: pos_integer(),
          stats_flush_interval_ms: pos_integer(),
          mode: :warmup | :measured
        ]

  @spec start_link(options()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec set_reqs_per_sec(pid() | atom(), float()) :: :ok
  def set_reqs_per_sec(worker, reqs_per_sec), do: GenServer.cast(worker, {:set_reqs_per_sec, reqs_per_sec})

  @spec set_payload_size(pid() | atom(), pos_integer()) :: :ok
  def set_payload_size(worker, payload_size), do: GenServer.cast(worker, {:set_payload_size, payload_size})

  @spec set_mode(pid() | atom(), :warmup | :measured) :: :ok
  def set_mode(worker, mode) when mode in [:warmup, :measured], do: GenServer.cast(worker, {:set_mode, mode})

  @spec summary(pid() | atom()) :: map()
  def summary(worker), do: GenServer.call(worker, :summary, 10_000)

  @spec export_keyspace(pid() | atom()) :: [String.t()]
  def export_keyspace(worker), do: GenServer.call(worker, :export_keyspace, 10_000)

  @spec import_keyspace(pid() | atom(), [String.t()]) :: :ok
  def import_keyspace(worker, keys) when is_list(keys), do: GenServer.call(worker, {:import_keyspace, keys}, 10_000)

  @impl true
  def init(opts) do
    adapter_opts = Keyword.fetch!(opts, :adapter_opts)
    uri = URI.parse(Map.fetch!(adapter_opts, :base_url))
    scheme = if uri.scheme == "https", do: :https, else: :http
    mode = Keyword.get(opts, :mode, :measured)

    state = %__MODULE__{
      id: Keyword.fetch!(opts, :id),
      adapter_opts: adapter_opts,
      logger: Keyword.fetch!(opts, :logger),
      stats_collector: Keyword.fetch!(opts, :stats_collector),
      payload_size: Keyword.fetch!(opts, :payload_size),
      reqs_per_sec: Keyword.fetch!(opts, :reqs_per_sec),
      read_ratio: Keyword.fetch!(opts, :read_ratio),
      write_ratio: Keyword.fetch!(opts, :write_ratio),
      delete_ratio: Keyword.fetch!(opts, :delete_ratio),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      stats_flush_interval_ms:
        Keyword.get(opts, :stats_flush_interval_ms, @default_stats_flush_interval_ms),
      mode: mode,
      scheme: scheme,
      host: uri.host,
      port: uri.port || default_port(scheme),
      measured_started_at_ms: if(mode == :measured, do: System.monotonic_time(:millisecond), else: nil)
    }

    Process.send_after(self(), :tick, state.tick_ms)
    Process.send_after(self(), :flush_stats, state.stats_flush_interval_ms)
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case Mint.HTTP.connect(state.scheme, state.host, state.port) do
      {:ok, conn} -> {:noreply, %{state | conn: conn, conn_status: :connected}}
      {:error, _reason} -> {:noreply, schedule_reconnect(state)}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    tick_started_us = System.monotonic_time(:microsecond)
    token_balance = state.token_balance + state.reqs_per_sec * (state.tick_ms / 1000)
    requests_ready = trunc(token_balance)
    slot_available? = state.conn_status == :connected and map_size(state.in_flight) == 0
    requests_to_dispatch = if slot_available?, do: min(requests_ready, 1), else: 0

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
  def handle_info(:reconnect, state) do
    case Mint.HTTP.connect(state.scheme, state.host, state.port) do
      {:ok, conn} -> {:noreply, %{state | conn: conn, conn_status: :connected}}
      {:error, _reason} -> {:noreply, schedule_reconnect(state)}
    end
  end

  @impl true
  def handle_info({:request_timeout, ref}, state) do
    if Map.has_key?(state.in_flight, ref) do
      {:noreply, state |> finalize_request(ref, :error) |> disconnect()}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(message, %{conn: conn} = state) when not is_nil(conn) do
    case Mint.HTTP.stream(conn, message) do
      :unknown ->
        {:noreply, state}

      {:ok, conn, responses} ->
        {:noreply, Enum.reduce(responses, %{state | conn: conn}, &apply_response/2)}

      {:error, conn, _reason, responses} ->
        state = Enum.reduce(responses, %{state | conn: conn}, &apply_response/2)
        {:noreply, disconnect(state)}
    end
  end

  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_cast({:set_reqs_per_sec, reqs_per_sec}, state) when is_number(reqs_per_sec) do
    {:noreply, %{state | reqs_per_sec: reqs_per_sec / 1}}
  end

  @impl true
  def handle_cast({:set_payload_size, payload_size}, state) when is_integer(payload_size) and payload_size > 0 do
    {:noreply, %{state | payload_size: payload_size}}
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
  def handle_call(:export_keyspace, _from, state) do
    {:reply, state.local_key_list, state}
  end

  @impl true
  def handle_call({:import_keyspace, keys}, _from, state) do
    imported =
      Enum.reduce(keys, state.local_keys, fn key, acc ->
        if is_binary(key), do: MapSet.put(acc, key), else: acc
      end)

    {:reply, :ok, %{state | local_keys: imported, local_key_list: MapSet.to_list(imported)}}
  end

  @impl true
  def terminate(_reason, state) do
    if state.conn, do: Mint.HTTP.close(state.conn)
    _ = flush_local_buffers(state)
    :ok
  end

  defp default_port(:https), do: 443
  defp default_port(:http), do: 80

  defp schedule_reconnect(state) do
    backoff_ms = Map.get(state.adapter_opts, :retry_backoff_ms, @default_retry_backoff_ms)
    Process.send_after(self(), :reconnect, max(backoff_ms, @default_retry_backoff_ms))
    %{state | conn: nil, conn_status: :disconnected}
  end

  defp disconnect(state) do
    if state.conn, do: Mint.HTTP.close(state.conn)
    schedule_reconnect(state)
  end

  defp dispatch_requests(state, count) when count <= 0, do: state
  defp dispatch_requests(state, count), do: Enum.reduce(1..count, state, fn _, acc -> dispatch_one_request(acc) end)

  defp dispatch_one_request(state) do
    request_type = choose_request_type(state)
    {key, state_with_key} = choose_key_for_request(request_type, state)
    {method, path, body, headers} = build_request(request_type, key, state_with_key)
    started_at_ms = System.system_time(:millisecond)
    started_at_us = System.monotonic_time(:microsecond)

    case Mint.HTTP.request(state_with_key.conn, method, path, headers, body) do
      {:ok, conn, ref} ->
        timeout_ms = Map.get(state_with_key.adapter_opts, :request_timeout_ms, @default_request_timeout_ms)
        timer_ref = Process.send_after(self(), {:request_timeout, ref}, timeout_ms)

        entry = %{
          request_type: request_type,
          key: key,
          mode: state_with_key.mode,
          started_at_ms: started_at_ms,
          started_at_us: started_at_us,
          timer_ref: timer_ref,
          status: nil
        }

        %{state_with_key | conn: conn, in_flight: Map.put(state_with_key.in_flight, ref, entry)}

      {:error, conn, _reason} ->
        %{state_with_key | conn: conn}
        |> record_result(request_type, key, started_at_ms, started_at_us, 0, state_with_key.mode)
        |> disconnect()
    end
  end

  defp build_request(:read, key, state) do
    path = Map.get(state.adapter_opts, :read_path, "/db/v0.1")
    {"GET", "#{path}/#{key}", nil, []}
  end

  defp build_request(:write, key, state) do
    path = Map.get(state.adapter_opts, :write_path, "/db/v0.1")
    {"POST", "#{path}/#{key}", payload(state.payload_size), [{"content-type", "application/json"}]}
  end

  defp build_request(:delete, key, state) do
    path = Map.get(state.adapter_opts, :delete_path, "/db/v0.1")
    {"DELETE", "#{path}/#{key}", nil, []}
  end

  defp payload(size), do: "\"" <> :binary.copy("x", max(size, 1)) <> "\""

  defp apply_response({:status, ref, status}, state), do: update_in_flight(state, ref, &Map.put(&1, :status, status))
  defp apply_response({:headers, _ref, _headers}, state), do: state
  defp apply_response({:data, _ref, _data}, state), do: state
  defp apply_response({:done, ref}, state), do: finalize_request(state, ref, :ok)
  defp apply_response({:error, ref, _reason}, state), do: finalize_request(state, ref, :error)
  defp apply_response(_other, state), do: state

  defp update_in_flight(state, ref, fun) do
    case Map.fetch(state.in_flight, ref) do
      {:ok, entry} -> %{state | in_flight: Map.put(state.in_flight, ref, fun.(entry))}
      :error -> state
    end
  end

  defp finalize_request(state, ref, outcome) do
    case Map.pop(state.in_flight, ref) do
      {nil, in_flight} ->
        %{state | in_flight: in_flight}

      {entry, in_flight} ->
        Process.cancel_timer(entry.timer_ref)
        response_code = if outcome == :ok, do: entry.status || 0, else: 0

        %{state | in_flight: in_flight}
        |> record_result(entry.request_type, entry.key, entry.started_at_ms, entry.started_at_us, response_code, entry.mode)
    end
  end

  defp record_result(state, request_type, key, started_at_ms, started_at_us, response_code, mode) do
    db_latency_us = max(System.monotonic_time(:microsecond) - started_at_us, 0)
    RuntimeState.record_worker_response(response_code)

    metric = %{
      source_node: Atom.to_string(node()),
      request_type: request_type,
      started_at_ms: started_at_ms,
      db_latency_us: db_latency_us,
      response_code: response_code
    }

    state
    |> maybe_update_local_key_pool(request_type, key, response_code)
    |> local_record(metric, db_latency_us, response_code, mode)
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
    StatsCollector.record_batch(state.stats_collector, %{
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
    MetricsLogger.log_batch(state.logger, Enum.reverse(state.local_metrics))
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

  defp maybe_update_local_key_pool(state, :write, key, status) when status >= 200 and status < 300,
    do: note_local_write_success(state, key)

  defp maybe_update_local_key_pool(state, :delete, key, status) when status >= 200 and status < 300,
    do: note_local_delete_success(state, key)

  defp maybe_update_local_key_pool(state, _type, _key, _status), do: state

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
