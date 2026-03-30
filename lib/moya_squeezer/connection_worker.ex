defmodule MoyaSqueezer.ConnectionWorker do
  @moduledoc """
  Represents one logical client connection that continuously emits load.
  """

  use GenServer

  @tick_ms 10

  defstruct [
    :id,
    :adapter,
    :adapter_opts,
    :logger,
    :stats_collector,
    :payload_size,
    :reqs_per_sec,
    :read_ratio,
    :write_ratio,
    :delete_ratio,
    :key_pool,
    :mode,
    token_balance: 0.0
  ]

  @type options :: [
          id: pos_integer(),
          adapter: module(),
          adapter_opts: map(),
          logger: pid() | atom(),
          stats_collector: pid() | atom(),
          payload_size: pos_integer(),
          reqs_per_sec: float(),
          read_ratio: float(),
          write_ratio: float(),
          delete_ratio: float(),
          key_pool: pid() | atom(),
          mode: :warmup | :measured
        ]

  @spec start_link(options()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec set_reqs_per_sec(pid() | atom(), float()) :: :ok
  def set_reqs_per_sec(worker, reqs_per_sec), do: GenServer.cast(worker, {:set_reqs_per_sec, reqs_per_sec})

  @spec set_mode(pid() | atom(), :warmup | :measured) :: :ok
  def set_mode(worker, mode) when mode in [:warmup, :measured], do: GenServer.cast(worker, {:set_mode, mode})

  @impl true
  def init(opts) do
    state = %__MODULE__{
      id: Keyword.fetch!(opts, :id),
      adapter: Keyword.fetch!(opts, :adapter),
      adapter_opts: Keyword.fetch!(opts, :adapter_opts),
      logger: Keyword.fetch!(opts, :logger),
      stats_collector: Keyword.fetch!(opts, :stats_collector),
      payload_size: Keyword.fetch!(opts, :payload_size),
      reqs_per_sec: Keyword.fetch!(opts, :reqs_per_sec),
      read_ratio: Keyword.fetch!(opts, :read_ratio),
      write_ratio: Keyword.fetch!(opts, :write_ratio),
      delete_ratio: Keyword.fetch!(opts, :delete_ratio),
      key_pool: Keyword.fetch!(opts, :key_pool),
      mode: Keyword.get(opts, :mode, :measured)
    }

    Process.send_after(self(), :tick, @tick_ms)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    token_balance = state.token_balance + state.reqs_per_sec * (@tick_ms / 1000)
    requests_to_send = trunc(token_balance)
    remaining = token_balance - requests_to_send

    if requests_to_send > 0 do
      Enum.each(1..requests_to_send, fn _ -> send_one_request(state) end)
    end

    Process.send_after(self(), :tick, @tick_ms)
    {:noreply, %{state | token_balance: remaining}}
  end

  @impl true
  def handle_cast({:set_reqs_per_sec, reqs_per_sec}, state) when is_number(reqs_per_sec) do
    {:noreply, %{state | reqs_per_sec: reqs_per_sec / 1}}
  end

  @impl true
  def handle_cast({:set_mode, mode}, state) when mode in [:warmup, :measured] do
    {:noreply, %{state | mode: mode}}
  end

  defp send_one_request(state) do
    request_type = choose_request_type(state)
    key = choose_key_for_request(request_type, state)
    started_at_ms = System.system_time(:millisecond)

    {response_code, db_latency_us} =
      case state.adapter.request(request_type, state.payload_size, state.adapter_opts, key) do
        {:ok, status, latency_us} -> {status, latency_us}
        {:error, _reason, latency_us} -> {0, latency_us}
      end

    maybe_update_key_pool(request_type, key, response_code, state)

    MoyaSqueezer.MetricsLogger.log(state.logger, %{
      source_node: Atom.to_string(node()),
      request_type: request_type,
      started_at_ms: started_at_ms,
      db_latency_us: db_latency_us,
      response_code: response_code
    })

    MoyaSqueezer.StatsCollector.record(state.stats_collector, %{
      request_type: request_type,
      started_at_ms: started_at_ms,
      db_latency_us: db_latency_us,
      response_code: response_code
    })
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
      p <= state.read_ratio ->
        :read

      p <= state.read_ratio + state.write_ratio ->
        :write

      true ->
        :delete
    end
  end

  defp choose_key_for_request(:write, state), do: safe_next_new_key(state)

  defp choose_key_for_request(_type, state) do
    case safe_random_existing_key(state) do
      {:ok, key} -> key
      :empty -> safe_next_new_key(state)
    end
  end

  defp maybe_update_key_pool(:write, key, status, state) when status >= 200 and status < 300 do
    safe_note_write_success(state, key)
  end

  defp maybe_update_key_pool(:delete, key, status, state) when status >= 200 and status < 300 do
    safe_note_delete_success(state, key)
  end

  defp maybe_update_key_pool(_type, _key, _status, _state), do: :ok

  defp safe_random_existing_key(state) do
    try do
      MoyaSqueezer.KeyPool.random_existing_key(state.key_pool)
    catch
      :exit, _ -> :empty
    end
  end

  defp safe_next_new_key(state) do
    try do
      MoyaSqueezer.KeyPool.next_new_key(state.key_pool)
    catch
      :exit, _ -> fallback_key(state)
    end
  end

  defp safe_note_write_success(state, key) do
    try do
      MoyaSqueezer.KeyPool.note_write_success(state.key_pool, key)
    catch
      :exit, _ -> :ok
    end
  end

  defp safe_note_delete_success(state, key) do
    try do
      MoyaSqueezer.KeyPool.note_delete_success(state.key_pool, key)
    catch
      :exit, _ -> :ok
    end
  end

  defp fallback_key(state) do
    "fallback_#{node()}_#{state.id}_#{System.unique_integer([:positive])}"
  end
end
