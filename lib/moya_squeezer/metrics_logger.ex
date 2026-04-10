defmodule MoyaSqueezer.MetricsLogger do
  @moduledoc """
  Buffered append-only metrics writer.

  Flushes on a configurable interval (default 10ms) so high request rates do not
  perform one disk write per request.
  """

  use GenServer

  @default_flush_interval_ms 10

  @type metric :: %{
          request_type: :read | :write | :delete,
          source_node: String.t(),
          started_at_ms: integer(),
          db_latency_us: integer(),
          response_code: integer()
        }

  defstruct [:io_device, :buffer, :flush_interval_ms, :compact]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @spec log(pid() | atom(), metric()) :: :ok
  def log(server, metric), do: GenServer.cast(server, {:log, metric})

  @spec log_batch(pid() | atom(), [metric()]) :: :ok
  def log_batch(server, metrics) when is_list(metrics), do: GenServer.cast(server, {:log_batch, metrics})

  @impl true
  def init(opts) do
    log_path = Keyword.fetch!(opts, :log_path)
    flush_interval_ms = Keyword.get(opts, :flush_interval_ms, @default_flush_interval_ms)
    compact = Keyword.get(opts, :compact, true)

    if not (is_integer(flush_interval_ms) and flush_interval_ms > 0) do
      raise ArgumentError, "flush_interval_ms must be a positive integer"
    end

    if not is_boolean(compact) do
      raise ArgumentError, "compact must be a boolean"
    end

    File.mkdir_p!(Path.dirname(log_path))
    {:ok, io_device} = File.open(log_path, [:append, :utf8])

    :ok =
      IO.binwrite(
        io_device,
        csv_header(compact)
      )

    Process.send_after(self(), :flush, flush_interval_ms)

    {:ok, %__MODULE__{io_device: io_device, buffer: [], flush_interval_ms: flush_interval_ms, compact: compact}}
  end

  @impl true
  def handle_cast({:log, metric}, state) do
    {:noreply, %{state | buffer: [metric | state.buffer]}}
  end

  @impl true
  def handle_cast({:log_batch, metrics}, state) do
    {:noreply, %{state | buffer: Enum.reverse(metrics) ++ state.buffer}}
  end

  @impl true
  def handle_info(:flush, %{buffer: []} = state) do
    Process.send_after(self(), :flush, state.flush_interval_ms)
    {:noreply, state}
  end

  def handle_info(:flush, state) do
    payload = state.buffer |> Enum.reverse() |> buffer_to_payload(state)
    :ok = IO.binwrite(state.io_device, payload)
    Process.send_after(self(), :flush, state.flush_interval_ms)
    {:noreply, %{state | buffer: []}}
  end

  @impl true
  def terminate(_reason, state) do
    if state.buffer != [] do
      payload = state.buffer |> Enum.reverse() |> buffer_to_payload(state)
      :ok = IO.binwrite(state.io_device, payload)
    end

    File.close(state.io_device)
    :ok
  end

  defp csv_header(true),
    do: "bucket_ms,source_node,request_type,response_code,count,sum_db_latency_us\n"

  defp csv_header(false),
    do: "bucket_ms,source_node,request_type,started_at_ms,db_latency_us,response_code\n"

  defp buffer_to_payload(metrics, %{compact: true, flush_interval_ms: flush_interval_ms}) do
    metrics
    |> compact_groups(flush_interval_ms)
    |> Enum.map(fn {{bucket_ms, source_node, request_type, response_code}, {count, sum_latency_us}} ->
      [
        Integer.to_string(bucket_ms),
        ?,,
        source_node,
        ?,,
        Atom.to_string(request_type),
        ?,,
        Integer.to_string(response_code),
        ?,,
        Integer.to_string(count),
        ?,,
        Integer.to_string(sum_latency_us),
        ?\n
      ]
    end)
    |> IO.iodata_to_binary()
  end

  defp buffer_to_payload(metrics, %{compact: false, flush_interval_ms: flush_interval_ms}) do
    metrics
    |> Enum.map(&metric_to_csv(&1, flush_interval_ms))
    |> IO.iodata_to_binary()
  end

  defp compact_groups(metrics, flush_interval_ms) do
    Enum.reduce(metrics, %{}, fn metric, acc ->
      bucket_ms = metric.started_at_ms |> div(flush_interval_ms) |> Kernel.*(flush_interval_ms)
      key = {bucket_ms, metric.source_node, metric.request_type, metric.response_code}

      Map.update(acc, key, {1, max(metric.db_latency_us, 0)}, fn {count, sum} ->
        {count + 1, sum + max(metric.db_latency_us, 0)}
      end)
    end)
  end

  defp metric_to_csv(metric, flush_interval_ms) do
    bucket_ms = metric.started_at_ms |> div(flush_interval_ms) |> Kernel.*(flush_interval_ms)

    "#{bucket_ms},#{metric.source_node},#{metric.request_type},#{metric.started_at_ms}," <>
      "#{metric.db_latency_us},#{metric.response_code}\n"
  end
end
