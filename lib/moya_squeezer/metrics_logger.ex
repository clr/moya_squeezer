defmodule MoyaSqueezer.MetricsLogger do
  @moduledoc """
  Buffered append-only metrics writer.

  Flushes every 5ms so high request rates do not perform one disk write per request.
  """

  use GenServer

  @flush_interval_ms 5

  @type metric :: %{
          request_type: :read | :write | :delete,
          source_node: String.t(),
          started_at_ms: integer(),
          db_latency_us: integer(),
          response_code: integer()
        }

  defstruct [:io_device, :buffer]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @spec log(pid() | atom(), metric()) :: :ok
  def log(server, metric), do: GenServer.cast(server, {:log, metric})

  @impl true
  def init(opts) do
    log_path = Keyword.fetch!(opts, :log_path)
    File.mkdir_p!(Path.dirname(log_path))
    {:ok, io_device} = File.open(log_path, [:append, :utf8])

    :ok =
      IO.binwrite(
        io_device,
        "bucket_ms,source_node,request_type,started_at_ms,db_latency_us,response_code\n"
      )

    Process.send_after(self(), :flush, @flush_interval_ms)

    {:ok, %__MODULE__{io_device: io_device, buffer: []}}
  end

  @impl true
  def handle_cast({:log, metric}, state) do
    line = metric_to_csv(metric)
    {:noreply, %{state | buffer: [line | state.buffer]}}
  end

  @impl true
  def handle_info(:flush, %{buffer: []} = state) do
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:noreply, state}
  end

  def handle_info(:flush, state) do
    payload = state.buffer |> Enum.reverse() |> IO.iodata_to_binary()
    :ok = IO.binwrite(state.io_device, payload)
    Process.send_after(self(), :flush, @flush_interval_ms)
    {:noreply, %{state | buffer: []}}
  end

  @impl true
  def terminate(_reason, state) do
    if state.buffer != [] do
      payload = state.buffer |> Enum.reverse() |> IO.iodata_to_binary()
      :ok = IO.binwrite(state.io_device, payload)
    end

    File.close(state.io_device)
    :ok
  end

  defp metric_to_csv(metric) do
    bucket_ms = metric.started_at_ms |> div(@flush_interval_ms) |> Kernel.*(@flush_interval_ms)

    "#{bucket_ms},#{metric.source_node},#{metric.request_type},#{metric.started_at_ms}," <>
      "#{metric.db_latency_us},#{metric.response_code}\n"
  end
end
