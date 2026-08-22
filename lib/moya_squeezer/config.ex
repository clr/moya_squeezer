defmodule MoyaSqueezer.Config do
  @moduledoc """
  Parses and validates squeeze-test TOML configuration files.
  """

  @required_integer_fields ~w(connections_per_worker requests_per_second payload_size duration_seconds)a
  @optional_nonneg_integer_fields ~w(max_retries retry_backoff_ms warmup_seconds rps_step feel_the_burn_seconds payload_step_bytes)a
  @optional_positive_integer_fields ~w(request_timeout_ms step_interval_seconds baseline_window_seconds worker_tick_ms worker_inflight_limit metrics_flush_interval_ms stats_flush_interval_ms total_target_rps initial_active_workers worker_step worker_step_interval_seconds worker_container_pool max_active_workers latency_breach_consecutive_windows error_breach_consecutive_windows)a
  @required_ratio_fields ~w(read_ratio write_ratio delete_ratio)a

  @enforce_keys [
    :connections_per_worker,
    :requests_per_second,
    :read_ratio,
    :write_ratio,
    :delete_ratio,
    :payload_size,
    :duration_seconds,
    :warmup_seconds,
    :base_url,
    :log_path,
    :request_timeout_ms,
    :max_retries,
    :retry_backoff_ms,
    :start_requests_per_second,
    :rps_step,
    :step_interval_seconds,
    :baseline_window_seconds,
    :max_error_rate_pct,
    :error_breach_consecutive_windows,
    :stop_latency_percentile,
    :latency_breach_consecutive_windows,
    :feel_the_burn_seconds,
    :worker_tick_ms,
    :worker_inflight_limit,
    :metrics_flush_interval_ms,
    :metrics_compact,
    :stats_flush_interval_ms,
    :ramp_mode,
    :total_target_rps,
    :initial_active_workers,
    :worker_step,
    :worker_step_interval_seconds,
    :worker_container_pool,
    :payload_step_bytes
  ]
  defstruct [
    :connections_per_worker,
    :requests_per_second,
    :read_ratio,
    :write_ratio,
    :delete_ratio,
    :payload_size,
    :duration_seconds,
    :warmup_seconds,
    :base_url,
    :log_path,
    :request_timeout_ms,
    :max_retries,
    :retry_backoff_ms,
    :start_requests_per_second,
    :rps_step,
    :step_interval_seconds,
    :baseline_window_seconds,
    :max_error_rate_pct,
    :error_breach_consecutive_windows,
    :stop_latency_percentile,
    :latency_breach_consecutive_windows,
    :feel_the_burn_seconds,
    :worker_tick_ms,
    :worker_inflight_limit,
    :metrics_flush_interval_ms,
    :metrics_compact,
    :stats_flush_interval_ms,
    :ramp_mode,
    :total_target_rps,
    :initial_active_workers,
    :worker_step,
    :worker_step_interval_seconds,
    :worker_container_pool,
    :payload_step_bytes,
    read_path: "/db/v0.1",
    write_path: "/db/v0.1",
    delete_path: "/db/v0.1"
  ]

  @type t :: %__MODULE__{
          connections_per_worker: pos_integer(),
          requests_per_second: pos_integer(),
          read_ratio: float(),
          write_ratio: float(),
          delete_ratio: float(),
          payload_size: pos_integer(),
          duration_seconds: pos_integer(),
          warmup_seconds: non_neg_integer(),
          base_url: String.t(),
          log_path: String.t(),
          request_timeout_ms: pos_integer(),
          max_retries: non_neg_integer(),
          retry_backoff_ms: non_neg_integer(),
          start_requests_per_second: pos_integer(),
          rps_step: non_neg_integer(),
          step_interval_seconds: pos_integer(),
          baseline_window_seconds: pos_integer(),
          max_error_rate_pct: float(),
          error_breach_consecutive_windows: pos_integer(),
          stop_latency_percentile: float(),
          latency_breach_consecutive_windows: pos_integer(),
          feel_the_burn_seconds: non_neg_integer(),
          worker_tick_ms: pos_integer(),
          worker_inflight_limit: pos_integer(),
          metrics_flush_interval_ms: pos_integer(),
          metrics_compact: boolean(),
          stats_flush_interval_ms: pos_integer(),
          ramp_mode: :rps | :concurrency,
          total_target_rps: pos_integer(),
          initial_active_workers: pos_integer(),
          worker_step: pos_integer(),
          worker_step_interval_seconds: pos_integer(),
          worker_container_pool: pos_integer(),
          payload_step_bytes: non_neg_integer(),
          read_path: String.t(),
          write_path: String.t(),
          delete_path: String.t()
        }

  @spec from_toml_file(String.t()) :: {:ok, t()} | {:error, String.t()}
  def from_toml_file(path) do
    with {:ok, content} <- File.read(path),
         {:ok, decoded} <- Toml.decode(content),
         {:ok, config} <- from_map(decoded) do
      {:ok, config}
    else
      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, "failed to parse config: #{inspect(reason)}"}
    end
  end

  @spec from_map(map()) :: {:ok, t()} | {:error, String.t()}
  def from_map(map) when is_map(map) do
    with :ok <- validate_required_integer_fields(map),
         :ok <- validate_required_ratio_fields(map),
         :ok <- validate_optional_nonneg_integer_fields(map),
         :ok <- validate_optional_positive_integer_fields(map),
         :ok <- validate_optional_boolean_fields(map),
         :ok <- validate_optional_percentile_fields(map),
         :ok <- validate_ramp_mode(map),
         :ok <- validate_ratios_sum(map) do
      {:ok,
       %__MODULE__{
         connections_per_worker: fetch_required(map, :connections_per_worker),
         requests_per_second: fetch_required(map, :requests_per_second),
         read_ratio: ratio(fetch_required(map, :read_ratio)),
         write_ratio: ratio(fetch_required(map, :write_ratio)),
         delete_ratio: ratio(fetch_required(map, :delete_ratio)),
         payload_size: fetch_required(map, :payload_size),
         duration_seconds: fetch_required(map, :duration_seconds),
         warmup_seconds: fetch_optional(map, :warmup_seconds, 0),
         base_url: fetch_optional(map, :base_url, "http://localhost:9000"),
         log_path: fetch_optional(map, :log_path, "squeeze_metrics.log"),
         request_timeout_ms: fetch_optional(map, :request_timeout_ms, 5_000),
         max_retries: fetch_optional(map, :max_retries, 0),
         retry_backoff_ms: fetch_optional(map, :retry_backoff_ms, 25),
         start_requests_per_second:
           fetch_optional(map, :start_requests_per_second, fetch_required(map, :requests_per_second)),
         rps_step: fetch_optional(map, :rps_step, 0),
         step_interval_seconds: fetch_optional(map, :step_interval_seconds, 5),
         baseline_window_seconds: fetch_optional(map, :baseline_window_seconds, 10),
         max_error_rate_pct: ratio(fetch_optional(map, :max_error_rate_pct, 1.0)),
         error_breach_consecutive_windows:
           fetch_optional(map, :error_breach_consecutive_windows, 1),
         stop_latency_percentile: ratio(fetch_optional(map, :stop_latency_percentile, 0.90)),
         latency_breach_consecutive_windows:
           fetch_optional(map, :latency_breach_consecutive_windows, 1),
          feel_the_burn_seconds: fetch_optional(map, :feel_the_burn_seconds, 0),
         worker_tick_ms: fetch_optional(map, :worker_tick_ms, 10),
         worker_inflight_limit: fetch_optional(map, :worker_inflight_limit, 1),
         metrics_flush_interval_ms: fetch_optional(map, :metrics_flush_interval_ms, 10),
         metrics_compact: fetch_optional(map, :metrics_compact, true),
         stats_flush_interval_ms: fetch_optional(map, :stats_flush_interval_ms, 100),
         ramp_mode: parse_ramp_mode(fetch_optional(map, :ramp_mode, "rps")),
         total_target_rps: fetch_optional(map, :total_target_rps, fetch_required(map, :requests_per_second)),
         initial_active_workers: fetch_optional(map, :initial_active_workers, 1),
         worker_step: fetch_optional(map, :worker_step, 1),
         worker_step_interval_seconds: fetch_optional(map, :worker_step_interval_seconds, fetch_optional(map, :step_interval_seconds, 5)),
         worker_container_pool:
           fetch_optional(
             map,
             :worker_container_pool,
             fetch_optional(map, :max_active_workers, fetch_required(map, :connections_per_worker))
           ),
         payload_step_bytes: fetch_optional(map, :payload_step_bytes, 1024),
         read_path: fetch_optional(map, :read_path, "/db/v0.1"),
         write_path: fetch_optional(map, :write_path, "/db/v0.1"),
         delete_path: fetch_optional(map, :delete_path, "/db/v0.1")
       }}
    end
  end

  defp validate_optional_boolean_fields(map) do
    value = fetch_optional(map, :metrics_compact, true)

    if is_boolean(value) do
      :ok
    else
      {:error, "optional field must be a boolean: metrics_compact"}
    end
  end

  defp validate_optional_percentile_fields(map) do
    value = ratio(fetch_optional(map, :stop_latency_percentile, 0.90))

    cond do
      not is_float(value) ->
        {:error, "optional field must be a float: stop_latency_percentile"}

      value <= 0.0 or value > 1.0 ->
        {:error, "optional field must be > 0.0 and <= 1.0: stop_latency_percentile"}

      true ->
        :ok
    end
  end

  defp validate_required_integer_fields(map) do
    Enum.reduce_while(@required_integer_fields, :ok, fn field, _acc ->
      value = fetch_required(map, field)

      cond do
        not is_integer(value) ->
          {:halt, {:error, "missing or invalid integer field: #{field}"}}

        value <= 0 ->
          {:halt, {:error, "field must be > 0: #{field}"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp validate_required_ratio_fields(map) do
    Enum.reduce_while(@required_ratio_fields, :ok, fn field, _acc ->
      value = fetch_required(map, field)

      cond do
        not (is_integer(value) or is_float(value)) ->
          {:halt, {:error, "missing or invalid ratio field: #{field}"}}

        value < 0 ->
          {:halt, {:error, "ratio must be >= 0: #{field}"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp validate_ratios_sum(map) do
    sum =
      ratio(fetch_required(map, :read_ratio)) +
        ratio(fetch_required(map, :write_ratio)) +
        ratio(fetch_required(map, :delete_ratio))

    if abs(sum - 1.0) <= 0.0001 do
      :ok
    else
      {:error, "read/write/delete ratios must sum to 1.0"}
    end
  end

  defp validate_optional_nonneg_integer_fields(map) do
    Enum.reduce_while(@optional_nonneg_integer_fields, :ok, fn field, _acc ->
      value = fetch_optional(map, field, 0)

      cond do
        not is_integer(value) ->
          {:halt, {:error, "optional field must be an integer: #{field}"}}

        value < 0 ->
          {:halt, {:error, "optional field must be >= 0: #{field}"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp validate_optional_positive_integer_fields(map) do
    Enum.reduce_while(@optional_positive_integer_fields, :ok, fn field, _acc ->
      value = fetch_optional(map, field, 1)

      cond do
        not is_integer(value) ->
          {:halt, {:error, "optional field must be an integer: #{field}"}}

        value <= 0 ->
          {:halt, {:error, "optional field must be > 0: #{field}"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp validate_ramp_mode(map) do
    case parse_ramp_mode(fetch_optional(map, :ramp_mode, "rps")) do
      mode when mode in [:rps, :concurrency, :payload] -> :ok
      _ -> {:error, "ramp_mode must be one of: rps, concurrency, payload"}
    end
  end

  defp parse_ramp_mode(value) when value in [:rps, "rps"], do: :rps
  defp parse_ramp_mode(value) when value in [:concurrency, "concurrency"], do: :concurrency
  defp parse_ramp_mode(value) when value in [:payload, "payload"], do: :payload
  defp parse_ramp_mode(_), do: :invalid

  defp ratio(value) when is_integer(value), do: value / 1
  defp ratio(value) when is_float(value), do: value

  defp fetch_required(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        Map.get(map, Atom.to_string(key))
    end
  end

  defp fetch_optional(map, key, default) do
    case fetch_required(map, key) do
      nil -> default
      value -> value
    end
  end
end
