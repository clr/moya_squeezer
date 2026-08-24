defmodule MoyaSqueezer.Adapters.RequestRetrier do
  @moduledoc """
  Shared retry/backoff/timing loop used by the HTTP-based load adapters.

  Each adapter supplies a `send_fun` that performs one attempt and returns
  `{:ok, status}` or `{:error, reason}`; this module handles retrying on
  5xx/error responses, backoff, and measuring latency.
  """

  @doc """
  Runs `send_fun.()` and retries it (with backoff) on 5xx status codes or
  errors, up to `max_retries` times. Returns `{:ok, status, latency_us}` or
  `{:error, reason, latency_us}`.
  """
  def run(send_fun, max_retries, retry_backoff_ms) do
    do_run(send_fun, max_retries, retry_backoff_ms, 0)
  end

  defp do_run(send_fun, max_retries, retry_backoff_ms, attempt) do
    started_us = System.monotonic_time(:microsecond)
    result = send_fun.()
    db_latency_us = System.monotonic_time(:microsecond) - started_us

    case result do
      {:ok, status} when status >= 500 and attempt < max_retries ->
        backoff_sleep(retry_backoff_ms, attempt)
        do_run(send_fun, max_retries, retry_backoff_ms, attempt + 1)

      {:ok, status} ->
        {:ok, status, db_latency_us}

      {:error, _reason} when attempt < max_retries ->
        backoff_sleep(retry_backoff_ms, attempt)
        do_run(send_fun, max_retries, retry_backoff_ms, attempt + 1)

      {:error, reason} ->
        {:error, reason, db_latency_us}
    end
  end

  defp backoff_sleep(backoff_ms, attempt) do
    Process.sleep(backoff_ms * (attempt + 1))
  end
end
