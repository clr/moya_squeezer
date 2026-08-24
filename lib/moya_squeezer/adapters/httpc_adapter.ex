defmodule MoyaSqueezer.Adapters.HttpcAdapter do
  @moduledoc """
  Lightweight adapter using Erlang :httpc (inets) for read/write/delete calls.
  """

  @behaviour MoyaSqueezer.LoadAdapter

  alias MoyaSqueezer.Adapters.RequestRetrier

  @impl true
  def request(type, payload_size, adapter_opts, key_override \\ nil) do
    _ = ensure_httpc_started()

    base_url = Map.fetch!(adapter_opts, :base_url)
    key = key_override || Integer.to_string(System.unique_integer([:positive, :monotonic]))
    payload = payload(payload_size)
    timeout_ms = Map.get(adapter_opts, :request_timeout_ms, 5_000)
    max_retries = Map.get(adapter_opts, :max_retries, 0)
    retry_backoff_ms = Map.get(adapter_opts, :retry_backoff_ms, 25)

    {method, url, body, headers} =
      case type do
        :read ->
          path = Map.get(adapter_opts, :read_path, "/db/v0.1")
          {:get, "#{base_url}#{path}/#{key}", ~c"", []}

        :write ->
          path = Map.get(adapter_opts, :write_path, "/db/v0.1")
          {:post, "#{base_url}#{path}/#{key}", payload, [{~c"content-type", ~c"application/json"}]}

        :delete ->
          path = Map.get(adapter_opts, :delete_path, "/db/v0.1")
          {:delete, "#{base_url}#{path}/#{key}", ~c"", []}
      end

    send_fun = fn -> safe_httpc_request(method, url, headers, body, timeout_ms) end
    RequestRetrier.run(send_fun, max_retries, retry_backoff_ms)
  end

  defp safe_httpc_request(method, url, headers, body, timeout_ms) do
    request =
      case method do
        :get -> {String.to_charlist(url), headers}
        :delete -> {String.to_charlist(url), headers}
        :post -> {String.to_charlist(url), headers, ~c"application/json", body}
      end

    opts = [timeout: timeout_ms]

    try do
      case :httpc.request(method, request, opts, []) do
        {:ok, {{_http, status, _reason}, _resp_headers, _resp_body}} -> {:ok, status}
        {:error, reason} -> {:error, reason}
      end
    rescue
      exception -> {:error, {:exception, exception}}
    catch
      :exit, reason -> {:error, {:exit, reason}}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp ensure_httpc_started do
    _ = Application.ensure_all_started(:inets)
    _ = Application.ensure_all_started(:ssl)
    :ok
  end

  defp payload(size) do
    to_charlist("\"" <> :binary.copy("x", max(size, 1)) <> "\"")
  end
end