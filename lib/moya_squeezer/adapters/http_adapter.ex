defmodule MoyaSqueezer.Adapters.HttpAdapter do
  @moduledoc """
  Default adapter that sends read/write/delete calls over HTTP.
  """

  @behaviour MoyaSqueezer.LoadAdapter

  alias MoyaSqueezer.Adapters.RequestRetrier

  @impl true
  def request(type, payload_size, adapter_opts, key_override \\ nil) do
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
          {:get, "#{base_url}#{path}/#{key}", "", []}

        :write ->
          path = Map.get(adapter_opts, :write_path, "/db/v0.1")
          {:post, "#{base_url}#{path}/#{key}", payload, [{"content-type", "application/json"}]}

        :delete ->
          path = Map.get(adapter_opts, :delete_path, "/db/v0.1")
          {:delete, "#{base_url}#{path}/#{key}", "", []}
      end

    send_fun = fn -> safe_finch_request(method, url, headers, body, timeout_ms) end
    RequestRetrier.run(send_fun, max_retries, retry_backoff_ms)
  end

  defp safe_finch_request(method, url, headers, body, timeout_ms) do
    try do
      case method
           |> Finch.build(url, headers, body)
           |> Finch.request(MoyaSqueezerFinch, receive_timeout: timeout_ms) do
        {:ok, %Finch.Response{status: status}} -> {:ok, status}
        {:error, reason} -> {:error, reason}
      end
    rescue
      exception -> {:error, {:exception, exception}}
    catch
      :exit, reason -> {:error, {:exit, reason}}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp payload(size) do
    "\"" <> :binary.copy("x", max(size, 1)) <> "\""
  end
end
