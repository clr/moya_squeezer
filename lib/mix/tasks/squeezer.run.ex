defmodule Mix.Tasks.Squeezer.Run do
  @moduledoc """
  Runs the squeeze-test routine in manager or worker mode.

      # Manager mode (coordinates test)
      mix squeezer.run config/local.toml --role manager --worker worker1@host --worker worker2@host

      # Worker mode (joins manager and waits for RPC)
      mix squeezer.run --role worker --manager manager@host
  """

  use Mix.Task

  @shortdoc "Run manager/worker squeeze test node"

  @switches [role: :string, manager: :string, worker: :keep, adapter: :string]

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    {opts, positional, _invalid} = OptionParser.parse(args, strict: @switches)
    maybe_set_adapter(opts)
    role = opts[:role] || "manager"

    case role do
      "manager" -> run_manager(positional, opts)
      "worker" -> run_worker(opts)
      other -> Mix.raise("Unknown role '#{other}'. Expected 'manager' or 'worker'.")
    end
  end

  defp run_manager([config_path], opts) do
    worker_nodes =
      opts
      |> Keyword.get_values(:worker)
      |> Enum.map(&String.to_atom/1)

    case MoyaSqueezer.run(config_path, worker_nodes: worker_nodes) do
      :ok -> Mix.shell().info("Squeeze test completed.")
      {:error, reason} -> Mix.raise("Squeeze test failed: #{reason}")
    end
  end

  defp run_manager(_args, _opts) do
    Mix.raise("Usage: mix squeezer.run <config.toml> [--role manager] [--worker name@host ...]")
  end

  defp run_worker(opts) do
    manager = opts[:manager] || Mix.raise("Usage: mix squeezer.run --role worker --manager name@host")

    case MoyaSqueezer.run_worker(String.to_atom(manager)) do
      :ok -> Mix.shell().info("Worker stopped.")
      {:error, reason} -> Mix.raise("Worker failed: #{reason}")
    end
  end

  defp maybe_set_adapter(opts) do
    case opts[:adapter] do
      nil -> :ok
      "finch" -> Application.put_env(:moya_squeezer, :load_adapter, MoyaSqueezer.Adapters.HttpAdapter)
      "httpc" -> Application.put_env(:moya_squeezer, :load_adapter, MoyaSqueezer.Adapters.HttpcAdapter)
      other -> Mix.raise("Unknown adapter '#{other}'. Expected 'finch' or 'httpc'.")
    end
  end
end
