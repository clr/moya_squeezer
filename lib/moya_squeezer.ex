defmodule MoyaSqueezer do
  @moduledoc """
  Entry point helpers for running squeeze tests.
  """

  alias MoyaSqueezer.Runner

  @spec run(String.t(), keyword()) :: :ok | {:error, term()}
  def run(config_path, opts \\ []), do: Runner.run_from_file(config_path, opts)

  @spec run_worker(node()) :: :ok | {:error, term()}
  def run_worker(manager_node) do
    cond do
      not Node.alive?() ->
        {:error, "worker node is not distributed; start with --sname/--name and --cookie"}

      Node.connect(manager_node) and Node.ping(manager_node) == :pong ->
        IO.puts("[worker] connected to manager #{manager_node}")
        wait_forever()

      true ->
        {:error, "unable to connect to manager node #{manager_node}"}
    end
  end

  defp wait_forever do
    receive do
      :stop -> :ok
    after
      :infinity -> :ok
    end
  end
end
