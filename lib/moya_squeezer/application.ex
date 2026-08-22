defmodule MoyaSqueezer.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    metrics_port = Application.get_env(:moya_squeezer, :metrics_port, 4001)
    metrics_server? = Application.get_env(:moya_squeezer, :metrics_server, true)

    children =
      [
        {Finch, name: MoyaSqueezerFinch},
        MoyaSqueezer.RuntimeState
      ] ++
        if(metrics_server?,
          do: [
            {Plug.Cowboy,
             scheme: :http,
             plug: MoyaSqueezer.MetricsRouter,
             options: [port: metrics_port]}
          ],
          else: []
        )

    opts = [strategy: :one_for_one, name: MoyaSqueezer.ApplicationSupervisor]
    Supervisor.start_link(children, opts)
  end
end
