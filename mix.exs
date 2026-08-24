defmodule MoyaSqueezer.MixProject do
  use Mix.Project

  def project do
    [
      app: :moya_squeezer,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {MoyaSqueezer.Application, []}
    ]
  end

  defp deps do
    [
      {:finch, "~> 0.19"},
      {:mint, "~> 1.6"},
      {:toml, path: "vendor/toml"},
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
    ]
  end
end
