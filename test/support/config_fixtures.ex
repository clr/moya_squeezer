defmodule MoyaSqueezer.ConfigFixtures do
  @moduledoc """
  Fixture data for building `MoyaSqueezer.Config.from_map/1` inputs in tests.
  """

  @doc """
  A minimal set of required attrs for `Config.from_map/1`, merged with `overrides`.
  """
  def valid_squeeze_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        connections_per_worker: 2,
        requests_per_second: 100,
        read_ratio: 0.7,
        write_ratio: 0.2,
        delete_ratio: 0.1,
        payload_size: 128,
        duration_seconds: 5
      },
      overrides
    )
  end
end
