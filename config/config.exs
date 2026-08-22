import Config

config :moya_squeezer, :load_adapter, MoyaSqueezer.Adapters.HttpAdapter

import_config "#{config_env()}.exs"
