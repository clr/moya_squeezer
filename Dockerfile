ARG BASE_IMAGE=elixir:1.19.0
FROM ${BASE_IMAGE}

WORKDIR /app

COPY mix.exs mix.lock ./
COPY config ./config
COPY deps ./deps
COPY _build ./_build
COPY lib ./lib
COPY scripts ./scripts

CMD ["elixir", "-e", "IO.puts(\"moya_squeezer image ready\")"]
