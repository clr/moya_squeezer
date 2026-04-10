ARG BASE_IMAGE=elixir:1.19.0
FROM ${BASE_IMAGE}

WORKDIR /app

COPY mix.exs mix.lock ./
COPY config ./config
COPY lib ./lib
COPY scripts ./scripts

# Build BEAM artifacts in-image from current source so runtime code is never
# dependent on stale host-side _build outputs.
RUN mix local.hex --force && \
    mix local.rebar --force && \
    MIX_ENV=dev mix deps.get && \
    MIX_ENV=dev mix deps.compile && \
    MIX_ENV=dev mix compile

CMD ["elixir", "-e", "IO.puts(\"moya_squeezer image ready\")"]
