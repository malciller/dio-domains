#!/bin/sh
# dio container entrypoint.
#
# The published image deliberately ships without a configuration. The engine
# needs /app/config.json to start; without it the process would fail deep in
# the config parser, so this guard turns that into a clear message. The
# dashboard attaches to a running engine over the Unix domain socket and does
# not need a config of its own.
set -eu

config_path="${DIO_CONFIG_PATH:-/app/config.json}"
env_path="${DIO_ENV_PATH:-/app/.env}"

if [ "$#" -eq 0 ]; then
  set -- dio
fi

case "$1" in
  dio)
    if [ ! -f "$config_path" ]; then
      cat >&2 <<EOF
dio: no configuration file at $config_path

The image ships no configuration on purpose. The templates are inside it; pull
them out, edit them, then mount them:

  docker run --rm -v "\$PWD:/out" --entrypoint cp \\
    ghcr.io/malciller/dio-domains:latest \\
    /usr/share/doc/dio/config.example.json /out/config.json
  docker run --rm -v "\$PWD:/out" --entrypoint cp \\
    ghcr.io/malciller/dio-domains:latest \\
    /usr/share/doc/dio/.env.example /out/.env

  docker run --rm -it \\
    -v "\$PWD/config.json:$config_path:ro" \\
    -v "\$PWD/.env:/app/.env:ro" \\
    -v dio-data:/app/data \\
    ghcr.io/malciller/dio-domains:latest

Without config.json the engine cannot start. See docs/DEPLOYMENT.md.
EOF
      exit 1
    fi
    if [ ! -f "$env_path" ]; then
      echo "dio: warning: no $env_path mounted; exchange credentials are unavailable." >&2
    fi
    ;;
esac

exec "$@"
