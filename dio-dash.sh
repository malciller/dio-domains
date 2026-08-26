#!/usr/bin/env bash
set -euo pipefail

################################################################################
# dio-dash.sh — attach to the single shared dashboard container.
#
# Only one dashboard container may exist (fixed name). If it is already
# running, this script attaches to it instead of spawning a duplicate;
# detach with ctrl-p ctrl-q and re-attach later from any SSH session.
#
# Usage: ./dio-dash.sh
################################################################################

IMAGE="dio"
NAME="dio_dashboard"
SOCK_VOL="dio-sock"

is_running() {
  [[ "$(docker ps -a --filter "name=^/${NAME}$" --format '{{.Running}}')" == "true" ]]
}

if is_running; then
  echo "Dashboard already running — attaching to ${NAME} (detach: ctrl-p ctrl-q)…"
  exec docker attach --detach-keys 'ctrl-p,ctrl-q' "${NAME}"
fi

# Stale exited container holds the name — remove it so the run below succeeds.
docker rm -f "${NAME}" >/dev/null 2>&1 || true

echo "Starting dashboard container ${NAME}…"
exec docker run --rm -it \
  --name "${NAME}" \
  -v "${SOCK_VOL}:/var/run/dio" \
  -e TERM="${TERM:-xterm-256color}" \
  "${IMAGE}" dio-dashboard
