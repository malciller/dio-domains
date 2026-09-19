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
# Engine config (holds the "theme" field). deploy.sh keeps it in the persistent
# data dir; override with DIO_DATA_DIR when it lives elsewhere.
CONFIG_FILE="${DIO_DATA_DIR:-${HOME}/dio-data}/config.json"

case "${1:-}" in
  stop)
    echo "Stopping ${NAME}…"
    docker rm -f "${NAME}" >/dev/null 2>&1 || true
    echo "${NAME} stopped."
    exit 0
    ;;
  restart)
    echo "Restarting ${NAME}…"
    docker rm -f "${NAME}" >/dev/null 2>&1 || true
    shift || true
    ;;
  status)
    docker ps -a --filter "name=^/${NAME}$" --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}"
    exit 0
    ;;
esac

is_running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "${NAME}" 2>/dev/null)" == "true" ]]
}

if is_running; then
  echo "Dashboard already running — attaching to ${NAME} (detach: ctrl-p ctrl-q)…"
  exec docker attach --detach-keys 'ctrl-p,ctrl-q' "${NAME}"
fi

# Stale exited container holds the name — remove it so the run below succeeds.
docker rm -f "${NAME}" >/dev/null 2>&1 || true

# The dashboard reads its theme from the engine config, which lives in the persistent data
# dir. Mount it read-only so the container can load the configured theme; a theme picked in
# the TUI is session-only. Without this mount the container has no config.json and silently
# falls back to the default theme.
CONFIG_MOUNT=()
if [[ -f "${CONFIG_FILE}" ]]; then
  CONFIG_MOUNT=(-v "${CONFIG_FILE}:/app/config.json:ro")
else
  echo "WARNING: ${CONFIG_FILE} not found — dashboard will use the default theme." >&2
fi

echo "Starting dashboard container ${NAME}…"
exec docker run --rm -it \
  --name "${NAME}" \
  -v "${SOCK_VOL}:/var/run/dio" \
  ${CONFIG_MOUNT[@]+"${CONFIG_MOUNT[@]}"} \
  -e TERM="${TERM:-xterm-256color}" \
  "${IMAGE}" dio-dashboard "$@"
