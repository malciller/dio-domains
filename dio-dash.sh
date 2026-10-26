#!/usr/bin/env bash
set -euo pipefail

################################################################################
# dio-dash.sh — attach a TUI client to the shared dashboard container.
#
# The container is a detached idle supervisor; the TUI is a `docker exec`
# client. An SSH drop kills only the client, never the container or its
# containerd shim. A pty-attached container whose client is gone cannot be
# removed safely, so deploy and this script rename such legacy containers
# rather than tearing them down.
#
# Multiple TUI sessions may attach concurrently (the engine caps dashboard
# clients). Detach with ctrl-p ctrl-q and re-attach from any SSH session.
#
# Usage:
#   ./dio-dash.sh                 # attach (starts the detached container if needed)
#   ./dio-dash.sh stop            # remove the container
#   ./dio-dash.sh restart         # recreate the container, then attach
#   ./dio-dash.sh status          # container state
#   ./dio-dash.sh --theme <id>    # pass flags through to dio-dashboard
################################################################################

IMAGE="dio"
NAME="dio_dashboard"
SOCK_VOL="dio-sock"
LIFECYCLE_LABEL='"dio.lifecycle":"detached"'
# Engine config (holds the "theme" field). deploy.sh keeps it in the persistent
# data dir; override with DIO_DATA_DIR when it lives elsewhere.
CONFIG_FILE="${DIO_DATA_DIR:-${HOME}/dio-data}/config.json"

container_exists() {
  docker ps -a --filter "name=^/${NAME}$" --format '{{.Names}}' 2>/dev/null | grep -qx "${NAME}"
}

# macOS has no `timeout`; the bound only matters on the Linux server.
with_timeout() {
  local seconds="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then timeout "$seconds" "$@"; else "$@"; fi
}

is_running() {
  [[ "$(with_timeout 5 docker inspect -f '{{.State.Running}}' "${NAME}" 2>/dev/null)" == "true" ]]
}

# New deployments label the detached supervisor. A container without the label
# is the old pty-attached TUI; never rm -f it blindly from here.
is_managed() {
  with_timeout 5 docker inspect -f '{{json .Config.Labels}}' "${NAME}" 2>/dev/null \
    | grep -q "${LIFECYCLE_LABEL}"
}

start_container() {
  if container_exists; then
    if ! is_managed; then
      echo "ERROR: ${NAME} exists but was started by the old pty-attached dio-dash.sh." >&2
      echo "       Refusing to remove it here: a pty whose owner is gone can wedge the" >&2
      echo "       containerd shim. Run deploy.sh (it renames the legacy container) or" >&2
      echo "       remove it from a live session with: timeout 10 docker rm -f ${NAME}" >&2
      return 1
    fi
    if is_running; then
      return 0
    fi
    with_timeout 10 docker rm -f "${NAME}" >/dev/null 2>&1 || true
  fi

  local -a mount_args=()
  if [[ -f "${CONFIG_FILE}" ]]; then
    mount_args=(-v "${CONFIG_FILE}:/app/config.json:ro")
  else
    echo "WARNING: ${CONFIG_FILE} not found — dashboard will use the default theme." >&2
  fi

  echo "Starting dashboard supervisor container ${NAME}…"
  if ! docker run -d \
    --name "${NAME}" \
    --restart unless-stopped \
    --network none \
    --read-only \
    --cap-drop=ALL \
    --user 1000:1000 \
    --tmpfs /tmp:rw,noexec,nosuid,size=16m \
    --label dio.role=dashboard \
    --label dio.lifecycle=detached \
    --log-opt max-size=10m --log-opt max-file=3 \
    -v "${SOCK_VOL}:/var/run/dio" \
    ${mount_args[@]+"${mount_args[@]}"} \
    -e TERM="${TERM:-xterm-256color}" \
    -e HOME=/tmp \
    --entrypoint /bin/sh \
    "${IMAGE}" -c 'while :; do sleep 3600; done' >/dev/null
  then
    echo "ERROR: could not start ${NAME}. Is the dio image deployed?" >&2
    return 1
  fi
}

attach() {
  if [[ ! -t 0 || ! -t 1 ]]; then
    echo "ERROR: the dashboard TUI needs a terminal; run this from an interactive SSH session." >&2
    exit 1
  fi
  exec docker exec -it \
    --detach-keys 'ctrl-p,ctrl-q' \
    -e TERM="${TERM:-xterm-256color}" \
    "${NAME}" dio-dashboard "$@"
}

case "${1:-}" in
  stop)
    echo "Stopping ${NAME}…"
    if container_exists && ! is_managed; then
      # Do NOT rm -f a pty-attached legacy container: if the docker client that
      # owns its pty is gone, teardown deadlocks the containerd shim and takes
      # the daemon lock with it. Renaming is enough to free the name for a new
      # detached supervisor; the old process exits with its client.
      legacy="${NAME}_legacy_$(date +%s)"
      if with_timeout 10 docker rename "${NAME}" "${legacy}" >/dev/null 2>&1; then
        echo "Legacy pty-attached container renamed to ${legacy} (not torn down)." >&2
        echo "It will exit with its client. If it is orphaned, remove it from a live session:" >&2
        echo "  timeout 10 docker rm -f ${legacy}" >&2
        exit 0
      fi
      echo "ERROR: could not rename ${NAME}; refusing to touch it (shim-wedge guard)." >&2
      exit 1
    fi
    if container_exists && ! with_timeout 10 docker rm -f "${NAME}" >/dev/null 2>&1; then
      echo "ERROR: ${NAME} did not stop; its shim may be wedged. Engine is unaffected." >&2
      exit 1
    fi
    echo "${NAME} stopped."
    exit 0
    ;;
  restart)
    echo "Restarting ${NAME}…"
    if container_exists && ! is_managed; then
      legacy="${NAME}_legacy_$(date +%s)"
      if ! with_timeout 10 docker rename "${NAME}" "${legacy}" >/dev/null 2>&1; then
        echo "ERROR: could not rename legacy ${NAME}; refusing to tear it down." >&2
        exit 1
      fi
      echo "Legacy pty-attached container renamed to ${legacy} (left running)." >&2
    else
      if container_exists && ! with_timeout 10 docker rm -f "${NAME}" >/dev/null 2>&1; then
        echo "ERROR: could not remove ${NAME}; its shim may be wedged." >&2
        exit 1
      fi
    fi
    shift || true
    ;;
  status)
    docker ps -a --filter "name=^/${NAME}$" \
      --format "table {{.Names}}\t{{.Status}}\t{{.RunningFor}}\t{{.Image}}"
    if container_exists; then
      if is_managed; then echo "lifecycle: detached (safe for deploy to recreate)"; else echo "lifecycle: legacy pty-attached"; fi
    fi
    exit 0
    ;;
esac

start_container
attach "$@"
