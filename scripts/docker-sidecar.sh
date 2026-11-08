#!/bin/sh
# Start the out-of-process persistence helper, then exec the real command.
#
# The helper owns the state-file fsync and rename, so that disk work never runs in the
# trading process (whose OCaml 5 runtime would otherwise pay for it between strategy
# cycles). It is deliberately a child of this container rather than a separate service: it
# needs the same /app/data mount and the same private /tmp (where the Unix socket lives),
# and both are per-container.
#
# Liveness contract (mirrors Persist_ipc):
#   - If the helper is missing or fails to bind, the engine runs with in-process
#     persistence, i.e. exactly the previous behaviour. Startup is never blocked on it.
#   - Once attached, a dead helper degrades durability (saves are dropped and counted);
#     it never stalls the trading loop.
#   - This script execs the engine, so the engine stays PID 1 and receives SIGTERM.
#     The engine's shutdown flush happens before the container exits, while the helper
#     is still alive to acknowledge it.
#
# Set DIO_PERSIST=0 to skip the helper entirely.

set -eu

if [ "$#" -eq 0 ]; then
  set -- dio
fi

if [ "${DIO_PERSIST:-1}" = "0" ]; then
  exec "$@"
fi

if ! command -v dio-persist >/dev/null 2>&1; then
  echo "dio: dio-persist not present; persistence stays in-process" >&2
  exec "$@"
fi

socket="${DIO_PERSIST_SOCK:-/tmp/dio-persist.sock}"

# A stale socket from a previous container in the same tmpfs would make the helper's
# bind fail; the helper is the only writer of this path.
rm -f "$socket"

dio-persist &
helper_pid=$!

# Bounded wait (2s). The engine also tolerates a late helper, so this is only to make
# the common case deterministic rather than to gate startup on it.
waited=0
while [ ! -S "$socket" ] && [ "$waited" -lt 100 ]; do
  if ! kill -0 "$helper_pid" 2>/dev/null; then
    break
  fi
  waited=$((waited + 1))
  sleep 0.02
done

if [ -S "$socket" ]; then
  echo "dio: persistence sidecar listening on $socket" >&2
else
  echo "dio: warning: dio-persist did not come up; persistence stays in-process" >&2
fi

exec "$@"
