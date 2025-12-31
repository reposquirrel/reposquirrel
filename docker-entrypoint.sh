#!/usr/bin/env bash
set -euo pipefail

HOST_ARG=${HOST:-0.0.0.0}
PORT_ARG=${PORT:-5001}
READ_ONLY_ARG=${READ_ONLY:-false}

declare -a EXTRA_ARGS
if [[ "${READ_ONLY_ARG,,}" == "true" ]]; then
  EXTRA_ARGS+=("--read-only")
fi

exec python dashboard_server.py --host "${HOST_ARG}" --port "${PORT_ARG}" "${EXTRA_ARGS[@]}" "$@"
