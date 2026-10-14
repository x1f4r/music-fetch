#!/usr/bin/env bash
# Run an mfidx command (or any command) in the builder's environment on the GPU host.
#   scripts/remote.sh mfidx status
#   scripts/remote.sh mfidx show "Adele" "Hello"
set -euo pipefail

HOST="${MFIDX_HOST:?set MFIDX_HOST to the ssh host of the GPU machine}"
REMOTE_ROOT="${MFIDX_REMOTE_ROOT:-melody-build}"
args=$(printf '%q ' "$@")

ssh "$HOST" bash -s <<EOF
set -euo pipefail
export PATH="\$HOME/.local/bin:\$PATH" MFIDX_ROOT="\$HOME/$REMOTE_ROOT"
cd "\$MFIDX_ROOT/code"
uv run --no-sync $args
EOF
