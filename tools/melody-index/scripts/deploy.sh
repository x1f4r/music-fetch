#!/usr/bin/env bash
# Copy the builder to the GPU host and install its environment there (uv, Python 3.12, CUDA torch).
set -euo pipefail

HOST="${MFIDX_HOST:-legion-cachy}"
REMOTE_ROOT="${MFIDX_REMOTE_ROOT:-melody-build}"
here="$(cd "$(dirname "$0")/.." && pwd)"

rsync -a --delete --exclude .venv --exclude __pycache__ --exclude .pytest_cache \
  "$here/" "$HOST:$REMOTE_ROOT/code/"

ssh "$HOST" bash -s <<EOF
set -euo pipefail
export PATH="\$HOME/.local/bin:\$PATH"
command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh
mkdir -p ~/$REMOTE_ROOT/{data,cache,state,logs,out,eval}
cd ~/$REMOTE_ROOT/code
uv sync --python 3.12 --quiet
uv run --no-sync python -c "import torch; print('torch', torch.__version__, 'cuda', torch.cuda.is_available())"
EOF
