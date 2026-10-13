#!/usr/bin/env bash
# Progress, throughput and ETA of the build on the GPU host, plus the latest log lines.
set -euo pipefail

HOST="${MFIDX_HOST:-legion-cachy}"
here="$(cd "$(dirname "$0")" && pwd)"

"$here/remote.sh" mfidx status
ssh "$HOST" bash -s <<'EOF'
cd ~/melody-build/logs
for f in pipeline.out resolve.log extract.log midi.log; do
  [ -f "$f" ] && { echo "== $f"; tail -n 3 "$f"; }
done
nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader 2>/dev/null | sed 's/^/gpu: /'
EOF
