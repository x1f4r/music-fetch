#!/usr/bin/env bash
# Full build on the GPU host. Every stage resumes where it stopped, so rerunning is safe.
#   setsid nohup scripts/pipeline.sh > ~/melody-build/logs/pipeline.out 2>&1 < /dev/null &
# SKIP_UNIVERSE=1 keeps the current song universe.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
export MFIDX_ROOT="${MFIDX_ROOT:-$HOME/melody-build}"
LOGS="$MFIDX_ROOT/logs"
# Optional rsync destination for the results, e.g. "workstation:music-fetch-data".
COPY_TO="${MFIDX_COPY_TO:-}"

exec 3>&2  # pipeline.out: one line per step, whatever the step's own redirection

run() {
  echo "$(date '+%F %T') mfidx $*" >&3
  uv run --no-sync mfidx "$@"
}

if [ "${SKIP_UNIVERSE:-0}" != 1 ]; then
  run universe > "$LOGS/universe.out" 2>&1 || exit 1
fi
run midi --step catalog > "$LOGS/midi-catalog.out" 2>&1 || exit 1
run midi --step link > "$LOGS/midi-link.out" 2>&1 || exit 1

# Stages 2-4 run side by side: resolution feeds extraction (fresh preview URLs),
# MIDI parsing only needs CPU.
run resolve > "$LOGS/resolve.out" 2>&1 &
pid_resolve=$!
sleep 20
run extract --follow > "$LOGS/extract.out" 2>&1 &
pid_extract=$!
run midi --step extract > "$LOGS/midi.out" 2>&1 &
pid_midi=$!
wait "$pid_resolve" "$pid_midi"
wait "$pid_extract"

# Second pass: transient errors, and unmatched songs once more.
run resolve --retry-not-found >> "$LOGS/resolve.out" 2>&1
run extract >> "$LOGS/extract.out" 2>&1
run years > "$LOGS/years.out" 2>&1

run validate > "$LOGS/validate.out" 2>&1
run famous > "$MFIDX_ROOT/out/famous.json" 2> "$LOGS/famous.out"
run export > "$LOGS/export.out" 2>&1 || exit 1
run eval-synth > "$LOGS/eval-synth.out" 2>&1
run eval-chad > "$LOGS/eval-chad.out" 2>&1

if [[ -z "$COPY_TO" ]]; then
  echo "$(date '+%F %T') results are in $MFIDX_ROOT/out and $MFIDX_ROOT/eval" >&2
elif rsync -a "$MFIDX_ROOT/out/" "$COPY_TO/melody/" && rsync -a "$MFIDX_ROOT/eval/" "$COPY_TO/eval/"; then
  echo "$(date '+%F %T') copied results to $COPY_TO" >&2
else
  echo "$(date '+%F %T') copy to $COPY_TO failed; run scripts/fetch-results.sh on your workstation" >&2
fi
echo "$(date '+%F %T') pipeline finished" >&2
