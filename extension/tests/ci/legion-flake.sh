#!/usr/bin/env bash
# Flake measurement on the Legion: CI's end-to-end stages, N times in a row (default 20),
# on a private Xvfb display and a private PipeWire null sink (nothing reaches the laptop's
# screen or speakers), with the browsers CI pins. Waits while the machine is busy (load
# over 20 or under 6 GB free) so the numbers measure the tests, not the neighbours.
#
#   tests/ci/legion-flake.sh [runs] [stage ...]      default stages: e2e runtime
#
# MF_FLAKE_FIRST=<n> continues a measurement at run n (the summary keeps earlier runs);
# MF_FLAKE_MAX_LOAD (default 20) is the 1-minute load above which a run waits (up to 30 min).
#
# Logs: ~/mf/logs/ext-ui-flake/run-<n>.log; one line per run in summary.tsv
# (run, exit code, seconds, load at start, failed tests). Run it as a user unit:
#   systemd-run --user --collect --unit=mf-ext-ui-flake -p StandardOutput=file:$HOME/mf/logs/ext-ui-flake.log \
#     bash -lc '~/mf/ext-ui/extension/tests/ci/legion-flake.sh 20'
set -uo pipefail
source ~/mf/env.sh
export PATH="$HOME/mf/tools/node-24/bin:$PATH"
cd "$(dirname "$0")/../.."

runs="${1:-20}"
first="${MF_FLAKE_FIRST:-1}"
max_load="${MF_FLAKE_MAX_LOAD:-20}"
shift || true
stages=("$@")
[[ ${#stages[@]} -gt 0 ]] || stages=(e2e runtime)
logs="$HOME/mf/logs/ext-ui-flake"
mkdir -p "$logs"
DISPLAY_NUM="${MF_FLAKE_DISPLAY:-:94}"
SINK="mf_ui_null"

if ! xdpyinfo -display "$DISPLAY_NUM" >/dev/null 2>&1; then
  Xvfb "$DISPLAY_NUM" -screen 0 1600x1000x24 -nolisten tcp -noreset >/dev/null 2>&1 &
  XVFB_PID=$!
  for _ in $(seq 1 50); do xdpyinfo -display "$DISPLAY_NUM" >/dev/null 2>&1 && break; sleep 0.1; done
fi
MODULE=$(pactl load-module module-null-sink sink_name="$SINK" sink_properties=device.description=mf-ui-null 2>/dev/null || true)
if [[ -z "$MODULE" ]]; then
  echo "no private null sink; not running"
  exit 2
fi
cleanup() {
  [[ -n "${MODULE:-}" ]] && pactl unload-module "$MODULE" 2>/dev/null
  [[ -n "${XVFB_PID:-}" ]] && kill "$XVFB_PID" 2>/dev/null
}
trap cleanup EXIT

unset WAYLAND_DISPLAY WAYLAND_SOCKET
export DISPLAY="$DISPLAY_NUM" XDG_SESSION_TYPE=x11 GDK_BACKEND=x11 MOZ_ENABLE_WAYLAND=0 PULSE_SINK="$SINK"
export CHROMIUM_BIN="${CHROMIUM_BIN:-$HOME/mf/browsers/chrome/linux-155.0.8059.84/chrome-linux64/chrome}"
export FIREFOX_BIN="${FIREFOX_BIN:-/usr/bin/firefox}"

wait_for_room() {
  for _ in $(seq 1 30); do
    load=$(cut -d' ' -f1 /proc/loadavg)
    free=$(awk '/MemAvailable/ {print int($2 / 1048576)}' /proc/meminfo)
    if awk -v l="$load" -v m="$max_load" 'BEGIN { exit !(l <= m) }' && (( free >= 6 )); then return 0; fi
    echo "busy (load $load, $free GB free): waiting"
    sleep 60
  done
}

nice -n 10 tests/ci/run.sh build >"$logs/build.log" 2>&1 || { echo "build failed"; exit 1; }
[[ -f "$logs/summary.tsv" ]] || printf 'run\texit\tseconds\tload\tfailed\n' >"$logs/summary.tsv"
for n in $(seq "$first" "$runs"); do
  wait_for_room
  load=$(cut -d' ' -f1 /proc/loadavg)
  start=$(date +%s)
  nice -n 10 tests/ci/run.sh "${stages[@]}" >"$logs/run-$n.log" 2>&1
  code=$?
  failed=$(grep -E '^\s+(×|FAIL) ' "$logs/run-$n.log" | sed -E 's/^\s+(×|FAIL) //' | tr '\n' ';' | cut -c1-400)
  printf '%s\t%s\t%s\t%s\t%s\n' "$n" "$code" "$(( $(date +%s) - start ))" "$load" "$failed" >>"$logs/summary.tsv"
  echo "run $n: exit $code"
done
echo "FLAKE-DONE"
