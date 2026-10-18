#!/usr/bin/env bash
# Run the runtime end-to-end tests on the Legion: a private Xvfb display and a private
# PipeWire null sink (nothing reaches the laptop's screen or speakers), the e2e builds,
# then Chromium and Firefox. Usage (from the extension dir): tests/e2e/legion-runtime.sh [file filter] [test name filter]
set -uo pipefail
source ~/mf/env.sh
export PATH="$HOME/mf/tools/node-24/bin:$PATH"
DISPLAY_NUM="${MF_RT_DISPLAY:-:93}"
SINK="${MF_RT_SINK:-mf_rt_null}"

if ! xdpyinfo -display "$DISPLAY_NUM" >/dev/null 2>&1; then
  Xvfb "$DISPLAY_NUM" -screen 0 1600x1000x24 -nolisten tcp -noreset >/dev/null 2>&1 &
  XVFB_PID=$!
  for _ in $(seq 1 50); do xdpyinfo -display "$DISPLAY_NUM" >/dev/null 2>&1 && break; sleep 0.1; done
fi
MODULE=$(pactl load-module module-null-sink sink_name="$SINK" sink_properties=device.description="$SINK" 2>/dev/null || true)
if [[ -z "$MODULE" ]]; then
  # Without the private sink the test pages would play on the machine's speakers.
  echo "no private null sink; not running"; echo "E2E-DONE 2"; exit 2
fi
cleanup() {
  [[ -n "${MODULE:-}" ]] && pactl unload-module "$MODULE" 2>/dev/null
  [[ -n "${XVFB_PID:-}" ]] && kill "$XVFB_PID" 2>/dev/null
}
trap cleanup EXIT

unset WAYLAND_DISPLAY WAYLAND_SOCKET
export DISPLAY="$DISPLAY_NUM" XDG_SESSION_TYPE=x11 GDK_BACKEND=x11 MOZ_ENABLE_WAYLAND=0 PULSE_SINK="$SINK" MF_RUNTIME_E2E=1
# MF_E2E_BUILD=build runs the scaffold's smoke tests on the release build instead.
npm run "${MF_E2E_BUILD:-build:e2e}" >/dev/null || { echo "build failed"; exit 1; }
# $2: an optional test name filter (vitest -t)
nice -n 10 npx vitest run --config vitest.e2e.config.ts "${1:-tests/e2e/runtime-}" ${2:+-t "$2"}
code=$?
echo "E2E-DONE $code"
exit $code
