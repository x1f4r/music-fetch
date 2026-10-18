#!/usr/bin/env bash
# The extension's checks as CI runs them (.github/workflows/ci.yml, job "extension"). The
# Legion runs the same script for the flake measurement (tests/ci/legion-flake.sh).
#
#   tests/ci/run.sh [stage ...]      stages, in this order by default:
#     check    svelte-check (types in .ts and .svelte)
#     unit     Vitest unit tests
#     build    release and e2e builds, Chromium and Firefox
#     lint     web-ext lint of the Firefox release build
#     screens  every demo state rendered, the axe audit, the Firefox layout check
#     e2e      the smoke flows on the release builds (Chromium, Firefox)
#     runtime  the runtime, shortcut, menu and Settings flows on the e2e builds
#
# Browsers come from CHROMIUM_BIN (Chrome for Testing or Chromium: branded Chrome can't load
# unpacked extensions) and FIREFOX_BIN. The e2e stages want a display (xvfb-run) so toolbar
# popups open; "runtime" also wants PULSE_SINK naming a null sink, because the fixture pages
# play synthetic songs and tab capture needs a working output device.
set -euo pipefail
cd "$(dirname "$0")/../.."

stages=("$@")
[[ ${#stages[@]} -gt 0 ]] || stages=(check unit build lint screens e2e runtime)

group() { [[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::group::$1" || echo "== $1"; }
endgroup() { [[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::endgroup::" || true; }

for stage in "${stages[@]}"; do
  group "$stage"
  case "$stage" in
    check) npm run check ;;
    unit) npm test ;;
    build) npm run build && npm run build:e2e ;;
    lint) npm run lint:firefox ;;
    screens) npm run screens ;;
    e2e) npx vitest run --config vitest.e2e.config.ts tests/e2e/chromium.e2e.ts tests/e2e/firefox.e2e.ts ;;
    runtime) MF_RUNTIME_E2E=1 npx vitest run --config vitest.e2e.config.ts tests/e2e/runtime- ;;
    *) echo "unknown stage: $stage" >&2; exit 2 ;;
  esac
  endgroup
done
