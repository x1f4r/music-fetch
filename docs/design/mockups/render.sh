#!/usr/bin/env bash
# Renders every mockup to mockups/png at 2x with headless Chrome.
# Usage: ./render.sh [name-filter]
set -euo pipefail
shopt -s nullglob
cd "$(dirname "$0")"
CHROME="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
mkdir -p png
filter="${1:-}"

render() { # file theme width height
  local f="$1" theme="$2" w="$3" h="$4"
  local name="${f%.html}"
  if [[ -n "$filter" && "$name" != *"$filter"* ]]; then return 0; fi
  local out="png/${name}.png" url="file://$PWD/$f"
  if [[ "$theme" == "dark" ]]; then out="png/${name}-dark.png"; url="$url#dark"; fi
  "$CHROME" --headless=new --disable-gpu --hide-scrollbars --no-first-run --no-default-browser-check \
    --force-device-scale-factor=2 --window-size="$w,$h" --virtual-time-budget=1500 \
    --screenshot="$out" "$url" >/dev/null 2>&1
  echo "$out"
}

for f in mac-*.html; do
  case "$f" in
    mac-menubar.html) w=520; h=560 ;;
    mac-settings-*.html) w=680; h=700 ;;
    *) w=1240; h=820 ;;
  esac
  render "$f" light "$w" "$h"
  case "$f" in
    mac-listen-idle.html|mac-listening.html|mac-hum.html|mac-result.html|mac-mix.html|mac-menubar.html) render "$f" dark "$w" "$h" ;;
  esac
done

for f in android-*.html; do
  render "$f" light 412 915
  case "$f" in
    android-listen-idle.html|android-listening.html|android-hum.html|android-result.html|android-candidates.html|android-share.html) render "$f" dark 412 915 ;;
  esac
done
