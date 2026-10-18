#!/usr/bin/env bash
# Offscreen toolbar geometry check (RenderHarness.swift) against an existing Debug build.
#
#   apple/tools/render-offscreen.sh [<out-dir>] [state,state,...]
#
# Runs a copy of the Debug app under its own bundle identifier
# (local.musicfetch.app.render), so the installed app's preferences, saved window state
# and privacy permissions are never touched; the copy's own preferences are removed
# afterwards. No window appears and the app never becomes active. Build first, e.g.
#   xcodebuild build -project apple/MusicFetch.xcodeproj -scheme MusicFetch -configuration Debug \
#     -derivedDataPath apple/build/DerivedData
# Exit status: 0 when every layout passes, 1 when one fails (see <out-dir>/geometry.json).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILT="${MF_DEBUG_APP:-$ROOT_DIR/apple/build/DerivedData/Build/Products/Debug/Music Fetch.app}"
OUT="${1:-$ROOT_DIR/apple/build/render}"
STATES="${2:-}"
BUNDLE_ID="local.musicfetch.app.render"

[[ -d "$BUILT" ]] || { echo "error: no Debug build at $BUILT" >&2; exit 2; }
work="$(mktemp -d)"
cleanup() {
  rm -rf "$work"
  defaults delete "$BUNDLE_ID" >/dev/null 2>&1 || true
  rm -rf "$HOME/Library/Saved Application State/$BUNDLE_ID.savedState"
}
trap cleanup EXIT

ditto "$BUILT" "$work/Render.app"
/usr/libexec/PlistBuddy -c "Set :CFBundleIdentifier $BUNDLE_ID" "$work/Render.app/Contents/Info.plist"
codesign --force --sign - --timestamp=none "$work/Render.app" >/dev/null 2>&1
rm -rf "$OUT"
mkdir -p "$OUT"
args=(-MFRender "$OUT" -ApplePersistenceIgnoreState YES ${MF_RENDER_ARGS:-})
[[ -n "$STATES" ]] && args+=(-MFRenderStates "$STATES")
nice -n 10 "$work/Render.app/Contents/MacOS/Music Fetch" "${args[@]}"
