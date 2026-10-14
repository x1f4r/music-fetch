#!/usr/bin/env bash
# Build "Music Fetch.app" for Apple silicon, ad-hoc signed with a stable designated
# requirement, and zip it for release.
#
#   scripts/build-macos-app.sh [--install] [--offline]
#
#   --install   also copy the app to /Applications (the previous copy is kept in
#               apple/build/install-backup/ and restored if the install fails;
#               MF_INSTALL_DIR overrides the destination)
#   --offline   reuse the cached media runtime instead of checking for new releases
#
# Output:
#   dist/Music Fetch.app
#   dist/MusicFetch-<version>-macOS-arm64.zip
#
# Steps: build-core-apple.sh, fetch-macos-runtime.sh, xcodegen, xcodebuild (Release),
# copy the runtime into Contents/Resources/runtime, sign every Mach-O in it ad-hoc,
# then sign the app with `designated => identifier "local.musicfetch.app"` so macOS
# privacy permissions (microphone, system audio) survive rebuilds.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APPLE_DIR="$ROOT_DIR/apple"
BUILD_DIR="$APPLE_DIR/build"
DERIVED_DATA="$BUILD_DIR/DerivedData"
DIST_DIR="$ROOT_DIR/dist"
APP_NAME="Music Fetch"
BUNDLE_ID="local.musicfetch.app"
REQUIREMENT="designated => identifier \"$BUNDLE_ID\""
BUILT_APP="$DERIVED_DATA/Build/Products/Release/$APP_NAME.app"
DIST_APP="$DIST_DIR/$APP_NAME.app"
INSTALL_DIR="${MF_INSTALL_DIR:-/Applications/$APP_NAME.app}"
INSTALL_BACKUP="$BUILD_DIR/install-backup/$APP_NAME.app"

INSTALL_APP=0
RUNTIME_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --install) INSTALL_APP=1 ;;
    --offline) RUNTIME_ARGS+=(--offline) ;;
    -h | --help)
      awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
      exit 0
      ;;
    *)
      echo "usage: $0 [--install] [--offline]" >&2
      exit 2
      ;;
  esac
done

log() { printf '==> %s\n' "$*"; }
die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}
dir_size() { du -sk "$1" | awk '{ printf "%.1f MB", $1 / 1024 }'; }
file_size() { stat -f %z "$1" | awk '{ printf "%.1f MB", $1 / 1048576 }'; }
# codesign reports "replacing existing signature" for every file; only show failures.
sign() {
  local output
  output="$(codesign --force --sign - --timestamp=none "$@" 2>&1)" || {
    printf '%s\n' "$output" >&2
    die "codesign failed for ${*: -1}"
  }
}

command -v xcodegen >/dev/null || die "xcodegen is required (brew install xcodegen)"

# 1. Engine and bindings, 2. media runtime.
"$ROOT_DIR/scripts/build-core-apple.sh"
"$ROOT_DIR/scripts/fetch-macos-runtime.sh" ${RUNTIME_ARGS[@]+"${RUNTIME_ARGS[@]}"}

# 3. Xcode project and Release build. ARCHS also applies to the package targets.
log "Generating Xcode project"
xcodegen generate --spec "$APPLE_DIR/project.yml" --quiet

log "Building $APP_NAME (Release)"
XCODEBUILD_LOG="$BUILD_DIR/xcodebuild.log"
if ! xcodebuild build \
  -project "$APPLE_DIR/MusicFetch.xcodeproj" \
  -scheme MusicFetch \
  -configuration Release \
  -destination "generic/platform=macOS" \
  -derivedDataPath "$DERIVED_DATA" \
  -clonedSourcePackagesDirPath "$BUILD_DIR/SourcePackages" \
  ARCHS=arm64 \
  ONLY_ACTIVE_ARCH=NO \
  CODE_SIGN_IDENTITY=- \
  CODE_SIGN_STYLE=Manual \
  ENABLE_HARDENED_RUNTIME=NO \
  >"$XCODEBUILD_LOG" 2>&1; then
  grep -E 'error:|\*\* BUILD' "$XCODEBUILD_LOG" | tail -n 30 >&2 || true
  die "xcodebuild failed (full log: ${XCODEBUILD_LOG#"$ROOT_DIR"/})"
fi
grep -E '^/.+: warning:' "$XCODEBUILD_LOG" | sort -u || true
[[ -d "$BUILT_APP" ]] || die "xcodebuild did not produce $BUILT_APP"

# 4. Assemble dist/Music Fetch.app with the runtime.
log "Assembling ${DIST_APP#"$ROOT_DIR"/}"
mkdir -p "$DIST_DIR"
rm -rf "$DIST_APP"
ditto "$BUILT_APP" "$DIST_APP"
RUNTIME_DEST="$DIST_APP/Contents/Resources/runtime"
ditto "$BUILD_DIR/runtime" "$RUNTIME_DEST"
# Names, versions and licenses of the Rust crates linked into the engine, for
# Settings › About › Licenses (nothing else from the metadata is kept).
LICENSES_DIR="$DIST_APP/Contents/Resources/Licenses"
mkdir -p "$LICENSES_DIR"
(
  cd "$ROOT_DIR/core"
  PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH" \
    cargo metadata --format-version 1 --locked --filter-platform aarch64-apple-darwin
) | jq '
  . as $m
  | ($m.resolve.nodes
      | map({key: .id, value: [.deps[] | select(any(.dep_kinds[]; .kind == null)) | .pkg]})
      | from_entries) as $graph
  | ($m.packages[] | select(.name == "mfcore") | .id) as $root
  | {seen: {}, queue: [$root]}
  | until(.queue | length == 0;
      .queue[0] as $id
      | .queue |= .[1:]
      | if .seen[$id] then . else (.seen[$id] = true | .queue += ($graph[$id] // [])) end)
  | (.seen | keys) as $ids
  | [$m.packages[] | select(.id as $id | $ids | index($id)) | select(.name != "mfcore")
      | {name, version, license}]
  | unique_by(.name + " " + .version)
  | sort_by(.name)' >"$LICENSES_DIR/rust-crates.json" || die "listing the engine crates failed"
echo "    $(jq length "$LICENSES_DIR/rust-crates.json") crate licenses listed"
# Finder info and other xattr detritus fail strict signature verification.
xattr -cr "$DIST_APP"

# 5. Sign nested code first (python, lib-dynload, qjs), then the bundle.
log "Signing"
signed=0
while IFS= read -r -d '' file; do
  if file -b "$file" | grep -q '^Mach-O'; then
    sign "$file"
    signed=$((signed + 1))
  fi
done < <(find "$RUNTIME_DEST" -type f -print0)
((signed > 0)) || die "no Mach-O files found in the runtime"
echo "    $signed runtime binaries signed ad-hoc"
# A fixed designated requirement keeps the TCC identity stable across ad-hoc builds.
sign --requirements "=$REQUIREMENT" "$DIST_APP"

codesign --verify --deep --strict --verbose=1 "$DIST_APP"
designated="$(codesign --display --requirements - "$DIST_APP" 2>/dev/null | sed -n 's/^designated => //p')"
[[ "$designated" == "identifier \"$BUNDLE_ID\"" ]] || die "unexpected designated requirement: $designated"
echo "    designated => $designated"

# 6. Release zip.
VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$DIST_APP/Contents/Info.plist")"
ZIP="$DIST_DIR/MusicFetch-$VERSION-macOS-arm64.zip"
rm -f "$ZIP"
ditto -c -k --sequesterRsrc --keepParent "$DIST_APP" "$ZIP"
log "Built ${DIST_APP#"$ROOT_DIR"/} ($(dir_size "$DIST_APP")) and ${ZIP#"$ROOT_DIR"/} ($(file_size "$ZIP"))"

# 7. Optional install with rollback.
if ((INSTALL_APP)); then
  log "Installing to $INSTALL_DIR"
  [[ -w "$(dirname "$INSTALL_DIR")" ]] || die "$(dirname "$INSTALL_DIR") is not writable for $(id -un)"
  # Running apps show their physical path (/tmp is /private/tmp).
  INSTALL_DIR="$(cd "$(dirname "$INSTALL_DIR")" && pwd -P)/$(basename "$INSTALL_DIR")"
  # Quit the installed copy (by path, not bundle id, so other copies keep running).
  installed_running() { pgrep -f "^$INSTALL_DIR/Contents/MacOS/" >/dev/null; }
  if installed_running; then
    osascript -e "tell application (POSIX file \"$INSTALL_DIR\" as text) to quit" >/dev/null 2>&1 || true
    for _ in $(seq 20); do
      installed_running || break
      sleep 0.5
    done
    if installed_running; then
      die "$APP_NAME is still running; quit it and try again"
    fi
  fi

  has_backup=0
  if [[ -e "$INSTALL_DIR" ]]; then
    rm -rf "$INSTALL_BACKUP"
    mkdir -p "$(dirname "$INSTALL_BACKUP")"
    mv "$INSTALL_DIR" "$INSTALL_BACKUP"
    has_backup=1
  fi

  rollback() {
    rm -rf "$INSTALL_DIR"
    if ((has_backup)); then
      mv "$INSTALL_BACKUP" "$INSTALL_DIR"
      echo "Restored the previous $INSTALL_DIR" >&2
    fi
    die "$1"
  }

  ditto "$DIST_APP" "$INSTALL_DIR" || rollback "copying to $INSTALL_DIR failed"
  codesign --verify --deep --strict "$INSTALL_DIR" || rollback "the installed app failed signature verification"

  log "Installed $INSTALL_DIR"
  if ((has_backup)); then
    echo "    previous version kept in ${INSTALL_BACKUP#"$ROOT_DIR"/}"
  fi
fi
