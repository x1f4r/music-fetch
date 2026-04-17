#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="Music Fetch"
APP_DIR="$ROOT_DIR/dist/$APP_NAME.app"
INSTALL_DIR="/Applications/$APP_NAME.app"
BUILD_DIR="$ROOT_DIR/macos/.build/release"
EXECUTABLE="$BUILD_DIR/MusicFetchMac"
APP_BACKEND_COMMAND="${MUSIC_FETCH_BACKEND_COMMAND:-music-fetch}"
ICON_SCRIPT="$ROOT_DIR/scripts/generate_app_icon.py"
ICON_ICNS="$ROOT_DIR/assets/app_icon/MusicFetch.icns"
INSTALL_APP=0

if [[ "${1:-}" == "--install" ]]; then
  INSTALL_APP=1
fi

if [[ "$APP_BACKEND_COMMAND" == /* || "$APP_BACKEND_COMMAND" == ~* ]]; then
    if [[ ! -x "$APP_BACKEND_COMMAND" ]]; then
        echo "Backend command path is not executable: $APP_BACKEND_COMMAND"
        exit 1
    fi
fi

uv run python "$ICON_SCRIPT"
swift build -c release --package-path "$ROOT_DIR/macos"

rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS" "$APP_DIR/Contents/Resources/Resources"
cp "$EXECUTABLE" "$APP_DIR/Contents/MacOS/MusicFetchMac"
cp "$ICON_ICNS" "$APP_DIR/Contents/Resources/MusicFetch.icns"
printf '%s\n' "$APP_BACKEND_COMMAND" > "$APP_DIR/Contents/Resources/Resources/backend-command.txt"

cat > "$APP_DIR/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>MusicFetchMac</string>
  <key>CFBundleIdentifier</key>
  <string>local.musicfetch.app</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleName</key>
  <string>${APP_NAME}</string>
  <key>CFBundleIconFile</key>
  <string>MusicFetch</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>0.3.7</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>LSMinimumSystemVersion</key>
  <string>14.0</string>
  <key>NSMicrophoneUsageDescription</key>
  <string>Music Fetch records short clips from your microphone to identify songs.</string>
  <key>NSHighResolutionCapable</key>
  <true/>
</dict>
</plist>
PLIST

codesign --force --deep --sign - "$APP_DIR"
echo "Built $APP_DIR"

if [[ "$INSTALL_APP" -eq 1 ]]; then
  osascript -e 'tell application "Music Fetch" to quit' >/dev/null 2>&1 || true
  sleep 1

  if [[ -e "$INSTALL_DIR" ]]; then
    mv "$INSTALL_DIR" "$INSTALL_DIR.previous"
  fi

  ditto "$APP_DIR" "$INSTALL_DIR"
  rm -rf "$INSTALL_DIR.previous"
  echo "Installed $INSTALL_DIR"
fi
