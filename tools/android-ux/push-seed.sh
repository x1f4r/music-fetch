#!/usr/bin/env bash
# Replace the app's library on a device with a seed library (see seed.py).
#
#   tools/android-ux/push-seed.sh <serial> <seed-dir>
#
# Needs a debug build (run-as). The app is stopped first; its settings are kept.
set -euo pipefail
serial="$1"
seed="$2"
pkg=app.musicfetch
adb=(adb -s "$serial")
ext="/sdcard/Android/data/$pkg/files/ux"

"${adb[@]}" shell -n am force-stop "$pkg"
"${adb[@]}" shell -n mkdir -p "$ext"
for f in "$seed"/art*.png "$seed"/preview.wav; do
  "${adb[@]}" push -q "$f" "$ext/" < /dev/null
done
"${adb[@]}" push -q "$seed/musicfetch.sqlite3" /data/local/tmp/mf-seed.sqlite3 < /dev/null
"${adb[@]}" shell -n "run-as $pkg sh -c 'mkdir -p files/engine && rm -f files/engine/musicfetch.sqlite3 files/engine/musicfetch.sqlite3-wal files/engine/musicfetch.sqlite3-shm'"
"${adb[@]}" shell -n "cat /data/local/tmp/mf-seed.sqlite3 | run-as $pkg sh -c 'cat > files/engine/musicfetch.sqlite3'"
"${adb[@]}" shell -n rm -f /data/local/tmp/mf-seed.sqlite3
echo "seeded $serial"
