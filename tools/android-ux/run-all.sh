#!/usr/bin/env bash
# Seed and run every scenario on one device (light), then the key screens dark and at a
# larger font. Screenshots: <out>/<label>/NN-name.png
#
#   tools/android-ux/run-all.sh <serial> <out> [label]
set -euo pipefail
serial="$1"
out="$2"
label="${3:-$serial}"
here="$(cd "$(dirname "$0")" && pwd)"
work="${UX_WORK:-$HOME/.cache/mf-ux}"
mkdir -p "$work"
python3 -I "$here/seed.py" "$work/seed" > /dev/null
python3 -I "$here/replays.py" "$work/replays" > /dev/null
export UX_SEED="$work/seed"
ux() { python3 -I "$here/ux.py" "$serial" "$@"; }
replays=(hum_wav="$work/replays/hum.wav" quiet_wav="$work/replays/quiet.wav")

setup() { python3 -I -c "import sys; sys.path.insert(0, '$here'); import ux; ux.Device('$serial', '$work/tmp').setup(night=$1, font=$2)"; }

setup False 1.0
ux "$out/$label-light" first_launch
"$here/push-seed.sh" "$serial" "$UX_SEED"
ux "$out/$label-light" stage result listen_again library_and_back library selection swipe_pin pinned_filter candidates empty mix delete_undo no_match "${replays[@]}"

"$here/push-seed.sh" "$serial" "$UX_SEED"
setup True 1.3
ux "$out/$label-dark-font1.3" stage result listen_again library selection candidates empty no_match "${replays[@]}"
setup False 1.0
