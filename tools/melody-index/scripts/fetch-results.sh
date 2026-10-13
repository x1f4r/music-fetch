#!/usr/bin/env bash
# Copy the JSONL, stats and evaluation sets from the GPU host to this Mac.
set -euo pipefail

HOST="${MFIDX_HOST:-legion-cachy}"
DEST="${MFIDX_DEST:-$HOME/Documents/projects/music-fetch-data}"

mkdir -p "$DEST/melody" "$DEST/eval"
rsync -a "$HOST:melody-build/out/" "$DEST/melody/"
rsync -a "$HOST:melody-build/eval/" "$DEST/eval/"
ls -la "$DEST/melody"
