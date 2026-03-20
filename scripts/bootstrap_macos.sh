#!/usr/bin/env bash
set -euo pipefail

brew install ffmpeg yt-dlp deno chromaprint fftw cmake curl || true

if ! command -v uv >/dev/null 2>&1; then
  brew install uv
fi

uv sync --extra separation --extra test

if ! command -v vibra >/dev/null 2>&1; then
  workdir="$(mktemp -d)"
  git clone https://github.com/BayernMuller/vibra.git "$workdir/vibra"
  cmake -S "$workdir/vibra" -B "$workdir/vibra/build"
  cmake --build "$workdir/vibra/build" --parallel
  sudo cmake --install "$workdir/vibra/build"
fi

echo "Bootstrap complete. Run: music-fetch doctor"
