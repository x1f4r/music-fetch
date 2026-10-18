#!/usr/bin/env bash
# Regenerates the codec fixtures: a synthetic signal (tones and a sweep; no
# recorded audio) encoded with ffmpeg in every format the decoder reads. The
# MediaRecorder recordings come from record.mjs (headless Chromium and Firefox).
# After regenerating, update expected.json:
#   cargo test -p mfcore --lib codec_fixtures -- --ignored --nocapture
set -euo pipefail
cd "$(dirname "$0")"
# Two notes, a fifth above, and a short upward sweep; channels differ slightly.
signal="aevalsrc=exprs='0.25*sin(2*PI*(220+110*gte(t,0.5))*t)+0.15*sin(2*PI*(330+165*gte(t,0.5))*t)+0.1*sin(2*PI*(500+900*t)*t)|0.2*sin(2*PI*(220+110*gte(t,0.5))*t)+0.15*sin(2*PI*(330+165*gte(t,0.5))*t)+0.12*sin(2*PI*(500+900*t)*t)'"
enc() { ffmpeg -nostdin -hide_banner -loglevel error -y -f lavfi -i "$signal:s=$1:d=$2" -bitexact -map_metadata -1 "${@:3}"; }
enc 22050 0.5 -c:a pcm_s16le wav-22k-stereo.wav
enc 44100 1 -c:a libmp3lame -b:a 64k mp3-44k-stereo.mp3
enc 44100 1 -c:a aac -b:a 64k -movflags +faststart aac-44k-stereo.m4a
enc 44100 0.5 -ac 1 -c:a flac flac-44k-mono.flac
enc 48000 1 -c:a libvorbis -q:a 1 vorbis-48k-stereo.ogg
enc 48000 1 -c:a libopus -b:a 48k opus-48k-stereo.opus
enc 48000 1 -c:a libopus -b:a 48k -f webm opus-48k-stereo.webm
ls -l
