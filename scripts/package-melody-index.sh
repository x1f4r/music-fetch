#!/usr/bin/env bash
# Packages a built melody index for publishing as a release asset.
#
#   scripts/package-melody-index.sh <melody-index.mfi> <version> <download-url-base>
#
# Writes dist/melody-index/{melody-index.mfi,manifest.json}. The manifest format is
# described in docs/CORE_API.md ("Melody index").
set -euo pipefail
cd "$(dirname "$0")/.."

index="$1"; version="$2"; base="${3%/}"
out="dist/melody-index"
mkdir -p "$out"
cp "$index" "$out/melody-index.mfi"
bytes=$(stat -f%z "$out/melody-index.mfi" 2>/dev/null || stat -c%s "$out/melody-index.mfi")
sha=$(shasum -a 256 "$out/melody-index.mfi" | cut -d' ' -f1)
songs=$(core/target/release/mf index info "$out/melody-index.mfi" | awk '/^songs/{print $2}')
min_core="$(sed -n 's/^version = "\(.*\)"/\1/p' core/Cargo.toml | head -1)"
cat > "$out/manifest.json" <<JSON
{"version": "$version", "url": "$base/melody-index.mfi", "sha256": "$sha", "bytes": $bytes, "songs": $songs, "min_core": "$min_core"}
JSON
cat "$out/manifest.json"
