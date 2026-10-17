#!/usr/bin/env bash
# Packages a built melody index for publishing as a release asset.
#
#   scripts/package-melody-index.sh <melody-index.mfi> <version> <download-url-base>
#
# Writes dist/melody-index/{melody-index.mfi,manifest.json}. The manifest format is
# described in docs/CORE_API.md ("Melody index"). Each index file format has its own
# release tag, melody-index-<format>, and <download-url-base> must point there: apps
# read the tag of the format their core reads, so a new format never reaches an older
# app (1.0.x apps read melody-index-1, format 1).
set -euo pipefail
cd "$(dirname "$0")/.."

index="$1"; version="$2"; base="${3%/}"
out="dist/melody-index"
mkdir -p "$out"
cp "$index" "$out/melody-index.mfi"
bytes=$(stat -f%z "$out/melody-index.mfi" 2>/dev/null || stat -c%s "$out/melody-index.mfi")
sha=$(shasum -a 256 "$out/melody-index.mfi" | cut -d' ' -f1)
info="$(core/target/release/mf index info "$out/melody-index.mfi")"
songs=$(awk '/^songs/{print $2}' <<<"$info")
format=$(awk '/^format/{print $2}' <<<"$info")
# The first core version that reads each format.
case "$format" in
  2) min_core="1.1.0" ;;
  *) echo "error: no minimum core version known for index format '$format'" >&2; exit 1 ;;
esac
case "$base" in
  */melody-index-"$format") ;;
  *) echo "error: format $format belongs under the release tag melody-index-$format, not $base" >&2; exit 1 ;;
esac
cat > "$out/manifest.json" <<JSON
{"version": "$version", "url": "$base/melody-index.mfi", "sha256": "$sha", "bytes": $bytes, "songs": $songs, "min_core": "$min_core", "format": $format}
JSON
cat "$out/manifest.json"
