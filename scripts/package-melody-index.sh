#!/usr/bin/env bash
# Packages a built melody index for publishing as a release asset.
#
#   [TIER=android|mac] scripts/package-melody-index.sh <melody-index.mfi> <version> <download-url-base> [<melody-encoder-<tier>.mfe>]
#
# Writes dist/melody-index/{melody-index.mfi,manifest.json}, plus the optional
# learned-encoder file built for this index (`mf index encoder build`), which the
# manifest then names under "encoder" (checked against the index first). With TIER
# set, the files are named per tier, melody-index-<tier>.mfi and manifest-<tier>.json,
# so several tiers share one release (the Android app reads manifest-android.json,
# the Mac app manifest-mac.json). The manifest format is described in
# docs/CORE_API.md ("Melody index"). Each index file format has its own release tag,
# melody-index-<format>, and <download-url-base> must point there: apps read the tag
# of the format their core reads, so a new format never reaches an older app (1.0.x
# apps read melody-index-1, format 1). MF overrides the mf binary used.
set -euo pipefail
cd "$(dirname "$0")/.."

index="$1"; version="$2"; base="${3%/}"; encoder="${4:-}"
tier="${TIER:-}"
case "$tier" in
  "") iname="melody-index.mfi"; mname="manifest.json" ;;
  android | mac) iname="melody-index-$tier.mfi"; mname="manifest-$tier.json" ;;
  *) echo "error: unknown tier '$tier' (android or mac)" >&2; exit 1 ;;
esac
out="dist/melody-index"
mkdir -p "$out"
cp "$index" "$out/$iname"
size() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1"; }
sha256() { if command -v shasum >/dev/null; then shasum -a 256 "$1"; else sha256sum "$1"; fi | cut -d' ' -f1; }
bytes=$(size "$out/$iname")
sha=$(sha256 "$out/$iname")
mf="${MF:-core/target/release/mf}"
info="$("$mf" index info "$out/$iname")"
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
extra=""
if [[ -n "$encoder" ]]; then
  # Refuses a file built for another index (the digest of the track table differs).
  einfo="$("$mf" index encoder info "$encoder" --index "$out/$iname")"
  eformat=$(awk '/^format/{print $2}' <<<"$einfo")
  # The first core version that downloads and uses each encoder format.
  case "$eformat" in
    1) emin="1.1.0" ;;
    *) echo "error: no minimum core version known for encoder format '$eformat'" >&2; exit 1 ;;
  esac
  ename=$(basename "$encoder")
  cp "$encoder" "$out/$ename"
  extra=", \"encoder\": {\"url\": \"$base/$ename\", \"sha256\": \"$(sha256 "$out/$ename")\", \"bytes\": $(size "$out/$ename"), \"format\": $eformat, \"min_core\": \"$emin\"}"
fi
cat > "$out/$mname" <<JSON
{"version": "$version", "url": "$base/$iname", "sha256": "$sha", "bytes": $bytes, "songs": $songs, "min_core": "$min_core", "format": $format$extra}
JSON
cat "$out/$mname"
