#!/usr/bin/env bash
# Download, verify and trim the media runtime bundled in the macOS app.
#
#   scripts/fetch-macos-runtime.sh [--offline]
#
# Result: apple/build/runtime/ (gitignored)
#   python/        python-build-standalone CPython (install_only_stripped), trimmed
#   yt-dlp         yt-dlp zipimport binary (includes the yt-dlp-ejs challenge scripts)
#   qjs            quickjs-ng, JS runtime for YouTube challenges
#   cacert.pem     CA bundle exported from the System Roots keychain
#   VERSIONS.json  what went in, with checksums
#
# Every artifact is checked against its release's published SHA-256 (SHA256SUMS,
# SHA2-256SUMS, GitHub's asset digest for quickjs-ng). Downloads are cached in
# apple/build/downloads/ and the runtime is only reassembled when a version changes.
#
# Latest releases are used unless pinned:
#   MF_PYTHON_SERIES   CPython minor series (default 3.13)
#   MF_PBS_RELEASE     python-build-standalone release tag, e.g. 20261009
#   MF_YTDLP_VERSION   yt-dlp release tag, e.g. 2026.08.19
#   MF_QJS_VERSION     quickjs-ng release tag, e.g. v0.17.0
#   GITHUB_TOKEN       optional, raises the GitHub API rate limit
#
# --offline reuses the existing runtime without checking for new releases. Without
# network access the existing runtime is reused with a warning.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/apple/build"
DOWNLOADS="$BUILD_DIR/downloads"
RUNTIME="$BUILD_DIR/runtime"
STAGING="$BUILD_DIR/runtime.staging"

PYTHON_SERIES="${MF_PYTHON_SERIES:-3.13}"
PBS_REPO="astral-sh/python-build-standalone"
YTDLP_REPO="yt-dlp/yt-dlp"
QJS_REPO="quickjs-ng/quickjs"
QJS_ASSET="qjs-darwin-arm64"
ROOTS_KEYCHAIN="/System/Library/Keychains/SystemRootCertificates.keychain"

OFFLINE=0
case "${1:-}" in
  "") ;;
  --offline) OFFLINE=1 ;;
  -h | --help)
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
    exit 0
    ;;
  *)
    echo "usage: $0 [--offline]" >&2
    exit 2
    ;;
esac

log() { printf '==> %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}
sha256() { shasum -a 256 "$1" | cut -d ' ' -f 1; }

runtime_complete() {
  [[ -f "$RUNTIME/VERSIONS.json" && -x "$RUNTIME/python/bin/python3" && -f "$RUNTIME/yt-dlp" &&
    -x "$RUNTIME/qjs" && -s "$RUNTIME/cacert.pem" ]]
}

reuse_existing() {
  runtime_complete || die "$1, and there is no runtime in ${RUNTIME#"$ROOT_DIR"/} to fall back to"
  warn "$1; reusing ${RUNTIME#"$ROOT_DIR"/} ($(jq -r '.summary' "$RUNTIME/VERSIONS.json"))"
  exit 0
}

if ((OFFLINE)); then
  runtime_complete || die "--offline needs an existing runtime in ${RUNTIME#"$ROOT_DIR"/}"
  log "Using existing runtime ($(jq -r '.summary' "$RUNTIME/VERSIONS.json"))"
  exit 0
fi

command -v jq >/dev/null || die "jq is required (macOS 15+ ships /usr/bin/jq)"
mkdir -p "$DOWNLOADS"

curl_base=(--fail --silent --show-error --retry 3 --retry-delay 2 --connect-timeout 15)
curl_args=("${curl_base[@]}" --location)

# Tag of a repository's latest release, from the /releases/latest redirect (no API quota).
latest_tag() {
  local url
  url="$(curl "${curl_base[@]}" --head --output /dev/null --write-out '%{redirect_url}' \
    "https://github.com/$1/releases/latest")" || return 1
  [[ "$url" == */releases/tag/* ]] || return 1
  printf '%s\n' "${url##*/releases/tag/}"
}

# Download $1 to $2 unless a cached copy exists. Small, immutable metadata only.
fetch_cached() {
  [[ -s "$2" ]] && return 0
  local extra=()
  if [[ "$1" == https://api.github.com/* ]]; then
    extra=(-H "Accept: application/vnd.github+json")
    [[ -n "${GITHUB_TOKEN:-}" ]] && extra+=(-H "Authorization: Bearer $GITHUB_TOKEN")
  fi
  curl "${curl_args[@]}" ${extra[@]+"${extra[@]}"} --output "$2.part" "$1" && mv "$2.part" "$2"
}

# Download $1 to $2 and verify it against SHA-256 $3; reuse a verified cached copy.
fetch_verified() {
  local url="$1" dest="$2" expected="$3" actual
  if [[ -f "$dest" && "$(sha256 "$dest")" == "$expected" ]]; then
    return 0
  fi
  log "Downloading ${url##*/}"
  curl "${curl_args[@]}" --output "$dest.part" "$url"
  actual="$(sha256 "$dest.part")"
  if [[ "$actual" != "$expected" ]]; then
    rm -f "$dest.part"
    die "checksum mismatch for $url (expected $expected, got $actual)"
  fi
  mv "$dest.part" "$dest"
}

# 1. Resolve versions and checksums.
log "Resolving runtime versions"
pbs_tag="${MF_PBS_RELEASE:-}"
ytdlp_tag="${MF_YTDLP_VERSION:-}"
qjs_tag="${MF_QJS_VERSION:-}"
[[ -n "$pbs_tag" ]] || pbs_tag="$(latest_tag "$PBS_REPO")" || reuse_existing "cannot reach GitHub"
[[ -n "$ytdlp_tag" ]] || ytdlp_tag="$(latest_tag "$YTDLP_REPO")" || reuse_existing "cannot reach GitHub"
[[ -n "$qjs_tag" ]] || qjs_tag="$(latest_tag "$QJS_REPO")" || reuse_existing "cannot reach GitHub"
[[ "$qjs_tag" == v* ]] || qjs_tag="v$qjs_tag"

pbs_sums="$DOWNLOADS/python-build-standalone-$pbs_tag.SHA256SUMS"
fetch_cached "https://github.com/$PBS_REPO/releases/download/$pbs_tag/SHA256SUMS" "$pbs_sums" ||
  die "cannot download SHA256SUMS for python-build-standalone $pbs_tag"
pbs_line="$(grep -E "  cpython-${PYTHON_SERIES//./\\.}\.[0-9]+\+${pbs_tag}-aarch64-apple-darwin-install_only_stripped\.tar\.gz\$" "$pbs_sums" | tail -n 1)" ||
  die "python-build-standalone $pbs_tag has no CPython $PYTHON_SERIES aarch64-apple-darwin install_only_stripped build"
pbs_sha="${pbs_line%% *}"
pbs_asset="${pbs_line##* }"
python_version="${pbs_asset#cpython-}"
python_version="${python_version%%+*}"

ytdlp_sums="$DOWNLOADS/yt-dlp-$ytdlp_tag.SHA2-256SUMS"
fetch_cached "https://github.com/$YTDLP_REPO/releases/download/$ytdlp_tag/SHA2-256SUMS" "$ytdlp_sums" ||
  die "cannot download SHA2-256SUMS for yt-dlp $ytdlp_tag"
ytdlp_sha="$(awk '$2 == "yt-dlp" { print $1 }' "$ytdlp_sums")"
[[ "$ytdlp_sha" =~ ^[0-9a-f]{64}$ ]] || die "yt-dlp $ytdlp_tag SHA2-256SUMS has no entry for the zipimport binary"

# quickjs-ng publishes no checksum file; GitHub records a SHA-256 digest per asset.
qjs_release="$DOWNLOADS/quickjs-ng-$qjs_tag.json"
fetch_cached "https://api.github.com/repos/$QJS_REPO/releases/tags/$qjs_tag" "$qjs_release" ||
  die "cannot read the quickjs-ng $qjs_tag release from the GitHub API (set GITHUB_TOKEN if rate limited)"
qjs_sha="$(jq -r --arg name "$QJS_ASSET" '.assets[] | select(.name == $name) | .digest // ""' "$qjs_release")"
qjs_sha="${qjs_sha#sha256:}"
[[ "$qjs_sha" =~ ^[0-9a-f]{64}$ ]] || die "quickjs-ng $qjs_tag has no SHA-256 digest for $QJS_ASSET"

# CA bundle: the System Roots keychain of the build machine.
ca_tmp="$(mktemp -t mf-cacert)"
trap 'rm -f "$ca_tmp"' EXIT
security find-certificate -a -p "$ROOTS_KEYCHAIN" >"$ca_tmp"
ca_count="$(grep -c 'BEGIN CERTIFICATE' "$ca_tmp" || true)"
((ca_count >= 50)) || die "only $ca_count certificates in $ROOTS_KEYCHAIN"
ca_sha="$(sha256 "$ca_tmp")"
macos_version="$(sw_vers -productVersion)"

summary="CPython $python_version ($pbs_tag), yt-dlp $ytdlp_tag, quickjs-ng ${qjs_tag#v}, $ca_count CA roots"
# Inputs plus this script, so a changed trim list also rebuilds the runtime.
fingerprint="$(printf '%s\n' "$pbs_asset" "$pbs_sha" "$ytdlp_tag" "$ytdlp_sha" "$qjs_tag" "$qjs_sha" "$ca_sha" |
  cat - "${BASH_SOURCE[0]}" | shasum -a 256 | cut -d ' ' -f 1)"

if runtime_complete && [[ "$(jq -r '.fingerprint // ""' "$RUNTIME/VERSIONS.json")" == "$fingerprint" ]]; then
  log "Runtime up to date: $summary ($(jq -r '.size_bytes' "$RUNTIME/VERSIONS.json" | awk '{ printf "%.1f MB", $1 / 1048576 }'))"
  exit 0
fi

# 2. Downloads (cached by version, verified every time).
pbs_file="$DOWNLOADS/$pbs_asset"
ytdlp_file="$DOWNLOADS/yt-dlp-$ytdlp_tag"
qjs_file="$DOWNLOADS/$QJS_ASSET-$qjs_tag"
fetch_verified "https://github.com/$PBS_REPO/releases/download/$pbs_tag/${pbs_asset//+/%2B}" "$pbs_file" "$pbs_sha"
fetch_verified "https://github.com/$YTDLP_REPO/releases/download/$ytdlp_tag/yt-dlp" "$ytdlp_file" "$ytdlp_sha"
fetch_verified "https://github.com/$QJS_REPO/releases/download/$qjs_tag/$QJS_ASSET" "$qjs_file" "$qjs_sha"

# 3. Assemble in a staging directory.
log "Assembling runtime: $summary"
rm -rf "$STAGING"
mkdir -p "$STAGING"
tar -xzf "$pbs_file" -C "$STAGING"
[[ -x "$STAGING/python/bin/python$PYTHON_SERIES" ]] || die "unexpected layout in $pbs_asset"
install -m 0755 "$ytdlp_file" "$STAGING/yt-dlp"
install -m 0755 "$qjs_file" "$STAGING/qjs"
install -m 0644 "$ca_tmp" "$STAGING/cacert.pem"

untrimmed_kb="$(du -sk "$STAGING/python" | cut -f 1)"

# 4. Trim CPython to what an embedded, non-interactive yt-dlp needs. ssl, sqlite3,
# ctypes, zlib, bz2, lzma and hashlib are built into the interpreter in these builds.
py="$STAGING/python"
stdlib="$py/lib/python$PYTHON_SERIES"
[[ -d "$stdlib" ]] || die "unexpected layout: no lib/python$PYTHON_SERIES"
if otool -L "$py/bin/python$PYTHON_SERIES" | grep -q libpython; then
  die "python$PYTHON_SERIES links libpython dynamically; the trim list needs updating"
fi
find "$py/bin" -mindepth 1 ! -name "python$PYTHON_SERIES" ! -name python3 -exec rm -rf {} +
rm -rf "$py/include" "$py/share" "$py/lib/pkgconfig" "$py/lib/libpython"*.dylib
# Tcl/Tk for tkinter.
rm -rf "$py/lib/"itcl* "$py/lib/"tcl* "$py/lib/"tk* "$py/lib/"thread* "$py/lib/"libtcl* "$py/lib/"libtk*
rm -rf "$stdlib/"{test,idlelib,tkinter,turtledemo,ensurepip,pydoc_data,lib2to3,distutils,unittest,__phello__}
rm -rf "$stdlib/turtle.py" "$stdlib/"config-* "$stdlib/lib-dynload/"_tkinter*
# pip is not needed at runtime (updates replace the zipimport file).
find "$stdlib/site-packages" -mindepth 1 ! -name README.txt -exec rm -rf {} +
find "$py" \( -name __pycache__ -type d -prune -exec rm -rf {} + \) -o \( -name '*.a' -type f -exec rm -f {} + \)
[[ -L "$py/bin/python3" ]] || ln -s "python$PYTHON_SERIES" "$py/bin/python3"

# 5. Smoke tests against the trimmed runtime. -B: nothing may be written into it.
log "Checking the trimmed runtime"
staged_python="$STAGING/python/bin/python3"
"$staged_python" -I -B -c '
import sys, ssl, sqlite3, ctypes, zlib, bz2, lzma, hashlib, json
ctx = ssl.create_default_context(cafile=sys.argv[1])
assert len(ctx.get_ca_certs()) >= 50, "CA bundle did not load"
hashlib.sha256(b"").hexdigest(); hashlib.md5(b"", usedforsecurity=False)
sqlite3.connect(":memory:").execute("select 1")
ctypes.CDLL(None)
' "$STAGING/cacert.pem" || die "trimmed CPython is missing a required module"

# Every stdlib module yt-dlp imports must still import (catches trimmed dependencies).
"$staged_python" -I -B - "$STAGING/yt-dlp" <<'PY' || die "trimmed CPython cannot import a stdlib module yt-dlp uses"
import importlib, re, sys, zipfile

pattern = re.compile(rb"^\s*(?:from\s+([\w.]+)\s+import|import\s+([\w.]+(?:\s*,\s*[\w.]+)*))", re.M)
skip = {"winreg", "msvcrt", "_winapi", "winsound", "nt", "_overlapped", "ctypes.wintypes", "this", "antigravity"}
names = set()
with zipfile.ZipFile(sys.argv[1]) as archive:
    for name in archive.namelist():
        if name.endswith(".py"):
            for from_name, import_names in pattern.findall(archive.read(name)):
                for module in (from_name or import_names).split(b","):
                    names.add(module.strip().decode())
stdlib = sorted(n for n in names if n.split(".")[0] in sys.stdlib_module_names and n not in skip)
failed = []
for module in stdlib:
    try:
        importlib.import_module(module)
    except ImportError as error:
        failed.append(f"{module}: {error}")
if failed:
    print("\n".join(failed), file=sys.stderr)
    sys.exit(1)
print(f"    {len(stdlib)} stdlib modules used by yt-dlp import fine")
PY

staged_ytdlp_version="$(PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 "$staged_python" "$STAGING/yt-dlp" --version)"
[[ "$staged_ytdlp_version" == "$ytdlp_tag" ]] || die "bundled yt-dlp reports $staged_ytdlp_version, expected $ytdlp_tag"
[[ "$("$STAGING/qjs" -e 'console.log(6 * 7)')" == "42" ]] || die "qjs does not run"
if [[ -n "$(find "$STAGING" -name __pycache__ -o -name '*.pyc' | head -n 1)" ]]; then
  die "bytecode was written into the runtime during the checks"
fi

# 6. Record versions and swap the staging directory in.
size_kb="$(du -sk "$STAGING" | cut -f 1)"
file_count="$(find "$STAGING" -type f | wc -l | tr -d ' ')"
jq -n \
  --arg summary "$summary" \
  --arg fingerprint "$fingerprint" \
  --arg python_version "$python_version" --arg pbs_tag "$pbs_tag" --arg pbs_asset "$pbs_asset" --arg pbs_sha "$pbs_sha" \
  --arg ytdlp_version "$ytdlp_tag" --arg ytdlp_sha "$ytdlp_sha" \
  --arg qjs_version "${qjs_tag#v}" --arg qjs_asset "$QJS_ASSET" --arg qjs_sha "$qjs_sha" \
  --arg ca_source "$ROOTS_KEYCHAIN" --arg macos "$macos_version" --argjson ca_count "$ca_count" --arg ca_sha "$ca_sha" \
  --argjson size "$((size_kb * 1024))" --argjson files "$file_count" \
  '{
    summary: $summary,
    python: {version: $python_version, release: $pbs_tag, asset: $pbs_asset, sha256: $pbs_sha},
    yt_dlp: {version: $ytdlp_version, asset: "yt-dlp", sha256: $ytdlp_sha},
    quickjs: {version: $qjs_version, asset: $qjs_asset, sha256: $qjs_sha},
    ca_bundle: {version: $macos, source: $ca_source, certificates: $ca_count, sha256: $ca_sha},
    size_bytes: $size,
    files: $files,
    fingerprint: $fingerprint
  }' >"$STAGING/VERSIONS.json"

rm -rf "$RUNTIME"
mv "$STAGING" "$RUNTIME"

printf '==> Runtime ready in %s: %s\n' "${RUNTIME#"$ROOT_DIR"/}" "$summary"
awk -v before="$untrimmed_kb" -v after="$size_kb" -v files="$file_count" 'BEGIN {
  printf "    CPython %.1f MB before trimming; runtime %.1f MB in %d files\n", before / 1024, after / 1024, files
}'
