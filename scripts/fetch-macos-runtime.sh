#!/usr/bin/env bash
# Download, verify, trim and merge the universal (arm64 + x86_64) media runtime
# bundled in the macOS app.
#
#   scripts/fetch-macos-runtime.sh [--offline]
#
# Result: apple/build/runtime/ (gitignored), every Mach-O universal, macOS 15 or later:
#   python/        python-build-standalone CPython (install_only_stripped), trimmed;
#                  the aarch64 and x86_64 builds of one release merged with lipo
#   yt-dlp         yt-dlp zipimport binary (includes the yt-dlp-ejs challenge scripts)
#   qjs            quickjs-ng, JS runtime for YouTube challenges, built from source
#                  (the published macOS binaries require macOS 26)
#   cacert.pem     CA bundle exported from the System Roots keychain
#   VERSIONS.json  what went in, with checksums
#
# python-build-standalone and yt-dlp are checked against their release's published
# SHA-256 (SHA256SUMS, SHA2-256SUMS). quickjs-ng is cloned at its release tag and the
# checked-out commit must match the tag (git verifies every object by hash). Downloads
# are cached in apple/build/downloads/ and the runtime is only reassembled when a
# version changes.
#
# Latest releases are used unless pinned:
#   MF_PYTHON_SERIES   CPython minor series (default 3.13)
#   MF_PBS_RELEASE     python-build-standalone release tag, e.g. 20261013
#   MF_YTDLP_VERSION   yt-dlp release tag, e.g. 2026.08.19
#   MF_QJS_VERSION     quickjs-ng release tag, e.g. v0.17.0
#
# The x86_64 slices are executed under Rosetta when it is installed; otherwise they are
# only checked with lipo and a warning is printed.
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
ROOTS_KEYCHAIN="/System/Library/Keychains/SystemRootCertificates.keychain"
# Oldest macOS the app supports (apple/project.yml).
MIN_MACOS="15.0"
# python-build-standalone triple -> Mach-O architecture.
PBS_TRIPLES=(aarch64-apple-darwin x86_64-apple-darwin)
ARCHS=(arm64 x86_64)
# Text files that legitimately differ between the two CPython builds: build-time
# configuration records (compiler flags and target triple). The arm64 copy is kept;
# sysconfig only reports them, yt-dlp never reads them.
ARCH_SPECIFIC_FILES_RE='/_sysconfigdata_[^/]*\.py$|/_sysconfig_vars_[^/]*\.json$|/build-details\.json$'

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
is_macho() { file -b "$1" | grep -q '^Mach-O'; }

runtime_complete() {
  [[ -f "$RUNTIME/VERSIONS.json" && -x "$RUNTIME/python/bin/python3" && -f "$RUNTIME/yt-dlp" &&
    -x "$RUNTIME/qjs" && -s "$RUNTIME/cacert.pem" ]] &&
    [[ "$(jq -r '.architectures // [] | join(" ")' "$RUNTIME/VERSIONS.json")" == "${ARCHS[*]}" ]]
}

reuse_existing() {
  runtime_complete || die "$1, and there is no universal runtime in ${RUNTIME#"$ROOT_DIR"/} to fall back to"
  warn "$1; reusing ${RUNTIME#"$ROOT_DIR"/} ($(jq -r '.summary' "$RUNTIME/VERSIONS.json"))"
  exit 0
}

if ((OFFLINE)); then
  runtime_complete || die "--offline needs an existing universal runtime in ${RUNTIME#"$ROOT_DIR"/}"
  log "Using existing runtime ($(jq -r '.summary' "$RUNTIME/VERSIONS.json"))"
  exit 0
fi

command -v jq >/dev/null || die "jq is required (macOS 15+ ships /usr/bin/jq)"
command -v git >/dev/null || die "git is required (xcode-select --install)"
xcrun --find clang >/dev/null 2>&1 || die "clang is required (Xcode or the Command Line Tools)"
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
  curl "${curl_args[@]}" --output "$2.part" "$1" && mv "$2.part" "$2"
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

# Commit a tag points to (peeled for annotated tags), via the git protocol.
tag_commit() {
  local refs
  refs="$(git ls-remote "https://github.com/$1.git" "refs/tags/$2" "refs/tags/$2^{}")" || return 1
  { grep "refs/tags/$2^{}\$" <<<"$refs" || grep "refs/tags/$2\$" <<<"$refs"; } | head -n 1 | cut -f 1
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
pbs_assets=()
pbs_shas=()
python_version=""
for triple in "${PBS_TRIPLES[@]}"; do
  line="$(grep -E "  cpython-${PYTHON_SERIES//./\\.}\.[0-9]+\+${pbs_tag}-${triple}-install_only_stripped\.tar\.gz\$" "$pbs_sums" | tail -n 1)" ||
    die "python-build-standalone $pbs_tag has no CPython $PYTHON_SERIES $triple install_only_stripped build"
  asset="${line##* }"
  version="${asset#cpython-}"
  version="${version%%+*}"
  [[ -z "$python_version" || "$python_version" == "$version" ]] ||
    die "python-build-standalone $pbs_tag has CPython $python_version and $version for the two architectures"
  python_version="$version"
  pbs_assets+=("$asset")
  pbs_shas+=("${line%% *}")
done

ytdlp_sums="$DOWNLOADS/yt-dlp-$ytdlp_tag.SHA2-256SUMS"
fetch_cached "https://github.com/$YTDLP_REPO/releases/download/$ytdlp_tag/SHA2-256SUMS" "$ytdlp_sums" ||
  die "cannot download SHA2-256SUMS for yt-dlp $ytdlp_tag"
ytdlp_sha="$(awk '$2 == "yt-dlp" { print $1 }' "$ytdlp_sums")"
[[ "$ytdlp_sha" =~ ^[0-9a-f]{64}$ ]] || die "yt-dlp $ytdlp_tag SHA2-256SUMS has no entry for the zipimport binary"

qjs_commit="$(tag_commit "$QJS_REPO" "$qjs_tag")" || reuse_existing "cannot reach GitHub"
[[ "$qjs_commit" =~ ^[0-9a-f]{40}$ ]] || die "quickjs-ng has no tag $qjs_tag"

# CA bundle: the System Roots keychain of the build machine.
ca_tmp="$(mktemp -t mf-cacert)"
trap 'rm -f "$ca_tmp"' EXIT
security find-certificate -a -p "$ROOTS_KEYCHAIN" >"$ca_tmp"
ca_count="$(grep -c 'BEGIN CERTIFICATE' "$ca_tmp" || true)"
((ca_count >= 50)) || die "only $ca_count certificates in $ROOTS_KEYCHAIN"
ca_sha="$(sha256 "$ca_tmp")"
macos_version="$(sw_vers -productVersion)"

summary="CPython $python_version ($pbs_tag), yt-dlp $ytdlp_tag, quickjs-ng ${qjs_tag#v}, $ca_count CA roots, ${ARCHS[*]}"
# Inputs plus this script, so a changed trim list also rebuilds the runtime.
fingerprint="$(printf '%s\n' "${pbs_assets[@]}" "${pbs_shas[@]}" "$ytdlp_tag" "$ytdlp_sha" "$qjs_tag" "$qjs_commit" "$ca_sha" |
  cat - "${BASH_SOURCE[0]}" | shasum -a 256 | cut -d ' ' -f 1)"

if runtime_complete && [[ "$(jq -r '.fingerprint // ""' "$RUNTIME/VERSIONS.json")" == "$fingerprint" ]]; then
  log "Runtime up to date: $summary ($(jq -r '.size_bytes' "$RUNTIME/VERSIONS.json" | awk '{ printf "%.1f MB", $1 / 1048576 }'))"
  exit 0
fi

# 2. Downloads (cached by version, verified every time).
pbs_files=()
for index in "${!pbs_assets[@]}"; do
  asset="${pbs_assets[$index]}"
  fetch_verified "https://github.com/$PBS_REPO/releases/download/$pbs_tag/${asset//+/%2B}" "$DOWNLOADS/$asset" "${pbs_shas[$index]}"
  pbs_files+=("$DOWNLOADS/$asset")
done
ytdlp_file="$DOWNLOADS/yt-dlp-$ytdlp_tag"
fetch_verified "https://github.com/$YTDLP_REPO/releases/download/$ytdlp_tag/yt-dlp" "$ytdlp_file" "$ytdlp_sha"

qjs_src="$DOWNLOADS/quickjs-ng-$qjs_tag"
if [[ ! -d "$qjs_src/.git" || "$(git -C "$qjs_src" rev-parse HEAD 2>/dev/null)" != "$qjs_commit" ]]; then
  log "Cloning quickjs-ng $qjs_tag"
  rm -rf "$qjs_src"
  git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$qjs_tag" "https://github.com/$QJS_REPO.git" "$qjs_src"
fi
[[ "$(git -C "$qjs_src" rev-parse HEAD)" == "$qjs_commit" ]] || die "quickjs-ng $qjs_tag checkout is not commit $qjs_commit"
git -C "$qjs_src" diff --quiet HEAD || die "the quickjs-ng checkout in ${qjs_src#"$ROOT_DIR"/} has local changes"

# 3. Assemble in a staging directory: one trimmed CPython per architecture, then merged.
log "Assembling runtime: $summary"
rm -rf "$STAGING"
mkdir -p "$STAGING/arch"

# Trim CPython to what an embedded, non-interactive yt-dlp needs. ssl, sqlite3, ctypes,
# zlib, bz2, lzma and hashlib are built into the interpreter in these builds.
trim_python() {
  local py="$1" stdlib="$1/lib/python$PYTHON_SERIES"
  [[ -x "$py/bin/python$PYTHON_SERIES" ]] || die "unexpected layout in ${py#"$STAGING"/}"
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
}

untrimmed_kb=0
for index in "${!ARCHS[@]}"; do
  tree="$STAGING/arch/${ARCHS[$index]}"
  mkdir -p "$tree"
  tar -xzf "${pbs_files[$index]}" -C "$tree"
  untrimmed_kb=$((untrimmed_kb + $(du -sk "$tree/python" | cut -f 1)))
  trim_python "$tree/python"
done

# Merge: Mach-O files become universal with lipo; everything else must be identical
# (apart from the build-configuration records above), and both trees must hold the
# same files and symlinks.
base="$STAGING/arch/${ARCHS[0]}/python"
other="$STAGING/arch/${ARCHS[1]}/python"
listing() { (cd "$1" && find . \( -type f -o -type l \) | LC_ALL=C sort); }
if ! diff <(listing "$base") <(listing "$other") >"$STAGING/tree.diff"; then
  cat "$STAGING/tree.diff" >&2
  die "the ${ARCHS[0]} and ${ARCHS[1]} CPython trees hold different files"
fi
ditto "$base" "$STAGING/python"
merged=0
identical=0
kept=()
while IFS= read -r -d '' file; do
  relative="${file#"$base"/}"
  counterpart="$other/$relative"
  target="$STAGING/python/$relative"
  if [[ -L "$file" ]]; then
    [[ "$(readlink "$file")" == "$(readlink "$counterpart")" ]] || die "symlink $relative differs between architectures"
  elif is_macho "$file"; then
    is_macho "$counterpart" || die "$relative is Mach-O only for ${ARCHS[0]}"
    lipo -create "$file" "$counterpart" -output "$target"
    merged=$((merged + 1))
  elif cmp -s "$file" "$counterpart"; then
    identical=$((identical + 1))
  elif [[ "/$relative" =~ $ARCH_SPECIFIC_FILES_RE ]]; then
    kept+=("$relative")
  else
    die "$relative differs between the ${ARCHS[0]} and ${ARCHS[1]} builds"
  fi
done < <(find "$base" \( -type f -o -type l \) -print0)
echo "    $merged Mach-O files merged, $identical other files identical on both architectures"
for relative in ${kept[@]+"${kept[@]}"}; do
  echo "    kept the ${ARCHS[0]} build record $relative"
done
rm -rf "$STAGING/arch"

install -m 0755 "$ytdlp_file" "$STAGING/yt-dlp"
install -m 0644 "$ca_tmp" "$STAGING/cacert.pem"

# quickjs-ng: the qjs CLI from its CMake sources, both architectures in one clang run.
log "Building quickjs-ng ${qjs_tag#v} (${ARCHS[*]}, macOS $MIN_MACOS)"
qjs_sources=(dtoa.c libregexp.c libunicode.c quickjs.c quickjs-libc.c gen/repl.c gen/standalone.c qjs.c)
for source in "${qjs_sources[@]}"; do
  [[ -f "$qjs_src/$source" ]] || die "quickjs-ng $qjs_tag has no $source; the build list needs updating"
done
arch_flags=()
for arch in "${ARCHS[@]}"; do arch_flags+=(-arch "$arch"); done
(
  cd "$qjs_src"
  xcrun clang -std=gnu11 -O2 -funsigned-char -D_GNU_SOURCE -DNDEBUG -w \
    "${arch_flags[@]}" -mmacosx-version-min="$MIN_MACOS" -ffile-prefix-map="$qjs_src"=/quickjs \
    -I . "${qjs_sources[@]}" -o "$STAGING/qjs" -lm
)
install -m 0644 "$qjs_src/LICENSE" "$STAGING/quickjs-LICENSE"

# 4. Every Mach-O must carry both architectures and run on the oldest supported macOS.
log "Checking architectures and deployment targets"
version_le() { [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -t . -k 1,1n -k 2,2n -k 3,3n | head -n 1)" == "$1" ]]; }
macho_count=0
while IFS= read -r -d '' file; do
  is_macho "$file" || continue
  macho_count=$((macho_count + 1))
  archs="$(lipo -archs "$file")"
  for arch in "${ARCHS[@]}"; do
    [[ " $archs " == *" $arch "* ]] || die "${file#"$STAGING"/} lacks $arch ($archs)"
  done
  while read -r minos; do
    version_le "$minos" "$MIN_MACOS" || die "${file#"$STAGING"/} requires macOS $minos"
  done < <(otool -l "$file" | awk '/LC_BUILD_VERSION/ { b = 1 } b && $1 == "minos" { print $2; b = 0 }
                                    /LC_VERSION_MIN_MACOSX/ { v = 1 } v && $1 == "version" { print $2; v = 0 }')
done < <(find "$STAGING" -type f -print0)
echo "    $macho_count Mach-O files, all ${ARCHS[*]}, macOS $MIN_MACOS or older"

# 5. Smoke tests against the trimmed runtime, on every architecture this Mac can run.
# -B: nothing may be written into it.
run_archs=(arm64)
[[ "$(uname -m)" == "x86_64" ]] && run_archs=(x86_64)
if [[ "$(uname -m)" == "arm64" ]]; then
  if arch -x86_64 /usr/bin/true 2>/dev/null; then
    run_archs+=(x86_64)
  else
    warn "Rosetta is not installed: the x86_64 runtime was only checked with lipo, not run"
  fi
fi
staged_python="$STAGING/python/bin/python3"
for arch in "${run_archs[@]}"; do
  log "Checking the trimmed runtime ($arch)"
  [[ "$(arch "-$arch" "$staged_python" -I -B -c 'import platform; print(platform.machine())')" == "$arch" ]] ||
    die "python3 did not run as $arch"
  arch "-$arch" "$staged_python" -I -B -c '
import sys, ssl, sqlite3, ctypes, zlib, bz2, lzma, hashlib, json
ctx = ssl.create_default_context(cafile=sys.argv[1])
assert len(ctx.get_ca_certs()) >= 50, "CA bundle did not load"
hashlib.sha256(b"").hexdigest(); hashlib.md5(b"", usedforsecurity=False)
sqlite3.connect(":memory:").execute("select 1")
ctypes.CDLL(None)
' "$STAGING/cacert.pem" || die "trimmed CPython ($arch) is missing a required module"

  # Every stdlib module yt-dlp imports must still import (catches trimmed dependencies).
  arch "-$arch" "$staged_python" -I -B - "$STAGING/yt-dlp" <<'PY' || die "trimmed CPython ($arch) cannot import a stdlib module yt-dlp uses"
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

  staged_ytdlp_version="$(PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 arch "-$arch" "$staged_python" "$STAGING/yt-dlp" --version)"
  [[ "$staged_ytdlp_version" == "$ytdlp_tag" ]] || die "bundled yt-dlp ($arch) reports $staged_ytdlp_version, expected $ytdlp_tag"
  [[ "$(arch "-$arch" "$STAGING/qjs" -e 'console.log(6 * 7)')" == "42" ]] || die "qjs ($arch) does not run"
done
if [[ -n "$(find "$STAGING" -name __pycache__ -o -name '*.pyc' | head -n 1)" ]]; then
  die "bytecode was written into the runtime during the checks"
fi

# 6. Record versions and swap the staging directory in.
size_kb="$(du -sk "$STAGING" | cut -f 1)"
file_count="$(find "$STAGING" -type f | wc -l | tr -d ' ')"
jq -n \
  --arg summary "$summary" \
  --arg fingerprint "$fingerprint" \
  --argjson archs "$(printf '%s\n' "${ARCHS[@]}" | jq -R . | jq -s .)" \
  --arg min_macos "$MIN_MACOS" \
  --arg python_version "$python_version" --arg pbs_tag "$pbs_tag" \
  --argjson pbs_assets "$(for i in "${!pbs_assets[@]}"; do jq -n --arg a "${pbs_assets[$i]}" --arg s "${pbs_shas[$i]}" '{asset: $a, sha256: $s}'; done | jq -s .)" \
  --arg ytdlp_version "$ytdlp_tag" --arg ytdlp_sha "$ytdlp_sha" \
  --arg qjs_version "${qjs_tag#v}" --arg qjs_commit "$qjs_commit" \
  --arg ca_source "$ROOTS_KEYCHAIN" --arg macos "$macos_version" --argjson ca_count "$ca_count" --arg ca_sha "$ca_sha" \
  --argjson size "$((size_kb * 1024))" --argjson files "$file_count" \
  '{
    summary: $summary,
    architectures: $archs,
    minimum_macos: $min_macos,
    python: {version: $python_version, release: $pbs_tag, builds: $pbs_assets},
    yt_dlp: {version: $ytdlp_version, asset: "yt-dlp", sha256: $ytdlp_sha},
    quickjs: {version: $qjs_version, source: "git tag", commit: $qjs_commit},
    ca_bundle: {version: $macos, source: $ca_source, certificates: $ca_count, sha256: $ca_sha},
    size_bytes: $size,
    files: $files,
    fingerprint: $fingerprint
  }' >"$STAGING/VERSIONS.json"

rm -rf "$RUNTIME"
mv "$STAGING" "$RUNTIME"

printf '==> Runtime ready in %s: %s\n' "${RUNTIME#"$ROOT_DIR"/}" "$summary"
awk -v before="$untrimmed_kb" -v after="$size_kb" -v files="$file_count" 'BEGIN {
  printf "    CPython %.1f MB before trimming (both architectures); runtime %.1f MB in %d files\n", before / 1024, after / 1024, files
}'
