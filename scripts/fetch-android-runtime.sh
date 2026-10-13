#!/usr/bin/env bash
# Fetch the JavaScript runtime yt-dlp uses on Android (quickjs-ng, static Linux builds).
#
#   scripts/fetch-android-runtime.sh [--latest] [--force]
#
# Installs android/app/src/main/jniLibs/<abi>/libqjs.so (gitignored). The app packages
# native libraries uncompressed and extracted, so the binary can be executed from
# nativeLibraryDir (Android 10+ refuses to exec files from writable app storage).
#
#   --latest   use the newest quickjs-ng release instead of the pinned one
#   --force    download again even if the installed binaries match
set -euo pipefail

QJS_VERSION="0.17.0"
# SHA-256 of the pinned release assets.
QJS_SHA256_aarch64="3372133484edf50a69f3c67903af41206d22a061e930e3cfb63269272ef56d2e"
QJS_SHA256_x86_64="0bfc02511a9f549c28b53880d988fc7cd5d361e90c5e8afdfcd7dc6774ceace5"
NDK_VERSION="29.0.14206865"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JNI_LIBS="$ROOT/android/app/src/main/jniLibs"

latest=0
force=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --latest) latest=1 ;;
    --force) force=1 ;;
    -h | --help) sed -n '2,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{ print $1 }'
  else
    shasum -a 256 "$1" | awk '{ print $1 }'
  fi
}

# llvm-readelf from the NDK (or a system readelf) checks that the binaries are static.
android_home="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
if [[ -z "$android_home" ]]; then
  if [[ "$(uname -s)" == Darwin ]]; then
    android_home="$HOME/Library/Android/sdk"
  else
    android_home="$HOME/Android/Sdk"
  fi
fi
ndk="${ANDROID_NDK_HOME:-$android_home/ndk/$NDK_VERSION}"
readelf_bin=""
for candidate in "$ndk"/toolchains/llvm/prebuilt/*/bin/llvm-readelf; do
  [[ -x "$candidate" ]] && readelf_bin="$candidate" && break
done
[[ -n "$readelf_bin" ]] || readelf_bin="$(command -v llvm-readelf || command -v readelf || true)"
[[ -n "$readelf_bin" ]] || { echo "error: llvm-readelf not found (set ANDROID_NDK_HOME)" >&2; exit 1; }

version="$QJS_VERSION"
sha_aarch64="$QJS_SHA256_aarch64"
sha_x86_64="$QJS_SHA256_x86_64"
if (( latest )); then
  release_json="$(curl -fsSL https://api.github.com/repos/quickjs-ng/quickjs/releases/latest)"
  digests="$(printf '%s' "$release_json" | python3 -I -c '
import json, sys
release = json.load(sys.stdin)
digests = {a["name"]: (a.get("digest") or "").removeprefix("sha256:") for a in release["assets"]}
print(release["tag_name"].lstrip("v"), digests.get("qjs-linux-aarch64", ""), digests.get("qjs-linux-x86_64", ""))
')"
  read -r version sha_aarch64 sha_x86_64 <<< "$digests"
  [[ -n "$sha_aarch64" && -n "$sha_x86_64" ]] || { echo "error: release v$version lacks Linux digests" >&2; exit 1; }
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

echo "==> quickjs-ng $version"
for abi in arm64-v8a x86_64; do
  if [[ "$abi" == arm64-v8a ]]; then
    arch=aarch64 want="$sha_aarch64" machine_pattern='AArch64'
  else
    arch=x86_64 want="$sha_x86_64" machine_pattern='X86-64'
  fi
  dest="$JNI_LIBS/$abi/libqjs.so"
  if (( !force )) && [[ -f "$dest" ]] && [[ "$(sha256 "$dest")" == "$want" ]]; then
    echo "    $abi: up to date"
    continue
  fi

  tmp="$workdir/qjs-linux-$arch"
  curl -fsSL --retry 3 -o "$tmp" "https://github.com/quickjs-ng/quickjs/releases/download/v$version/qjs-linux-$arch"
  actual="$(sha256 "$tmp")"
  if [[ "$actual" != "$want" ]]; then
    echo "error: qjs-linux-$arch checksum mismatch (got $actual)" >&2
    exit 1
  fi

  # Android has no glibc/musl loader for it: the binary must be static (no PT_INTERP).
  headers="$("$readelf_bin" -h -l "$tmp")"
  if grep -q 'INTERP' <<< "$headers"; then
    echo "error: qjs-linux-$arch is dynamically linked (has PT_INTERP)" >&2
    exit 1
  fi
  if ! grep -q "Machine:.*$machine_pattern" <<< "$headers"; then
    echo "error: qjs-linux-$arch is not an $machine_pattern binary" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$dest")"
  install -m 0755 "$tmp" "$dest"
  echo "    $abi: $(wc -c < "$dest" | tr -d ' ') bytes, static $machine_pattern"
done
