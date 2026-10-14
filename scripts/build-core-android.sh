#!/usr/bin/env bash
# Build mfcore for Android and generate its Kotlin bindings.
#
#   scripts/build-core-android.sh [--debug] [--abis arm64-v8a,x86_64]
#
# Outputs (both gitignored):
#   android/core/src/main/jniLibs/<abi>/libmfcore.so
#   android/core/src/main/kotlin/app/musicfetch/core/mfcore.kt
#
# Environment: ANDROID_HOME (or ANDROID_SDK_ROOT), ANDROID_NDK_HOME. Defaults to the
# SDK in ~/Library/Android/sdk (macOS) or ~/Android/Sdk (Linux) with NDK r29.
set -euo pipefail

# Keep local paths out of the binaries.
# shellcheck source=lib/rust-env.sh
source "$(dirname "$0")/lib/rust-env.sh"

NDK_VERSION="29.0.14206865"
MIN_SDK=29
ALIGN_MIN=16384

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORE="$ROOT/core"
JNI_LIBS="$ROOT/android/core/src/main/jniLibs"
KOTLIN_OUT="$ROOT/android/core/src/main/kotlin"
UNIFFI_CONFIG="$CORE/crates/mfcore/uniffi.toml"

profile=release
abis="arm64-v8a,x86_64"

usage() {
  sed -n '2,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug) profile=debug ;;
    --abis)
      [[ $# -ge 2 ]] || { echo "error: --abis needs a value" >&2; exit 2; }
      abis="$2"
      shift
      ;;
    --abis=*) abis="${1#--abis=}" ;;
    -h | --help) usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

# rustup's proxies must win over Homebrew's rust, which has no Android targets.
for dir in "$HOME/.cargo/bin" /opt/homebrew/opt/rustup/bin; do
  [[ -d "$dir" ]] && PATH="$dir:$PATH"
done
export PATH

if [[ -z "${ANDROID_HOME:-}" ]]; then
  if [[ -n "${ANDROID_SDK_ROOT:-}" ]]; then
    ANDROID_HOME="$ANDROID_SDK_ROOT"
  elif [[ "$(uname -s)" == Darwin ]]; then
    ANDROID_HOME="$HOME/Library/Android/sdk"
  else
    ANDROID_HOME="$HOME/Android/Sdk"
  fi
fi
export ANDROID_HOME
export ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-$ANDROID_HOME/ndk/$NDK_VERSION}"
[[ -d "$ANDROID_NDK_HOME" ]] || { echo "error: NDK not found at $ANDROID_NDK_HOME" >&2; exit 1; }

llvm_bin=""
for dir in "$ANDROID_NDK_HOME"/toolchains/llvm/prebuilt/*/bin; do
  [[ -x "$dir/llvm-objdump" ]] && llvm_bin="$dir" && break
done
[[ -n "$llvm_bin" ]] || { echo "error: llvm-objdump not found in $ANDROID_NDK_HOME" >&2; exit 1; }

command -v cargo-ndk >/dev/null || { echo "error: cargo-ndk is not installed (cargo install cargo-ndk --version 4.1.2 --locked)" >&2; exit 1; }

triple_for() {
  case "$1" in
    arm64-v8a) echo aarch64-linux-android ;;
    x86_64) echo x86_64-linux-android ;;
    *) return 1 ;;
  esac
}

IFS=',' read -r -a abi_list <<< "$abis"
[[ ${#abi_list[@]} -gt 0 ]] || { echo "error: no ABIs given" >&2; exit 2; }
target_args=()
for abi in "${abi_list[@]}"; do
  triple_for "$abi" >/dev/null || { echo "error: unsupported ABI '$abi' (arm64-v8a, x86_64)" >&2; exit 2; }
  target_args+=(-t "$abi")
done

cargo_profile_args=()
[[ "$profile" == release ]] && cargo_profile_args=(--release)

echo "==> Building mfcore ($profile) for ${abi_list[*]} (API $MIN_SDK, NDK $(basename "$ANDROID_NDK_HOME"))"
# Stale ABIs would ship libraries whose checksums no longer match the bindings.
rm -rf "$JNI_LIBS"
mkdir -p "$JNI_LIBS"
(
  cd "$CORE"
  cargo ndk "${target_args[@]}" -P "$MIN_SDK" -o "$JNI_LIBS" build -p mfcore ${cargo_profile_args[@]+"${cargo_profile_args[@]}"}
)

# Generate from the unstripped library of the same build that ships, so the
# embedded checksums match. Library mode reads core/crates/mfcore/uniffi.toml
# (package app.musicfetch.core) through cargo metadata, so run it inside core/.
first_triple="$(triple_for "${abi_list[0]}")"
bindgen_lib="$CORE/target/$first_triple/$profile/libmfcore.so"
[[ -f "$bindgen_lib" ]] || { echo "error: $bindgen_lib was not built" >&2; exit 1; }

echo "==> Generating Kotlin bindings from $first_triple/$profile/libmfcore.so"
rm -f "$KOTLIN_OUT/app/musicfetch/core/mfcore.kt"
mkdir -p "$KOTLIN_OUT"
(
  cd "$CORE"
  cargo build --quiet -p uniffi-bindgen --bin uniffi-bindgen
  ./target/debug/uniffi-bindgen generate "$bindgen_lib" \
    --language kotlin \
    --out-dir "$KOTLIN_OUT" \
    --no-format
)
generated="$KOTLIN_OUT/app/musicfetch/core/mfcore.kt"
[[ -f "$generated" ]] || { echo "error: bindings were not generated" >&2; exit 1; }
grep -q '^package app.musicfetch.core' "$generated" \
  || { echo "error: bindings ignored $UNIFFI_CONFIG (wrong package)" >&2; exit 1; }

echo "==> Checking 16 KB page alignment"
status=0
for abi in "${abi_list[@]}"; do
  lib="$JNI_LIBS/$abi/libmfcore.so"
  [[ -f "$lib" ]] || { echo "error: missing $lib" >&2; status=1; continue; }
  aligns="$("$llvm_bin/llvm-objdump" -p "$lib" | awk '$1 == "LOAD" { print $NF }')"
  [[ -n "$aligns" ]] || { echo "error: no LOAD segments in $lib" >&2; status=1; continue; }
  for align in $aligns; do
    exponent="${align#"2**"}"
    if [[ ! "$exponent" =~ ^[0-9]+$ ]] || (( (1 << exponent) < ALIGN_MIN )); then
      echo "error: $abi/libmfcore.so has a LOAD segment aligned to $align (need >= 2**14)" >&2
      status=1
    fi
  done
  size="$(wc -c < "$lib" | tr -d ' ')"
  printf '    %-10s libmfcore.so %6.1f MB, LOAD align %s\n' "$abi" "$(awk -v b="$size" 'BEGIN { print b / 1048576 }')" \
    "$(echo "$aligns" | sort -u | tr '\n' ' ')"
done
exit "$status"
