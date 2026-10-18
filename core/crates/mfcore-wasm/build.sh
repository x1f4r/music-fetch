#!/usr/bin/env bash
# Builds the browser engine: mfcore-wasm for wasm32-unknown-unknown, then
# wasm-bindgen (web target) and wasm-opt, into extension/src/engine/wasm/ (or --out).
#
#   crates/mfcore-wasm/build.sh [--dev] [--no-opt] [--out DIR]
#
#   --dev     the wasm-dev profile (no LTO, parallel codegen; several times faster)
#   --no-opt  skip wasm-opt
#
# Needs: the wasm32-unknown-unknown target (core/rust-toolchain.toml), wasm-bindgen-cli
# 0.2.129 (the version in Cargo.lock), a clang that targets wasm32 for SQLite (Linux
# clang, or Homebrew LLVM on macOS: Apple's clang has no wasm32 backend), and
# wasm-opt (binaryen; optional).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
core="$(cd "$here/../.." && pwd)"
repo="$(cd "$core/.." && pwd)"
profile=wasm
opt=1
out="$repo/extension/src/engine/wasm"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev) profile=wasm-dev ;;
    --no-opt) opt=0 ;;
    --out) out="$2"; shift ;;
    -h | --help) sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "error: unknown option $1" >&2; exit 2 ;;
  esac
  shift
done

# A C compiler for SQLite's wasm32 build.
if [[ -z "${CC_wasm32_unknown_unknown:-}" ]]; then
  for cc in /opt/homebrew/opt/llvm/bin/clang /usr/local/opt/llvm/bin/clang clang; do
    if command -v "$cc" >/dev/null && echo 'int x;' | "$cc" --target=wasm32-unknown-unknown -c -x c - -o /dev/null 2>/dev/null; then
      export CC_wasm32_unknown_unknown="$cc"
      ar="$(dirname "$(command -v "$cc")")/llvm-ar"
      [[ -x "$ar" ]] || ar="$(command -v llvm-ar || true)"
      [[ -n "$ar" ]] && export AR_wasm32_unknown_unknown="$ar"
      break
    fi
  done
fi
[[ -n "${CC_wasm32_unknown_unknown:-}" ]] || { echo "error: no clang that targets wasm32 (install LLVM)" >&2; exit 1; }

want="$(grep -A1 '^name = "wasm-bindgen"$' "$core/Cargo.lock" | sed -n 's/^version = "\(.*\)"/\1/p' | head -1)"
have="$(wasm-bindgen --version 2>/dev/null | awk '{print $2}')"
[[ "$have" == "$want" ]] || { echo "error: wasm-bindgen-cli $want is needed (found: ${have:-none}): cargo install --locked wasm-bindgen-cli --version $want" >&2; exit 1; }

# No local paths in the module (panic messages carry source paths).
export RUSTFLAGS="${RUSTFLAGS:-} -C target-feature=+simd128 --remap-path-prefix=$repo=/src --remap-path-prefix=${CARGO_HOME:-$HOME/.cargo}=/cargo --remap-path-prefix=${RUSTUP_HOME:-$HOME/.rustup}=/rustup"
target_dir="${CARGO_TARGET_DIR:-$core/target}"
(cd "$core" && cargo build -p mfcore-wasm --lib --target wasm32-unknown-unknown --profile "$profile")
module="$target_dir/wasm32-unknown-unknown/$profile/mfcore_wasm.wasm"

mkdir -p "$out"
wasm-bindgen --target web --out-dir "$out" --out-name mfcore_wasm "$module"
if (( opt )) && command -v wasm-opt >/dev/null; then
  wasm-opt -Oz --enable-simd --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext \
    --enable-mutable-globals --enable-reference-types --enable-multivalue \
    "$out/mfcore_wasm_bg.wasm" -o "$out/mfcore_wasm_bg.wasm.opt"
  mv "$out/mfcore_wasm_bg.wasm.opt" "$out/mfcore_wasm_bg.wasm"
fi
ls -l "$out"/mfcore_wasm_bg.wasm | awk '{printf "%s: %.1f MB\n", $NF, $5 / 1048576}'
