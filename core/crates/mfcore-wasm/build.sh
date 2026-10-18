#!/usr/bin/env bash
# Builds the browser engine: mfcore-wasm for wasm32-unknown-unknown, then
# wasm-bindgen (web target) and wasm-opt. The JS glue goes to extension/src/engine/wasm/
# (bundled with the engine Worker), the module to extension/public/engine/mfcore.wasm
# (copied as is: bundlers would otherwise inline 20 MB as base64 in some bundles).
#
#   crates/mfcore-wasm/build.sh [--dev] [--no-opt] [--out DIR] [--wasm-out FILE]
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
wasm_out="$repo/extension/public/engine/mfcore.wasm"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev) profile=wasm-dev ;;
    --no-opt) opt=0 ;;
    --out) out="$2"; shift ;;
    --wasm-out) wasm_out="$2"; shift ;;
    -h | --help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
# The glue's default module URL (`new URL('…_bg.wasm', import.meta.url)`) makes bundlers
# inline the 20 MB module into the worker bundle. Callers always pass the module (or
# its URL) to init, so the default goes.
sed -i.bak "s|new URL('mfcore_wasm_bg.wasm', import.meta.url)|undefined|" "$out/mfcore_wasm.js" && rm -f "$out/mfcore_wasm.js.bak"
grep -q "import.meta.url" "$out/mfcore_wasm.js" && { echo "error: the glue still names its module URL" >&2; exit 1; }
mkdir -p "$(dirname "$wasm_out")"
if (( opt )) && command -v wasm-opt >/dev/null; then
  wasm-opt -Oz --enable-simd --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext \
    --enable-mutable-globals --enable-reference-types --enable-multivalue \
    "$out/mfcore_wasm_bg.wasm" -o "$wasm_out"
  rm -f "$out/mfcore_wasm_bg.wasm"
else
  mv "$out/mfcore_wasm_bg.wasm" "$wasm_out"
fi
ls -l "$wasm_out" | awk '{printf "%s: %.1f MB\n", $NF, $5 / 1048576}'
