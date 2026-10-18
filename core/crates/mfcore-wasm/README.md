# mfcore-wasm

The Music Fetch engine for the browser extension: the whole `mfcore` built for
`wasm32-unknown-unknown`, behind a wasm-bindgen facade that mirrors the apps'
UniFFI `Engine` (see the crate docs in `src/lib.rs`). It runs in the extension's
engine Worker, with its library in SQLite on OPFS.

## Build

```sh
crates/mfcore-wasm/build.sh          # release: fat LTO, wasm-opt -Oz
crates/mfcore-wasm/build.sh --dev    # several times faster, for tests
```

The JS glue goes to `extension/src/engine/wasm/`, the module to
`extension/public/engine/mfcore.wasm` (both ignored by git; `npm run build:engine`
in `extension/` runs the same script). Needs wasm-bindgen-cli at the version in
`Cargo.lock`, a clang that targets wasm32 (Linux clang or Homebrew LLVM; Apple's
clang has none) and, optionally, wasm-opt.

## Test

```sh
# The TypeScript types (extension/src/engine/types.gen.ts) are current
cargo test -p mfcore-wasm --test types            # MF_WRITE_TYPES=1 rewrites them

# The facade's API in Node, then in Chrome and Firefox dedicated Workers
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test -p mfcore-wasm --target wasm32-unknown-unknown --profile wasm-dev --test api
WASM_BINDGEN_USE_DEDICATED_WORKER=1 CHROMEDRIVER=… cargo test … --test api

# A live session on a WAV in Node against a stub recognizer
node crates/mfcore-wasm/smoke/live-smoke.mjs [--wav file.wav] [--mf-stub http://127.0.0.1:47700]

# Hum parity: the melody path natively and in browser Workers (see tests/hum_parity.rs)
MF_PARITY_INDEX=index.mfi MF_PARITY_QUERIES=queries.tsv \
  cargo test -p mfcore-wasm --profile release-fast --test hum_parity -- --ignored --nocapture
smoke/cors-server.py <dir> 47811 &   # serves index.mfi, queries.tsv, manifest.json and the WAVs
MF_PARITY_URL=http://127.0.0.1:47811 WASM_BINDGEN_USE_DEDICATED_WORKER=1 CHROMEDRIVER=… \
  cargo test -p mfcore-wasm --target wasm32-unknown-unknown --profile wasm-dev --test hum_parity -- --nocapture
smoke/hum-parity.py native.log chrome.log firefox.log
```
