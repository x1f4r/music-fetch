#!/usr/bin/env bash
# Cloud-humming eval: the user's takes and a fixed CHAD sample through ACRCloud's
# humming engine, next to the local matcher, with every answer cached.
#
#   tools/eval/run-cloud-eval.sh [--offline]
#
# Needs the credentials from tools/eval/store-acrcloud-credentials.sh. Sends at most
# one request per take, one per second, never a take that is already cached
# (DATA/eval/acrcloud/*.json); --offline only reads the cache. About 320 requests on
# the first run, none after it. Results: DATA/eval/acrcloud/{user,chad300}-<index>.{jsonl,txt}.
#
# Environment: DATA (private data folder, default music-fetch-data next to the main
# checkout), INDEX and ENCODER (default the Android tier and its encoder file in
# DATA/index/tiers), PRICE (price of 1,000 requests from the ACRCloud console),
# MF (mf binary; built with --profile release-fast when missing).
set -euo pipefail
cd "$(dirname "$0")/../.."

# The main checkout's sibling (also from a worktree).
main="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
data="${DATA:-$(dirname "$main")/music-fetch-data}"
index="${INDEX:-$data/index/tiers/android.mfi}"
encoder="${ENCODER:-$data/index/tiers/melody-encoder-android.mfe}"
cache="$data/eval/acrcloud"
mf="${MF:-core/target/release-fast/mf}"
offline=()
[[ "${1:-}" == "--offline" ]] && offline=(--cloud-offline)

if [[ ! -x "$mf" ]]; then
  export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
  (cd core && CARGO_BUILD_JOBS=6 nice -n 15 cargo build --profile release-fast -p mf-cli)
fi
price=()
[[ -n "${PRICE:-}" ]] && price=(--cloud-price-per-1000 "$PRICE")
name="$(basename "$index" .mfi)"
run() { # label queries [extra...]
  local label="$1" queries="$2"
  shift 2
  echo "== $label ($name)"
  nice -n 15 "$mf" eval qbsh --index "$index" --queries "$queries" \
    --encoder "$encoder" --encoder-budget android --encoder-votes \
    --cloud acrcloud --cloud-cache "$cache" --cloud-pace-ms 1100 \
    --out "$cache/$label-$name.jsonl" ${offline[@]+"${offline[@]}"} ${price[@]+"${price[@]}"} "$@" \
    | tee "$cache/$label-$name.txt"
}
run user "$cache/q-user.tsv"
run chad300 "$cache/q-chad.tsv" --cloud-sample 300
