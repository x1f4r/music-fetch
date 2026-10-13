#!/usr/bin/env bash
# Run the mix evaluation on another machine so recognizer traffic never comes
# from the machine you listen on.
#
#   MF_EVAL_HOST=user@host tools/eval/mixes/run-remote.sh --mode both
#
# Environment:
#   MF_EVAL_HOST   SSH destination (required)
#   MF_EVAL_DIR    working directory on the host, relative to its home (default mf-eval)
#   MF_EVAL_TOOLS  directory on the host with yt-dlp and deno (default $MF_EVAL_DIR/tools)
#   MF_EVAL_SYNC   set to 0 to skip syncing the source
#
# The host needs rustup (the workspace pins its toolchain), yt-dlp and deno.
# Reports land in $MF_EVAL_DIR/reports on the host; copy them back with scp.
set -euo pipefail

host=${MF_EVAL_HOST:?set MF_EVAL_HOST to an SSH destination}
dir=${MF_EVAL_DIR:-mf-eval}
tools=${MF_EVAL_TOOLS:-$dir/tools}
root=$(cd "$(dirname "$0")/../../.." && pwd)

if [ "${MF_EVAL_SYNC:-1}" != 0 ]; then
  ssh "$host" "mkdir -p '$dir/src'"
  rsync -az --delete --exclude target --exclude .git --exclude android --exclude apple --exclude assets \
    "$root/" "$host:$dir/src/"
fi

args=$(printf ' %q' "$@")
# Login shells may not be POSIX (fish): hand the script to bash explicitly.
ssh "$host" bash -s <<EOF
set -euo pipefail
cd "\$HOME/$dir"
export PATH="\$HOME/.cargo/bin:\$PATH"
(cd src/core && nice -n 10 cargo build --release -q -p mf-cli)
exec nice -n 5 ./src/core/target/release/mf eval mixes src/tools/eval/mixes/manifest.json \
  --cache-db "\$HOME/$dir/cache/cache.sqlite3" --report-dir "\$HOME/$dir/reports" \
  --yt-dlp "\$HOME/$tools/yt-dlp" --js-runtime "deno:\$HOME/$tools/deno" $args
EOF
