#!/usr/bin/env bash
# Store the ACRCloud humming project's credentials for the cloud-humming eval.
#
#   tools/eval/store-acrcloud-credentials.sh [file]
#
# Run it in a terminal. It asks for the project's host (e.g.
# identify-eu-west-1.acrcloud.com), access key and access secret without showing
# what you type, and writes them to [file] (default
# ~/.config/music-fetch/acrcloud.json) readable by you only (mode 600). Nothing is
# echoed, logged or passed on a command line. `mf eval qbsh --cloud acrcloud`
# (tools/eval/run-cloud-eval.sh) reads the file.
set -euo pipefail

out="${1:-$HOME/.config/music-fetch/acrcloud.json}"
[[ -t 0 ]] || { echo "error: run this in a terminal (it reads the secrets from the keyboard)" >&2; exit 1; }
# No echo for the whole run, so nothing typed ahead of a prompt shows either.
tty_state="$(stty -g)"
trap 'stty "$tty_state"' EXIT
trap 'stty "$tty_state"; exit 130' INT TERM
stty -echo

ask() { # prompt variable
  local value=""
  while [[ -z "$value" ]]; do
    read -r -s -p "$1: " value
    echo >&2
  done
  printf -v "$2" '%s' "$value"
}
ask "Host (identify-….acrcloud.com)" host
ask "Access key" key
ask "Access secret" secret

mkdir -p "$(dirname "$out")"
chmod 700 "$(dirname "$out")" 2>/dev/null || true
umask 077
# The values travel through stdin (printf is a shell builtin), never through arguments.
printf '%s\n%s\n%s\n' "$host" "$key" "$secret" | python3 -I -c '
import json, os, sys
host, key, secret = (sys.stdin.readline().strip() for _ in range(3))
host = host.removeprefix("https://").rstrip("/")
path = sys.argv[1]
tmp = path + ".tmp"
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    json.dump({"host": host, "access_key": key, "access_secret": secret}, f)
    f.write("\n")
os.chmod(tmp, 0o600)
os.replace(tmp, path)
' "$out"
unset host key secret
echo "Saved to $out (mode 600)."
