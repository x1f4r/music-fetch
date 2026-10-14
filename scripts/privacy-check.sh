#!/usr/bin/env bash
# Fails when tracked files, commit metadata or build outputs contain secrets or
# personal data. Run before publishing a release.
#
#   scripts/privacy-check.sh [--range <rev-range>] [artifact ...]
#
# Generic patterns are built in. Personal strings that must never appear (names,
# email addresses, host names) are read from a file that is NOT in the repository:
# $MF_PRIVATE_PATTERNS (default ~/.config/music-fetch/private-patterns.txt), one
# case-insensitive fixed string per line.
set -euo pipefail

cd "$(dirname "$0")/.."
range=""
artifacts=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --range) range="$2"; shift 2 ;;
    *) artifacts+=("$1"); shift ;;
  esac
done

patterns_file="${MF_PRIVATE_PATTERNS:-$HOME/.config/music-fetch/private-patterns.txt}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Printable strings of every file in each artifact (APKs/zips are unpacked first),
# written to a file so grep results never depend on pipe exit codes.
artifact_strings() { # artifact -> path of a strings dump
  local artifact="$1" out="$tmp/$(echo "$1" | shasum | cut -c1-12).txt" src="$1"
  if [[ ! -s "$out" ]]; then
    if [[ -f "$artifact" && "$artifact" =~ \.(apk|zip|xpi|aab)$ ]]; then
      src="$tmp/unpacked-$(basename "$artifact")"
      mkdir -p "$src" && unzip -qq -o "$artifact" -d "$src" || true
    fi
    find "$src" -type f -print0 | xargs -0 strings -a > "$out" 2>/dev/null || true
  fi
  echo "$out"
}
status=0
fail() { echo "privacy-check: $*" >&2; status=1; }

# Generic leaks: home paths, tailnets, private keys, common token shapes, emails
# other than GitHub noreply / example domains.
generic='(/Users/[A-Za-z0-9._-]+/|/home/[a-z][A-Za-z0-9._-]+/|[a-z0-9-]+\.ts\.net|BEGIN [A-Z ]*PRIVATE KEY|ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{40,}|sk-[A-Za-z0-9]{32,}|AIza[0-9A-Za-z_-]{35}|xox[abp]-[A-Za-z0-9-]{10,})'
email='[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
email_ok='(users\.noreply\.github\.com|@example\.(com|org|net)|@localhost|noreply@|git@github\.com|@users\.noreply)'

echo "== tracked files"
if git ls-files -z | xargs -0 grep -InE "$generic" -- 2>/dev/null | grep -v '^scripts/privacy-check.sh:'; then
  fail "tracked files contain home paths, host names or secrets (above)"
fi
if git ls-files -z | xargs -0 grep -InoE "$email" -- 2>/dev/null | grep -vE "$email_ok" | grep -v '^scripts/privacy-check.sh:'; then
  fail "tracked files contain email addresses (above)"
fi

if [[ -f "$patterns_file" ]]; then
  while IFS= read -r needle; do
    [[ -z "$needle" || "$needle" == \#* ]] && continue
    if git ls-files -z | xargs -0 grep -Iil -F -- "$needle" 2>/dev/null; then
      fail "tracked files contain a private string from $patterns_file"
    fi
    if [[ -n "$range" ]] && git log --format='%an%n%ae%n%cn%n%ce%n%B' "$range" | grep -qiF -- "$needle"; then
      fail "commit metadata in $range contains a private string"
    fi
    for artifact in "${artifacts[@]+"${artifacts[@]}"}"; do
      if grep -qiF -- "$needle" "$(artifact_strings "$artifact")"; then
        fail "$artifact contains a private string"
      fi
    done
  done < "$patterns_file"
else
  echo "privacy-check: no private patterns file at $patterns_file (skipping personal strings)" >&2
fi

if [[ -n "$range" ]]; then
  echo "== commit metadata ($range)"
  if git log --format='%an <%ae>%n%cn <%ce>' "$range" | sort -u | grep -vE "$email_ok"; then
    fail "commits in $range use a non-noreply identity (above)"
  fi
fi

for artifact in "${artifacts[@]+"${artifacts[@]}"}"; do
  echo "== $artifact"
  dump="$(artifact_strings "$artifact")"
  # This machine's home folder or user name must never ship.
  if grep -E -- "(${HOME}/|/Users/${USER}/|/home/${USER}/)" "$dump" > "$tmp/own.txt"; then
    sort -u "$tmp/own.txt" | head -20
    fail "$artifact embeds this machine's home path (above)"
  fi
  # Other build-machine paths usually come from prebuilt third-party libraries; list them for review.
  if grep -oE '(/Users/[A-Za-z0-9._-]+/|/home/[a-z][A-Za-z0-9._-]+/)' "$dump" | grep -vE "^(/Users/${USER}/|/home/${USER}/)" | sort | uniq -c | sort -rn > "$tmp/other.txt" && [[ -s "$tmp/other.txt" ]]; then
    echo "privacy-check: note: other build paths in $artifact (third-party prebuilt code?):" >&2
    head -10 "$tmp/other.txt" >&2
  fi
done

if command -v gitleaks >/dev/null 2>&1; then
  echo "== gitleaks"
  gitleaks detect --no-banner --redact --config .gitleaks.toml --source . ${range:+--log-opts="$range"} || fail "gitleaks found secrets"
else
  fail "gitleaks is not installed"
fi

[[ $status -eq 0 ]] && echo "privacy-check: clean"
exit $status
