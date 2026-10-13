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
      if find "$artifact" -type f -print0 | xargs -0 strings -a 2>/dev/null | grep -qiF -- "$needle"; then
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
  if find "$artifact" -type f -print0 | xargs -0 strings -a 2>/dev/null | grep -E '(/Users/[A-Za-z0-9._-]+/|/home/[a-z][A-Za-z0-9._-]+/)' | sort -u | head -20 | grep .; then
    fail "$artifact embeds local paths (above)"
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
