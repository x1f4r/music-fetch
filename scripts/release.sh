#!/usr/bin/env bash
# Builds the release artifacts for both apps into dist/ and checks them.
#
#   scripts/release.sh            # macOS zip + signed Android APK + SHA256SUMS
#
# Publishing is a separate, manual step (gh release create …) after reviewing
# dist/ — this script never uploads anything.
set -euo pipefail
cd "$(dirname "$0")/.."

version="$(sed -n 's/^version = "\(.*\)"/\1/p' core/Cargo.toml | head -1)"
echo "==> Music Fetch $version"

rm -rf dist
scripts/build-macos-app.sh
scripts/build-android-app.sh

cd dist
rm -rf "Music Fetch.app.dSYM"
shasum -a 256 MusicFetch-* > SHA256SUMS.txt
cat SHA256SUMS.txt
cd ..

# The app bundle itself is checked too (strings in every Mach-O).
scripts/privacy-check.sh dist
echo "==> Release artifacts in dist/"
