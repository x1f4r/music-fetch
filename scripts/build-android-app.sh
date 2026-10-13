#!/usr/bin/env bash
# Build the signed Android release APK into dist/MusicFetch-<version>-android.apk.
#
#   scripts/build-android-app.sh [--unsigned] [--create-keystore] [--abis arm64-v8a,x86_64]
#
# Steps: fetch the JS runtime, build mfcore and its bindings, assembleRelease, copy the
# APK to dist/ and verify it (signature, 16 KB zip alignment).
#
# Signing: MF_KEYSTORE_PATH / MF_KEYSTORE_PASSWORD / MF_KEY_ALIAS / MF_KEY_PASSWORD from
# the environment, or else on macOS the keystore at ~/.config/music-fetch/android-release.jks
# with its password from the login keychain (service music-fetch-android-release).
#   --create-keystore   create that keystore and keychain item once (RSA 4096, 30 years)
#   --unsigned          skip signing (what CI does)
set -euo pipefail

KEYSTORE_DEFAULT="$HOME/.config/music-fetch/android-release.jks"
KEYCHAIN_SERVICE="music-fetch-android-release"
KEY_ALIAS_DEFAULT="musicfetch"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ANDROID_DIR="$ROOT/android"
DIST="$ROOT/dist"

unsigned=0
create_keystore=0
abis="arm64-v8a,x86_64"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --unsigned) unsigned=1 ;;
    --create-keystore) create_keystore=1 ;;
    --abis)
      [[ $# -ge 2 ]] || { echo "error: --abis needs a value" >&2; exit 2; }
      abis="$2"
      shift
      ;;
    --abis=*) abis="${1#--abis=}" ;;
    -h | --help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

# --- Toolchain ---------------------------------------------------------------

if [[ -z "${JAVA_HOME:-}" ]] || ! "$JAVA_HOME/bin/java" -version 2>&1 | grep -q 'version "21'; then
  if [[ -x /usr/libexec/java_home ]] && jdk21="$(/usr/libexec/java_home -v 21 2>/dev/null)"; then
    export JAVA_HOME="$jdk21"
  elif [[ -z "${JAVA_HOME:-}" ]]; then
    echo "error: JDK 21 not found; set JAVA_HOME" >&2
    exit 1
  fi
fi
if [[ -z "${ANDROID_HOME:-}" ]]; then
  if [[ -n "${ANDROID_SDK_ROOT:-}" ]]; then
    export ANDROID_HOME="$ANDROID_SDK_ROOT"
  elif [[ "$(uname -s)" == Darwin ]]; then
    export ANDROID_HOME="$HOME/Library/Android/sdk"
  else
    export ANDROID_HOME="$HOME/Android/Sdk"
  fi
fi

build_tools=""
for dir in "$ANDROID_HOME"/build-tools/*/; do
  [[ -x "$dir/apksigner" && -x "$dir/zipalign" ]] && build_tools="${dir%/}"
done
[[ -n "$build_tools" ]] || { echo "error: no build-tools with apksigner/zipalign in $ANDROID_HOME" >&2; exit 1; }

# --- Release keystore --------------------------------------------------------

keychain_password() {
  security find-generic-password -s "$KEYCHAIN_SERVICE" -a "$KEY_ALIAS_DEFAULT" -w 2>/dev/null
}

create_release_keystore() {
  command -v security >/dev/null || { echo "error: --create-keystore needs the macOS keychain" >&2; exit 1; }
  if [[ -e "$KEYSTORE_DEFAULT" ]]; then
    echo "==> Keystore already exists: $KEYSTORE_DEFAULT"
    return
  fi
  if keychain_password >/dev/null; then
    echo "error: keychain item $KEYCHAIN_SERVICE exists but $KEYSTORE_DEFAULT does not;" \
      "restore the keystore instead of creating a new signing key" >&2
    exit 1
  fi
  echo "==> Creating release keystore $KEYSTORE_DEFAULT"
  mkdir -p "$(dirname "$KEYSTORE_DEFAULT")"
  chmod 700 "$(dirname "$KEYSTORE_DEFAULT")"
  # 48 alphanumeric characters (~285 bits); never printed, passed via env and stdin only.
  local password
  password="$(head -c 1024 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9')"
  password="${password:0:48}"
  [[ ${#password} -eq 48 ]] || { echo "error: could not generate a password" >&2; exit 1; }
  if ! MF_NEW_KEYSTORE_PASSWORD="$password" "$JAVA_HOME/bin/keytool" -genkeypair \
    -keystore "$KEYSTORE_DEFAULT" -storetype PKCS12 \
    -alias "$KEY_ALIAS_DEFAULT" -keyalg RSA -keysize 4096 -validity 10958 \
    -dname "CN=Music Fetch" \
    -storepass:env MF_NEW_KEYSTORE_PASSWORD -keypass:env MF_NEW_KEYSTORE_PASSWORD >/dev/null; then
    rm -f "$KEYSTORE_DEFAULT"
    echo "error: keytool failed" >&2
    exit 1
  fi
  chmod 600 "$KEYSTORE_DEFAULT"
  # `security -i` reads the command from stdin, keeping the password out of argv.
  printf 'add-generic-password -s %s -a %s -l "Music Fetch Android release keystore" -w %s\n' \
    "$KEYCHAIN_SERVICE" "$KEY_ALIAS_DEFAULT" "$password" | security -i >/dev/null
  if [[ "$(keychain_password || true)" != "$password" ]]; then
    rm -f "$KEYSTORE_DEFAULT"
    echo "error: storing the password in the keychain failed" >&2
    exit 1
  fi
  echo "    created (alias $KEY_ALIAS_DEFAULT); password stored in the login keychain as $KEYCHAIN_SERVICE"
}

(( create_keystore )) && create_release_keystore

if (( unsigned )); then
  unset MF_KEYSTORE_PATH MF_KEYSTORE_PASSWORD MF_KEY_ALIAS MF_KEY_PASSWORD
elif [[ -z "${MF_KEYSTORE_PATH:-}" ]]; then
  if [[ -f "$KEYSTORE_DEFAULT" ]] && command -v security >/dev/null && password="$(keychain_password)"; then
    export MF_KEYSTORE_PATH="$KEYSTORE_DEFAULT"
    export MF_KEYSTORE_PASSWORD="$password"
    export MF_KEY_ALIAS="$KEY_ALIAS_DEFAULT"
    export MF_KEY_PASSWORD="$password"
  else
    echo "error: no release keystore. Run with --create-keystore once, set MF_KEYSTORE_*," \
      "or pass --unsigned" >&2
    exit 1
  fi
fi

# --- Build -------------------------------------------------------------------

"$ROOT/scripts/fetch-android-runtime.sh"
"$ROOT/scripts/build-core-android.sh" --abis "$abis"

echo "==> assembleRelease"
"$ANDROID_DIR/gradlew" -p "$ANDROID_DIR" --console=plain "-Pmusicfetch.abis=$abis" :app:assembleRelease

outputs="$ANDROID_DIR/app/build/outputs/apk/release"
if (( unsigned )); then
  apk="$outputs/app-release-unsigned.apk"
else
  apk="$outputs/app-release.apk"
fi
[[ -f "$apk" ]] || { echo "error: $apk was not built" >&2; exit 1; }

version="$("$build_tools/aapt2" dump badging "$apk" | sed -n "s/.*versionName='\([^']*\)'.*/\1/p" | head -1)"
[[ -n "$version" ]] || { echo "error: could not read versionName from $apk" >&2; exit 1; }
suffix=""
(( unsigned )) && suffix="-unsigned"
mkdir -p "$DIST"
out="$DIST/MusicFetch-$version-android$suffix.apk"
cp "$apk" "$out"

# --- Verify ------------------------------------------------------------------

echo "==> $out"
printf '    size: %s bytes (%.1f MB)\n' "$(wc -c < "$out" | tr -d ' ')" "$(awk -v b="$(wc -c < "$out")" 'BEGIN { print b / 1048576 }')"
if ! zipalign_log="$("$build_tools/zipalign" -c -P 16 -v 4 "$out" 2>&1)"; then
  echo "$zipalign_log" | grep -v '(OK' >&2
  echo "error: zipalign -c -P 16 failed" >&2
  exit 1
fi
echo "    zipalign -c -P 16: $(echo "$zipalign_log" | tail -1)"
if (( !unsigned )); then
  signer="$("$build_tools/apksigner" verify --print-certs --verbose "$out" 2>/dev/null)"
  echo "$signer" | grep -E '^Verified using .*: true' | sed 's/^/    /'
  echo "$signer" | grep -E 'Signer: certificate (DN|SHA-256 digest):' | sed 's/^V[0-9.]* Signer: /    /' | sort -u
fi
