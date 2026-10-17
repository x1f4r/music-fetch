#!/usr/bin/env bash
# Build mfcore as a universal (arm64 + x86_64) static library for macOS 15 and later
# and refresh the local Swift package that wraps it.
#
#   scripts/build-core-apple.sh [--debug]
#
# Outputs (gitignored):
#   apple/MusicFetchCore/MfCoreFFI.xcframework                         libmfcore.a (arm64, x86_64) + MfCoreFFI.h + module.modulemap
#   apple/MusicFetchCore/Sources/MusicFetchCore/MusicFetchCore.swift   UniFFI Swift bindings
#
# Each architecture is built with cargo, then `lipo` joins them into one static library
# (an xcframework keeps a single library per platform).
#
# The module map carries `link` directives for the native libraries rustc reports
# (--print native-static-libs), so anything importing MfCoreFFI autolinks them. The
# same list is checked against Package.swift's explicit linker settings.
#
# Outputs are only replaced when their content changes, so an unchanged core does not
# invalidate Xcode's build.
set -euo pipefail

# Keep local paths out of the binaries.
# shellcheck source=lib/rust-env.sh
source "$(dirname "$0")/lib/rust-env.sh"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORE_DIR="$ROOT_DIR/core"
PACKAGE_DIR="$ROOT_DIR/apple/MusicFetchCore"
STAGE_DIR="$ROOT_DIR/apple/build/core"
TARGETS=(aarch64-apple-darwin x86_64-apple-darwin)
FFI_MODULE="MfCoreFFI"
SWIFT_FILE="$PACKAGE_DIR/Sources/MusicFetchCore/MusicFetchCore.swift"
XCFRAMEWORK="$PACKAGE_DIR/$FFI_MODULE.xcframework"

PROFILE="release"
case "${1:-}" in
  "") ;;
  --debug) PROFILE="debug" ;;
  -h | --help)
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
    exit 0
    ;;
  *)
    echo "usage: $0 [--debug]" >&2
    exit 2
    ;;
esac

# rustup's proxies must win over a Homebrew rust, which lacks the pinned toolchain.
export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
# Must match the app's deployment target (apple/project.yml), or ld warns about every
# object file.
export MACOSX_DEPLOYMENT_TARGET="15.0"

log() { printf '==> %s\n' "$*"; }
die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

mkdir -p "$STAGE_DIR"

# 1. Static library per architecture, then one universal archive. cargo must run
# inside core/ so rust-toolchain.toml applies.
cargo_profile_args=()
[[ "$PROFILE" == "release" ]] && cargo_profile_args=(--release)
thin_libs=()
for target in "${TARGETS[@]}"; do
  log "Building mfcore ($PROFILE, $target)"
  (
    cd "$CORE_DIR"
    cargo rustc -p mfcore --lib --target "$target" --crate-type staticlib \
      ${cargo_profile_args[@]+"${cargo_profile_args[@]}"} -- --print native-static-libs
  ) 2>&1 | tee "$STAGE_DIR/cargo-$PROFILE-$target.log"
  thin="$CORE_DIR/target/$target/$PROFILE/libmfcore.a"
  [[ -f "$thin" ]] || die "cargo did not produce $thin"
  thin_libs+=("$thin")
done
# The first target's log carries the native libraries (they are the same on both).
CARGO_LOG="$STAGE_DIR/cargo-$PROFILE-${TARGETS[0]}.log"
# Bindgen reads the UniFFI metadata from a single-architecture library.
METADATA_LIB="${thin_libs[0]}"

LIB="$STAGE_DIR/universal-$PROFILE/libmfcore.a"
mkdir -p "$(dirname "$LIB")"
lipo -create "${thin_libs[@]}" -output "$LIB.tmp"
# Keep the old file (and its mtime) when nothing changed, so the xcframework stamp holds.
if cmp -s "$LIB.tmp" "$LIB"; then rm -f "$LIB.tmp"; else mv "$LIB.tmp" "$LIB"; fi
archs="$(lipo -archs "$LIB")"
for arch in arm64 x86_64; do
  [[ " $archs " == *" $arch "* ]] || die "universal library lacks $arch (has: $archs)"
done
log "Universal library: $archs ($(du -h "$LIB" | cut -f 1))"

# 2. Native libraries the static library needs at link time.
native_libs="$(sed -n 's/^note: native-static-libs: //p' "$CARGO_LOG" | tail -n 1)"
[[ -n "$native_libs" ]] || die "rustc did not report native-static-libs (see $CARGO_LOG)"
for target in "${TARGETS[@]:1}"; do
  other="$(sed -n 's/^note: native-static-libs: //p' "$STAGE_DIR/cargo-$PROFILE-$target.log" | tail -n 1)"
  [[ "$other" == "$native_libs" ]] || die "native-static-libs differ for $target: $other"
done

frameworks=()
libraries=()
read -r -a tokens <<<"$native_libs"
i=0
while ((i < ${#tokens[@]})); do
  token="${tokens[$i]}"
  case "$token" in
    -framework)
      i=$((i + 1))
      ((i < ${#tokens[@]})) || die "dangling -framework in native-static-libs"
      case " ${frameworks[*]-} " in
        *" ${tokens[$i]} "*) ;;
        *) frameworks+=("${tokens[$i]}") ;;
      esac
      ;;
    -lSystem | -lc | -lm) ;; # part of libSystem, always linked
    -l*)
      case " ${libraries[*]-} " in
        *" ${token#-l} "*) ;;
        *) libraries+=("${token#-l}") ;;
      esac
      ;;
    *) die "unexpected native-static-libs entry: $token" ;;
  esac
  i=$((i + 1))
done

log "native-static-libs: $native_libs"
log "Linker settings: frameworks [${frameworks[*]-}] libraries [${libraries[*]-}]"

for framework in ${frameworks[@]+"${frameworks[@]}"}; do
  grep -q "linkedFramework(\"$framework\")" "$PACKAGE_DIR/Package.swift" ||
    echo "warning: Package.swift does not list .linkedFramework(\"$framework\"); relying on module map autolinking" >&2
done
for library in ${libraries[@]+"${libraries[@]}"}; do
  grep -q "linkedLibrary(\"$library\")" "$PACKAGE_DIR/Package.swift" ||
    echo "warning: Package.swift does not list .linkedLibrary(\"$library\"); relying on module map autolinking" >&2
done

# 3. Swift bindings, C header and module map.
log "Generating Swift bindings"
(
  cd "$CORE_DIR"
  cargo build -p uniffi-bindgen --release --bin uniffi-bindgen-swift
)
BINDGEN="$CORE_DIR/target/release/uniffi-bindgen-swift"

GEN_DIR="$STAGE_DIR/generated"
rm -rf "$GEN_DIR"
mkdir -p "$GEN_DIR/headers"
link_framework_args=()
for framework in ${frameworks[@]+"${frameworks[@]}"}; do
  link_framework_args+=(--link-frameworks "$framework")
done
# Library mode reads core/crates/mfcore/uniffi.toml (module and file names) through
# cargo metadata. The module map is a plain one (no --xcframework): the xcframework
# wraps a static library, not a .framework bundle.
(
  cd "$CORE_DIR"
  "$BINDGEN" "$METADATA_LIB" "$GEN_DIR" --swift-sources --headers --modulemap \
    --module-name "$FFI_MODULE" --modulemap-filename module.modulemap \
    ${link_framework_args[@]+"${link_framework_args[@]}"} --metadata-no-deps
)

generated_swift="$GEN_DIR/MusicFetchCore.swift"
modulemap="$GEN_DIR/headers/module.modulemap"
[[ -f "$generated_swift" ]] || die "bindgen did not write MusicFetchCore.swift (check uniffi.toml module_name)"
[[ -f "$GEN_DIR/$FFI_MODULE.h" ]] || die "bindgen did not write $FFI_MODULE.h (check uniffi.toml ffi_module_filename)"
mv "$GEN_DIR/$FFI_MODULE.h" "$GEN_DIR/module.modulemap" "$GEN_DIR/headers/"
grep -q "^module $FFI_MODULE " "$modulemap" || die "unexpected module map: $(head -n 1 "$modulemap")"
grep -q "^import $FFI_MODULE\$" "$generated_swift" || die "bindings do not import $FFI_MODULE (check uniffi.toml ffi_module_name)"

# bindgen only emits `link framework`; add plain libraries (e.g. iconv) the same way.
if ((${#libraries[@]})); then
  link_lines="$(printf '    link "%s"\n' "${libraries[@]}")"
  awk -v lines="$link_lines" '/^}/ && !done { print lines; done = 1 } { print }' "$modulemap" >"$modulemap.tmp"
  mv "$modulemap.tmp" "$modulemap"
fi

# 4. Swift sources -> package (only when changed, to keep Xcode's build incremental).
# The bindings are used as generated. The known UniFFI 0.32.2 / Swift 6.2 problems
# (default MainActor isolation #2818, async foreign traits #2929) are avoided by the
# package's Swift 5 mode without default isolation; "Ambiguous use of 'bytes'" (#2803)
# does not occur with Xcode 26.0. Patch the file here if a newer toolchain needs it.
mkdir -p "$(dirname "$SWIFT_FILE")"
if cmp -s "$generated_swift" "$SWIFT_FILE"; then
  log "Swift bindings unchanged"
else
  cp "$generated_swift" "$SWIFT_FILE"
  log "Wrote ${SWIFT_FILE#"$ROOT_DIR"/}"
fi

# 5. XCFramework (only when the library, header or module map changed).
stamp="$(
  {
    stat -f '%m %z' "$LIB"
    shasum -a 256 "$GEN_DIR/headers/$FFI_MODULE.h" "$modulemap" | cut -d ' ' -f 1
    echo "$PROFILE"
  } | shasum -a 256 | cut -d ' ' -f 1
)"
STAMP_FILE="$STAGE_DIR/xcframework.stamp"
if [[ -d "$XCFRAMEWORK" && -f "$STAMP_FILE" && "$(cat "$STAMP_FILE")" == "$stamp" ]]; then
  log "XCFramework unchanged"
else
  staged="$STAGE_DIR/$FFI_MODULE.xcframework"
  rm -rf "$staged"
  xcodebuild -create-xcframework -library "$LIB" -headers "$GEN_DIR/headers" -output "$staged" >/dev/null
  rm -rf "$XCFRAMEWORK"
  mv "$staged" "$XCFRAMEWORK"
  printf '%s\n' "$stamp" >"$STAMP_FILE"
  log "Wrote ${XCFRAMEWORK#"$ROOT_DIR"/} ($(du -sh "$XCFRAMEWORK" | cut -f 1))"
fi

log "Core $PROFILE build ready"
