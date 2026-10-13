# shellcheck shell=bash
# Sourced by the core build scripts.
#
# Release binaries embed source paths in panic locations. Remap every local
# prefix to a neutral one so no home directory or checkout location ships.

_mf_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
_mf_cargo_home="${CARGO_HOME:-$HOME/.cargo}"
_mf_rustup_home="${RUSTUP_HOME:-$HOME/.rustup}"

MF_REMAP_FLAGS="--remap-path-prefix=${_mf_cargo_home}/registry/src=/cargo/registry"
MF_REMAP_FLAGS+=" --remap-path-prefix=${_mf_cargo_home}/git/checkouts=/cargo/git"
MF_REMAP_FLAGS+=" --remap-path-prefix=${_mf_rustup_home}=/rustup"
MF_REMAP_FLAGS+=" --remap-path-prefix=${_mf_repo_root}=/music-fetch"
MF_REMAP_FLAGS+=" --remap-path-prefix=${HOME}=/home"

export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }${MF_REMAP_FLAGS}"
unset _mf_repo_root _mf_cargo_home _mf_rustup_home
