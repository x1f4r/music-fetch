# opus-decoder 0.1.1 (patched)

Source: the `opus-decoder` 0.1.1 crate from crates.io
(https://github.com/TadeuszWolfGang/Rusopus), licensed MIT OR Apache-2.0. Only `src/`
is vendored; tests and benchmarks are left out.

The published sources append developer debug logs to a hard-coded file below the
author's home folder (and to `.cursor/` below the working directory) while decoding,
some of them on every CELT frame. The patch:

- adds `src/no_debug_log.rs`, an `OpenOptions` stand-in whose `open` always fails;
- routes every `std::fs::OpenOptions::new()` in `src/` through it;
- blanks the absolute log paths, so no local path ends up in release binaries.

Decoding is unchanged. Drop the `[patch.crates-io]` entry in `core/Cargo.toml` and this
folder once a release without the debug logging is published.
