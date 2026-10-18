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

A second patch fixes `extract_collapse_mask` in `src/celt/vq.rs`: it built the mask in a
`u8`, but a band split in time can have more than eight blocks, so `1 << i` overflowed
(a panic with overflow checks, a wrong mask without). It now computes 32 bits, as
libopus does; ffmpeg's libopus encoder at 48 kbit/s stereo produces such bands
(`core/crates/mfcore/tests/fixtures/codecs/opus-48k-stereo.*`).

Drop the `[patch.crates-io]` entry in `core/Cargo.toml` and this folder once a release
with both fixes is published.
