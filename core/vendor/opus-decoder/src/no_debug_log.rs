//! Music Fetch patch: the published 0.1.1 sources append developer debug logs to a
//! hard-coded file below the author's home folder (and to `.cursor/` below the
//! working directory) while decoding. Every such write goes through this stand-in,
//! which never opens anything.

use std::io;
use std::path::Path;

/// Mirrors the subset of `std::fs::OpenOptions` the debug code uses.
pub(crate) struct OpenOptions;

impl OpenOptions {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn create(&mut self, _create: bool) -> &mut Self {
        self
    }

    pub(crate) fn append(&mut self, _append: bool) -> &mut Self {
        self
    }

    /// Always fails, so the logging branches never run.
    #[inline(always)]
    pub(crate) fn open<P: AsRef<Path>>(&self, _path: P) -> io::Result<std::fs::File> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "debug logging is disabled"))
    }
}
