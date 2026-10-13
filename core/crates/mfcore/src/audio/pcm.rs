//! 16 kHz mono PCM stored as raw little-endian i16 in a file, read by window.

use std::path::{Path, PathBuf};

use crate::error::Result;

pub struct PcmFile {
    path: PathBuf,
    len_samples: u64,
}

impl PcmFile {
    pub fn open(path: &Path) -> Result<Self> {
        let _ = path;
        todo!("audio::pcm")
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len_samples(&self) -> u64 {
        self.len_samples
    }

    pub fn duration_ms(&self) -> u64 {
        self.len_samples * 1000 / super::SAMPLE_RATE as u64
    }

    /// Read `[start_ms, start_ms + len_ms)` clamped to the file.
    pub fn read_ms(&self, start_ms: u64, len_ms: u64) -> Result<Vec<i16>> {
        let _ = (start_ms, len_ms);
        todo!("audio::pcm")
    }
}

/// Appends samples to a new PCM file.
pub struct PcmWriter {
    path: PathBuf,
}

impl PcmWriter {
    pub fn create(path: &Path) -> Result<Self> {
        Ok(Self { path: path.to_path_buf() })
    }

    pub fn push(&mut self, samples: &[i16]) -> Result<()> {
        let _ = samples;
        todo!("audio::pcm")
    }

    pub fn finish(self) -> Result<PcmFile> {
        let _ = &self.path;
        todo!("audio::pcm")
    }
}
