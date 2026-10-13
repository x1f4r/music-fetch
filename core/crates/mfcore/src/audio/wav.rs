//! Minimal WAV I/O (16-bit PCM).

use std::path::Path;

use crate::error::Result;

/// Encode 16-bit mono samples as a WAV file in memory.
pub fn encode_wav(samples: &[i16], sample_rate: u32) -> Vec<u8> {
    let _ = (samples, sample_rate);
    todo!("audio::wav")
}

pub fn write_wav(path: &Path, samples: &[i16], sample_rate: u32) -> Result<()> {
    std::fs::write(path, encode_wav(samples, sample_rate))?;
    Ok(())
}
