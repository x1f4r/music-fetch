//! PCM handling: streaming resampling, decoding to a 16 kHz mono PCM file,
//! WAV helpers and frame features for segmentation.

pub mod decode;
pub mod features;
pub mod pcm;
pub mod resample;
pub mod wav;

/// Analysis sample rate used everywhere in the engine.
pub const SAMPLE_RATE: u32 = 16_000;

pub use pcm::{PcmFile, PcmWriter};
