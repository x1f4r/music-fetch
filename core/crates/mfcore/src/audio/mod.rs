//! PCM handling: streaming resampling, decoding to a 16 kHz mono PCM file,
//! WAV helpers and frame features for segmentation.

#[cfg(feature = "engine")]
pub mod decode;
pub mod features;
#[cfg(feature = "engine")]
mod mp4;
#[cfg(feature = "engine")]
mod opus;
pub mod pcm;
pub mod resample;
pub mod wav;

#[cfg(test)]
pub(crate) mod testutil;

/// Analysis sample rate used everywhere in the engine.
pub const SAMPLE_RATE: u32 = 16_000;

pub use pcm::{PcmFile, PcmWriter};
