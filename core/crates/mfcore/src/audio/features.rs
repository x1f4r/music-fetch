//! Frame features for segmentation and voice/music classification.

use crate::audio::PcmFile;
use crate::error::Result;
use crate::util::cancel::CancelToken;

#[derive(Debug, Clone)]
pub struct FrameFeatures {
    pub t_ms: u64,
    pub rms: f32,
    pub flatness: f32,
    pub zcr: f32,
    pub centroid_hz: f32,
    pub onset: f32,
    pub chroma: [f32; 12],
    pub mfcc: [f32; 13],
}

/// Features every `hop_ms` over the whole file.
pub fn compute(pcm: &PcmFile, hop_ms: u32, cancel: &CancelToken) -> Result<Vec<FrameFeatures>> {
    let _ = (pcm, hop_ms, cancel);
    todo!("audio::features")
}
