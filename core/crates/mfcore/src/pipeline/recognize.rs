//! Recording recognition shared by every pipeline: persistent cache, the
//! engine-wide Shazam budget, rate-limit notices and the optional paid providers.

use crate::error::Result;
use crate::model::{EvidenceSource, Track};
use crate::pipeline::ctx::SessionCtx;
use crate::shazam::Priority;

/// One recognizer answer for a clip.
#[derive(Debug, Clone)]
pub struct RecognitionHit {
    pub track: Track,
    pub source: EvidenceSource,
    /// Calibrated 0..=1 for this single answer.
    pub score: f32,
    /// Position in the reference track where the clip starts, when known.
    pub offset_ms: Option<i64>,
    pub timeskew: f32,
    pub frequency_skew: f32,
}

/// Recognize 16 kHz mono audio (3–12 s) with Shazam through the persistent cache
/// and the shared budget. Emits `Notice::RateLimited` while throttled and waits
/// within the priority's limit.
pub async fn shazam(ctx: &SessionCtx, samples: &[i16], priority: Priority) -> Result<Option<RecognitionHit>> {
    let _ = (ctx, samples, priority);
    todo!("pipeline::recognize")
}

/// Recognize with the configured paid providers (AudD, then ACRCloud), if any.
pub async fn paid(ctx: &SessionCtx, samples: &[i16]) -> Result<Option<RecognitionHit>> {
    let _ = (ctx, samples);
    todo!("pipeline::recognize")
}
