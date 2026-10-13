//! Impression analysis (hum, whistle, sing). See docs/PIPELINES.md.

use crate::error::Result;
use crate::model::{Outcome, PitchFrame};
use crate::pipeline::ctx::SessionCtx;

/// Analyze captured 16 kHz mono audio as an impression: melody matching, sung
/// lyrics and ACRCloud humming, fused into candidates. Emits `Candidates` events
/// and returns the outcome; the caller emits the terminal event. `frames` are the
/// live pitch frames when already computed.
pub async fn run_impression(ctx: &SessionCtx, audio: &[f32], frames: Option<Vec<PitchFrame>>) -> Result<Outcome> {
    let _ = (ctx, audio, frames);
    todo!("pipeline::hum")
}
