//! Monophonic pitch tracking (SwiftF0 ONNX via tract), with a whistle path.

use std::sync::Arc;

use crate::error::Result;
use crate::model::PitchFrame;

pub struct PitchTracker {}

impl PitchTracker {
    pub fn new() -> Result<Self> {
        todo!("melody::pitch")
    }

    /// Pitch frames for 16 kHz mono audio; `t_ms` is relative to the first sample.
    pub fn analyze(&self, samples: &[f32]) -> Vec<PitchFrame> {
        let _ = samples;
        todo!("melody::pitch")
    }
}

/// Incremental tracker for live input.
pub struct StreamingPitch {
    _tracker: Arc<PitchTracker>,
}

impl StreamingPitch {
    pub fn new(tracker: Arc<PitchTracker>) -> Self {
        Self { _tracker: tracker }
    }

    /// Push 16 kHz mono samples; returns frames that became final. `t_ms` counts
    /// from the first pushed sample.
    pub fn push(&mut self, samples: &[f32]) -> Vec<PitchFrame> {
        let _ = samples;
        todo!("melody::pitch")
    }
}
