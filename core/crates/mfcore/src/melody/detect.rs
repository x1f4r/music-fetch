//! Decides whether live input is a person humming/whistling/singing rather than
//! recorded music.

use crate::model::PitchFrame;

#[derive(Default)]
pub struct ImpressionDetector {}

impl ImpressionDetector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed pitch frames and the matching 16 kHz audio chunk.
    pub fn push(&mut self, frames: &[PitchFrame], samples: &[f32]) {
        let _ = (frames, samples);
        todo!("melody::detect")
    }

    /// Some(true) once the input is very likely an impression, Some(false) once it
    /// is very likely recorded music, None while undecided.
    pub fn verdict(&self) -> Option<bool> {
        todo!("melody::detect")
    }

    /// Total voiced duration seen so far.
    pub fn voiced_ms(&self) -> u64 {
        todo!("melody::detect")
    }

    /// Milliseconds since the last voiced frame.
    pub fn silence_ms(&self) -> u64 {
        todo!("melody::detect")
    }
}
