//! From pitch frames to a matchable query: cleaned contour and notes.

use crate::model::PitchFrame;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Note {
    /// MIDI note number (fractional).
    pub pitch: f32,
    pub onset_ms: u32,
    pub dur_ms: u32,
}

#[derive(Debug, Clone)]
pub struct MelodyQuery {
    /// 20 fps contour in MIDI semitones; NaN = unvoiced.
    pub contour: Vec<f32>,
    pub notes: Vec<Note>,
    pub voiced_ms: u32,
}

/// None when there is too little voiced material to search with.
pub fn query_from_frames(frames: &[PitchFrame]) -> Option<MelodyQuery> {
    let _ = frames;
    todo!("melody::notes")
}
