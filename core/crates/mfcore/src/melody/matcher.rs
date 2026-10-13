//! Two-stage melody matcher: n-gram retrieval with offset voting, then linear
//! scaling + DTW + note edit distance re-ranking, fused into a calibrated score.

use crate::melody::{MelodyIndex, MelodyQuery, SongMeta};
use crate::model::MelodyAlignment;

#[derive(Debug, Clone)]
pub struct MelodyMatch {
    pub song: SongMeta,
    /// Calibrated 0..=1.
    pub score: f32,
    /// Semitones added to the reference to meet the query.
    pub key_shift: f32,
    /// Query tempo relative to the reference.
    pub tempo: f32,
    pub alignment: MelodyAlignment,
}

/// Top `k` matches, best first.
pub fn search(index: &MelodyIndex, query: &MelodyQuery, k: usize) -> Vec<MelodyMatch> {
    let _ = (index, query, k);
    todo!("melody::matcher")
}

/// Score one specific song (e.g. a lyric-search candidate) against the query.
pub fn score_song(index: &MelodyIndex, query: &MelodyQuery, song_id: u32) -> Option<MelodyMatch> {
    let _ = (index, query, song_id);
    todo!("melody::matcher")
}
