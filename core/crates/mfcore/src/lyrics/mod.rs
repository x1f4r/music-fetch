//! Find songs from sung (transcribed) or typed lyrics.

use crate::error::Result;
use crate::model::Track;
use crate::util::cancel::CancelToken;

#[derive(Debug, Clone)]
pub struct LyricMatch {
    pub track: Track,
    /// Calibrated 0..=1.
    pub score: f32,
    /// The lyric line that matched best.
    pub matched_line: Option<String>,
}

/// Search with one or more transcription hypotheses (best first).
pub async fn search(
    http: &reqwest::Client,
    hypotheses: &[String],
    country: &str,
    cancel: &CancelToken,
) -> Result<Vec<LyricMatch>> {
    let _ = (http, hypotheses, country, cancel);
    todo!("lyrics")
}
