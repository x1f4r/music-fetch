//! Parse yt-dlp `--dump-single-json --flat-playlist` output.

use crate::error::Result;

#[derive(Debug, Clone, Default)]
pub struct Chapter {
    pub start_ms: u64,
    pub end_ms: Option<u64>,
    pub title: String,
}

#[derive(Debug, Clone, Default)]
pub struct MediaEntry {
    pub index: u32,
    pub url: String,
    pub title: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct MediaInfo {
    pub id: Option<String>,
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub duration_ms: Option<u64>,
    pub webpage_url: Option<String>,
    pub extractor: Option<String>,
    pub thumbnail: Option<String>,
    pub description: Option<String>,
    pub chapters: Vec<Chapter>,
    /// Track/artist fields some extractors fill (YouTube Music, SoundCloud, Bandcamp).
    pub track: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    /// Non-empty for playlists.
    pub entries: Vec<MediaEntry>,
}

pub fn parse_probe(json: &str) -> Result<MediaInfo> {
    let _ = json;
    todo!("media::probe")
}

/// Timestamped lines in a description ("12:34 Artist - Title"), as (ms, text).
pub fn description_timestamps(description: &str) -> Vec<(u64, String)> {
    let _ = description;
    todo!("media::probe")
}
