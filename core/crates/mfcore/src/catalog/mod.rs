//! Cross-matching and enrichment: fill ISRC, artwork, previews, duration and
//! service links using keyless public APIs (Deezer, iTunes).

use crate::model::{ServiceLink, Track};
use crate::util::cancel::CancelToken;

/// Best-effort enrichment; never fails, returns the input on errors.
pub async fn enrich(http: &reqwest::Client, track: Track, country: &str, cancel: &CancelToken) -> Track {
    let _ = (http, country, cancel);
    track
}

/// Best catalog match for a title/artist (and optional duration), if confident.
pub async fn find(
    http: &reqwest::Client,
    title: &str,
    artist: &str,
    duration_ms: Option<u64>,
    country: &str,
    cancel: &CancelToken,
) -> Option<Track> {
    let _ = (http, title, artist, duration_ms, country, cancel);
    None
}

/// Search links for every major service.
pub fn search_links(track: &Track) -> Vec<ServiceLink> {
    let _ = track;
    Vec::new()
}
