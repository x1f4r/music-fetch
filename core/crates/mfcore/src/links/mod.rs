//! Links: canonicalization, shortener resolution with SSRF guard, and
//! site-specific resolvers.

use crate::error::Result;
use crate::model::{SourceInfo, Track};
use crate::util::cancel::CancelToken;

/// What a link resolves to.
#[derive(Debug, Clone)]
pub enum Resolution {
    /// Tracks known from service metadata, no audio needed (Spotify, Apple Music,
    /// Deezer, Tidal, Amazon Music track/album/playlist pages).
    Tracks { source: SourceInfo, tracks: Vec<Track> },
    /// Media to fetch: through the media backend, or directly when `direct` is
    /// true (a plain audio/video file URL).
    Media { source: SourceInfo, url: String, direct: bool },
}

/// True when `text` (trimmed) is an http(s) URL or a bare domain link.
pub fn looks_like_url(text: &str) -> bool {
    let _ = text;
    todo!("links")
}

/// Offline normalization: tracking parameters stripped, canonical hosts and
/// paths (ported 0.4 rules). Errors on non-http(s) input.
pub fn canonicalize(url: &str) -> Result<String> {
    let _ = url;
    todo!("links")
}

/// Rejects URLs whose host resolves to a non-public address.
pub fn assert_public_url(url: &str) -> Result<()> {
    let _ = url;
    todo!("links")
}

/// Canonicalize, expand shorteners, then route to a resolver.
pub async fn resolve(http: &reqwest::Client, url: &str, country: &str, cancel: &CancelToken) -> Result<Resolution> {
    let _ = (http, url, country, cancel);
    todo!("links")
}

/// A link to `at_ms` within the source, when the site supports it.
pub fn deep_link(url: &str, at_ms: u64) -> Option<String> {
    let _ = (url, at_ms);
    todo!("links")
}
