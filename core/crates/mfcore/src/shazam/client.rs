//! Shazam HTTP client with a serial queue, spacing, circuit breaker and cache.

use crate::error::Result;
use crate::model::Track;
use crate::shazam::Signature;
use crate::util::cancel::CancelToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// A person is waiting (live listening, short files).
    Interactive,
    /// Long-mix scanning.
    Batch,
}

#[derive(Debug, Clone)]
pub struct ShazamMatch {
    /// Title, artist, album, isrc, artwork, links and ids filled from the response.
    pub track: Track,
    /// Position in the reference track where the query starts.
    pub offset_ms: i64,
    pub timeskew: f32,
    pub frequency_skew: f32,
}

pub struct ShazamClient {
    _http: reqwest::Client,
}

impl ShazamClient {
    pub fn new(http: reqwest::Client, country: &str, language: &str) -> Self {
        let _ = (country, language);
        Self { _http: http }
    }

    /// `Ok(None)` means "no match". `Err(RateLimited)` when the breaker is open
    /// longer than the priority allows.
    pub async fn recognize(&self, signature: &Signature, priority: Priority, cancel: &CancelToken) -> Result<Option<ShazamMatch>> {
        let _ = (signature, priority, cancel);
        todo!("shazam::client")
    }
}
