//! Lyric-search backends: request builders and response parsers.
//!
//! Parsers are tolerant: missing fields become `None`, malformed items are
//! skipped, and only a body that is not the expected JSON at all is an error.

pub(crate) mod deezer;
pub(crate) mod genius;
pub(crate) mod itunes;
pub(crate) mod ytmusic;

use super::Backend;

/// Results requested per backend query.
pub(crate) const RESULTS_PER_QUERY: usize = 10;

/// One search result from one backend query.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Hit {
    pub title: String,
    /// Display artist, e.g. "Elton John & Britney Spears".
    pub artist: String,
    /// Individual artists when the backend lists them.
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub duration_ms: Option<u64>,
    pub release_year: Option<u16>,
    pub genre: Option<String>,
    pub isrc: Option<String>,
    pub artwork_url: Option<String>,
    pub preview_url: Option<String>,
    /// The backend's own id for the track.
    pub id: Option<String>,
    /// Track page on the backend's service.
    pub url: Option<String>,
    /// Lyric excerpt around the match (Genius).
    pub snippet: Option<String>,
    /// Query words found in the lyrics, and how many of them without typos (Genius).
    pub matched_words: Option<u32>,
    pub exact_words: Option<u32>,
    /// Popularity hint: plays (YouTube Music), rank (Deezer) or page views (Genius).
    pub plays: Option<u64>,
}

/// Why a backend response could not be used.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ParseError {
    /// The service reported a quota or throttling error inside a 200 response.
    Throttled,
    /// The body is not the expected JSON.
    Malformed(String),
}

impl From<serde_json::Error> for ParseError {
    fn from(e: serde_json::Error) -> Self {
        ParseError::Malformed(e.to_string())
    }
}

/// Build the request for `query`.
pub(crate) fn request(backend: Backend, http: &reqwest::Client, query: &str, country: &str) -> reqwest::RequestBuilder {
    match backend {
        Backend::ITunes => itunes::request(http, query, country),
        Backend::Genius => genius::request(http, query),
        Backend::YouTubeMusic => ytmusic::request(http, query, country),
        Backend::Deezer => deezer::request(http, query),
    }
}

/// Stable cache key of a request (excludes volatile parts like client versions).
pub(crate) fn cache_key(backend: Backend, query: &str, country: &str) -> String {
    match backend {
        Backend::ITunes | Backend::YouTubeMusic => format!("{}|{}|{query}", backend.name(), country),
        Backend::Genius | Backend::Deezer => format!("{}|{query}", backend.name()),
    }
}

/// Parse a response body into ranked hits.
pub(crate) fn parse(backend: Backend, body: &str) -> Result<Vec<Hit>, ParseError> {
    let mut hits = match backend {
        Backend::ITunes => itunes::parse(body)?,
        Backend::Genius => genius::parse(body)?,
        Backend::YouTubeMusic => ytmusic::parse(body)?,
        Backend::Deezer => deezer::parse(body)?,
    };
    hits.retain(|h| !h.title.trim().is_empty() && !h.artist.trim().is_empty());
    hits.truncate(RESULTS_PER_QUERY);
    Ok(hits)
}

/// Empty strings become `None`.
pub(crate) fn non_empty(value: Option<String>) -> Option<String> {
    value.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}
