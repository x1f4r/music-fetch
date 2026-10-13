//! Deezer public search (keyless). Matches lyrics too, but only when every word
//! matches, so it is queried with short windows.

use serde::Deserialize;

use super::{Hit, ParseError, RESULTS_PER_QUERY, non_empty};

const URL: &str = "https://api.deezer.com/search";

/// Deezer's "quota exceeded" error code.
const QUOTA_EXCEEDED: i64 = 4;

pub(crate) fn request(http: &reqwest::Client, query: &str) -> reqwest::RequestBuilder {
    let limit = RESULTS_PER_QUERY.to_string();
    http.get(URL).query(&[("q", query), ("limit", &limit)])
}

#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    data: Vec<serde_json::Value>,
    error: Option<ErrorBody>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ErrorBody {
    code: Option<i64>,
    message: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Item {
    id: Option<u64>,
    title: Option<String>,
    isrc: Option<String>,
    link: Option<String>,
    duration: Option<u64>,
    rank: Option<u64>,
    preview: Option<String>,
    artist: Option<Artist>,
    album: Option<Album>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Artist {
    name: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Album {
    title: Option<String>,
    cover_xl: Option<String>,
    cover_big: Option<String>,
}

pub(crate) fn parse(body: &str) -> Result<Vec<Hit>, ParseError> {
    let response: Response = serde_json::from_str(body)?;
    if let Some(error) = response.error {
        return Err(if error.code == Some(QUOTA_EXCEEDED) {
            ParseError::Throttled
        } else {
            ParseError::Malformed(error.message.unwrap_or_else(|| "error response".into()))
        });
    }
    let hits = response
        .data
        .into_iter()
        .filter_map(|value| serde_json::from_value::<Item>(value).ok())
        .map(|item| {
            let album = item.album.unwrap_or_default();
            Hit {
                title: item.title.unwrap_or_default(),
                artist: item.artist.and_then(|a| a.name).unwrap_or_default(),
                album: non_empty(album.title),
                duration_ms: item.duration.filter(|&s| s > 0).and_then(|s| s.checked_mul(1000)),
                isrc: non_empty(item.isrc),
                artwork_url: non_empty(album.cover_xl).or_else(|| non_empty(album.cover_big)),
                preview_url: non_empty(item.preview),
                id: item.id.map(|id| id.to_string()),
                url: non_empty(item.link),
                plays: item.rank,
                ..Hit::default()
            }
        })
        .collect();
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = r#"{"data":[{"id":4091937401,"title":"Bohemian Rhapsody","isrc":"GBUM71029604",
        "link":"https://www.deezer.com/track/4091937401","duration":354,"rank":958949,"preview":"https://cdnt-preview.dzcdn.net/x.mp3",
        "artist":{"id":412,"name":"Queen"},
        "album":{"id":1,"title":"A Night At The Opera","cover_xl":"https://cdn-images.dzcdn.net/images/cover/0b/1000x1000-000000-80-0-0.jpg"}}],
        "total":103}"#;

    #[test]
    fn parses_tracks() {
        let hits = parse(BODY).unwrap_or_default();
        assert_eq!(hits.len(), 1);
        let h = &hits[0];
        assert_eq!(h.title, "Bohemian Rhapsody");
        assert_eq!(h.artist, "Queen");
        assert_eq!(h.isrc.as_deref(), Some("GBUM71029604"));
        assert_eq!(h.duration_ms, Some(354_000));
        assert_eq!(h.plays, Some(958_949));
        assert!(h.artwork_url.as_deref().unwrap_or_default().contains("1000x1000"));
    }

    #[test]
    fn quota_errors_are_throttling() {
        let body = r#"{"error":{"type":"Exception","message":"Quota limit exceeded","code":4}}"#;
        assert_eq!(parse(body), Err(ParseError::Throttled));
        let body = r#"{"error":{"type":"Exception","message":"Bad","code":800}}"#;
        assert!(matches!(parse(body), Err(ParseError::Malformed(_))));
    }
}
