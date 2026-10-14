//! iTunes Search API (keyless). Its song search also matches lyrics, which is
//! undocumented, and it is the most tolerant backend for transcription errors.

use serde::Deserialize;

use super::{Hit, ParseError, RESULTS_PER_QUERY, non_empty};

const URL: &str = "https://itunes.apple.com/search";

pub(crate) fn request(http: &reqwest::Client, query: &str, country: &str) -> reqwest::RequestBuilder {
    let limit = RESULTS_PER_QUERY.to_string();
    http.get(URL).query(&[("term", query), ("entity", "song"), ("limit", &limit), ("country", country)])
}

#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    results: Vec<serde_json::Value>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct Item {
    wrapper_type: Option<String>,
    kind: Option<String>,
    track_id: Option<u64>,
    track_name: Option<String>,
    artist_name: Option<String>,
    collection_name: Option<String>,
    track_view_url: Option<String>,
    preview_url: Option<String>,
    artwork_url100: Option<String>,
    track_time_millis: Option<u64>,
    release_date: Option<String>,
    primary_genre_name: Option<String>,
}

pub(crate) fn parse(body: &str) -> Result<Vec<Hit>, ParseError> {
    let response: Response = serde_json::from_str(body)?;
    let hits = response
        .results
        .into_iter()
        .filter_map(|value| serde_json::from_value::<Item>(value).ok())
        .filter(|item| item.wrapper_type.as_deref().is_none_or(|w| w == "track"))
        .filter(|item| item.kind.as_deref().is_none_or(|k| k == "song"))
        .map(|item| Hit {
            title: item.track_name.unwrap_or_default(),
            artist: item.artist_name.unwrap_or_default(),
            album: non_empty(item.collection_name),
            duration_ms: item.track_time_millis.filter(|&ms| ms > 0),
            release_year: item.release_date.as_deref().and_then(|d| d.get(..4)).and_then(|y| y.parse().ok()),
            genre: non_empty(item.primary_genre_name),
            artwork_url: item.artwork_url100.as_deref().map(large_artwork),
            preview_url: non_empty(item.preview_url),
            id: item.track_id.map(|id| id.to_string()),
            url: item.track_view_url.as_deref().map(strip_tracking),
            ..Hit::default()
        })
        .collect();
    Ok(hits)
}

/// Apple serves any size from the same path: ask for 1000 px instead of 100 px.
fn large_artwork(url: &str) -> String {
    match url.rfind('/') {
        Some(slash) if url[slash..].contains("x100bb") || url[slash..].contains("x100-") => {
            let (base, file) = url.split_at(slash + 1);
            let ext = file.rsplit('.').next().unwrap_or("jpg");
            format!("{base}1000x1000bb.{ext}")
        }
        _ => url.to_string(),
    }
}

/// Drop the `uo` affiliate-tracking parameter from Apple links.
fn strip_tracking(url: &str) -> String {
    let Some((base, query)) = url.split_once('?') else { return url.to_string() };
    let kept: Vec<&str> = query.split('&').filter(|p| !p.starts_with("uo=") && !p.is_empty()).collect();
    if kept.is_empty() { base.to_string() } else { format!("{base}?{}", kept.join("&")) }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = r#"{"resultCount":3,"results":[
        {"wrapperType":"track","kind":"song","trackId":6781080302,"artistName":"Queen","collectionName":"Greatest Hits",
         "trackName":"Bohemian Rhapsody","trackViewUrl":"https://music.apple.com/us/album/bohemian-rhapsody/6781080300?i=6781080302&uo=4",
         "previewUrl":"https://audio-ssl.itunes.apple.com/x.m4a",
         "artworkUrl100":"https://is1-ssl.mzstatic.com/image/thumb/Music211/v4/b0/602527594347.jpg/100x100bb.jpg",
         "trackTimeMillis":355145,"releaseDate":"1975-10-31T12:00:00Z","primaryGenreName":"Rock"},
        {"wrapperType":"track","kind":"music-video","trackId":1,"artistName":"Queen","trackName":"Video"},
        {"wrapperType":"track","kind":"song","trackName":"No Artist"}
    ]}"#;

    #[test]
    fn parses_songs() {
        let hits = parse(BODY).unwrap_or_default();
        assert_eq!(hits.len(), 2);
        let h = &hits[0];
        assert_eq!(h.title, "Bohemian Rhapsody");
        assert_eq!(h.artist, "Queen");
        assert_eq!(h.album.as_deref(), Some("Greatest Hits"));
        assert_eq!(h.duration_ms, Some(355_145));
        assert_eq!(h.release_year, Some(1975));
        assert_eq!(h.id.as_deref(), Some("6781080302"));
        assert_eq!(
            h.url.as_deref(),
            Some("https://music.apple.com/us/album/bohemian-rhapsody/6781080300?i=6781080302")
        );
        assert_eq!(
            h.artwork_url.as_deref(),
            Some("https://is1-ssl.mzstatic.com/image/thumb/Music211/v4/b0/602527594347.jpg/1000x1000bb.jpg")
        );
        // The artist-less item is dropped later by `sources::parse`.
        assert_eq!(hits[1].artist, "");
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse("<html>").is_err());
        assert_eq!(parse("{}").map(|h| h.len()), Ok(0));
    }

    #[test]
    fn tracking_parameter_is_removed() {
        assert_eq!(strip_tracking("https://x/a?uo=4"), "https://x/a");
        assert_eq!(strip_tracking("https://x/a?i=1&uo=4&b=2"), "https://x/a?i=1&b=2");
        assert_eq!(strip_tracking("https://x/a"), "https://x/a");
    }
}
