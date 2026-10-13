//! AudD (`api.audd.io`): paid fingerprint recognition of short clips.

use std::collections::HashMap;

use reqwest::multipart::{Form, Part};
use serde_json::Value;

use crate::error::{MfError, Result};
use crate::model::{Service, ServiceLink, Track};
use crate::providers::{
    ProviderMatch, apple_artwork, join_artists, number_at, send_json, text_at, year_of,
};
use crate::util::cancel::CancelToken;

const ENDPOINT: &str = "https://api.audd.io/";
/// Largest upload the standard endpoint accepts.
const MAX_BYTES: usize = 10 * 1024 * 1024;
/// Calibrated confidence of a single AudD match.
const SCORE: f32 = 0.8;
const SERVICE: &str = "AudD";

/// AudD client for one API token.
pub struct AudD {
    http: reqwest::Client,
    token: String,
    endpoint: String,
}

impl AudD {
    pub fn new(http: reqwest::Client, token: String) -> Self {
        Self {
            http,
            token,
            endpoint: ENDPOINT.to_string(),
        }
    }

    /// Send requests to another URL (tests, proxies).
    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = endpoint.to_string();
        self
    }

    /// Recognize a WAV clip (≤ 12 s).
    pub async fn recognize(
        &self,
        wav: Vec<u8>,
        cancel: &CancelToken,
    ) -> Result<Option<ProviderMatch>> {
        cancel.check()?;
        let token = self.token.trim();
        if token.is_empty() {
            return Err(MfError::Platform {
                message: "AudD needs an API token.".into(),
            });
        }
        if wav.is_empty() {
            return Err(MfError::invalid("The clip is empty."));
        }
        if wav.len() > MAX_BYTES {
            return Err(MfError::invalid("The clip is too large for AudD."));
        }
        let file = Part::bytes(wav)
            .file_name("clip.wav")
            .mime_str("audio/wav")
            .map_err(|_| MfError::internal("invalid MIME type"))?;
        let form = Form::new()
            .text("api_token", token.to_string())
            .text("return", "apple_music,spotify")
            .part("file", file);
        let body = send_json(
            self.http.post(&self.endpoint).multipart(form),
            cancel,
            SERVICE,
        )
        .await?;
        parse(&body)
    }
}

/// Map an AudD answer: `Ok(None)` for "no match", errors for API failures
/// (codes from the AudD docs: 900/901 token, 400 file too large).
pub(crate) fn parse(body: &Value) -> Result<Option<ProviderMatch>> {
    if body.get("status").and_then(Value::as_str) == Some("error") {
        let code = number_at(body, "/error/error_code")
            .map(|c| c as i64)
            .unwrap_or(0);
        return Err(match code {
            900 | 901 => MfError::Platform {
                message: "AudD rejected the API token.".into(),
            },
            400 => MfError::invalid("The clip is too large for AudD."),
            _ => MfError::Platform {
                message: format!("AudD could not recognize the clip (error {code})."),
            },
        });
    }
    let Some(result) = body.get("result").filter(|r| r.is_object()) else {
        return Ok(None);
    };
    let Some(title) = text_at(result, "/title") else {
        return Ok(None);
    };
    let apple = result.get("apple_music").filter(|v| v.is_object());
    let spotify = result.get("spotify").filter(|v| v.is_object());
    let at = |source: Option<&Value>, pointer: &str| source.and_then(|v| text_at(v, pointer));

    let mut artists: Vec<String> = spotify
        .and_then(|s| s.get("artists"))
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(|a| text_at(a, "/name")).collect())
        .unwrap_or_default();
    let artist = text_at(result, "/artist").unwrap_or_else(|| join_artists(&artists));
    if artists.is_empty() && !artist.is_empty() {
        artists.push(artist.clone());
    }

    let mut links = Vec::new();
    if let Some(url) = at(spotify, "/external_urls/spotify").filter(|u| u.starts_with("https://")) {
        links.push(ServiceLink {
            service: Service::Spotify,
            url,
            is_search: false,
        });
    }
    if let Some(url) = at(apple, "/url").filter(|u| u.starts_with("https://")) {
        links.push(ServiceLink {
            service: Service::AppleMusic,
            url,
            is_search: false,
        });
    }
    let song_link = text_at(result, "/song_link").filter(|u| u.starts_with("https://"));
    if let Some(url) = &song_link {
        links.push(ServiceLink {
            service: Service::Web,
            url: url.clone(),
            is_search: false,
        });
    }

    let mut ids = HashMap::new();
    if let Some(link) = song_link {
        ids.insert("audd".to_string(), link);
    }
    if let Some(id) = at(spotify, "/id") {
        ids.insert("spotify".to_string(), id);
    }
    if let Some(id) = at(apple, "/playParams/id") {
        ids.insert("apple".to_string(), id);
    }

    let genre = apple
        .and_then(|a| a.get("genreNames"))
        .and_then(Value::as_array)
        .and_then(|g| {
            g.iter()
                .filter_map(Value::as_str)
                .find(|g| !g.is_empty() && *g != "Music")
        })
        .map(String::from);
    let track = Track {
        album: text_at(result, "/album")
            .or_else(|| at(apple, "/albumName"))
            .or_else(|| at(spotify, "/album/name")),
        isrc: text_at(result, "/isrc")
            .or_else(|| at(spotify, "/external_ids/isrc"))
            .or_else(|| at(apple, "/isrc"))
            .map(|s| s.to_ascii_uppercase()),
        duration_ms: apple
            .and_then(|a| number_at(a, "/durationInMillis"))
            .or_else(|| spotify.and_then(|s| number_at(s, "/duration_ms")))
            .filter(|d| *d > 0.0)
            .map(|d| d as u64),
        release_year: text_at(result, "/release_date")
            .or_else(|| at(apple, "/releaseDate"))
            .and_then(|d| year_of(&d)),
        genre,
        artwork_url: at(apple, "/artwork/url")
            .map(|t| apple_artwork(&t))
            .or_else(|| at(spotify, "/album/images/0/url"))
            .filter(|u| u.starts_with("https://")),
        preview_url: at(apple, "/previews/0/url")
            .or_else(|| at(spotify, "/preview_url"))
            .filter(|u| u.starts_with("https://")),
        links,
        ids,
        identity_key: String::new(),
        title,
        artist,
        artists,
    };
    let offset_ms = text_at(result, "/timecode").and_then(|t| timecode_ms(&t));
    Ok(Some(ProviderMatch {
        track,
        score: SCORE,
        offset_ms,
    }))
}

/// "MM:SS" or "H:MM:SS" → milliseconds.
fn timecode_ms(timecode: &str) -> Option<i64> {
    let mut total: i64 = 0;
    let mut parts = 0;
    for part in timecode.trim().split(':') {
        let value: i64 = part.trim().parse().ok().filter(|v| *v >= 0)?;
        total = total.checked_mul(60)?.checked_add(value)?;
        parts += 1;
    }
    (2..=3).contains(&parts).then_some(total * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shazam::test_server::{Reply, TestServer, http_client};
    use serde_json::json;

    const FIXTURE: &str = include_str!("testdata/audd_match.json");

    #[test]
    fn parses_documented_response() {
        let body: Value = serde_json::from_str(FIXTURE).unwrap();
        let m = parse(&body).unwrap().unwrap();
        let t = &m.track;
        assert_eq!(t.title, "Warriors");
        assert_eq!(t.artist, "Imagine Dragons");
        assert_eq!(t.artists, ["Imagine Dragons"]);
        assert_eq!(t.album.as_deref(), Some("Smoke + Mirrors (Deluxe)"));
        assert_eq!(t.isrc.as_deref(), Some("USUM71414163"));
        assert_eq!(t.duration_ms, Some(170_393));
        assert_eq!(t.release_year, Some(2014));
        assert_eq!(t.genre.as_deref(), Some("Alternative"));
        assert!(
            t.artwork_url
                .as_deref()
                .unwrap()
                .ends_with("/source/1000x1000bb.jpg")
        );
        assert!(t.preview_url.as_deref().unwrap().ends_with(".m4a"));
        assert_eq!(
            t.links
                .iter()
                .map(|l| (l.service, l.url.as_str(), l.is_search))
                .collect::<Vec<_>>(),
            [
                (
                    Service::Spotify,
                    "https://open.spotify.com/track/1lgN0A2Vki2FTON5PYq42m",
                    false
                ),
                (
                    Service::AppleMusic,
                    "https://music.apple.com/us/album/warriors/1440831203?i=1440831624",
                    false
                ),
                (Service::Web, "https://lis.tn/Warriors", false),
            ]
        );
        assert_eq!(
            t.ids.get("audd").map(String::as_str),
            Some("https://lis.tn/Warriors")
        );
        assert_eq!(
            t.ids.get("spotify").map(String::as_str),
            Some("1lgN0A2Vki2FTON5PYq42m")
        );
        assert_eq!(t.ids.get("apple").map(String::as_str), Some("1440831624"));
        assert_eq!(m.score, 0.8);
        assert_eq!(m.offset_ms, Some(62_000));
    }

    #[test]
    fn minimal_result_without_service_blocks() {
        let body = json!({"status": "success", "result": {"artist": "A", "title": "T", "timecode": "1:02:03"}});
        let m = parse(&body).unwrap().unwrap();
        assert_eq!(m.track.artists, ["A"]);
        assert!(m.track.links.is_empty());
        assert_eq!(m.offset_ms, Some(3_723_000));
    }

    #[test]
    fn no_result_and_malformed_results_are_none() {
        for body in [
            json!({"status": "success", "result": null}),
            json!({"status": "success", "result": []}),
            json!({"status": "success"}),
            json!({"status": "success", "result": {"artist": "No title"}}),
            json!({"status": "success", "result": {"title": ["x"]}}),
        ] {
            assert_eq!(parse(&body).unwrap(), None, "{body}");
        }
    }

    #[test]
    fn errors_map_to_engine_errors() {
        let err = |code: i64| {
            parse(&json!({"status": "error", "error": {"error_code": code, "error_message": "x"}}))
                .unwrap_err()
        };
        assert_eq!(
            err(900),
            MfError::Platform {
                message: "AudD rejected the API token.".into()
            }
        );
        assert_eq!(
            err(901),
            MfError::Platform {
                message: "AudD rejected the API token.".into()
            }
        );
        assert!(matches!(err(400), MfError::InvalidInput { .. }));
        assert_eq!(
            err(300),
            MfError::Platform {
                message: "AudD could not recognize the clip (error 300).".into()
            }
        );
    }

    #[test]
    fn timecodes() {
        assert_eq!(timecode_ms("00:56"), Some(56_000));
        assert_eq!(timecode_ms("2:32"), Some(152_000));
        assert_eq!(timecode_ms("1:00:00"), Some(3_600_000));
        assert_eq!(timecode_ms("56"), None);
        assert_eq!(timecode_ms("-1:00"), None);
        assert_eq!(timecode_ms("aa:bb"), None);
    }

    #[tokio::test]
    async fn sends_multipart_upload() {
        let server = TestServer::start(vec![Reply::json(200, FIXTURE)]);
        let audd = AudD::new(http_client(), " secret-token ".into())
            .with_endpoint(&format!("{}/", server.url()));
        let wav = b"RIFF\x24\x00\x00\x00WAVEfake".to_vec();
        let m = audd
            .recognize(wav.clone(), &CancelToken::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.track.title, "Warriors");

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        let r = &requests[0];
        assert_eq!((r.method.as_str(), r.target.as_str()), ("POST", "/"));
        assert!(
            r.header("content-type")
                .unwrap()
                .starts_with("multipart/form-data; boundary=")
        );
        assert!(r.header("user-agent").unwrap().starts_with("MusicFetch/"));
        let body = String::from_utf8_lossy(&r.body);
        assert!(body.contains("name=\"api_token\"\r\n\r\nsecret-token\r\n"));
        assert!(body.contains("name=\"return\"\r\n\r\napple_music,spotify\r\n"));
        assert!(body.contains(
            "name=\"file\"; filename=\"clip.wav\"\r\nContent-Type: audio/wav\r\n\r\nRIFF"
        ));
    }

    #[tokio::test]
    async fn http_failures_map_to_errors() {
        let server = TestServer::start(vec![
            Reply::new(429, "").header("Retry-After", "7"),
            Reply::new(503, ""),
            Reply::new(200, "<html>"),
            Reply::json(200, r#"{"status":"success","result":null}"#),
        ]);
        let audd = AudD::new(http_client(), "t".into()).with_endpoint(&server.url());
        let cancel = CancelToken::new();
        let wav = || vec![1u8; 100];
        assert_eq!(
            audd.recognize(wav(), &cancel).await.unwrap_err(),
            MfError::RateLimited { retry_in_ms: 7000 }
        );
        assert_eq!(
            audd.recognize(wav(), &cancel).await.unwrap_err(),
            MfError::network("AudD is unavailable (HTTP 503).")
        );
        assert_eq!(
            audd.recognize(wav(), &cancel).await.unwrap_err(),
            MfError::network("AudD sent an unreadable answer.")
        );
        assert_eq!(audd.recognize(wav(), &cancel).await.unwrap(), None);
    }

    #[tokio::test]
    async fn validates_before_sending() {
        let audd = AudD::new(http_client(), "  ".into()).with_endpoint("http://127.0.0.1:9");
        assert!(matches!(
            audd.recognize(vec![1], &CancelToken::new()).await,
            Err(MfError::Platform { .. })
        ));
        let audd = AudD::new(http_client(), "t".into()).with_endpoint("http://127.0.0.1:9");
        assert!(matches!(
            audd.recognize(Vec::new(), &CancelToken::new()).await,
            Err(MfError::InvalidInput { .. })
        ));
        let cancel = CancelToken::new();
        cancel.cancel();
        assert_eq!(
            audd.recognize(vec![1], &cancel).await.unwrap_err(),
            MfError::Canceled
        );
    }
}
