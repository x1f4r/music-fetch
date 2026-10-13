//! ACRCloud identification API v1 (`POST /v1/identify`, HMAC-SHA1 signed):
//! recordings through a fingerprint project, humming through a project with
//! the "Cover Song (humming) Identification" engine.

use std::collections::HashMap;

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::multipart::{Form, Part};
use serde_json::Value;
use sha1::Sha1;

use crate::config::AcrCredentials;
use crate::error::{MfError, Result};
use crate::model::{Service, ServiceLink, Track};
use crate::providers::{ProviderMatch, join_artists, number_at, send_json, text_at, year_of};
use crate::util::cancel::CancelToken;

const PATH: &str = "/v1/identify";
const DATA_TYPE: &str = "audio";
const SIGNATURE_VERSION: &str = "1";
/// Documented sample limit ("below 5M bytes").
const MAX_BYTES: usize = 5_000_000;
const MAX_HUMMING: usize = 5;
/// Calibration ceiling: no single provider answer is treated as certain.
const SCORE_CEILING: f32 = 0.95;
/// Fingerprint score assumed when an item has none (the documented minimum).
const MIN_MUSIC_SCORE: f64 = 70.0;
const SERVICE: &str = "ACRCloud";

/// Client for one ACRCloud project (fingerprint or humming engine).
pub struct AcrCloud {
    http: reqwest::Client,
    creds: AcrCredentials,
    base_url: Option<String>,
}

impl AcrCloud {
    pub fn new(http: reqwest::Client, creds: AcrCredentials) -> Self {
        Self {
            http,
            creds,
            base_url: None,
        }
    }

    /// Send requests to another origin instead of `https://{host}` (tests, proxies).
    pub fn with_base_url(mut self, base_url: &str) -> Self {
        self.base_url = Some(base_url.trim_end_matches('/').to_string());
        self
    }

    /// Fingerprint recognition of a WAV clip (≤ 12 s). Returns the best-scored match.
    pub async fn recognize(
        &self,
        wav: Vec<u8>,
        cancel: &CancelToken,
    ) -> Result<Option<ProviderMatch>> {
        Ok(self
            .identify(wav, cancel)
            .await?
            .and_then(|body| best_music(&body)))
    }

    /// Humming recognition (project with the humming engine). Ranked, best first, at most 5.
    pub async fn recognize_humming(
        &self,
        wav: Vec<u8>,
        cancel: &CancelToken,
    ) -> Result<Vec<ProviderMatch>> {
        Ok(self
            .identify(wav, cancel)
            .await?
            .map(|body| humming(&body))
            .unwrap_or_default())
    }

    /// Upload the clip; `None` when ACRCloud found nothing.
    async fn identify(&self, wav: Vec<u8>, cancel: &CancelToken) -> Result<Option<Value>> {
        cancel.check()?;
        let access_key = self.creds.access_key.trim();
        let secret = self.creds.access_secret.trim();
        if access_key.is_empty() || secret.is_empty() {
            return Err(MfError::Platform {
                message: "ACRCloud needs an access key and secret.".into(),
            });
        }
        let origin = match &self.base_url {
            Some(url) => url.clone(),
            None => format!("https://{}", normalize_host(&self.creds.host)?),
        };
        if wav.is_empty() {
            return Err(MfError::invalid("The clip is empty."));
        }
        if wav.len() > MAX_BYTES {
            return Err(MfError::invalid("The clip is too large for ACRCloud."));
        }
        let timestamp = crate::util::now_ms() / 1000;
        let sample_bytes = wav.len();
        let sample = Part::bytes(wav)
            .file_name("sample.wav")
            .mime_str("audio/wav")
            .map_err(|_| MfError::internal("invalid MIME type"))?;
        let form = Form::new()
            .text("access_key", access_key.to_string())
            .text("sample_bytes", sample_bytes.to_string())
            .text("timestamp", timestamp.to_string())
            .text("signature", sign(access_key, secret, timestamp)?)
            .text("data_type", DATA_TYPE)
            .text("signature_version", SIGNATURE_VERSION)
            .part("sample", sample);
        let body = send_json(
            self.http.post(format!("{origin}{PATH}")).multipart(form),
            cancel,
            SERVICE,
        )
        .await?;
        check_status(&body)?;
        Ok(Some(body).filter(|b| status_code(b) == Some(0)))
    }
}

/// `"POST\n/v1/identify\n{access_key}\naudio\n1\n{timestamp}"`
pub(crate) fn string_to_sign(access_key: &str, timestamp: i64) -> String {
    format!("POST\n{PATH}\n{access_key}\n{DATA_TYPE}\n{SIGNATURE_VERSION}\n{timestamp}")
}

/// `base64(HMAC-SHA1(secret, string_to_sign))`
pub(crate) fn sign(access_key: &str, secret: &str, timestamp: i64) -> Result<String> {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret.as_bytes())
        .map_err(|_| MfError::internal("invalid HMAC key"))?;
    mac.update(string_to_sign(access_key, timestamp).as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

/// "identify-eu-west-1.acrcloud.com" (an `https://` prefix and trailing slash are tolerated).
fn normalize_host(host: &str) -> Result<String> {
    let host = host.trim();
    let host = host
        .strip_prefix("https://")
        .unwrap_or(host)
        .trim_end_matches('/');
    let valid = !host.is_empty()
        && host.len() <= 253
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':'))
        && !host.starts_with(['.', '-', ':']);
    if valid {
        Ok(host.to_ascii_lowercase())
    } else {
        Err(MfError::invalid("The ACRCloud host is not valid."))
    }
}

fn status_code(body: &Value) -> Option<i64> {
    number_at(body, "/status/code").map(|c| c as i64)
}

/// Map ACRCloud status codes (docs: SDK reference, error codes).
fn check_status(body: &Value) -> Result<()> {
    let Some(code) = status_code(body) else {
        return Err(MfError::network("ACRCloud sent an unreadable answer."));
    };
    match code {
        // 1001: no result; 2004: no fingerprint could be made (silence, too short).
        0 | 1001 | 2004 => Ok(()),
        3001 | 3014 => Err(MfError::Platform {
            message: "ACRCloud rejected the credentials.".into(),
        }),
        // Request quota of the plan exhausted.
        3003 => Err(MfError::RateLimited {
            retry_in_ms: 60 * 60 * 1000,
        }),
        // Queries-per-second limit.
        3015 => Err(MfError::RateLimited { retry_in_ms: 2000 }),
        3000 => Err(MfError::network("ACRCloud is unavailable.")),
        _ => Err(MfError::Platform {
            message: format!("ACRCloud could not recognize the clip (error {code})."),
        }),
    }
}

/// The highest-scored `metadata.music[]` item (first on ties).
fn best_music(body: &Value) -> Option<ProviderMatch> {
    let items = body.pointer("/metadata/music")?.as_array()?;
    items
        .iter()
        .filter_map(|item| {
            let score = number_at(item, "/score").unwrap_or(MIN_MUSIC_SCORE);
            item_match(item, calibrate(score / 100.0))
        })
        .fold(None, |best: Option<ProviderMatch>, m| match best {
            Some(b) if b.score >= m.score => Some(b),
            _ => Some(m),
        })
}

/// `metadata.humming[]`, best first, at most five.
fn humming(body: &Value) -> Vec<ProviderMatch> {
    let Some(items) = body.pointer("/metadata/humming").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut matches: Vec<ProviderMatch> = items
        .iter()
        .filter_map(|item| item_match(item, calibrate(number_at(item, "/score").unwrap_or(0.0))))
        .collect();
    matches.sort_by(|a, b| b.score.total_cmp(&a.score));
    matches.truncate(MAX_HUMMING);
    matches
}

fn calibrate(raw: f64) -> f32 {
    raw.clamp(0.0, 1.0) as f32 * SCORE_CEILING
}

fn item_match(item: &Value, score: f32) -> Option<ProviderMatch> {
    let title = text_at(item, "/title")?;
    let artists: Vec<String> = item
        .get("artists")
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(|a| text_at(a, "/name")).collect())
        .unwrap_or_default();

    let mut links = Vec::new();
    let mut ids = HashMap::new();
    if let Some(acrid) = text_at(item, "/acrid") {
        ids.insert("acrcloud".to_string(), acrid);
    }
    let external = |service: &str, pointer: &str| {
        let entry = item.pointer(&format!("/external_metadata/{service}"))?;
        // Usually an object; some projects return a list of candidates.
        let entry = entry
            .as_array()
            .and_then(|list| list.first())
            .unwrap_or(entry);
        text_at(entry, pointer).filter(|id| {
            id.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
    };
    if let Some(id) = external("spotify", "/track/id") {
        links.push(ServiceLink {
            service: Service::Spotify,
            url: format!("https://open.spotify.com/track/{id}"),
            is_search: false,
        });
        ids.insert("spotify".to_string(), id);
    }
    if let Some(id) = external("deezer", "/track/id") {
        links.push(ServiceLink {
            service: Service::Deezer,
            url: format!("https://www.deezer.com/track/{id}"),
            is_search: false,
        });
        ids.insert("deezer".to_string(), id);
    }
    if let Some(id) = external("youtube", "/vid") {
        links.push(ServiceLink {
            service: Service::YouTube,
            url: format!("https://www.youtube.com/watch?v={id}"),
            is_search: false,
        });
        ids.insert("youtube".to_string(), id);
    }

    let track = Track {
        artist: join_artists(&artists),
        artists,
        album: text_at(item, "/album/name"),
        isrc: text_at(item, "/external_ids/isrc").map(|s| s.to_ascii_uppercase()),
        duration_ms: number_at(item, "/duration_ms")
            .filter(|d| *d > 0.0)
            .map(|d| d as u64),
        release_year: text_at(item, "/release_date").and_then(|d| year_of(&d)),
        genre: text_at(item, "/genres/0/name"),
        artwork_url: None,
        preview_url: None,
        links,
        ids,
        identity_key: String::new(),
        title,
    };
    let offset_ms = number_at(item, "/play_offset_ms").map(|o| o as i64);
    Some(ProviderMatch {
        track,
        score,
        offset_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shazam::test_server::{Reply, TestServer, http_client};
    use serde_json::json;

    const MUSIC: &str = include_str!("testdata/acr_music.json");
    const HUMMING: &str = include_str!("testdata/acr_humming.json");
    const NO_RESULT: &str = r#"{"status":{"msg":"No result","version":"1.0","code":1001}}"#;

    fn creds() -> AcrCredentials {
        AcrCredentials {
            host: "identify-eu-west-1.acrcloud.com".into(),
            access_key: "a1b2c3d4e5f6a7b8".into(),
            access_secret: "Example0Secret0Key0123456789abcdefABCDEF".into(),
        }
    }

    /// Value of a text field in a multipart body.
    fn field(body: &str, name: &str) -> Option<String> {
        let needle = format!("name=\"{name}\"\r\n\r\n");
        let start = body.find(&needle)? + needle.len();
        let end = body[start..].find("\r\n")? + start;
        Some(body[start..end].to_string())
    }

    #[test]
    fn signature_matches_documented_construction() {
        // Expected value from the Python sample in ACRCloud's identification API docs:
        // base64.b64encode(hmac.new(secret, string_to_sign, digestmod=hashlib.sha1).digest())
        assert_eq!(
            string_to_sign("a1b2c3d4e5f6a7b8", 1_791_900_000),
            "POST\n/v1/identify\na1b2c3d4e5f6a7b8\naudio\n1\n1791900000"
        );
        assert_eq!(
            sign(
                "a1b2c3d4e5f6a7b8",
                "Example0Secret0Key0123456789abcdefABCDEF",
                1_791_900_000
            )
            .unwrap(),
            "mp/xuHc17uqDkJOHO74HuITLOZk="
        );
        // RFC 2202 HMAC-SHA1 test case 2.
        let mut mac = Hmac::<Sha1>::new_from_slice(b"Jefe").unwrap();
        mac.update(b"what do ya want for nothing?");
        let hex: String = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(hex, "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79");
    }

    #[test]
    fn parses_music_and_picks_the_best_score() {
        let body: Value = serde_json::from_str(MUSIC).unwrap();
        check_status(&body).unwrap();
        let m = best_music(&body).unwrap();
        let t = &m.track;
        assert_eq!(t.title, "Hello");
        assert_eq!(t.artist, "Adele");
        assert_eq!(t.artists, ["Adele"]);
        assert_eq!(t.album.as_deref(), Some("Hello"));
        assert_eq!(t.isrc.as_deref(), Some("GBBKS1500214"));
        assert_eq!(t.duration_ms, Some(295_000));
        assert_eq!(t.release_year, Some(2015));
        assert_eq!(t.genre.as_deref(), Some("Pop"));
        assert_eq!(m.score, 0.95);
        assert_eq!(m.offset_ms, Some(9040));
        assert_eq!(
            t.links
                .iter()
                .map(|l| (l.service, l.url.as_str()))
                .collect::<Vec<_>>(),
            [
                (
                    Service::Spotify,
                    "https://open.spotify.com/track/4aebBr4JAihzJQR0CiIZJv"
                ),
                (Service::Deezer, "https://www.deezer.com/track/110265034"),
                (
                    Service::YouTube,
                    "https://www.youtube.com/watch?v=YQHsXMglC9A"
                ),
            ]
        );
        assert!(t.links.iter().all(|l| !l.is_search));
        assert_eq!(
            t.ids.get("acrcloud").map(String::as_str),
            Some("6049f11da7095e8bb8266871d4a70873")
        );
        assert_eq!(
            t.ids.get("spotify").map(String::as_str),
            Some("4aebBr4JAihzJQR0CiIZJv")
        );
        assert_eq!(t.ids.get("deezer").map(String::as_str), Some("110265034"));
        assert_eq!(
            t.ids.get("youtube").map(String::as_str),
            Some("YQHsXMglC9A")
        );

        // A lower-scored item listed first loses; string numbers are accepted.
        let swapped = json!({"metadata": {"music": [body["metadata"]["music"][1], body["metadata"]["music"][0]]}});
        assert_eq!(best_music(&swapped).unwrap().track.title, "Hello");
        let cover = json!({"metadata": {"music": [body["metadata"]["music"][1]]}});
        let m = best_music(&cover).unwrap();
        assert_eq!(m.track.artist, "Adele & Someone Else");
        assert_eq!(m.track.duration_ms, Some(296_000));
        assert!((m.score - 0.76 * 0.95).abs() < 1e-6);
    }

    #[test]
    fn parses_humming_ranked_and_capped() {
        let body: Value = serde_json::from_str(HUMMING).unwrap();
        let list = humming(&body);
        assert_eq!(list.len(), 5);
        let titles: Vec<&str> = list.iter().map(|m| m.track.title.as_str()).collect();
        assert_eq!(
            titles,
            [
                "As Long As You Love Me",
                "As Long As You Love Me",
                "If You Love Me",
                "Hurt (Album Version)",
                "打开爱"
            ]
        );
        assert!((list[0].score - 0.88 * 0.95).abs() < 1e-6);
        assert!(list.windows(2).all(|w| w[0].score >= w[1].score));
        assert_eq!(list[0].track.artist, "Backstreet Boys");
        assert_eq!(
            list[0].track.ids.get("deezer").map(String::as_str),
            Some("602351")
        );
        assert_eq!(list[1].track.duration_ms, Some(213_093));
        assert_eq!(list[1].track.genre.as_deref(), Some("Pop"));
        assert_eq!(list[2].track.album, None, "empty album names are dropped");
    }

    #[test]
    fn status_codes() {
        let status = |code: i64| check_status(&json!({"status": {"code": code, "msg": "x"}}));
        assert!(status(0).is_ok() && status(1001).is_ok() && status(2004).is_ok());
        let creds_err = MfError::Platform {
            message: "ACRCloud rejected the credentials.".into(),
        };
        assert_eq!(status(3001).unwrap_err(), creds_err);
        assert_eq!(status(3014).unwrap_err(), creds_err);
        assert!(matches!(
            status(3003).unwrap_err(),
            MfError::RateLimited { .. }
        ));
        assert!(matches!(
            status(3015).unwrap_err(),
            MfError::RateLimited { .. }
        ));
        assert!(matches!(status(3000).unwrap_err(), MfError::Network { .. }));
        assert_eq!(
            status(3006).unwrap_err(),
            MfError::Platform {
                message: "ACRCloud could not recognize the clip (error 3006).".into()
            }
        );
        assert!(check_status(&json!({"metadata": {}})).is_err());
    }

    #[test]
    fn malformed_items_are_skipped() {
        let body = json!({"metadata": {"music": [{"score": 99}, "x", {"title": "Ok", "score": "bad", "artists": "nope",
            "external_metadata": {"spotify": [{"track": {"id": "abc/../x"}}], "deezer": {"track": {"id": 42}}}}]}});
        let m = best_music(&body).unwrap();
        assert_eq!(m.track.title, "Ok");
        assert!(m.track.artists.is_empty());
        assert!((m.score - 0.7 * 0.95).abs() < 1e-6);
        assert_eq!(m.track.links.len(), 1, "unsafe ids are dropped");
        assert_eq!(m.track.links[0].url, "https://www.deezer.com/track/42");
        assert!(best_music(&json!({"metadata": {}})).is_none());
        assert!(humming(&json!({"metadata": {"humming": [{"score": "0.9"}]}})).is_empty());
    }

    #[test]
    fn hosts() {
        assert_eq!(
            normalize_host(" https://Identify-EU-West-1.acrcloud.com/ ").unwrap(),
            "identify-eu-west-1.acrcloud.com"
        );
        for bad in [
            "",
            "http://x.com",
            "x.com/path",
            "x.com?q",
            "-x.com",
            "a b.com",
        ] {
            assert!(normalize_host(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn sends_signed_multipart_request() {
        let server = TestServer::start(vec![Reply::json(200, MUSIC)]);
        let acr = AcrCloud::new(http_client(), creds()).with_base_url(&server.url());
        let wav = vec![7u8; 1234];
        let m = acr
            .recognize(wav, &CancelToken::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.track.title, "Hello");

        let requests = server.requests();
        let r = &requests[0];
        assert_eq!(
            (r.method.as_str(), r.target.as_str()),
            ("POST", "/v1/identify")
        );
        assert!(
            r.header("content-type")
                .unwrap()
                .starts_with("multipart/form-data; boundary=")
        );
        let body = String::from_utf8_lossy(&r.body);
        assert_eq!(
            field(&body, "access_key").as_deref(),
            Some("a1b2c3d4e5f6a7b8")
        );
        assert_eq!(field(&body, "sample_bytes").as_deref(), Some("1234"));
        assert_eq!(field(&body, "data_type").as_deref(), Some("audio"));
        assert_eq!(field(&body, "signature_version").as_deref(), Some("1"));
        let timestamp: i64 = field(&body, "timestamp").unwrap().parse().unwrap();
        assert!((timestamp - crate::util::now_ms() / 1000).abs() < 60);
        let expected = sign(
            "a1b2c3d4e5f6a7b8",
            "Example0Secret0Key0123456789abcdefABCDEF",
            timestamp,
        )
        .unwrap();
        assert_eq!(field(&body, "signature"), Some(expected));
        assert!(body.contains(
            "name=\"sample\"; filename=\"sample.wav\"\r\nContent-Type: audio/wav\r\n\r\n"
        ));
        assert!(
            !body.contains("Example0Secret"),
            "the secret never leaves the device"
        );
    }

    #[tokio::test]
    async fn humming_and_no_result_over_http() {
        let server = TestServer::start(vec![
            Reply::json(200, HUMMING),
            Reply::json(200, NO_RESULT),
            Reply::json(200, NO_RESULT),
        ]);
        let acr = AcrCloud::new(http_client(), creds()).with_base_url(&server.url());
        let cancel = CancelToken::new();
        assert_eq!(
            acr.recognize_humming(vec![1; 10], &cancel)
                .await
                .unwrap()
                .len(),
            5
        );
        assert!(
            acr.recognize_humming(vec![1; 10], &cancel)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(acr.recognize(vec![1; 10], &cancel).await.unwrap(), None);
    }

    #[tokio::test]
    async fn errors_before_and_after_sending() {
        let server = TestServer::start(vec![
            Reply::json(
                200,
                r#"{"status":{"msg":"invalid signature","version":"1.0","code":3014}}"#,
            ),
            Reply::new(502, ""),
        ]);
        let acr = AcrCloud::new(http_client(), creds()).with_base_url(&server.url());
        let cancel = CancelToken::new();
        assert!(matches!(
            acr.recognize(vec![1; 10], &cancel).await,
            Err(MfError::Platform { .. })
        ));
        assert_eq!(
            acr.recognize(vec![1; 10], &cancel).await.unwrap_err(),
            MfError::network("ACRCloud is unavailable (HTTP 502).")
        );
        assert!(matches!(
            acr.recognize(vec![1; MAX_BYTES + 1], &cancel).await,
            Err(MfError::InvalidInput { .. })
        ));
        assert_eq!(server.requests().len(), 2);

        let no_secret = AcrCredentials {
            access_secret: " ".into(),
            ..creds()
        };
        let acr = AcrCloud::new(http_client(), no_secret);
        assert!(matches!(
            acr.recognize(vec![1], &cancel).await,
            Err(MfError::Platform { .. })
        ));
        let bad_host = AcrCredentials {
            host: "evil.example/path".into(),
            ..creds()
        };
        let acr = AcrCloud::new(http_client(), bad_host);
        assert!(matches!(
            acr.recognize(vec![1], &cancel).await,
            Err(MfError::InvalidInput { .. })
        ));
    }
}
