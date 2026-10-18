//! Optional paid recognizers.

pub mod acrcloud;
pub mod audd;

use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde_json::Value;

use crate::error::{MfError, Result};
use crate::model::Track;
use crate::util::cancel::CancelToken;

/// Per-request timeout for provider uploads.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Cool-down reported for HTTP 429 without `Retry-After`.
const DEFAULT_RETRY: Duration = Duration::from_secs(60);

/// One answer from a paid provider.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderMatch {
    pub track: Track,
    /// Calibrated 0..=1.
    pub score: f32,
    /// Position in the reference track where the clip starts, when reported.
    pub offset_ms: Option<i64>,
}

/// Send `request`, racing cancellation, and return the JSON body of a 2xx answer.
async fn send_json(request: reqwest::RequestBuilder, cancel: &CancelToken, service: &str) -> Result<Value> {
    let response = tokio::select! {
        r = crate::util::http::prepare(request.timeout(REQUEST_TIMEOUT)).send() => r,
        _ = cancel.canceled() => return Err(MfError::Canceled),
    };
    let response = response.map_err(|e| transport_error(&e, service))?;
    let status = response.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let wait = retry_after(response.headers()).unwrap_or(DEFAULT_RETRY);
        return Err(MfError::RateLimited { retry_in_ms: wait.as_millis() as u64 });
    }
    if status.is_server_error() {
        return Err(MfError::network(format!("{service} is unavailable (HTTP {}).", status.as_u16())));
    }
    if !status.is_success() {
        return Err(MfError::network(format!("{service} rejected the request (HTTP {}).", status.as_u16())));
    }
    let body = tokio::select! {
        b = response.bytes() => b,
        _ = cancel.canceled() => return Err(MfError::Canceled),
    };
    let body = body.map_err(|e| transport_error(&e, service))?;
    serde_json::from_slice(&body).map_err(|_| MfError::network(format!("{service} sent an unreadable answer.")))
}

fn transport_error(e: &reqwest::Error, service: &str) -> MfError {
    if e.is_timeout() {
        MfError::network(format!("{service} did not answer in time."))
    } else if crate::util::http::is_connect(e) {
        MfError::network(format!("Could not connect to {service}."))
    } else {
        MfError::network(format!("The connection to {service} failed."))
    }
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds = headers.get(RETRY_AFTER)?.to_str().ok()?.trim().parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds.min(24 * 60 * 60)))
}

/// Non-empty trimmed string at a JSON pointer (numbers are formatted).
fn text_at(value: &Value, pointer: &str) -> Option<String> {
    match value.pointer(pointer)? {
        Value::String(s) => Some(s.trim()).filter(|s| !s.is_empty()).map(String::from),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Number at a JSON pointer, also accepting numeric strings ("0.88", "213093").
fn number_at(value: &Value, pointer: &str) -> Option<f64> {
    match value.pointer(pointer)? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
    .filter(|n: &f64| n.is_finite())
}

/// Year from a date like "2015-10-23".
fn year_of(date: &str) -> Option<u16> {
    date.get(..4)?.parse::<u16>().ok().filter(|y| (1000..=2999).contains(y))
}

/// Display string for credited artists: "A", "A & B", "A, B & C".
fn join_artists(artists: &[String]) -> String {
    match artists {
        [] => String::new(),
        [one] => one.clone(),
        [init @ .., last] => format!("{} & {last}", init.join(", ")),
    }
}

/// Apple artwork template (`.../{w}x{h}bb.jpg`) at 1000 px.
fn apple_artwork(template: &str) -> String {
    template.replace("{w}", "1000").replace("{h}", "1000").replace("{f}", "jpg")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        let v = serde_json::json!({"a": {"b": " x "}, "n": "0.88", "m": 70, "bad": "x"});
        assert_eq!(text_at(&v, "/a/b").as_deref(), Some("x"));
        assert_eq!(text_at(&v, "/m").as_deref(), Some("70"));
        assert_eq!(number_at(&v, "/n"), Some(0.88));
        assert_eq!(number_at(&v, "/m"), Some(70.0));
        assert_eq!(number_at(&v, "/bad"), None);
        assert_eq!(year_of("2015-10-23"), Some(2015));
        assert_eq!(year_of("15"), None);
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(join_artists(&names(&["A"])), "A");
        assert_eq!(join_artists(&names(&["A", "B"])), "A & B");
        assert_eq!(join_artists(&names(&["A", "B", "C"])), "A, B & C");
        assert_eq!(
            apple_artwork("https://is1-ssl.mzstatic.com/image/thumb/x/{w}x{h}bb.jpg"),
            "https://is1-ssl.mzstatic.com/image/thumb/x/1000x1000bb.jpg"
        );
    }
}
