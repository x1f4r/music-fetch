//! Shazam HTTP client with a serial queue, spacing, circuit breaker and cache.
//!
//! Every request of one client goes through a single queue. Interactive
//! requests are spaced at least 3.5 s apart and batch requests 8 s (plus up to
//! 1 s jitter); batch requests wait for their slot outside the queue, so an
//! interactive request that arrives meanwhile goes first. HTTP 429, a burst of
//! 5xx answers or a 200 with a non-JSON body opens a circuit breaker (30 s,
//! doubling up to 60 min). While it is open a request waits if the remaining
//! cool-down fits its priority (interactive 10 s, batch 10 min) and fails with
//! `RateLimited` otherwise. Answers, including "no match", are cached by the
//! SHA-256 of the signature.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use reqwest::header::{ACCEPT, CONTENT_LANGUAGE, HeaderMap, RETRY_AFTER, USER_AGENT};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::error::{MfError, Result};
use crate::model::Track;
use crate::shazam::Signature;
use crate::shazam::response::parse_match;
use crate::shazam::signature::MIN_SAMPLES;
use crate::util::cancel::CancelToken;
use crate::util::ratelimit::RateLimiter;

const ENDPOINT: &str = "https://amp.shazam.com";
const QUERY: &str = "sync=true&webv3=true&sampling=true&connected=&shazamapiversion=v3&sharehub=true&video=v3";
const CACHE_CAPACITY: usize = 256;
/// Upper bound on breaker rounds within one `recognize` call.
const MAX_THROTTLE_ROUNDS: u32 = 6;
/// Longest `Retry-After` taken at face value.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(24 * 60 * 60);
/// Fixed request context; never derived from the user.
const TIMEZONE: &str = "Europe/Paris";

/// Generic Android client strings, one picked per request.
const USER_AGENTS: [&str; 8] = [
    "Dalvik/2.1.0 (Linux; U; Android 14; Pixel 8 Build/AP2A.240805.005)",
    "Dalvik/2.1.0 (Linux; U; Android 14; SM-S921B Build/UP1A.231005.007)",
    "Dalvik/2.1.0 (Linux; U; Android 13; SM-S911B Build/TP1A.220624.014)",
    "Dalvik/2.1.0 (Linux; U; Android 13; Pixel 7 Build/TQ3A.230901.001)",
    "Dalvik/2.1.0 (Linux; U; Android 12; SM-G991B Build/SP1A.210812.016)",
    "Dalvik/2.1.0 (Linux; U; Android 11; Mi 10T Pro Build/RKQ1.200826.002)",
    "Dalvik/2.1.0 (Linux; U; Android 10; SM-G973F Build/QP1A.190711.020)",
    "Dalvik/2.1.0 (Linux; U; Android 9; SM-G960F Build/PPR1.180610.011)",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// A person is waiting (live listening, short files).
    Interactive,
    /// Long-mix scanning.
    Batch,
}

/// Pacing, retry and breaker settings. The defaults suit the public endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct QueuePolicy {
    /// Minimum gap before an interactive request.
    pub interactive_spacing: Duration,
    /// Minimum gap before a batch request.
    pub batch_spacing: Duration,
    /// Random extra gap added to either spacing (0..=jitter).
    pub jitter: Duration,
    /// Longest breaker cool-down an interactive request waits out.
    pub interactive_max_wait: Duration,
    /// Longest breaker cool-down a batch request waits out.
    pub batch_max_wait: Duration,
    /// First breaker cool-down; doubles with every further throttle.
    pub breaker_base: Duration,
    /// Longest breaker cool-down (a longer `Retry-After` still wins).
    pub breaker_max: Duration,
    /// Delays before retrying a 5xx or failed request (never closer than the spacing).
    pub retry_delays: Vec<Duration>,
    /// Timeout of one HTTP exchange.
    pub request_timeout: Duration,
}

impl Default for QueuePolicy {
    fn default() -> Self {
        Self {
            interactive_spacing: Duration::from_millis(3500),
            batch_spacing: Duration::from_secs(8),
            jitter: Duration::from_secs(1),
            interactive_max_wait: Duration::from_secs(10),
            batch_max_wait: Duration::from_secs(10 * 60),
            breaker_base: Duration::from_secs(30),
            breaker_max: Duration::from_secs(60 * 60),
            retry_delays: vec![Duration::from_secs(2), Duration::from_secs(6)],
            request_timeout: Duration::from_secs(15),
        }
    }
}

impl QueuePolicy {
    fn max_wait(&self, priority: Priority) -> Duration {
        match priority {
            Priority::Interactive => self.interactive_max_wait,
            Priority::Batch => self.batch_max_wait,
        }
    }
}

/// A recognized recording and how the query aligns with it.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ShazamMatch {
    /// Title, artist, album, isrc, artwork, links and ids filled from the response.
    /// `duration_ms` and `identity_key` are left for enrichment.
    pub track: Track,
    /// Position in the reference track where the query starts.
    pub offset_ms: i64,
    /// Speed ratio minus one (0.03 = 3 % faster).
    pub timeskew: f32,
    /// Pitch ratio minus one.
    pub frequency_skew: f32,
}

/// Client for Shazam's `discovery/v5` tag endpoint. Share one per install.
pub struct ShazamClient {
    http: reqwest::Client,
    base_url: String,
    country: String,
    language: String,
    content_language: String,
    policy: QueuePolicy,
    limiter: RateLimiter,
    queue: tokio::sync::Mutex<QueueState>,
    breaker_until: Mutex<Option<Instant>>,
    cache: Mutex<LruCache>,
}

struct QueueState {
    last_sent: Option<Instant>,
}

/// Outcome of one HTTP exchange.
enum Attempt {
    Answer(Option<Box<ShazamMatch>>),
    Throttled(Option<Duration>),
    ServerError(u16),
    /// Timeout or connection failure; worth a retry.
    Transient(MfError),
    Fatal(MfError),
}

enum Failure {
    Throttled(Option<Duration>),
    Error(MfError),
}

impl ShazamClient {
    /// `country` is an ISO 3166-1 alpha-2 storefront ("US"), `language` a BCP-47
    /// tag ("en-US"); invalid values fall back to US / en-US.
    pub fn new(http: reqwest::Client, country: &str, language: &str) -> Self {
        let policy = QueuePolicy::default();
        let language = normalize_language(language);
        Self {
            http,
            base_url: ENDPOINT.to_string(),
            country: normalize_country(country),
            content_language: language.replace('-', "_"),
            language,
            limiter: RateLimiter::new(policy.interactive_spacing, policy.jitter),
            policy,
            queue: tokio::sync::Mutex::new(QueueState { last_sent: None }),
            breaker_until: Mutex::new(None),
            cache: Mutex::new(LruCache::new(CACHE_CAPACITY)),
        }
    }

    /// Replace the pacing policy. Call before the first request.
    pub fn with_policy(mut self, policy: QueuePolicy) -> Self {
        self.limiter = RateLimiter::new(policy.interactive_spacing, policy.jitter);
        self.policy = policy;
        self
    }

    /// Send requests to another origin (tests, proxies), e.g. "http://127.0.0.1:8080".
    pub fn with_base_url(mut self, base_url: &str) -> Self {
        self.base_url = base_url.trim_end_matches('/').to_string();
        self
    }

    pub fn policy(&self) -> &QueuePolicy {
        &self.policy
    }

    /// Remaining circuit-breaker cool-down, if it is open.
    pub fn cooldown(&self) -> Option<Duration> {
        let mut until = self.breaker_until.lock();
        match *until {
            Some(t) if t > Instant::now() => Some(t - Instant::now()),
            Some(_) => {
                *until = None;
                None
            }
            None => None,
        }
    }

    /// `Ok(None)` means "no match". `Err(RateLimited)` when the breaker is open
    /// longer than the priority allows.
    pub async fn recognize(
        &self,
        signature: &Signature,
        priority: Priority,
        cancel: &CancelToken,
    ) -> Result<Option<ShazamMatch>> {
        cancel.check()?;
        if (signature.sample_count as usize) < MIN_SAMPLES {
            return Err(MfError::invalid("The clip is too short to recognize; at least 3 seconds are needed."));
        }
        let key: [u8; 32] = Sha256::digest(&signature.bytes).into();
        if let Some(hit) = self.cache.lock().get(&key) {
            return Ok(hit);
        }

        let max_wait = self.policy.max_wait(priority);
        let batch_jitter = random_up_to(self.policy.jitter);
        let mut throttles = 0;
        loop {
            // Wait out an open breaker without holding the queue.
            if let Some(remaining) = self.cooldown() {
                if remaining > max_wait || throttles >= MAX_THROTTLE_ROUNDS {
                    return Err(MfError::RateLimited { retry_in_ms: remaining.as_millis() as u64 });
                }
                cancel.sleep(remaining).await?;
            }
            let mut queue = tokio::select! {
                guard = self.queue.lock() => guard,
                _ = cancel.canceled() => return Err(MfError::Canceled),
            };
            if self.cooldown().is_some() {
                continue;
            }
            if priority == Priority::Batch
                && let Some(ready) =
                    queue.last_sent.map(|t| later(t, self.policy.batch_spacing.saturating_add(batch_jitter)))
                && ready > Instant::now()
            {
                drop(queue);
                cancel.sleep(ready - Instant::now()).await?;
                continue;
            }
            match self.send_with_retries(&mut queue, signature, max_wait, cancel).await {
                Ok(result) => {
                    self.limiter.success();
                    *self.breaker_until.lock() = None;
                    self.cache.lock().insert(key, result.clone());
                    return Ok(result);
                }
                Err(Failure::Throttled(retry_after)) => {
                    throttles += 1;
                    let retry_after = retry_after.map(|d| d.min(MAX_RETRY_AFTER));
                    let wait = self.limiter.throttled(self.policy.breaker_base, self.policy.breaker_max, retry_after);
                    *self.breaker_until.lock() = Some(later(Instant::now(), wait));
                    tracing::warn!(cooldown_s = wait.as_secs(), "shazam throttled; circuit breaker open");
                }
                Err(Failure::Error(e)) => return Err(e),
            }
        }
    }

    async fn send_with_retries(
        &self,
        queue: &mut QueueState,
        signature: &Signature,
        max_wait: Duration,
        cancel: &CancelToken,
    ) -> std::result::Result<Option<ShazamMatch>, Failure> {
        let attempts = self.policy.retry_delays.len() + 1;
        let mut server_errors = 0;
        let mut last_error = None;
        for attempt in 0..attempts {
            if attempt > 0 {
                cancel.sleep(self.policy.retry_delays[attempt - 1]).await.map_err(Failure::Error)?;
            }
            self.limiter.acquire(cancel, max_wait).await.map_err(Failure::Error)?;
            queue.last_sent = Some(Instant::now());
            match self.send_once(signature, cancel).await {
                Attempt::Answer(result) => return Ok(result.map(|m| *m)),
                Attempt::Throttled(retry_after) => return Err(Failure::Throttled(retry_after)),
                Attempt::Fatal(e) => return Err(Failure::Error(e)),
                Attempt::ServerError(status) => {
                    server_errors += 1;
                    tracing::debug!(status, attempt, "shazam server error");
                    last_error = Some(MfError::network(format!("Shazam is unavailable (HTTP {status}).")));
                }
                Attempt::Transient(e) => {
                    tracing::debug!(attempt, error = %e, "shazam request failed");
                    last_error = Some(e);
                }
            }
        }
        if server_errors == attempts {
            return Err(Failure::Throttled(None));
        }
        Err(Failure::Error(last_error.unwrap_or_else(|| MfError::network("Shazam did not answer."))))
    }

    async fn send_once(&self, signature: &Signature, cancel: &CancelToken) -> Attempt {
        let url = format!(
            "{}/discovery/v5/{}/{}/android/-/tag/{}/{}?{QUERY}",
            self.base_url,
            self.language,
            self.country,
            uuid::Uuid::new_v4().to_string().to_ascii_uppercase(),
            uuid::Uuid::new_v4(),
        );
        let request = self
            .http
            .post(url)
            .header(USER_AGENT, USER_AGENTS[fastrand::usize(..USER_AGENTS.len())])
            .header(CONTENT_LANGUAGE, &self.content_language)
            .header(ACCEPT, "application/json")
            .json(&request_body(signature, crate::util::now_ms()))
            .timeout(self.policy.request_timeout);
        let response = tokio::select! {
            r = request.send() => r,
            _ = cancel.canceled() => return Attempt::Fatal(MfError::Canceled),
        };
        let response = match response {
            Ok(r) => r,
            Err(e) => return Attempt::Transient(transport_error(&e)),
        };
        let status = response.status();
        if status.as_u16() == 429 {
            return Attempt::Throttled(retry_after(response.headers()));
        }
        if status.is_server_error() {
            return Attempt::ServerError(status.as_u16());
        }
        if !status.is_success() {
            return Attempt::Fatal(MfError::network(format!(
                "Shazam rejected the request (HTTP {}).",
                status.as_u16()
            )));
        }
        let body = tokio::select! {
            b = response.bytes() => b,
            _ = cancel.canceled() => return Attempt::Fatal(MfError::Canceled),
        };
        match body {
            Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(json) => Attempt::Answer(parse_match(&json, &self.country).map(Box::new)),
                // Throttled clients get HTML or empty pages with status 200.
                Err(_) => Attempt::Throttled(None),
            },
            Err(e) => Attempt::Transient(transport_error(&e)),
        }
    }
}

fn request_body(signature: &Signature, now_ms: i64) -> Value {
    json!({
        "geolocation": {"altitude": 300, "latitude": 45, "longitude": 2},
        "signature": {"samplems": signature.sample_ms(), "timestamp": now_ms, "uri": signature.data_uri()},
        "timestamp": now_ms,
        "timezone": TIMEZONE,
    })
}

fn transport_error(e: &reqwest::Error) -> MfError {
    let message = if e.is_timeout() {
        "Shazam did not answer in time."
    } else if e.is_connect() {
        "Could not connect to Shazam."
    } else {
        "The connection to Shazam failed."
    };
    MfError::network(message)
}

/// `Retry-After` in seconds (HTTP dates are ignored).
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds = headers.get(RETRY_AFTER)?.to_str().ok()?.trim().parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

/// `t + d` without overflow (waits are capped at a year).
fn later(t: Instant, d: Duration) -> Instant {
    t.checked_add(d.min(Duration::from_secs(365 * 24 * 60 * 60))).unwrap_or(t)
}

fn random_up_to(max: Duration) -> Duration {
    if max.is_zero() { Duration::ZERO } else { Duration::from_millis(fastrand::u64(0..=max.as_millis() as u64)) }
}

fn normalize_country(country: &str) -> String {
    let country = country.trim().to_ascii_uppercase();
    if country.len() == 2 && country.bytes().all(|b| b.is_ascii_uppercase()) { country } else { "US".to_string() }
}

fn normalize_language(language: &str) -> String {
    let language = language.trim().replace('_', "-");
    let valid = (2..=16).contains(&language.len())
        && language.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        && language.bytes().next().is_some_and(|b| b.is_ascii_alphabetic());
    if valid { language } else { "en-US".to_string() }
}

/// Small LRU map from signature hash to answer.
struct LruCache {
    capacity: usize,
    map: HashMap<[u8; 32], Option<ShazamMatch>>,
    order: VecDeque<[u8; 32]>,
}

impl LruCache {
    fn new(capacity: usize) -> Self {
        Self { capacity, map: HashMap::with_capacity(capacity), order: VecDeque::with_capacity(capacity) }
    }

    fn get(&mut self, key: &[u8; 32]) -> Option<Option<ShazamMatch>> {
        let value = self.map.get(key)?.clone();
        self.touch(key);
        Some(value)
    }

    fn insert(&mut self, key: [u8; 32], value: Option<ShazamMatch>) {
        if self.map.insert(key, value).is_some() {
            self.touch(&key);
            return;
        }
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            }
        }
    }

    fn touch(&mut self, key: &[u8; 32]) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(*key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shazam::test_server::{Reply, TestServer, http_client};

    const FIXTURE: &str = include_str!("testdata/match_get_lucky.json");
    const NO_MATCH: &str = r#"{"matches":[],"timestamp":1791924035549,"timezone":"Europe/Paris","tagid":"6E3B1F0C-2D4A-4C61-9B3A-0F6C2A8E1D21"}"#;

    fn policy() -> QueuePolicy {
        QueuePolicy {
            interactive_spacing: Duration::from_millis(40),
            batch_spacing: Duration::from_millis(150),
            jitter: Duration::ZERO,
            interactive_max_wait: Duration::from_millis(100),
            batch_max_wait: Duration::from_secs(2),
            breaker_base: Duration::from_millis(300),
            breaker_max: Duration::from_millis(1200),
            retry_delays: vec![Duration::from_millis(20), Duration::from_millis(60)],
            request_timeout: Duration::from_millis(400),
        }
    }

    fn client(server: &TestServer) -> ShazamClient {
        ShazamClient::new(http_client(), "de", "de-DE").with_policy(policy()).with_base_url(&server.url())
    }

    fn sig(seed: u8) -> Signature {
        Signature { bytes: vec![seed; 64], sample_count: 192_000 }
    }

    #[tokio::test]
    async fn sends_songrec_shaped_request_and_parses_match() {
        let server = TestServer::start(vec![Reply::json(200, FIXTURE)]);
        let client = client(&server);
        let m = client.recognize(&sig(1), Priority::Interactive, &CancelToken::new()).await.unwrap().unwrap();
        assert_eq!(m.track.title, "Get Lucky");
        assert_eq!(m.offset_ms, 47_996);
        assert!(m.track.links.iter().any(|l| l.url == "https://music.apple.com/de/song/617154366"));

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        let r = &requests[0];
        assert_eq!(r.method, "POST");
        let (path, query) = r.target.split_once('?').unwrap();
        assert_eq!(query, QUERY);
        let rest = path.strip_prefix("/discovery/v5/de-DE/DE/android/-/tag/").unwrap();
        let (uuid1, uuid2) = rest.split_once('/').unwrap();
        assert_eq!(uuid1.len(), 36);
        assert_eq!(uuid1, uuid1.to_ascii_uppercase());
        assert!(uuid::Uuid::parse_str(uuid1).is_ok() && uuid::Uuid::parse_str(uuid2).is_ok());
        assert!(r.header("user-agent").unwrap().starts_with("Dalvik/2.1.0 (Linux; U; Android "));
        assert_eq!(r.header("content-language"), Some("de_DE"));
        assert_eq!(r.header("content-type"), Some("application/json"));

        let body: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["signature"]["uri"], sig(1).data_uri());
        assert_eq!(body["signature"]["samplems"], 12_000);
        assert!(body["signature"]["timestamp"].as_i64().unwrap() > 1_700_000_000_000);
        assert_eq!(body["timestamp"], body["signature"]["timestamp"]);
        assert_eq!(body["geolocation"]["latitude"], 45);
        assert_eq!(body["timezone"], TIMEZONE);
    }

    #[tokio::test]
    async fn no_match_is_none_and_cached() {
        let server = TestServer::start(vec![Reply::json(200, NO_MATCH), Reply::json(200, FIXTURE)]);
        let client = client(&server);
        let cancel = CancelToken::new();
        assert_eq!(client.recognize(&sig(2), Priority::Interactive, &cancel).await.unwrap(), None);
        assert_eq!(client.recognize(&sig(2), Priority::Interactive, &cancel).await.unwrap(), None);
        assert_eq!(server.requests().len(), 1);
        assert!(client.recognize(&sig(3), Priority::Interactive, &cancel).await.unwrap().is_some());
        assert!(client.recognize(&sig(3), Priority::Batch, &cancel).await.unwrap().is_some());
        assert_eq!(server.requests().len(), 2);
    }

    #[tokio::test]
    async fn rejects_too_short_clips_without_a_request() {
        let server = TestServer::start(vec![Reply::json(200, FIXTURE)]);
        let short = Signature { bytes: vec![1; 64], sample_count: 47_999 };
        let err = client(&server).recognize(&short, Priority::Interactive, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, MfError::InvalidInput { .. }));
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn spacing_per_priority() {
        let server = TestServer::start(vec![Reply::json(200, NO_MATCH)]);
        let spaced = QueuePolicy {
            interactive_spacing: Duration::from_millis(120),
            batch_spacing: Duration::from_millis(360),
            ..policy()
        };
        let client = client(&server).with_policy(spaced);
        let cancel = CancelToken::new();
        for seed in 10..13 {
            client.recognize(&sig(seed), Priority::Interactive, &cancel).await.unwrap();
        }
        for seed in 13..15 {
            client.recognize(&sig(seed), Priority::Batch, &cancel).await.unwrap();
        }
        // Server-side timestamps carry some scheduling noise; allow 30 ms.
        let at: Vec<Instant> = server.requests().iter().map(|r| r.at).collect();
        assert_eq!(at.len(), 5);
        for pair in at[..3].windows(2) {
            let gap = pair[1] - pair[0];
            assert!(gap >= Duration::from_millis(90) && gap < Duration::from_millis(330), "interactive gap {gap:?}");
        }
        for pair in at[2..].windows(2) {
            assert!(pair[1] - pair[0] >= Duration::from_millis(330), "batch gap {:?}", pair[1] - pair[0]);
        }
    }

    #[tokio::test]
    async fn throttle_fails_interactive_fast_and_keeps_breaker_open() {
        let server = TestServer::start(vec![Reply::new(429, ""), Reply::json(200, FIXTURE)]);
        let client = client(&server);
        let cancel = CancelToken::new();
        let err = client.recognize(&sig(20), Priority::Interactive, &cancel).await.unwrap_err();
        let MfError::RateLimited { retry_in_ms } = err else { panic!("expected RateLimited, got {err:?}") };
        assert!((250..=340).contains(&retry_in_ms), "retry_in_ms {retry_in_ms}");
        assert!(client.cooldown().is_some());
        // While open, interactive requests fail without touching the network.
        let err = client.recognize(&sig(21), Priority::Interactive, &cancel).await.unwrap_err();
        assert!(matches!(err, MfError::RateLimited { .. }));
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn batch_waits_out_the_breaker_and_retries() {
        let server = TestServer::start(vec![Reply::new(429, ""), Reply::json(200, FIXTURE)]);
        let client = client(&server);
        let started = Instant::now();
        let m = client.recognize(&sig(22), Priority::Batch, &CancelToken::new()).await.unwrap();
        assert!(m.is_some());
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].at - requests[0].at >= Duration::from_millis(290));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(client.cooldown().is_none(), "success closes the breaker");
    }

    #[tokio::test]
    async fn retry_after_is_honoured() {
        let server = TestServer::start(vec![Reply::new(429, "").header("Retry-After", "1")]);
        let client = client(&server);
        let err = client.recognize(&sig(23), Priority::Interactive, &CancelToken::new()).await.unwrap_err();
        let MfError::RateLimited { retry_in_ms } = err else { panic!("expected RateLimited, got {err:?}") };
        assert!((990..=1200).contains(&retry_in_ms), "retry_in_ms {retry_in_ms}");
    }

    #[tokio::test]
    async fn batch_gives_up_when_the_cooldown_exceeds_its_limit() {
        let server = TestServer::start(vec![Reply::new(429, "").header("Retry-After", "60")]);
        let client = client(&server);
        let started = Instant::now();
        let err = client.recognize(&sig(24), Priority::Batch, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, MfError::RateLimited { .. }));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn non_json_200_opens_the_breaker() {
        let server = TestServer::start(vec![Reply::new(200, "<html>Too many requests</html>")]);
        let client = client(&server);
        let err = client.recognize(&sig(25), Priority::Interactive, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, MfError::RateLimited { .. }));
        assert!(client.cooldown().is_some());
    }

    #[tokio::test]
    async fn server_errors_are_retried() {
        let server = TestServer::start(vec![Reply::new(503, ""), Reply::new(502, ""), Reply::json(200, FIXTURE)]);
        let client = client(&server);
        let m = client.recognize(&sig(26), Priority::Interactive, &CancelToken::new()).await.unwrap();
        assert!(m.is_some());
        let at: Vec<Instant> = server.requests().iter().map(|r| r.at).collect();
        assert_eq!(at.len(), 3);
        assert!(at[1] - at[0] >= Duration::from_millis(35), "retries keep the spacing");
        assert!(at[2] - at[1] >= Duration::from_millis(55), "second retry after 60 ms");
    }

    #[tokio::test]
    async fn a_burst_of_server_errors_opens_the_breaker() {
        let server = TestServer::start(vec![Reply::new(500, "")]);
        let client = client(&server);
        let err = client.recognize(&sig(27), Priority::Interactive, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, MfError::RateLimited { .. }));
        assert_eq!(server.requests().len(), 3);
        assert!(client.cooldown().is_some());
    }

    #[tokio::test]
    async fn bad_request_is_not_retried() {
        let server = TestServer::start(vec![Reply::new(400, "")]);
        let client = client(&server);
        let err = client.recognize(&sig(28), Priority::Interactive, &CancelToken::new()).await.unwrap_err();
        assert_eq!(err, MfError::network("Shazam rejected the request (HTTP 400)."));
        assert_eq!(server.requests().len(), 1);
        assert!(client.cooldown().is_none());
    }

    #[tokio::test]
    async fn timeouts_are_retried_then_reported() {
        let server = TestServer::start(vec![Reply::json(200, FIXTURE).delayed(Duration::from_millis(1500))]);
        let client = client(&server);
        let err = client.recognize(&sig(29), Priority::Interactive, &CancelToken::new()).await.unwrap_err();
        assert_eq!(err, MfError::network("Shazam did not answer in time."));
        assert_eq!(server.requests().len(), 3);
        assert!(client.cooldown().is_none(), "timeouts do not open the breaker");
    }

    #[tokio::test]
    async fn connection_failure_is_a_network_error() {
        let url = {
            let server = TestServer::start(vec![]);
            server.url()
        };
        let client = ShazamClient::new(http_client(), "US", "en-US").with_policy(policy()).with_base_url(&url);
        let err = client.recognize(&sig(30), Priority::Interactive, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, MfError::Network { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn cancel_interrupts_breaker_wait_and_requests() {
        let server =
            TestServer::start(vec![Reply::new(429, ""), Reply::json(200, FIXTURE).delayed(Duration::from_secs(5))]);
        let client = client(&server);
        let cancel = CancelToken::new();
        let canceller = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                cancel.cancel();
            })
        };
        let started = Instant::now();
        let err = client.recognize(&sig(31), Priority::Batch, &cancel).await.unwrap_err();
        assert_eq!(err, MfError::Canceled);
        assert!(started.elapsed() < Duration::from_millis(250));
        canceller.await.unwrap();
        assert_eq!(client.recognize(&sig(32), Priority::Batch, &cancel).await.unwrap_err(), MfError::Canceled);
    }

    #[test]
    fn normalizes_locale() {
        assert_eq!(normalize_country("de"), "DE");
        assert_eq!(normalize_country("Germany"), "US");
        assert_eq!(normalize_language("en_GB"), "en-GB");
        assert_eq!(normalize_language("../x"), "en-US");
        assert_eq!(normalize_language(""), "en-US");
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let mut cache = LruCache::new(2);
        cache.insert([1; 32], None);
        cache.insert([2; 32], None);
        assert!(cache.get(&[1; 32]).is_some());
        cache.insert([3; 32], None);
        assert!(cache.get(&[2; 32]).is_none());
        assert!(cache.get(&[1; 32]).is_some());
        assert!(cache.get(&[3; 32]).is_some());
        cache.insert([3; 32], None);
        assert_eq!(cache.map.len(), 2);
        assert_eq!(cache.order.len(), 2);
    }
}
