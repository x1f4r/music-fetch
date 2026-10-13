//! HTTP access for lyric search: per-service request budgets, spacing and
//! circuit breakers shared by all searches in the process, failure mapping, and
//! an optional on-disk response cache for development and evaluation.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use reqwest::StatusCode;

use crate::error::MfError;
use crate::util::cancel::CancelToken;
use crate::util::ratelimit::RateLimiter;

/// Per-request timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// Attempts for `503 Service Unavailable` (LRCLIB answers it intermittently).
const ATTEMPTS_ON_503: usize = 3;
/// Longest `Retry-After` honoured inline.
const MAX_INLINE_RETRY: Duration = Duration::from_secs(3);
/// Breaker cool-down after throttling: base, doubling up to the max.
const BREAKER_BASE: Duration = Duration::from_secs(20);
const BREAKER_MAX: Duration = Duration::from_secs(600);

/// A remote service used by lyric search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Remote {
    ITunes,
    Genius,
    YouTubeMusic,
    Deezer,
    Lrclib,
}

impl Remote {
    pub fn name(self) -> &'static str {
        match self {
            Remote::ITunes => "itunes",
            Remote::Genius => "genius",
            Remote::YouTubeMusic => "ytmusic",
            Remote::Deezer => "deezer",
            Remote::Lrclib => "lrclib",
        }
    }

    fn limits(self) -> &'static Limits {
        static ITUNES: LazyLock<Limits> = LazyLock::new(|| Limits::new(250, 8.0, 20.0 / 60.0));
        static GENIUS: LazyLock<Limits> = LazyLock::new(|| Limits::new(250, 8.0, 1.0));
        static YOUTUBE_MUSIC: LazyLock<Limits> = LazyLock::new(|| Limits::new(250, 8.0, 1.0));
        static DEEZER: LazyLock<Limits> = LazyLock::new(|| Limits::new(120, 10.0, 5.0));
        static LRCLIB: LazyLock<Limits> = LazyLock::new(|| Limits::new(100, 16.0, 4.0));
        match self {
            Remote::ITunes => &ITUNES,
            Remote::Genius => &GENIUS,
            Remote::YouTubeMusic => &YOUTUBE_MUSIC,
            Remote::Deezer => &DEEZER,
            Remote::Lrclib => &LRCLIB,
        }
    }
}

/// Request budget (token bucket: burst, then a sustained rate) plus minimum
/// spacing and a circuit breaker.
struct Limits {
    limiter: RateLimiter,
    bucket: Mutex<(f64, Instant)>,
    capacity: f64,
    per_second: f64,
}

impl Limits {
    fn new(spacing_ms: u64, capacity: f64, per_second: f64) -> Self {
        Self {
            limiter: RateLimiter::new(Duration::from_millis(spacing_ms), Duration::from_millis(50)),
            bucket: Mutex::new((capacity, Instant::now())),
            capacity,
            per_second,
        }
    }

    async fn acquire(&self, cancel: &CancelToken, max_wait: Duration) -> Result<(), FetchError> {
        loop {
            let wait = {
                let mut bucket = self.bucket.lock();
                let now = Instant::now();
                let refill = now.duration_since(bucket.1).as_secs_f64() * self.per_second;
                *bucket = ((bucket.0 + refill).min(self.capacity), now);
                if bucket.0 >= 1.0 {
                    bucket.0 -= 1.0;
                    break;
                }
                Duration::from_secs_f64((1.0 - bucket.0) / self.per_second)
            };
            if wait > max_wait {
                return Err(FetchError::Throttled { retry_in: wait });
            }
            cancel.sleep(wait).await.map_err(|_| FetchError::Canceled)?;
        }
        self.limiter.acquire(cancel, max_wait).await.map_err(|e| match e {
            MfError::RateLimited { retry_in_ms } => {
                FetchError::Throttled { retry_in: Duration::from_millis(retry_in_ms) }
            }
            _ => FetchError::Canceled,
        })
    }
}

/// Why a request produced no usable body.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FetchError {
    Canceled,
    /// The service is throttling us (or our own budget is spent).
    Throttled {
        retry_in: Duration,
    },
    NotFound,
    Status(u16),
    Network(String),
    /// The body is not what the service normally returns.
    Malformed,
}

/// A response body and whether it came from the disk cache.
pub(crate) struct Fetched {
    pub body: String,
    pub from_cache: bool,
}

/// Request counters for diagnostics.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    pub requests: AtomicU32,
    pub cache_hits: AtomicU32,
}

/// Performs requests for one search.
pub(crate) struct Fetcher<'a> {
    pub http: &'a reqwest::Client,
    pub cancel: &'a CancelToken,
    pub cache_dir: Option<&'a Path>,
    pub max_wait: Duration,
    pub stats: &'a Stats,
}

impl Fetcher<'_> {
    /// Send the request built by `build` (rebuilt for retries) unless `cache_key`
    /// is cached. Successful bodies are not cached here; call [`Fetcher::remember`]
    /// once the body parsed.
    pub async fn fetch(
        &self,
        remote: Remote,
        cache_key: &str,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<Fetched, FetchError> {
        if let Some(cached) = self.read_cache(remote, cache_key).await {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached.map(|body| Fetched { body, from_cache: true });
        }
        let limits = remote.limits();
        for attempt in 1..=ATTEMPTS_ON_503 {
            limits.acquire(self.cancel, self.max_wait).await?;
            self.cancel.check().map_err(|_| FetchError::Canceled)?;
            self.stats.requests.fetch_add(1, Ordering::Relaxed);
            let response = build().timeout(REQUEST_TIMEOUT).send().await.map_err(|e| {
                FetchError::Network(if e.is_timeout() { "timed out".into() } else { e.without_url().to_string() })
            })?;
            let status = response.status();
            let retry_after = retry_after(response.headers());
            match status {
                s if s.is_success() => {
                    let body = response.text().await.map_err(|e| FetchError::Network(e.without_url().to_string()))?;
                    limits.limiter.success();
                    return Ok(Fetched { body, from_cache: false });
                }
                StatusCode::NOT_FOUND => {
                    self.write_cache(remote, cache_key, None).await;
                    return Err(FetchError::NotFound);
                }
                StatusCode::SERVICE_UNAVAILABLE if attempt < ATTEMPTS_ON_503 => {
                    let wait = retry_after.unwrap_or(Duration::from_secs(1)).min(MAX_INLINE_RETRY);
                    self.cancel.sleep(wait).await.map_err(|_| FetchError::Canceled)?;
                }
                StatusCode::TOO_MANY_REQUESTS | StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                    let retry_in = limits.limiter.throttled(BREAKER_BASE, BREAKER_MAX, retry_after);
                    return Err(FetchError::Throttled { retry_in });
                }
                other => return Err(FetchError::Status(other.as_u16())),
            }
        }
        Err(FetchError::Status(StatusCode::SERVICE_UNAVAILABLE.as_u16()))
    }

    /// The service signalled throttling inside a successful response.
    pub async fn throttled(&self, remote: Remote) -> FetchError {
        let retry_in = remote.limits().limiter.throttled(BREAKER_BASE, BREAKER_MAX, None);
        FetchError::Throttled { retry_in }
    }

    /// Cache a body that parsed.
    pub async fn remember(&self, remote: Remote, cache_key: &str, fetched: &Fetched) {
        if !fetched.from_cache {
            self.write_cache(remote, cache_key, Some(&fetched.body)).await;
        }
    }

    fn cache_path(&self, remote: Remote, key: &str, found: bool) -> Option<PathBuf> {
        let dir = self.cache_dir?;
        let ext = if found { "json" } else { "404" };
        Some(dir.join(format!("{}-{:016x}.{ext}", remote.name(), fnv1a(key))))
    }

    /// `Some(Ok(body))` for a cached body, `Some(Err(NotFound))` for a cached 404.
    async fn read_cache(&self, remote: Remote, key: &str) -> Option<Result<String, FetchError>> {
        let found = self.cache_path(remote, key, true)?;
        if let Ok(body) = tokio::fs::read_to_string(&found).await {
            return Some(Ok(body));
        }
        let missing = self.cache_path(remote, key, false)?;
        tokio::fs::metadata(&missing).await.ok().map(|_| Err(FetchError::NotFound))
    }

    async fn write_cache(&self, remote: Remote, key: &str, body: Option<&str>) {
        let Some(path) = self.cache_path(remote, key, body.is_some()) else { return };
        if let Some(dir) = path.parent()
            && let Err(e) = tokio::fs::create_dir_all(dir).await
        {
            tracing::debug!("lyrics cache: {e}");
            return;
        }
        if let Err(e) = tokio::fs::write(&path, body.unwrap_or_default()).await {
            tracing::debug!("lyrics cache: {e}");
        }
    }
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let seconds: f64 = value.trim().parse().ok()?;
    (seconds.is_finite() && seconds >= 0.0).then(|| Duration::from_secs_f64(seconds.min(3600.0)))
}

/// FNV-1a: a stable file name for a cache key across builds and platforms.
fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv_is_stable() {
        assert_eq!(fnv1a(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a("a"), 0xaf63_dc4c_8601_ec8c);
    }

    #[tokio::test]
    async fn bucket_allows_a_burst_then_throttles() {
        let limits = Limits::new(0, 2.0, 0.5);
        let cancel = CancelToken::new();
        assert!(limits.acquire(&cancel, Duration::ZERO).await.is_ok());
        assert!(limits.acquire(&cancel, Duration::ZERO).await.is_ok());
        assert!(matches!(limits.acquire(&cancel, Duration::ZERO).await, Err(FetchError::Throttled { .. })));
    }

    #[tokio::test]
    async fn cache_round_trip() {
        let dir = std::env::temp_dir().join(format!("mf-lyrics-cache-{}", crate::util::new_id()));
        let http = crate::lyrics::tests::test_client();
        let cancel = CancelToken::new();
        let stats = Stats::default();
        let fetcher =
            Fetcher { http: &http, cancel: &cancel, cache_dir: Some(&dir), max_wait: Duration::ZERO, stats: &stats };
        let fetched = Fetched { body: "{\"ok\":1}".into(), from_cache: false };
        fetcher.remember(Remote::Deezer, "k", &fetched).await;
        fetcher.write_cache(Remote::Lrclib, "missing", None).await;
        let hit = fetcher.fetch(Remote::Deezer, "k", || http.get("http://127.0.0.1:9")).await;
        assert_eq!(hit.map(|f| (f.body, f.from_cache)), Ok(("{\"ok\":1}".to_string(), true)));
        let miss = fetcher.fetch(Remote::Lrclib, "missing", || http.get("http://127.0.0.1:9")).await;
        assert_eq!(miss.err(), Some(FetchError::NotFound));
        assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 2);
        assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
