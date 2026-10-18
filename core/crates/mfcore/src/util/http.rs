//! HTTP client factory and the header policy. One shared client per engine.
//!
//! Requirements: rustls with webpki roots on every platform (no JNI setup on
//! Android), gzip/brotli, a neutral `MusicFetch/<version>` user agent, sane
//! timeouts. Never put personal data in headers.
//!
//! TLS is a preconfigured rustls `ClientConfig` (ring provider, Mozilla roots
//! from `webpki-roots`), so reqwest never touches `rustls-platform-verifier` or
//! a process-wide crypto provider.
//!
//! On wasm32 reqwest runs on the browser's fetch(), which owns TLS, connection
//! reuse and redirects (up to 20 hops); timeouts are per request there.
//!
//! [`HeaderRule`]s are the one table of the headers some services need (a
//! Dalvik agent for Shazam, a crawler agent for Spotify and Amazon pages,
//! YouTube's consent cookie, YouTube Music's origin). Native call sites apply
//! their rule from it; the browser extension installs the same table as
//! declarativeNetRequest session rules ([`header_rules`]), because fetch() cannot
//! set these headers itself.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
#[cfg(not(target_family = "wasm"))]
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::error::{MfError, Result};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest wait of a [`reachable`] check.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(4);

pub fn user_agent(app_version: &str) -> String {
    format!("MusicFetch/{app_version} (+https://github.com/x1f4r/music-fetch)")
}

/// A browser-like user agent for sites that refuse unknown clients.
pub const BROWSER_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36";

/// Crawler user agent that makes many sites return Open Graph metadata.
pub const CRAWLER_UA: &str = "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)";

/// rustls configuration shared by every client: ring, TLS 1.2 + 1.3, webpki roots,
/// ALPN for HTTP/2 and HTTP/1.1.
#[cfg(not(target_family = "wasm"))]
pub fn tls_config() -> Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| MfError::internal(format!("tls setup: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    // reqwest only fills ALPN for configs it builds itself.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

#[cfg(not(target_family = "wasm"))]
pub fn build_client(app_version: &str) -> Result<reqwest::Client> {
    let client = reqwest::Client::builder()
        .tls_backend_preconfigured(tls_config()?)
        .user_agent(user_agent(app_version))
        .timeout(DEFAULT_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_idle_timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .map_err(|e| MfError::internal(format!("http client: {e}")))?;
    Ok(client)
}

/// The browser sets TLS, redirects and connection reuse itself. Chrome ignores
/// the user agent; Firefox sends it.
#[cfg(target_family = "wasm")]
pub fn build_client(app_version: &str) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(user_agent(app_version))
        .build()
        .map_err(|e| MfError::internal(format!("http client: {e}")))
}

/// Settings every engine request gets. In the browser no request carries the
/// user's cookies or HTTP credentials (`credentials: 'omit'`). Test runs that
/// installed [`TestEndpoints`](crate::util::endpoints::TestEndpoints) get the
/// request sent to the local stub instead.
pub fn prepare(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let request = crate::util::endpoints::reroute(request);
    #[cfg(target_family = "wasm")]
    {
        request.fetch_credentials_omit()
    }
    #[cfg(not(target_family = "wasm"))]
    {
        request
    }
}

/// True for connection failures. fetch() reports every transport failure (DNS,
/// refused connection, TLS, offline) as one opaque network error, which counts.
pub fn is_connect(err: &reqwest::Error) -> bool {
    #[cfg(not(target_family = "wasm"))]
    {
        err.is_connect()
    }
    #[cfg(target_family = "wasm")]
    {
        err.is_request()
    }
}

/// Whether `url` can be reached at all: one bodiless HEAD request, any HTTP
/// answer counts. False only for transport failures (no network, DNS failure,
/// refused or timed-out connection, TLS failure such as a captive portal).
pub async fn reachable(http: &reqwest::Client, url: &str, timeout: Duration) -> bool {
    match prepare(http.head(url).timeout(timeout)).send().await {
        Ok(_) => true,
        Err(e) if is_connect(&e) || e.is_timeout() || e.is_request() => {
            tracing::debug!("{url} is unreachable: {e}");
            false
        }
        // Builder or redirect problems are not connectivity: assume online.
        Err(_) => true,
    }
}

// ------------------------------------------------------------------ header policy

/// The generic Android clients Shazam's own app sends; the client picks one per request.
pub const SHAZAM_USER_AGENTS: [&str; 8] = [
    "Dalvik/2.1.0 (Linux; U; Android 14; Pixel 8 Build/AP2A.240805.005)",
    "Dalvik/2.1.0 (Linux; U; Android 14; SM-S921B Build/UP1A.231005.007)",
    "Dalvik/2.1.0 (Linux; U; Android 13; SM-S911B Build/TP1A.220624.014)",
    "Dalvik/2.1.0 (Linux; U; Android 13; Pixel 7 Build/TQ3A.230901.001)",
    "Dalvik/2.1.0 (Linux; U; Android 12; SM-G991B Build/SP1A.210812.016)",
    "Dalvik/2.1.0 (Linux; U; Android 11; Mi 10T Pro Build/RKQ1.200826.002)",
    "Dalvik/2.1.0 (Linux; U; Android 10; SM-G973F Build/QP1A.190711.020)",
    "Dalvik/2.1.0 (Linux; U; Android 9; SM-G960F Build/PPR1.180610.011)",
];

/// YouTube's consent wall answered "necessary cookies only"; EU visitors get the
/// wall instead of the page without it. Static, never the user's own cookies.
pub const YOUTUBE_CONSENT_COOKIE: &str = "SOCS=CAI; CONSENT=YES+1";
/// YouTube pages and InnerTube answers in English, so section titles parse.
pub const YOUTUBE_LANGUAGE: &str = "en-US,en;q=0.9";
/// Origin YouTube Music's InnerTube search expects.
pub const YTMUSIC_ORIGIN: &str = "https://music.youtube.com";
/// Origin YouTube's InnerTube expects (the web client's).
pub const YOUTUBE_ORIGIN: &str = "https://www.youtube.com";

/// The requests that need particular headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HeaderRuleId {
    /// Shazam's tag endpoint: an Android client's agent, no `Origin`.
    Shazam,
    /// Spotify and Amazon Music pages serve their Open Graph tags to link-preview crawlers.
    Crawler,
    /// YouTube watch, Shorts and playlist pages: the consent cookie, English.
    Youtube,
    /// YouTube's InnerTube API (`next`, `browse`): as `Youtube`, plus the site's own origin.
    Innertube,
    /// YouTube Music's InnerTube search: its own origin and referer.
    Ytmusic,
    /// Genius refuses browser agents (S4: 403) and answers the neutral one. Natively the
    /// shared client's default agent already is the neutral one, so no call site sets it.
    Neutral,
}

impl HeaderRuleId {
    pub const ALL: [HeaderRuleId; 6] = [
        HeaderRuleId::Shazam,
        HeaderRuleId::Crawler,
        HeaderRuleId::Youtube,
        HeaderRuleId::Innertube,
        HeaderRuleId::Ytmusic,
        HeaderRuleId::Neutral,
    ];
}

/// Headers one kind of request needs, and the URLs it covers.
///
/// `prefixes` are anchored at the start of the URL (declarativeNetRequest
/// `urlFilter: "|<prefix>"`), so a rule never reaches further than the requests
/// that need it. Header names are lower case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeaderRule {
    pub id: HeaderRuleId,
    pub prefixes: Vec<String>,
    pub set: BTreeMap<String, String>,
    pub remove: Vec<String>,
}

impl HeaderRule {
    /// The rule `id`. `shazam_agent` picks one of [`SHAZAM_USER_AGENTS`] (wrapping).
    pub fn new(id: HeaderRuleId, app_version: &str, shazam_agent: usize) -> Self {
        let set = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
        };
        let prefixes = |list: &[&str]| -> Vec<String> { list.iter().map(|p| p.to_string()).collect() };
        match id {
            HeaderRuleId::Shazam => Self {
                id,
                prefixes: prefixes(&["https://amp.shazam.com/discovery/"]),
                set: set(&[("user-agent", SHAZAM_USER_AGENTS[shazam_agent % SHAZAM_USER_AGENTS.len()])]),
                // Browsers add `Origin: chrome-extension://…` to POSTs; the Android app sends none.
                remove: vec!["origin".into()],
            },
            HeaderRuleId::Crawler => Self {
                id,
                prefixes: prefixes(&[
                    "https://open.spotify.com/track/",
                    "https://open.spotify.com/album/",
                    "https://open.spotify.com/playlist/",
                    "https://open.spotify.com/artist/",
                    // Every storefront: music.amazon.com, .de, .co.uk, …
                    "https://music.amazon.",
                ]),
                set: set(&[("user-agent", CRAWLER_UA)]),
                remove: Vec::new(),
            },
            HeaderRuleId::Youtube => Self {
                id,
                prefixes: prefixes(&[
                    "https://www.youtube.com/watch",
                    "https://www.youtube.com/shorts/",
                    "https://www.youtube.com/playlist",
                ]),
                set: set(&[("cookie", YOUTUBE_CONSENT_COOKIE), ("accept-language", YOUTUBE_LANGUAGE)]),
                remove: Vec::new(),
            },
            HeaderRuleId::Innertube => Self {
                id,
                prefixes: prefixes(&["https://www.youtube.com/youtubei/v1/"]),
                set: set(&[
                    ("cookie", YOUTUBE_CONSENT_COOKIE),
                    ("accept-language", YOUTUBE_LANGUAGE),
                    ("origin", YOUTUBE_ORIGIN),
                    ("referer", "https://www.youtube.com/"),
                ]),
                remove: Vec::new(),
            },
            HeaderRuleId::Ytmusic => Self {
                id,
                prefixes: prefixes(&["https://music.youtube.com/youtubei/v1/search"]),
                set: set(&[("origin", YTMUSIC_ORIGIN), ("referer", "https://music.youtube.com/")]),
                remove: Vec::new(),
            },
            HeaderRuleId::Neutral => Self {
                id,
                prefixes: prefixes(&["https://genius.com/api/"]),
                set: BTreeMap::from([("user-agent".to_string(), user_agent(app_version))]),
                remove: Vec::new(),
            },
        }
    }

    /// True when the rule covers `url`.
    pub fn matches(&self, url: &str) -> bool {
        self.prefixes.iter().any(|p| url.starts_with(p.as_str()))
    }

    /// Set the rule's headers on a request to `url` (natively; removals need nothing
    /// there, the native client never adds those headers). `url` must be one the
    /// rule covers, so the browser's rules reach the same requests.
    pub fn apply(&self, request: reqwest::RequestBuilder, url: &str) -> reqwest::RequestBuilder {
        debug_assert!(self.matches(url), "header rule {:?} applied to {url}, outside its prefixes", self.id);
        self.set.iter().fold(request, |r, (name, value)| r.header(name.as_str(), value.as_str()))
    }
}

/// The whole header policy, for the browser: the extension compiles it into
/// declarativeNetRequest session rules scoped to its own requests. The Shazam rule
/// carries agent `shazam_agent`; [`HeaderRuleHost`] swaps it per request.
pub fn header_rules(app_version: &str, shazam_agent: usize) -> Vec<HeaderRule> {
    HeaderRuleId::ALL.iter().map(|&id| HeaderRule::new(id, app_version, shazam_agent)).collect()
}

/// Installs a rule variant in a runtime that applies headers itself (the browser
/// extension's declarativeNetRequest rules) before a request that needs it. The
/// Shazam client calls `prepare(Shazam, i)` with the agent it picked for the next
/// request and sends once that resolves. Requests to one service are serialized,
/// so a variant stays in place for its request. Natively there is no host: the
/// client sets the header itself.
pub trait HeaderRuleHost: Send + Sync {
    fn prepare(&self, rule: HeaderRuleId, variant: usize) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reachability_needs_an_answer() {
        let client = build_client("1.0.0").expect("client");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let open = format!("http://{}/", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        });
        assert!(reachable(&client, &open, PROBE_TIMEOUT).await, "any status counts");
        server.join().unwrap();
        let closed = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}/", listener.local_addr().unwrap())
        };
        assert!(!reachable(&client, &closed, PROBE_TIMEOUT).await, "refused connection");
    }

    #[test]
    fn tls_config_has_roots_and_alpn() {
        let config = tls_config().expect("tls config");
        assert_eq!(config.alpn_protocols, vec![b"h2".to_vec(), b"http/1.1".to_vec()]);
        assert!(webpki_roots::TLS_SERVER_ROOTS.len() > 100);
    }

    #[test]
    fn client_accepts_preconfigured_tls() {
        // An unrecognized preconfigured backend (rustls version skew) fails here.
        build_client("1.0.0").expect("client builds");
    }

    #[test]
    fn header_rules_cover_exactly_the_requests_that_need_them() {
        let rule = |id| HeaderRule::new(id, "1.1.0", 0);
        let covered = [
            (HeaderRuleId::Shazam, "https://amp.shazam.com/discovery/v5/en-US/US/android/-/tag/A/b?sync=true"),
            (HeaderRuleId::Crawler, "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC"),
            (HeaderRuleId::Crawler, "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"),
            (HeaderRuleId::Crawler, "https://music.amazon.com/albums/B000TDBNDG"),
            (HeaderRuleId::Crawler, "https://music.amazon.co.uk/tracks/B000TDFHJW"),
            (HeaderRuleId::Youtube, "https://www.youtube.com/watch?v=dQw4w9WgXcQ&hl=en&persist_hl=1"),
            (HeaderRuleId::Youtube, "https://www.youtube.com/shorts/abc?hl=en"),
            (HeaderRuleId::Youtube, "https://www.youtube.com/playlist?list=PL1"),
            (HeaderRuleId::Innertube, "https://www.youtube.com/youtubei/v1/next?prettyPrint=false"),
            (HeaderRuleId::Innertube, "https://www.youtube.com/youtubei/v1/browse?prettyPrint=false"),
            (HeaderRuleId::Ytmusic, "https://music.youtube.com/youtubei/v1/search?alt=json"),
            (HeaderRuleId::Neutral, "https://genius.com/api/search/lyric?q=x"),
        ];
        for (id, url) in covered {
            assert!(rule(id).matches(url), "{id:?} should cover {url}");
            for other in HeaderRuleId::ALL.into_iter().filter(|o| *o != id) {
                assert!(!rule(other).matches(url), "{other:?} must not cover {url}");
            }
        }
        let untouched = [
            "https://amp.shazam.com",
            "https://open.spotify.com/embed/track/4uLU6hMCjMI75M1A2tKUQC",
            "https://open.spotify.com/oembed?url=x",
            "https://www.youtube.com/oembed?url=x",
            "https://music.youtube.com/watch?v=x",
            "https://genius.com/Queen-bohemian-rhapsody-lyrics",
            "https://api.deezer.com/track/3135556",
        ];
        for url in untouched {
            assert!(HeaderRuleId::ALL.iter().all(|&id| !rule(id).matches(url)), "{url} needs no rule");
        }
    }

    #[test]
    fn the_exported_table_is_what_the_extension_installs() {
        let rules = serde_json::to_value(header_rules("1.1.0", 9)).unwrap();
        let expected = serde_json::json!([
            {"id": "shazam", "prefixes": ["https://amp.shazam.com/discovery/"],
             "set": {"user-agent": SHAZAM_USER_AGENTS[1]}, "remove": ["origin"]},
            {"id": "crawler", "prefixes": ["https://open.spotify.com/track/", "https://open.spotify.com/album/",
                "https://open.spotify.com/playlist/", "https://open.spotify.com/artist/", "https://music.amazon."],
             "set": {"user-agent": CRAWLER_UA}, "remove": []},
            {"id": "youtube", "prefixes": ["https://www.youtube.com/watch", "https://www.youtube.com/shorts/",
                "https://www.youtube.com/playlist"],
             "set": {"accept-language": "en-US,en;q=0.9", "cookie": "SOCS=CAI; CONSENT=YES+1"}, "remove": []},
            {"id": "innertube", "prefixes": ["https://www.youtube.com/youtubei/v1/"],
             "set": {"accept-language": "en-US,en;q=0.9", "cookie": "SOCS=CAI; CONSENT=YES+1",
                     "origin": "https://www.youtube.com", "referer": "https://www.youtube.com/"}, "remove": []},
            {"id": "ytmusic", "prefixes": ["https://music.youtube.com/youtubei/v1/search"],
             "set": {"origin": "https://music.youtube.com", "referer": "https://music.youtube.com/"}, "remove": []},
            {"id": "neutral", "prefixes": ["https://genius.com/api/"],
             "set": {"user-agent": "MusicFetch/1.1.0 (+https://github.com/x1f4r/music-fetch)"}, "remove": []},
        ]);
        assert_eq!(rules, expected);
        // Nothing personal and no cookie but YouTube's static consent answer.
        for rule in header_rules("1.1.0", 0) {
            for (name, value) in &rule.set {
                assert_eq!(name, &name.to_ascii_lowercase());
                assert!(name != "cookie" || value == YOUTUBE_CONSENT_COOKIE);
            }
        }
    }

    #[test]
    fn user_agent_is_neutral() {
        let ua = user_agent("1.2.3");
        assert!(ua.starts_with("MusicFetch/1.2.3 "));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "live network: cargo test -p mfcore http -- --ignored"]
    async fn live_https_get() {
        let client = build_client("1.0.0").expect("client");
        let resp = client.get("https://api.deezer.com/track/3135556").send().await.expect("request");
        assert!(resp.status().is_success(), "status {}", resp.status());
        // ALPN from the preconfigured TLS config negotiates HTTP/2.
        assert_eq!(resp.version(), reqwest::Version::HTTP_2);
        let body: serde_json::Value = resp.json().await.expect("json body");
        assert_eq!(body["id"].as_u64(), Some(3135556));
        assert!(body["title"].as_str().is_some_and(|t| !t.is_empty()));
    }
}
