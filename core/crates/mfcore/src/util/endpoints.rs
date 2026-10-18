//! Test endpoints: send the engine's requests to a local stub instead of the
//! public services.
//!
//! `mf dev stub-recognizer` serves the synthetic fixtures as Shazam, Deezer,
//! iTunes, LRCLIB and the melody-index host. Tools and test builds install a
//! [`TestEndpoints`] map from service origins to the stub; [`reroute`] (called
//! by [`crate::util::http::prepare`], which every engine request goes through)
//! then rewrites matching requests. With `isolate` set, every other request to a
//! non-loopback host goes to the stub's sink as well, so a test run never leaves
//! the machine.
//!
//! The map can only point at loopback addresses. Nothing installs one in the
//! apps; the extension does so only in its test builds.

use std::sync::{Arc, RwLock};

use url::{Host, Url};

use crate::error::{MfError, Result};

/// Services the stub recognizer stands in for: origin and the path prefix it
/// gets on the stub.
pub const STUB_ROUTES: &[(&str, &str)] = &[
    ("https://amp.shazam.com", "/shazam"),
    ("https://api.deezer.com", "/deezer"),
    ("https://cdnt-preview.dzcdn.net", "/dzcdn"),
    ("https://itunes.apple.com", "/itunes"),
    ("https://audio-ssl.itunes.apple.com", "/itunes-audio"),
    ("https://lrclib.net", "/lrclib"),
    ("https://github.com", "/github"),
    // Fixture media as if it were on a public site (`.invalid` never resolves).
    ("https://fixtures.invalid", "/media"),
];

/// Where requests the map does not cover go when isolated.
pub const STUB_SINK: &str = "/blocked";

/// Origin → loopback base URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestEndpoints {
    routes: Vec<(String, Url)>,
    isolate: Option<Url>,
}

impl TestEndpoints {
    /// Every [`STUB_ROUTES`] origin to `base` (the stub, e.g. `http://127.0.0.1:47700`),
    /// isolated: requests to other hosts go to the stub's sink.
    pub fn stub(base: &str) -> Result<Self> {
        let base = base.trim_end_matches('/');
        let routes = STUB_ROUTES.iter().map(|(origin, path)| (origin.to_string(), format!("{base}{path}"))).collect();
        Self::new(routes, Some(format!("{base}{STUB_SINK}")))
    }

    /// `routes` maps origins (`https://api.deezer.com`) to loopback base URLs.
    pub fn new(routes: Vec<(String, String)>, isolate: Option<String>) -> Result<Self> {
        let routes = routes
            .into_iter()
            .map(|(origin, target)| {
                let origin = origin.trim_end_matches('/').to_ascii_lowercase();
                if Url::parse(&origin).ok().is_none_or(|u| u.path() != "/" || u.host().is_none()) {
                    return Err(MfError::invalid(format!("test endpoint origin {origin} is not an origin")));
                }
                Ok((origin, loopback(&target)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let isolate = isolate.map(|s| loopback(&s)).transpose()?;
        Ok(Self { routes, isolate })
    }

    /// `{"routes": {"<origin>": "<base>", …}, "isolate": "<sink>" | null}`: the form
    /// the stub prints and serves at `/__endpoints`, and the extension's test builds embed.
    pub fn from_json(json: &str) -> Result<Self> {
        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|e| MfError::invalid(format!("test endpoints: {e}")))?;
        let routes = value
            .get("routes")
            .and_then(|r| r.as_object())
            .ok_or_else(|| MfError::invalid("test endpoints: no routes"))?
            .iter()
            .map(|(k, v)| {
                Ok((k.clone(), v.as_str().ok_or_else(|| MfError::invalid("test endpoints: bad route"))?.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let isolate = value.get("isolate").and_then(|v| v.as_str()).map(str::to_string);
        Self::new(routes, isolate)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let routes: serde_json::Map<String, serde_json::Value> = self
            .routes
            .iter()
            .map(|(origin, target)| {
                (origin.clone(), serde_json::Value::String(target.as_str().trim_end_matches('/').into()))
            })
            .collect();
        serde_json::json!({"routes": routes, "isolate": self.isolate.as_ref().map(|u| u.as_str().to_string())})
    }

    /// Where a request to `url` goes instead, if anywhere.
    pub fn route(&self, url: &Url) -> Option<Url> {
        if !matches!(url.scheme(), "http" | "https") || is_loopback(url) {
            return None;
        }
        let origin = url.origin().ascii_serialization();
        if let Some((_, target)) = self.routes.iter().find(|(o, _)| *o == origin) {
            let mut out = target.clone();
            let path = format!("{}{}", target.path().trim_end_matches('/'), url.path());
            out.set_path(&path);
            out.set_query(url.query());
            return Some(out);
        }
        let sink = self.isolate.as_ref()?;
        let mut out = sink.clone();
        out.query_pairs_mut().clear().append_pair("url", url.as_str());
        Some(out)
    }

    /// True when requests to `url` stay on this machine (routed or sunk).
    pub fn covers(&self, url: &Url) -> bool {
        self.route(url).is_some()
    }
}

fn loopback(target: &str) -> Result<Url> {
    let url =
        Url::parse(target.trim()).map_err(|_| MfError::invalid(format!("test endpoint {target} is not a URL")))?;
    if !matches!(url.scheme(), "http" | "https") || !is_loopback(&url) {
        return Err(MfError::invalid(format!("test endpoint {target} must be on 127.0.0.1, ::1 or localhost")));
    }
    Ok(url)
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        Some(Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

static INSTALLED: RwLock<Option<Arc<TestEndpoints>>> = RwLock::new(None);

/// Serializes the unit tests that install process-wide endpoints.
#[cfg(test)]
pub(crate) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Install (or with `None` remove) the process-wide test endpoints.
pub fn install(endpoints: Option<TestEndpoints>) {
    *INSTALLED.write().unwrap_or_else(|e| e.into_inner()) = endpoints.map(Arc::new);
}

/// The installed map, if any.
pub fn installed() -> Option<Arc<TestEndpoints>> {
    INSTALLED.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Where the installed map sends `url`, if it covers it.
pub fn route(url: &str) -> Option<String> {
    let endpoints = installed()?;
    endpoints.route(&Url::parse(url).ok()?).map(String::from)
}

/// True when the installed map keeps `url` on this machine; the SSRF guard lets
/// such requests through without resolving the original host.
pub fn covers(url: &Url) -> bool {
    installed().is_some_and(|e| e.covers(url))
}

/// Rewrite a request for the installed map (no-op without one).
#[cfg(feature = "net")]
pub(crate) fn reroute(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let Some(endpoints) = installed() else {
        return request;
    };
    let (client, built) = request.build_split();
    match built {
        Ok(mut built) => {
            if let Some(target) = endpoints.route(built.url()) {
                tracing::trace!(from = %built.url(), to = %target, "test endpoint");
                *built.url_mut() = target;
            }
            reqwest::RequestBuilder::from_parts(client, built)
        }
        // The original request could not be built either: keep failing at send().
        Err(e) => {
            tracing::debug!("request not routed: {e}");
            client.get("invalid request")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_origins_and_sinks_everything_else() {
        let e = TestEndpoints::stub("http://127.0.0.1:47700/").unwrap();
        let r = |u: &str| e.route(&Url::parse(u).unwrap()).map(String::from);
        assert_eq!(
            r("https://amp.shazam.com/discovery/v5/en-US/US/android/-/tag/A/b?sync=true").as_deref(),
            Some("http://127.0.0.1:47700/shazam/discovery/v5/en-US/US/android/-/tag/A/b?sync=true")
        );
        assert_eq!(
            r("https://api.deezer.com/track/isrc:ZZ1").as_deref(),
            Some("http://127.0.0.1:47700/deezer/track/isrc:ZZ1")
        );
        assert_eq!(r("https://amp.shazam.com").as_deref(), Some("http://127.0.0.1:47700/shazam/"));
        assert_eq!(
            r("https://open.spotify.com/track/x").as_deref(),
            Some("http://127.0.0.1:47700/blocked?url=https%3A%2F%2Fopen.spotify.com%2Ftrack%2Fx")
        );
        // Loopback requests (the stub itself, local test servers) are left alone.
        assert_eq!(r("http://127.0.0.1:9/x"), None);
        assert!(e.covers(&Url::parse("https://example.com/").unwrap()));
    }

    #[test]
    fn only_loopback_targets() {
        assert!(TestEndpoints::stub("http://10.0.0.2:8080").is_err());
        assert!(TestEndpoints::stub("https://example.com").is_err());
        assert!(
            TestEndpoints::new(vec![("https://api.deezer.com/x".into(), "http://[::1]:5/d".into())], None).is_err()
        );
        let e =
            TestEndpoints::new(vec![("https://API.deezer.com".into(), "http://localhost:5/d".into())], None).unwrap();
        assert_eq!(
            e.route(&Url::parse("https://api.deezer.com/search?q=a").unwrap()).unwrap().as_str(),
            "http://localhost:5/d/search?q=a"
        );
        assert_eq!(e.route(&Url::parse("https://lrclib.net/").unwrap()), None, "not isolated");
    }

    #[test]
    fn json_round_trip() {
        let e = TestEndpoints::stub("http://127.0.0.1:47700").unwrap();
        let back = TestEndpoints::from_json(&e.to_json().to_string()).unwrap();
        assert_eq!(e.to_json(), back.to_json());
        assert!(TestEndpoints::from_json(r#"{"routes": {"https://amp.shazam.com": "http://192.168.1.2/s"}}"#).is_err());
    }
}
