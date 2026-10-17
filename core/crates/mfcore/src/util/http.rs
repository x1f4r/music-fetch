//! HTTP client factory. One shared client per engine.
//!
//! Requirements: rustls with webpki roots on every platform (no JNI setup on
//! Android), gzip/brotli, a neutral `MusicFetch/<version>` user agent, sane
//! timeouts. Never put personal data in headers.
//!
//! TLS is a preconfigured rustls `ClientConfig` (ring provider, Mozilla roots
//! from `webpki-roots`), so reqwest never touches `rustls-platform-verifier` or
//! a process-wide crypto provider.

use std::sync::Arc;
use std::time::Duration;

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

/// Whether `url` can be reached at all: one bodiless HEAD request, any HTTP
/// answer counts. False only for transport failures (no network, DNS failure,
/// refused or timed-out connection, TLS failure such as a captive portal).
pub async fn reachable(http: &reqwest::Client, url: &str, timeout: Duration) -> bool {
    match http.head(url).timeout(timeout).send().await {
        Ok(_) => true,
        Err(e) if e.is_connect() || e.is_timeout() || e.is_request() => {
            tracing::debug!("{url} is unreachable: {e}");
            false
        }
        // Builder or redirect problems are not connectivity: assume online.
        Err(_) => true,
    }
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
