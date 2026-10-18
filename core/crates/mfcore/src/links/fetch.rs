//! HTTP helpers shared by `links` and `catalog`.
//!
//! Two kinds of requests:
//! - fixed public hosts (service APIs and pages) go through the engine's shared
//!   client;
//! - user-supplied hosts (shorteners, generic pages, media probes) go through a
//!   guarded client that never follows redirects on its own, checks every hop
//!   with the SSRF guard and only connects to public addresses.
//!
//! Every await is cancellable and bodies are read up to a size cap.
//!
//! In the browser (wasm32) fetch() follows redirects itself, so the guarded
//! helpers see only the final response and check that its URL is still public.

use std::future::Future;
use std::sync::LazyLock;
#[cfg(not(target_family = "wasm"))]
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use regex::Regex;
use reqwest::header::{ACCEPT, ACCEPT_LANGUAGE, CONTENT_TYPE, COOKIE, LOCATION, RETRY_AFTER, USER_AGENT};
use reqwest::{RequestBuilder, Response, StatusCode};
use url::Url;

use super::ssrf;
use crate::error::{MfError, Result};
use crate::util::cancel::CancelToken;
use crate::util::http::{BROWSER_UA, CRAWLER_UA, is_connect, prepare, user_agent};

/// Largest HTML page read (YouTube watch pages are about 1.5 MB).
pub(crate) const MAX_PAGE_BYTES: usize = 6 * 1024 * 1024;
/// Largest JSON response read.
pub(crate) const MAX_JSON_BYTES: usize = 4 * 1024 * 1024;
/// Redirect hops followed for user-supplied links.
pub(crate) const MAX_REDIRECTS: usize = 5;
pub(crate) const PAGE_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const API_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const SHORTENER_TIMEOUT: Duration = Duration::from_secs(8);

/// User agent to present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Agent {
    /// The client's neutral `MusicFetch/<version>` agent.
    Neutral,
    /// A desktop browser, for sites that refuse unknown clients.
    Browser,
    /// A link-preview crawler, for sites that only serve Open Graph tags to crawlers.
    Crawler,
}

/// How to fetch a page.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PageRequest<'a> {
    pub agent: Agent,
    pub cookies: Option<&'a str>,
    pub accept_language: Option<&'a str>,
    pub max_bytes: usize,
    pub timeout: Duration,
    /// Site name for error messages ("Spotify", "the website").
    pub site: &'a str,
}

impl<'a> PageRequest<'a> {
    pub(crate) fn new(site: &'a str) -> Self {
        Self {
            agent: Agent::Neutral,
            cookies: None,
            accept_language: None,
            max_bytes: MAX_PAGE_BYTES,
            timeout: PAGE_TIMEOUT,
            site,
        }
    }

    pub(crate) fn agent(mut self, agent: Agent) -> Self {
        self.agent = agent;
        self
    }

    pub(crate) fn cookies(mut self, cookies: &'a str) -> Self {
        self.cookies = Some(cookies);
        self
    }

    pub(crate) fn language(mut self, accept_language: &'a str) -> Self {
        self.accept_language = Some(accept_language);
        self
    }

    pub(crate) fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    pub(crate) fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn apply(&self, builder: RequestBuilder) -> RequestBuilder {
        let mut builder = builder
            .timeout(self.timeout)
            .header(ACCEPT, "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8");
        builder = match self.agent {
            Agent::Neutral => builder,
            Agent::Browser => builder.header(USER_AGENT, BROWSER_UA),
            Agent::Crawler => builder.header(USER_AGENT, CRAWLER_UA),
        };
        if let Some(cookies) = self.cookies {
            builder = builder.header(COOKIE, cookies);
        }
        if let Some(language) = self.accept_language {
            builder = builder.header(ACCEPT_LANGUAGE, language);
        }
        builder
    }
}

/// A fetched page.
#[derive(Debug, Clone)]
pub(crate) struct Page {
    /// Final URL after redirects.
    pub url: Url,
    pub status: StatusCode,
    pub content_type: Option<String>,
    pub body: String,
}

impl Page {
    pub(crate) fn is_html(&self) -> bool {
        self.content_type.as_deref().is_none_or(|ct| ct.contains("html") || ct.starts_with("text/plain"))
    }
}

/// Result of a HEAD probe.
#[derive(Debug, Clone)]
pub(crate) struct Probe {
    pub status: StatusCode,
    pub content_type: Option<String>,
}

/// Runs `fut` unless `cancel` fires first.
pub(crate) async fn cancellable<T>(cancel: &CancelToken, fut: impl Future<Output = T>) -> Result<T> {
    tokio::select! {
        biased;
        _ = cancel.canceled() => Err(MfError::Canceled),
        out = fut => Ok(out),
    }
}

/// Short, user-presentable error for a failed request.
pub(crate) fn request_error(err: &reqwest::Error, site: &str) -> MfError {
    if ssrf::is_non_public_error(err) {
        return ssrf::private_address_error();
    }
    if err.is_timeout() {
        MfError::network(format!("{site} didn't respond in time"))
    } else if is_connect(err) {
        MfError::network(format!("couldn't connect to {site}"))
    } else {
        MfError::network(format!("the request to {site} failed"))
    }
}

/// Error for an unsuccessful HTTP status.
pub(crate) fn status_error(status: StatusCode, retry_after: Option<Duration>, site: &str) -> MfError {
    match status.as_u16() {
        404 | 410 => MfError::MediaUnavailable { message: format!("This {site} link doesn't exist or was removed.") },
        401 | 403 | 451 => MfError::MediaUnavailable {
            message: format!("{site} refused the request. The content may be private or unavailable in your region."),
        },
        429 => MfError::RateLimited { retry_in_ms: retry_after.unwrap_or(Duration::from_secs(30)).as_millis() as u64 },
        500..=599 => MfError::network(format!("{site} is having problems right now")),
        code => MfError::network(format!("{site} answered with status {code}")),
    }
}

pub(crate) fn retry_after(response: &Response) -> Option<Duration> {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

async fn send(builder: RequestBuilder, cancel: &CancelToken, site: &str) -> Result<Response> {
    cancellable(cancel, prepare(builder).send()).await?.map_err(|e| request_error(&e, site))
}

/// One hop of a guarded request to `current`. Natively the guarded client never
/// follows redirects. fetch() always does: continue from where it ended, which
/// must still be a public address.
#[cfg_attr(not(target_family = "wasm"), allow(unused_variables))]
async fn send_hop(builder: RequestBuilder, current: &mut Url, cancel: &CancelToken, site: &str) -> Result<Response> {
    let response = send(builder, cancel, site).await?;
    #[cfg(target_family = "wasm")]
    if response.url() != current {
        cancellable(cancel, ssrf::ensure_public(response.url())).await??;
        *current = response.url().clone();
    }
    Ok(response)
}

#[cfg(not(target_family = "wasm"))]
async fn read_body(mut response: Response, max_bytes: usize, cancel: &CancelToken, site: &str) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = cancellable(cancel, response.chunk()).await?.map_err(|e| request_error(&e, site))? {
        let room = max_bytes.saturating_sub(body.len());
        if chunk.len() >= room {
            // Metadata lives near the top; a truncated page is still useful.
            body.extend_from_slice(&chunk[..room]);
            break;
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The fetch backend reads the body in one piece; the cap still applies to what
/// is kept.
#[cfg(target_family = "wasm")]
async fn read_body(response: Response, max_bytes: usize, cancel: &CancelToken, site: &str) -> Result<Vec<u8>> {
    let mut body = cancellable(cancel, response.bytes()).await?.map_err(|e| request_error(&e, site))?.to_vec();
    body.truncate(max_bytes);
    Ok(body)
}

fn content_type(response: &Response) -> Option<String> {
    response.headers().get(CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(|v| v.to_ascii_lowercase())
}

async fn into_page(response: Response, max_bytes: usize, cancel: &CancelToken, site: &str) -> Result<Page> {
    let url = response.url().clone();
    let status = response.status();
    let content_type = content_type(&response);
    let body = read_body(response, max_bytes, cancel, site).await?;
    Ok(Page { url, status, content_type, body: String::from_utf8_lossy(&body).into_owned() })
}

/// GET a page on a fixed public host with the shared client. Non-success
/// statuses are returned, not raised; see `require_success`.
pub(crate) async fn get_page(
    http: &reqwest::Client,
    url: &str,
    req: &PageRequest<'_>,
    cancel: &CancelToken,
) -> Result<Page> {
    let response = send(req.apply(http.get(url)), cancel, req.site).await?;
    into_page(response, req.max_bytes, cancel, req.site).await
}

/// `page` if its status is a success, else the matching error.
pub(crate) fn require_success(page: Page, site: &str) -> Result<Page> {
    if page.status.is_success() { Ok(page) } else { Err(status_error(page.status, None, site)) }
}

/// A JSON response with its status; `value` is None when the body is not JSON.
#[derive(Debug, Clone)]
pub(crate) struct JsonResponse {
    pub status: StatusCode,
    pub retry_after: Option<Duration>,
    pub value: Option<serde_json::Value>,
}

/// GET JSON from a fixed public host with the shared client, whatever the status.
pub(crate) async fn fetch_json(
    http: &reqwest::Client,
    url: &str,
    site: &str,
    cancel: &CancelToken,
) -> Result<JsonResponse> {
    let request = http.get(url).timeout(API_TIMEOUT).header(ACCEPT, "application/json");
    let response = send(request, cancel, site).await?;
    let status = response.status();
    let retry_after = retry_after(&response);
    let body = read_body(response, MAX_JSON_BYTES, cancel, site).await?;
    Ok(JsonResponse { status, retry_after, value: serde_json::from_slice(&body).ok() })
}

/// GET JSON from a fixed public host with the shared client; errors on failure statuses.
pub(crate) async fn get_json(
    http: &reqwest::Client,
    url: &str,
    site: &str,
    cancel: &CancelToken,
) -> Result<serde_json::Value> {
    let response = fetch_json(http, url, site, cancel).await?;
    if !response.status.is_success() {
        return Err(status_error(response.status, response.retry_after, site));
    }
    response.value.ok_or_else(|| MfError::network(format!("{site} sent an unreadable response")))
}

#[cfg(not(target_family = "wasm"))]
static GUARDED: OnceLock<std::result::Result<reqwest::Client, String>> = OnceLock::new();

/// rustls with the ring provider and webpki roots: no process-wide provider and
/// no platform verifier (which needs JNI setup on Android).
#[cfg(not(target_family = "wasm"))]
fn tls_config() -> std::result::Result<rustls::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(tls)
}

#[cfg(not(target_family = "wasm"))]
fn build_guarded() -> std::result::Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent(user_agent(env!("CARGO_PKG_VERSION")))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(ssrf::PublicOnlyResolver))
        .connect_timeout(Duration::from_secs(8))
        .timeout(Duration::from_secs(30))
        .tls_backend_preconfigured(tls_config()?)
        .build()
        .map_err(|e| e.to_string())
}

/// An HTTP client like the engine's shared one (neutral user agent, timeouts,
/// at most 5 redirects), for tools that call `links` and `catalog` without an
/// `Engine`.
#[cfg(not(target_family = "wasm"))]
pub fn http_client(app_version: &str) -> Result<reqwest::Client> {
    let tls = tls_config().map_err(|e| MfError::internal(format!("tls: {e}")))?;
    reqwest::Client::builder()
        .user_agent(user_agent(app_version))
        .timeout(crate::util::http::DEFAULT_TIMEOUT)
        .connect_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(MAX_REDIRECTS))
        .tls_backend_preconfigured(tls)
        .build()
        .map_err(|e| MfError::internal(format!("http client: {e}")))
}

/// The engine's client in the browser (fetch() owns redirects and timeouts).
#[cfg(target_family = "wasm")]
pub fn http_client(app_version: &str) -> Result<reqwest::Client> {
    crate::util::http::build_client(app_version)
}

#[cfg(not(target_family = "wasm"))]
fn guarded_client() -> Result<&'static reqwest::Client> {
    GUARDED.get_or_init(build_guarded).as_ref().map_err(|e| MfError::internal(format!("http client: {e}")))
}

/// fetch() has no DNS hook: user-supplied hosts get the offline SSRF checks
/// before each request and on the final URL (`send_hop`).
#[cfg(target_family = "wasm")]
fn guarded_client() -> Result<&'static reqwest::Client> {
    static GUARDED: std::sync::OnceLock<std::result::Result<reqwest::Client, String>> = std::sync::OnceLock::new();
    GUARDED
        .get_or_init(|| {
            reqwest::Client::builder()
                .user_agent(user_agent(env!("CARGO_PKG_VERSION")))
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|e| MfError::internal(format!("http client: {e}")))
}

/// Next hop of a redirect response, if any.
fn redirect_target(current: &Url, response: &Response) -> Result<Option<Url>> {
    let status = response.status();
    if !status.is_redirection() || status == StatusCode::NOT_MODIFIED {
        return Ok(None);
    }
    let Some(location) = response.headers().get(LOCATION).and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let next = current.join(location.trim()).map_err(|_| MfError::network("the website sent a broken redirect"))?;
    if !matches!(next.scheme(), "http" | "https") {
        return Err(MfError::invalid("The link redirects to an unsupported address."));
    }
    Ok(Some(next))
}

/// GET a user-supplied URL: manual redirects, SSRF check on every hop.
pub(crate) async fn guarded_get(start: &Url, req: &PageRequest<'_>, cancel: &CancelToken) -> Result<Page> {
    let client = guarded_client()?;
    let mut current = start.clone();
    for _ in 0..=MAX_REDIRECTS {
        cancellable(cancel, ssrf::ensure_public(&current)).await??;
        let request = req.apply(client.get(current.as_str()));
        let response = send_hop(request, &mut current, cancel, req.site).await?;
        if let Some(next) = redirect_target(&current, &response)? {
            current = next;
            continue;
        }
        return into_page(response, req.max_bytes, cancel, req.site).await;
    }
    Err(MfError::network("the link redirects too many times"))
}

/// HEAD a user-supplied URL: manual redirects, SSRF check on every hop.
pub(crate) async fn guarded_head(start: &Url, cancel: &CancelToken) -> Result<Probe> {
    let client = guarded_client()?;
    let req = PageRequest::new("the website").timeout(SHORTENER_TIMEOUT);
    let mut current = start.clone();
    for _ in 0..=MAX_REDIRECTS {
        cancellable(cancel, ssrf::ensure_public(&current)).await??;
        let request = req.apply(client.head(current.as_str()));
        let response = send_hop(request, &mut current, cancel, req.site).await?;
        if let Some(next) = redirect_target(&current, &response)? {
            current = next;
            continue;
        }
        return Ok(Probe { status: response.status(), content_type: content_type(&response) });
    }
    Err(MfError::network("the link redirects too many times"))
}

/// Hosts a short link may lead to; used to find the target inside HTML landing pages.
fn is_landing_target(url: &Url) -> bool {
    let host = super::canonical::host_of(url);
    matches!(
        host.as_str(),
        "open.spotify.com"
            | "www.deezer.com"
            | "deezer.com"
            | "music.apple.com"
            | "tidal.com"
            | "listen.tidal.com"
            | "soundcloud.com"
            | "www.tiktok.com"
            | "www.youtube.com"
            | "music.youtube.com"
    ) || host.starts_with("music.amazon.")
}

/// Landing pages that redirect with HTML or JavaScript (Branch `*.app.link`,
/// Firebase `*.page.link`) or a meta refresh.
fn landing_page_target(page: &Page) -> Option<Url> {
    static META_REFRESH: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)<meta[^>]+http-equiv=["']?refresh["']?[^>]+content=["']?\s*\d+\s*;\s*url=([^"'>\s]+)"#)
            .expect("valid regex")
    });
    static ABSOLUTE_URL: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#"https?://[^\s"'<>\\]+"#).expect("valid regex"));
    if let Some(target) =
        META_REFRESH.captures(&page.body).and_then(|c| page.url.join(&c[1].replace("&amp;", "&")).ok())
    {
        return Some(target);
    }
    let host = super::canonical::host_of(&page.url);
    if !(host.ends_with("app.link") || host.ends_with("page.link")) {
        return None;
    }
    ABSOLUTE_URL
        .find_iter(&page.body)
        .filter_map(|m| Url::parse(&m.as_str().replace("&amp;", "&")).ok())
        .find(is_landing_target)
}

/// Follows a short link to its target: HEAD first, GET when HEAD is refused or
/// the hop is an HTML landing page; at most `MAX_REDIRECTS` hops, each checked
/// by the SSRF guard. Network failures return the furthest URL reached (the
/// media backend may still handle it); SSRF violations and cancellation are errors.
pub(crate) async fn expand_short_url(start: &Url, cancel: &CancelToken) -> Result<Url> {
    let client = guarded_client()?;
    let req = PageRequest::new("the short link").timeout(SHORTENER_TIMEOUT).max_bytes(512 * 1024);
    let mut current = start.clone();
    let mut hops = 0;
    loop {
        cancellable(cancel, ssrf::ensure_public(&current)).await??;
        if hops > MAX_REDIRECTS {
            return Err(MfError::network("the link redirects too many times"));
        }
        let landing = {
            let host = super::canonical::host_of(&current);
            host.ends_with("app.link") || host.ends_with("page.link")
        };
        if !landing {
            let request = req.apply(client.head(current.as_str()));
            match send_hop(request, &mut current, cancel, req.site).await {
                Ok(response) => {
                    if let Some(next) = redirect_target(&current, &response)? {
                        current = next;
                        hops += 1;
                        continue;
                    }
                    if response.status().is_success() {
                        return Ok(current);
                    }
                }
                Err(e @ (MfError::Canceled | MfError::InvalidInput { .. })) => return Err(e),
                Err(_) => {}
            }
        }
        let request = req.apply(client.get(current.as_str()));
        let response = match send_hop(request, &mut current, cancel, req.site).await {
            Ok(response) => response,
            Err(e @ (MfError::Canceled | MfError::InvalidInput { .. })) => return Err(e),
            Err(e) => {
                tracing::debug!("short link expansion stopped: {e}");
                return Ok(current);
            }
        };
        if let Some(next) = redirect_target(&current, &response)? {
            current = next;
            hops += 1;
            continue;
        }
        if response.status().is_success() {
            match into_page(response, req.max_bytes, cancel, req.site).await {
                Ok(page) => {
                    if let Some(target) = landing_page_target(&page) {
                        current = target;
                        hops += 1;
                        continue;
                    }
                }
                Err(MfError::Canceled) => return Err(MfError::Canceled),
                Err(e) => tracing::debug!("short link page unreadable: {e}"),
            }
        }
        return Ok(current);
    }
}

/// Shared-style client for tests.
#[cfg(test)]
pub(crate) fn test_client() -> reqwest::Client {
    http_client(env!("CARGO_PKG_VERSION")).expect("test client")
}

/// Minimal HTTP/1.1 server on 127.0.0.1 for tests; `respond(method, path)`
/// returns `(status line, extra headers, body)`. Returns the port, which the
/// SSRF guard lets tests reach.
#[cfg(test)]
pub(crate) async fn mock_server(respond: fn(&str, &str, u16) -> (String, String, String)) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    ssrf::TEST_LOOPBACK_PORTS.lock().push(port);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]).to_string();
                let mut first = request.lines().next().unwrap_or_default().split(' ');
                let (method, path) = (first.next().unwrap_or_default(), first.next().unwrap_or_default());
                let (status, headers, body) = respond(method, path, port);
                let body = if method == "HEAD" { String::new() } else { body };
                let response = format!(
                    "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    port
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(url: &str, body: &str) -> Page {
        Page {
            url: Url::parse(url).unwrap(),
            status: StatusCode::OK,
            content_type: Some("text/html".into()),
            body: body.into(),
        }
    }

    #[test]
    fn finds_branch_landing_target() {
        let body = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/links/spotify_branch_landing.html"
        ))
        .expect("fixture");
        let target = landing_page_target(&page("https://spotify.app.link/6tpneu0iVIb?_p=c11c", &body)).expect("target");
        assert_eq!(target.host_str(), Some("open.spotify.com"));
        assert_eq!(target.path(), "/playlist/37i9dQZF1DXcBWIGoYBM5M");
    }

    #[test]
    fn finds_meta_refresh_target() {
        let p = page(
            "https://short.example/x",
            r#"<meta http-equiv="refresh" content="0; url=https://open.spotify.com/track/abc?x=1&amp;y=2">"#,
        );
        assert_eq!(landing_page_target(&p).unwrap().as_str(), "https://open.spotify.com/track/abc?x=1&y=2");
        assert!(
            landing_page_target(&page("https://example.com/", "<a href=\"https://open.spotify.com/track/x\">"))
                .is_none()
        );
    }

    #[test]
    fn status_errors_are_short() {
        assert!(matches!(status_error(StatusCode::NOT_FOUND, None, "Deezer"), MfError::MediaUnavailable { .. }));
        assert_eq!(
            status_error(StatusCode::TOO_MANY_REQUESTS, Some(Duration::from_secs(5)), "Deezer"),
            MfError::RateLimited { retry_in_ms: 5000 }
        );
        assert!(matches!(status_error(StatusCode::BAD_GATEWAY, None, "Deezer"), MfError::Network { .. }));
    }

    #[test]
    fn guarded_client_builds() {
        assert!(guarded_client().is_ok());
    }

    #[tokio::test]
    async fn guarded_get_blocks_private_hosts_before_connecting() {
        let cancel = CancelToken::new();
        let url = Url::parse("http://169.254.169.254/latest/meta-data/").unwrap();
        let err = guarded_get(&url, &PageRequest::new("the website"), &cancel).await.unwrap_err();
        assert_eq!(err, ssrf::private_address_error());
        let err = expand_short_url(&Url::parse("http://127.0.0.1/x").unwrap(), &cancel).await.unwrap_err();
        assert_eq!(err, ssrf::private_address_error());
    }

    fn redirect(location: &str) -> (String, String, String) {
        ("302 Found".into(), format!("Location: {location}\r\n"), String::new())
    }

    fn ok(content_type: &str, body: &str) -> (String, String, String) {
        ("200 OK".into(), format!("Content-Type: {content_type}\r\n"), body.into())
    }

    #[tokio::test]
    async fn shortener_redirect_to_private_host_is_blocked() {
        let port = mock_server(|_, _, _| redirect("http://169.254.169.254/latest/meta-data/")).await;
        let start = Url::parse(&format!("http://127.0.0.1:{port}/demo")).unwrap();
        let err = expand_short_url(&start, &CancelToken::new()).await.unwrap_err();
        assert_eq!(err, ssrf::private_address_error());
    }

    #[tokio::test]
    async fn shortener_falls_back_to_get_and_follows_hops() {
        let port = mock_server(|method, path, port| match (method, path) {
            ("HEAD", "/demo") => ("405 Method Not Allowed".into(), String::new(), String::new()),
            ("GET", "/demo") => redirect("/hop"),
            (_, "/hop") => redirect(&format!("http://127.0.0.1:{port}/final?si=x")),
            _ => ok("text/html", "<html></html>"),
        })
        .await;
        let start = Url::parse(&format!("http://127.0.0.1:{port}/demo")).unwrap();
        let end = expand_short_url(&start, &CancelToken::new()).await.unwrap();
        assert_eq!(end.as_str(), format!("http://127.0.0.1:{port}/final?si=x"));
    }

    #[tokio::test]
    async fn redirect_loops_stop() {
        let port = mock_server(|_, path, _| {
            let n: u32 = path.trim_start_matches("/r").parse().unwrap_or(0);
            redirect(&format!("/r{}", n + 1))
        })
        .await;
        let start = Url::parse(&format!("http://127.0.0.1:{port}/r0")).unwrap();
        let err = expand_short_url(&start, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, MfError::Network { .. }), "{err:?}");
        let err = guarded_get(&start, &PageRequest::new("the website"), &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, MfError::Network { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn guarded_get_checks_every_hop() {
        let port = mock_server(|_, path, _| match path {
            "/page" => redirect("http://10.0.0.1/admin"),
            "/file" => redirect("ftp://example.com/x"),
            _ => ok("text/html; charset=utf-8", "<title>ok</title>"),
        })
        .await;
        let cancel = CancelToken::new();
        let page = Url::parse(&format!("http://127.0.0.1:{port}/page")).unwrap();
        assert_eq!(
            guarded_get(&page, &PageRequest::new("the website"), &cancel).await.unwrap_err(),
            ssrf::private_address_error()
        );
        let file = Url::parse(&format!("http://127.0.0.1:{port}/file")).unwrap();
        assert!(matches!(guarded_head(&file, &cancel).await, Err(MfError::InvalidInput { .. })));
        let fine = Url::parse(&format!("http://127.0.0.1:{port}/ok")).unwrap();
        let got = guarded_get(&fine, &PageRequest::new("the website").max_bytes(5), &cancel).await.unwrap();
        assert_eq!(got.body, "<titl");
        assert!(got.is_html());
    }

    #[tokio::test]
    async fn cancellation_wins() {
        let cancel = CancelToken::new();
        cancel.cancel();
        let out = cancellable(&cancel, std::future::pending::<()>()).await;
        assert_eq!(out, Err(MfError::Canceled));
    }
}
