//! SSRF guard: user-supplied links must not reach loopback, private, link-local
//! or otherwise non-public addresses (cloud metadata services, routers, NAS boxes).
//!
//! `ensure_public_url` checks a URL before each request and each redirect hop;
//! `PublicOnlyResolver` re-checks the addresses the HTTP client actually connects
//! to, which closes the DNS-rebinding gap between the check and the connection.
//!
//! Browsers expose no DNS lookup, so on wasm32 host names get the offline checks
//! only (IP literals, `localhost`, `.local` and similar names), and the fetch
//! helpers check the final URL after fetch() followed redirects.

#[cfg(not(target_family = "wasm"))]
use std::net::ToSocketAddrs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use url::{Host, Url};

use crate::error::{MfError, Result};

const PRIVATE_MESSAGE: &str = "Links to local or private network addresses aren't supported.";

/// Error returned for links that point at non-public addresses.
pub(crate) fn private_address_error() -> MfError {
    MfError::invalid(PRIVATE_MESSAGE)
}

#[cfg_attr(target_family = "wasm", allow(dead_code))]
fn unresolvable_error() -> MfError {
    MfError::network("couldn't find that website")
}

/// True when `ip` is globally routable unicast.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => is_public_v6(v6),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || a == 0 // "this network"
        || (a == 100 && (b & 0xc0) == 64) // 100.64.0.0/10, carrier-grade NAT
        || (a == 192 && b == 0 && c == 0) // 192.0.0.0/24, IETF protocol assignments
        || (a == 198 && (b & 0xfe) == 18) // 198.18.0.0/15, benchmarking
        || a >= 240) // 240.0.0.0/4, reserved
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_public_v4(v4);
    }
    let s = ip.segments();
    let embedded_v4 = |hi: u16, lo: u16| Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    // NAT64 well-known prefix 64:ff9b::/96 carries an IPv4 address.
    if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return is_public_v4(embedded_v4(s[6], s[7]));
    }
    // Only global unicast (2000::/3) is public; everything else is reserved,
    // loopback, unspecified, ULA (fc00::/7), link-local (fe80::/10) or multicast.
    if (s[0] & 0xe000) != 0x2000 {
        return false;
    }
    // 6to4 (2002::/16) embeds an IPv4 address.
    if s[0] == 0x2002 {
        return is_public_v4(embedded_v4(s[1], s[2]));
    }
    !((s[0] == 0x2001 && s[1] < 0x0200) // 2001::/23, IETF protocol assignments incl. Teredo
        || (s[0] == 0x2001 && s[1] == 0x0db8) // 2001:db8::/32, documentation
        || (s[0] == 0x3fff && s[1] < 0x1000)) // 3fff::/20, documentation
}

/// Rejects host names that never denote a public site.
fn check_host_name(host: &str) -> Result<()> {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    let local_suffixes = [".localhost", ".local", ".internal", ".home.arpa", ".localdomain"];
    if host.is_empty() || host == "localhost" || local_suffixes.iter().any(|s| host.ends_with(s)) {
        return Err(private_address_error());
    }
    Ok(())
}

/// What still needs checking after the offline rules.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
enum Target {
    Ip(IpAddr),
    Name(String, u16),
}

fn target(url: &Url) -> Result<Target> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(MfError::invalid("Only http and https links are supported."));
    }
    let port = url.port_or_known_default().unwrap_or(443);
    match url.host() {
        Some(Host::Ipv4(ip)) => Ok(Target::Ip(IpAddr::V4(ip))),
        Some(Host::Ipv6(ip)) => Ok(Target::Ip(IpAddr::V6(ip))),
        Some(Host::Domain(name)) => {
            check_host_name(name)?;
            Ok(Target::Name(name.trim_end_matches('.').to_string(), port))
        }
        None => Err(MfError::invalid("That link has no website in it.")),
    }
}

#[cfg_attr(target_family = "wasm", allow(dead_code))]
fn check_resolved(addrs: impl IntoIterator<Item = SocketAddr>) -> Result<()> {
    let mut any = false;
    for addr in addrs {
        any = true;
        if !is_public_ip(addr.ip()) {
            return Err(private_address_error());
        }
    }
    if any { Ok(()) } else { Err(unresolvable_error()) }
}

/// Rejects URLs whose host resolves to a non-public address.
///
/// Blocking (system resolver); use `ensure_public_url` from async code.
pub fn assert_public_url(url: &str) -> Result<()> {
    let url = Url::parse(url.trim()).map_err(|_| MfError::invalid("That link isn't valid."))?;
    match target(&url)? {
        Target::Ip(ip) => is_public_ip(ip).then_some(()).ok_or_else(private_address_error),
        #[cfg(not(target_family = "wasm"))]
        Target::Name(name, port) => {
            let addrs = (name.as_str(), port).to_socket_addrs().map_err(|_| unresolvable_error())?;
            check_resolved(addrs)
        }
        #[cfg(target_family = "wasm")]
        Target::Name(..) => Ok(()),
    }
}

/// Async variant of `assert_public_url` (tokio resolver).
pub async fn ensure_public_url(url: &str) -> Result<()> {
    let url = Url::parse(url.trim()).map_err(|_| MfError::invalid("That link isn't valid."))?;
    ensure_public(&url).await
}

/// Loopback ports tests may reach (mock servers). Never set outside tests.
#[cfg(test)]
pub(crate) static TEST_LOOPBACK_PORTS: parking_lot::Mutex<Vec<u16>> = parking_lot::Mutex::new(Vec::new());

#[cfg(test)]
fn allowed_in_test(url: &Url) -> bool {
    matches!(url.host(), Some(Host::Ipv4(ip)) if ip.is_loopback())
        && url.port().is_some_and(|port| TEST_LOOPBACK_PORTS.lock().contains(&port))
}

pub(crate) async fn ensure_public(url: &Url) -> Result<()> {
    #[cfg(test)]
    if allowed_in_test(url) {
        return Ok(());
    }
    match target(url)? {
        Target::Ip(ip) => is_public_ip(ip).then_some(()).ok_or_else(private_address_error),
        #[cfg(not(target_family = "wasm"))]
        Target::Name(name, port) => {
            let addrs = tokio::net::lookup_host((name.as_str(), port)).await.map_err(|_| unresolvable_error())?;
            check_resolved(addrs)
        }
        #[cfg(target_family = "wasm")]
        Target::Name(..) => Ok(()),
    }
}

/// Marker error raised by `PublicOnlyResolver`, recognised when mapping
/// connection errors.
#[derive(Debug)]
#[cfg_attr(target_family = "wasm", allow(dead_code))]
pub(crate) struct NonPublicAddress;

impl std::fmt::Display for NonPublicAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("host resolves to a non-public address")
    }
}

impl std::error::Error for NonPublicAddress {}

/// DNS resolver for the guarded client: refuses names that resolve to any
/// non-public address.
#[cfg(not(target_family = "wasm"))]
pub(crate) struct PublicOnlyResolver;

#[cfg(not(target_family = "wasm"))]
impl reqwest::dns::Resolve for PublicOnlyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            if check_host_name(&host).is_err() {
                return Err(Box::new(NonPublicAddress) as Box<dyn std::error::Error + Send + Sync>);
            }
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if addrs.iter().any(|a| !is_public_ip(a.ip())) {
                return Err(Box::new(NonPublicAddress) as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// True when `err` (or a source of it) is the resolver's non-public refusal.
pub(crate) fn is_non_public_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(e) = current {
        if e.is::<NonPublicAddress>() {
            return true;
        }
        current = e.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn classifies_ipv4() {
        for blocked in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.178.1",
            "169.254.169.254",
            "100.64.0.1",
            "100.127.255.255",
            "0.0.0.0",
            "0.1.2.3",
            "224.0.0.1",
            "239.255.255.250",
            "255.255.255.255",
            "240.0.0.1",
            "192.0.0.8",
            "192.0.2.1",
            "198.18.0.1",
            "198.51.100.7",
            "203.0.113.9",
        ] {
            assert!(!is_public_ip(ip(blocked)), "{blocked} must be blocked");
        }
        for allowed in ["8.8.8.8", "1.1.1.1", "100.128.0.1", "172.32.0.1", "151.101.1.140", "192.0.1.1"] {
            assert!(is_public_ip(ip(allowed)), "{allowed} must be allowed");
        }
    }

    #[test]
    fn classifies_ipv6() {
        for blocked in [
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::127.0.0.1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "2001:db8::1",
            "2001::1",
            "64:ff9b::7f00:1",
            "64:ff9b::a9fe:a9fe",
            "2002:7f00:1::1",
            "2002:c0a8:101::1",
            "100::1",
            "3fff::1",
        ] {
            assert!(!is_public_ip(ip(blocked)), "{blocked} must be blocked");
        }
        for allowed in [
            "2606:4700:4700::1111",
            "2a00:1450:4001:80b::200e",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
        ] {
            assert!(is_public_ip(ip(allowed)), "{allowed} must be allowed");
        }
    }

    #[test]
    fn rejects_private_urls_offline() {
        for url in [
            "http://127.0.0.1/admin",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]:8080/",
            "http://[fd00::1]/",
            "http://localhost:3000/",
            "http://printer.local/",
            "http://metadata.google.internal/",
            "http://0x7f.1/",
            "http://2130706433/",
            "http://0/",
            "ftp://example.com/",
        ] {
            assert!(assert_public_url(url).is_err(), "{url} must be rejected");
        }
    }

    #[tokio::test]
    async fn async_guard_rejects_private_literals() {
        assert_eq!(ensure_public_url("http://10.0.0.1/").await, Err(private_address_error()));
        assert_eq!(ensure_public_url("https://[::ffff:192.168.1.1]/").await, Err(private_address_error()));
        assert!(ensure_public_url("https://8.8.8.8/").await.is_ok());
    }

    #[test]
    fn detects_resolver_refusal_in_error_chain() {
        #[derive(Debug)]
        struct Wrapper(NonPublicAddress);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("wrapped")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        assert!(is_non_public_error(&Wrapper(NonPublicAddress)));
        assert!(!is_non_public_error(&std::io::Error::other("x")));
    }
}
