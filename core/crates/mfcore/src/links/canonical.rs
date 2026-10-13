//! Offline URL handling: link detection, extraction from shared text and the
//! canonical form (ported 0.4 rules plus the streaming services added in 1.0).
//!
//! Canonical links of known sites are `https://<canonical host><path>?<kept query>`
//! with no trailing slash, no fragment and no tracking parameters. Unknown sites
//! keep their scheme and port; only tracking parameters and the fragment go.

use url::Url;

use crate::error::{MfError, Result};

/// Query keys removed everywhere (plus every key starting with `utm_`).
pub const TRACKING_QUERY_KEYS: &[&str] = &[
    "fbclid",
    "gclid",
    "igsh",
    "mc_cid",
    "mc_eid",
    "rdt",
    "si",
    "spm",
    "s",
    "share_id",
    "share_app_id",
    "utm_campaign",
    "utm_content",
    "utm_id",
    "utm_medium",
    "utm_name",
    "utm_source",
    "utm_term",
];

/// Hosts whose links only redirect elsewhere and must be expanded over the network.
pub const SHORTENER_HOSTS: &[&str] = &[
    "t.co",
    "vm.tiktok.com",
    "vt.tiktok.com",
    "spotify.link",
    "spoti.fi",
    "spotify.app.link",
    "link.deezer.com",
    "deezer.page.link",
    "dzr.page.link",
    "on.soundcloud.com",
    "redd.it",
    "apple.co",
    "link.tidal.com",
    "tidal.link",
    "amzn.to",
    "amzn.eu",
    "bit.ly",
    "tinyurl.com",
    "ow.ly",
    "buff.ly",
    "is.gd",
];

/// Redirect wrappers whose target sits in a query parameter.
const WRAPPER_HOSTS: &[(&str, &str)] = &[
    ("l.instagram.com", "u"),
    ("l.facebook.com", "u"),
    ("lm.facebook.com", "u"),
];

/// Top-level domains accepted for a bare host without a path ("example.com").
/// Other bare tokens with a dot ("song.mp3", "Mr.Brightside") are not links
/// unless they have a path, start with "www." or name a known site.
const COMMON_TLDS: &[&str] = &[
    "app", "at", "au", "be", "biz", "br", "ca", "ch", "co", "com", "de", "dev", "dk", "es", "eu",
    "fi", "fm", "fr", "info", "io", "it", "jp", "kr", "link", "ly", "me", "music", "net", "nl",
    "no", "nz", "online", "org", "page", "pl", "pt", "ru", "se", "site", "to", "tv", "uk", "us",
];

/// True when `text` (trimmed) is an http(s) URL or a bare domain link, or shared
/// text that contains one (see `extract_url`).
pub fn looks_like_url(text: &str) -> bool {
    extract_url(text).is_some()
}

/// The first link in `text`: an explicit `http(s)://` URL anywhere ("Check this
/// out https://… via @app"), else the first bare domain link such as
/// `youtu.be/abc` or `open.spotify.com/track/…` (returned with `https://`).
pub fn extract_url(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if let Some(url) = find_explicit_url(text) {
        return Some(url);
    }
    let single = !text.contains(char::is_whitespace);
    text.split_whitespace().map(trim_token).find_map(|token| {
        let (host, rest) = split_bare(token);
        let has_path = !rest.is_empty() && rest != "/";
        // In running text only accept domains with a path; a lone token may be a bare host.
        if (has_path || single) && is_bare_domain(host, has_path) {
            Url::parse(&format!("https://{token}"))
                .ok()
                .filter(|u| u.host_str().is_some())
                .map(|u| u.to_string())
        } else {
            None
        }
    })
}

fn find_explicit_url(text: &str) -> Option<String> {
    let lower = text.to_ascii_lowercase();
    let mut from = 0;
    while let Some(pos) = lower[from..].find("http").map(|p| p + from) {
        from = pos + 4;
        let rest = &lower[pos..];
        if !(rest.starts_with("http://") || rest.starts_with("https://")) {
            continue;
        }
        // Only at a word start ("xhttp://" is not a link).
        if pos > 0
            && text[..pos]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric())
        {
            continue;
        }
        let end = text[pos..]
            .find(|c: char| {
                c.is_whitespace()
                    || matches!(c, '<' | '>' | '"' | '`' | '|' | '{' | '}' | '\\' | '^')
            })
            .map_or(text.len(), |e| pos + e);
        let candidate = trim_token(&text[pos..end]);
        if let Ok(url) = Url::parse(candidate)
            && matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some_and(|h| !h.is_empty())
        {
            return Some(url.to_string());
        }
    }
    None
}

/// Strips trailing punctuation that belongs to the surrounding sentence, keeping
/// balanced closing brackets that are part of the URL.
fn trim_token(token: &str) -> &str {
    let token = token.trim_start_matches(['(', '[', '<', '"', '\'', '«', '“', '‘']);
    let mut end = token.len();
    while let Some(last) = token[..end].chars().next_back() {
        let strip = match last {
            '.' | ',' | ';' | ':' | '!' | '?' | '\'' | '"' | '»' | '”' | '’' | '…' => true,
            ')' => token[..end].matches('(').count() < token[..end].matches(')').count(),
            ']' => token[..end].matches('[').count() < token[..end].matches(']').count(),
            _ => false,
        };
        if !strip {
            break;
        }
        end -= last.len_utf8();
    }
    &token[..end]
}

/// Splits a bare link into host (with optional port) and the rest.
fn split_bare(token: &str) -> (&str, &str) {
    match token.find(['/', '?', '#']) {
        Some(i) => (&token[..i], &token[i..]),
        None => (token, ""),
    }
}

fn is_bare_domain(host_port: &str, has_path: bool) -> bool {
    let host = match host_port.rsplit_once(':') {
        Some((h, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => h,
        Some(_) => return false,
        None => host_port,
    };
    let labels: Vec<&str> = host.split('.').collect();
    if labels.len() < 2 || host.len() > 253 {
        return false;
    }
    let label_ok = |l: &&str| {
        !l.is_empty()
            && l.len() <= 63
            && !l.starts_with('-')
            && !l.ends_with('-')
            && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    };
    if !labels.iter().all(label_ok) {
        return false;
    }
    let tld = labels[labels.len() - 1].to_ascii_lowercase();
    let tld_ok =
        (tld.len() >= 2 && tld.chars().all(|c| c.is_ascii_alphabetic())) || tld.starts_with("xn--");
    if !tld_ok {
        return false;
    }
    let lower = host.to_ascii_lowercase();
    has_path
        || lower.starts_with("www.")
        || COMMON_TLDS.contains(&tld.as_str())
        || super::Site::of_host(&lower) != super::Site::Other
}

/// Parses an http(s) URL or a bare domain link; anything else is invalid input.
pub(crate) fn parse_http_url(input: &str) -> Result<Url> {
    let input = input.trim();
    if input.is_empty() {
        return Err(MfError::invalid("Enter a link."));
    }
    let lower = input.to_ascii_lowercase();
    let url = if lower.starts_with("http://") || lower.starts_with("https://") {
        Url::parse(input).map_err(|_| MfError::invalid("That link isn't valid."))?
    } else if input.contains("://") || lower.starts_with("mailto:") || lower.starts_with("file:") {
        return Err(MfError::invalid("Only http and https links are supported."));
    } else {
        let (host, rest) = split_bare(input);
        if !is_bare_domain(host, !rest.is_empty()) {
            return Err(MfError::invalid("That doesn't look like a link."));
        }
        Url::parse(&format!("https://{input}"))
            .map_err(|_| MfError::invalid("That link isn't valid."))?
    };
    if url.host_str().is_none_or(str::is_empty) {
        return Err(MfError::invalid("That link has no website in it."));
    }
    Ok(url)
}

/// Offline normalization: tracking parameters stripped, canonical hosts and
/// paths (ported 0.4 rules). Errors on non-http(s) input.
pub fn canonicalize(url: &str) -> Result<String> {
    let parsed = parse_http_url(url)?;
    Ok(canonical_url(&parsed))
}

/// Lowercase host without a trailing dot.
pub(crate) fn host_of(url: &Url) -> String {
    url.host_str()
        .unwrap_or_default()
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

/// True when `url` must be expanded over the network before it can be routed.
pub(crate) fn is_shortener(url: &Url) -> bool {
    let host = host_of(url);
    if SHORTENER_HOSTS.contains(&host.as_str()) {
        return true;
    }
    let parts = segments(url);
    match host.as_str() {
        // TikTok and Reddit share links: /t/<code>, /r/<sub>/s/<code>.
        "www.tiktok.com" | "tiktok.com" | "m.tiktok.com" => {
            parts.first() == Some(&"t") && parts.len() == 2
        }
        "www.reddit.com" | "reddit.com" => parts.len() == 4 && parts[0] == "r" && parts[2] == "s",
        _ => false,
    }
}

/// The target of a redirect wrapper such as `l.instagram.com/?u=<url>`.
pub(crate) fn unwrap_wrapper(url: &Url) -> Option<Url> {
    let host = host_of(url);
    let key = WRAPPER_HOSTS
        .iter()
        .find(|(h, _)| *h == host)
        .map(|(_, k)| *k)?;
    url.query_pairs()
        .find(|(k, v)| k == key && (v.starts_with("http://") || v.starts_with("https://")))
        .and_then(|(_, v)| Url::parse(&v).ok())
}

pub(crate) fn segments(url: &Url) -> Vec<&str> {
    url.path_segments()
        .map(|s| s.filter(|p| !p.is_empty()).collect())
        .unwrap_or_default()
}

fn is_tracking(key: &str) -> bool {
    key.starts_with("utm_") || TRACKING_QUERY_KEYS.contains(&key)
}

/// Query pairs without blank values and tracking keys, optionally restricted to `allowed`.
fn filtered_query(url: &Url, allowed: Option<&[&str]>) -> Vec<(String, String)> {
    url.query_pairs()
        .filter(|(k, v)| !v.is_empty() && !is_tracking(k))
        .filter(|(k, _)| allowed.is_none_or(|a| a.contains(&k.as_ref())))
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn build(scheme: &str, authority: &str, path: &str, query: &[(String, String)]) -> String {
    let mut path = if path.is_empty() { "/" } else { path };
    if path != "/" {
        path = path.trim_end_matches('/');
        if path.is_empty() {
            path = "/";
        }
    }
    let mut out = format!("{scheme}://{authority}{path}");
    if !query.is_empty() {
        let encoded = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(query)
            .finish();
        out.push('?');
        out.push_str(&encoded);
    }
    out
}

fn https(host: &str, path: &str, query: &[(String, String)]) -> String {
    build("https", host, path, query)
}

fn join(parts: &[&str]) -> String {
    format!("/{}", parts.join("/"))
}

/// Canonical form of an already parsed http(s) URL.
pub(crate) fn canonical_url(url: &Url) -> String {
    canonical_at_depth(url, 0)
}

fn canonical_at_depth(url: &Url, depth: u8) -> String {
    if depth < 3
        && let Some(target) = unwrap_wrapper(url)
    {
        return canonical_at_depth(&target, depth + 1);
    }
    let host = host_of(url);
    let normalizers: [fn(&Url, &str) -> Option<String>; 17] = [
        youtube,
        instagram,
        tiktok,
        x_twitter,
        spotify,
        apple_music,
        deezer,
        tidal,
        amazon_music,
        reddit,
        pornhub,
        vimeo,
        soundcloud,
        bandcamp,
        mixcloud,
        twitch,
        shortener,
    ];
    normalizers
        .iter()
        .find_map(|f| f(url, &host))
        .unwrap_or_else(|| generic(url, &host))
}

fn generic(url: &Url, host: &str) -> String {
    let authority = match url.port() {
        Some(port) => format!("{}:{port}", host_display(url, host)),
        None => host_display(url, host),
    };
    build(
        url.scheme(),
        &authority,
        url.path(),
        &filtered_query(url, None),
    )
}

/// IPv6 literals keep their brackets.
fn host_display(url: &Url, host: &str) -> String {
    match url.host() {
        Some(url::Host::Ipv6(ip)) => format!("[{ip}]"),
        _ => host.to_string(),
    }
}

/// Shortener links stay as they are (minus tracking) until expanded.
fn shortener(url: &Url, host: &str) -> Option<String> {
    SHORTENER_HOSTS
        .contains(&host)
        .then(|| https(host, url.path(), &filtered_query(url, None)))
}

/// The video id of a single-video YouTube link (`watch?v=`, `youtu.be/`,
/// `/shorts/`, `/embed/`, `/live/`, `/v/`).
pub(crate) fn youtube_video_id(url: &Url) -> Option<String> {
    let host = host_of(url);
    let parts = segments(url);
    if host == "youtu.be" {
        return parts.first().map(|id| (*id).to_string());
    }
    if !(host.contains("youtube.") || host.ends_with("youtube-nocookie.com")) {
        return None;
    }
    if url.path() == "/watch" {
        return url
            .query_pairs()
            .find(|(k, v)| k == "v" && !v.is_empty())
            .map(|(_, v)| v.into_owned());
    }
    match parts.as_slice() {
        [kind, id, ..] if matches!(*kind, "shorts" | "embed" | "live" | "v") => {
            Some((*id).to_string())
        }
        _ => None,
    }
}

fn youtube(url: &Url, host: &str) -> Option<String> {
    let is_youtube =
        host == "youtu.be" || host.contains("youtube.") || host.ends_with("youtube-nocookie.com");
    if !is_youtube {
        return None;
    }
    let canonical_host = if host == "music.youtube.com" {
        "music.youtube.com"
    } else {
        "www.youtube.com"
    };
    if let Some(id) = youtube_video_id(url) {
        // Radio/mix lists (`list=RD…&start_radio=1`) and every other extra are dropped.
        let mut query = vec![("v".to_string(), id)];
        query.extend(filtered_query(url, Some(&["t", "start"][..])));
        return Some(https(canonical_host, "/watch", &query));
    }
    if url.path() == "/playlist" {
        return Some(https(
            canonical_host,
            "/playlist",
            &filtered_query(url, Some(&["list", "index"][..])),
        ));
    }
    let canonical_host = if matches!(host, "youtube.com" | "www.youtube.com" | "m.youtube.com") {
        "www.youtube.com"
    } else {
        host
    };
    Some(https(
        canonical_host,
        url.path(),
        &filtered_query(url, None),
    ))
}

fn instagram(url: &Url, host: &str) -> Option<String> {
    if !host.contains("instagram.") {
        return None;
    }
    let canonical_host = if matches!(
        host,
        "instagram.com" | "www.instagram.com" | "m.instagram.com"
    ) {
        "www.instagram.com"
    } else {
        host
    };
    let parts = segments(url);
    if parts.len() >= 2 && matches!(parts[0], "p" | "reel" | "reels" | "tv") {
        return Some(https(
            canonical_host,
            &join(&parts[..2]),
            &filtered_query(url, Some(&["img_index"][..])),
        ));
    }
    Some(https(
        canonical_host,
        url.path(),
        &filtered_query(url, None),
    ))
}

fn tiktok(url: &Url, host: &str) -> Option<String> {
    if !host.contains("tiktok.") {
        return None;
    }
    let canonical_host = if matches!(host, "tiktok.com" | "www.tiktok.com" | "m.tiktok.com") {
        "www.tiktok.com"
    } else {
        host
    };
    let parts = segments(url);
    let path =
        if parts.len() >= 3 && parts[0].starts_with('@') && matches!(parts[1], "video" | "photo") {
            join(&parts[..3])
        } else {
            url.path().to_string()
        };
    Some(https(canonical_host, &path, &[]))
}

fn x_twitter(url: &Url, host: &str) -> Option<String> {
    if !matches!(
        host,
        "twitter.com"
            | "www.twitter.com"
            | "mobile.twitter.com"
            | "x.com"
            | "www.x.com"
            | "mobile.x.com"
    ) {
        return None;
    }
    let parts = segments(url);
    let path = if parts.len() >= 3 && parts[1] == "status" {
        join(&parts[..3])
    } else {
        url.path().to_string()
    };
    Some(https("x.com", &path, &[]))
}

fn spotify(url: &Url, host: &str) -> Option<String> {
    if !host.contains("spotify.") || SHORTENER_HOSTS.contains(&host) {
        return None;
    }
    let canonical_host = if matches!(
        host,
        "spotify.com" | "www.spotify.com" | "open.spotify.com" | "play.spotify.com"
    ) {
        "open.spotify.com"
    } else {
        host
    };
    let mut parts = segments(url);
    if parts.first().is_some_and(|p| p.starts_with("intl-")) {
        parts.remove(0);
    }
    if parts.first() == Some(&"embed") {
        parts.remove(0);
    }
    let entity = matches!(
        parts.first(),
        Some(&("track" | "album" | "playlist" | "artist" | "episode" | "show"))
    );
    let query = if entity {
        Vec::new()
    } else {
        filtered_query(url, None)
    };
    Some(https(canonical_host, &join(&parts), &query))
}

fn apple_music(url: &Url, host: &str) -> Option<String> {
    if !matches!(
        host,
        "music.apple.com"
            | "geo.music.apple.com"
            | "embed.music.apple.com"
            | "itunes.apple.com"
            | "geo.itunes.apple.com"
    ) {
        return None;
    }
    Some(https(
        "music.apple.com",
        url.path(),
        &filtered_query(url, Some(&["i"][..])),
    ))
}

fn deezer(url: &Url, host: &str) -> Option<String> {
    if !host.contains("deezer.") || SHORTENER_HOSTS.contains(&host) {
        return None;
    }
    let canonical_host = if matches!(host, "deezer.com" | "www.deezer.com") {
        "www.deezer.com"
    } else {
        host
    };
    let mut parts = segments(url);
    if parts.len() >= 2 && parts[0].len() == 2 {
        parts.remove(0);
    }
    // Entity links carry only sharing data (including the sharer's user id).
    let entity = matches!(
        parts.first(),
        Some(&("track" | "album" | "playlist" | "artist" | "show" | "episode"))
    );
    let query = if entity {
        Vec::new()
    } else {
        filtered_query(url, None)
    };
    Some(https(canonical_host, &join(&parts), &query))
}

fn tidal(url: &Url, host: &str) -> Option<String> {
    if !host.contains("tidal.") || SHORTENER_HOSTS.contains(&host) {
        return None;
    }
    // listen.tidal.com now redirects to tidal.com.
    let canonical_host = if matches!(host, "tidal.com" | "www.tidal.com" | "listen.tidal.com") {
        "tidal.com"
    } else {
        host
    };
    let mut parts = segments(url);
    if parts.first() == Some(&"browse") {
        parts.remove(0);
    }
    let entity = matches!(
        parts.first(),
        Some(&("track" | "album" | "playlist" | "video" | "artist" | "mix"))
    );
    let query = if entity {
        Vec::new()
    } else {
        filtered_query(url, None)
    };
    Some(https(canonical_host, &join(&parts), &query))
}

fn amazon_music(url: &Url, host: &str) -> Option<String> {
    if !host.starts_with("music.amazon.") {
        return None;
    }
    Some(https(
        host,
        url.path(),
        &filtered_query(url, Some(&["trackAsin"][..])),
    ))
}

fn reddit(url: &Url, host: &str) -> Option<String> {
    if !matches!(
        host,
        "reddit.com" | "www.reddit.com" | "old.reddit.com" | "new.reddit.com" | "redd.it"
    ) {
        return None;
    }
    let canonical_host = if host == "redd.it" {
        host
    } else {
        "www.reddit.com"
    };
    Some(https(canonical_host, url.path(), &[]))
}

fn pornhub(url: &Url, host: &str) -> Option<String> {
    if !host.contains("pornhub.") {
        return None;
    }
    let canonical_host = if matches!(host, "pornhub.com" | "www.pornhub.com") {
        "www.pornhub.com"
    } else {
        host
    };
    let allowed: Option<&[&str]> = (url.path() == "/view_video.php").then_some(&["viewkey"][..]);
    Some(https(
        canonical_host,
        url.path(),
        &filtered_query(url, allowed),
    ))
}

fn vimeo(url: &Url, host: &str) -> Option<String> {
    if !host.contains("vimeo.") {
        return None;
    }
    let parts = segments(url);
    // Embedded player links point at the video page.
    if host == "player.vimeo.com"
        && let ["video", id, ..] = parts.as_slice()
    {
        return Some(https("vimeo.com", &format!("/{id}"), &[]));
    }
    let canonical_host = if matches!(host, "vimeo.com" | "www.vimeo.com") {
        "vimeo.com"
    } else {
        host
    };
    Some(https(
        canonical_host,
        url.path(),
        &filtered_query(url, None),
    ))
}

fn soundcloud(url: &Url, host: &str) -> Option<String> {
    if !host.contains("soundcloud.") || SHORTENER_HOSTS.contains(&host) {
        return None;
    }
    if matches!(
        host,
        "soundcloud.com" | "www.soundcloud.com" | "m.soundcloud.com"
    ) {
        // Share links append ref/p/c/si; nothing in the query identifies the track.
        return Some(https("soundcloud.com", url.path(), &[]));
    }
    Some(https(host, url.path(), &filtered_query(url, None)))
}

fn bandcamp(url: &Url, host: &str) -> Option<String> {
    (host == "bandcamp.com" || host.ends_with(".bandcamp.com"))
        .then(|| https(host, url.path(), &[]))
}

fn mixcloud(url: &Url, host: &str) -> Option<String> {
    matches!(host, "mixcloud.com" | "www.mixcloud.com" | "m.mixcloud.com")
        .then(|| https("www.mixcloud.com", url.path(), &[]))
}

fn twitch(url: &Url, host: &str) -> Option<String> {
    matches!(host, "twitch.tv" | "www.twitch.tv" | "m.twitch.tv")
        .then(|| https("www.twitch.tv", url.path(), &filtered_query(url, None)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canon(url: &str) -> String {
        canonicalize(url).unwrap_or_else(|e| panic!("{url}: {e}"))
    }

    // Expectations ported from 0.4 tests/test_sources.py.
    #[test]
    fn ported_expectations() {
        assert_eq!(
            canon("https://www.youtube.com/watch?v=gBT60YL3lNw&list=RDgBT60YL3lNw&start_radio=1"),
            "https://www.youtube.com/watch?v=gBT60YL3lNw"
        );
        assert_eq!(
            canon("https://www.youtube.com/shorts/abc123?feature=share"),
            "https://www.youtube.com/watch?v=abc123"
        );
        assert_eq!(
            canon(
                "https://www.instagram.com/reel/Cxyz123/?utm_source=ig_web_copy_link&igsh=MzRlODBiNWFlZA=="
            ),
            "https://www.instagram.com/reel/Cxyz123"
        );
        assert_eq!(
            canon(
                "https://www.tiktok.com/@artist/video/1234567890?is_from_webapp=1&sender_device=pc"
            ),
            "https://www.tiktok.com/@artist/video/1234567890"
        );
        assert_eq!(
            canon("https://twitter.com/example/status/1234567890?s=20&t=abcdef"),
            "https://x.com/example/status/1234567890"
        );
        assert_eq!(
            canon("https://open.spotify.com/track/abc123?si=deadbeef&utm_source=copy-link"),
            "https://open.spotify.com/track/abc123"
        );
        assert_eq!(
            canon("https://www.pornhub.com/view_video.php?viewkey=ph12345&foo=bar"),
            "https://www.pornhub.com/view_video.php?viewkey=ph12345"
        );
        assert_eq!(
            canon(
                "https://www.reddit.com/r/test/comments/abc123/example_post/?utm_source=share&utm_medium=ios_app&rdt=12345"
            ),
            "https://www.reddit.com/r/test/comments/abc123/example_post"
        );
        // Playlist URLs stay expandable.
        assert_eq!(
            canon("https://music.youtube.com/playlist?list=PL123"),
            "https://music.youtube.com/playlist?list=PL123"
        );
    }

    #[test]
    fn youtube_rules() {
        assert_eq!(
            canon("https://youtu.be/dQw4w9WgXcQ?si=abc"),
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        );
        assert_eq!(
            canon("https://youtu.be/dQw4w9WgXcQ?t=42"),
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42"
        );
        assert_eq!(
            canon("https://m.youtube.com/watch?v=dQw4w9WgXcQ&feature=youtu.be"),
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        );
        assert_eq!(
            canon("https://youtube.com/embed/dQw4w9WgXcQ?autoplay=1"),
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        );
        assert_eq!(
            canon("https://www.youtube.com/live/abcdefghijk/"),
            "https://www.youtube.com/watch?v=abcdefghijk"
        );
        assert_eq!(
            canon("https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ"),
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        );
        assert_eq!(
            canon("https://music.youtube.com/watch?v=lYBUbBu4W08&list=RDAMVMlYBUbBu4W08"),
            "https://music.youtube.com/watch?v=lYBUbBu4W08"
        );
        assert_eq!(
            canon("https://www.youtube.com/playlist?list=PLx&index=3&si=zz"),
            "https://www.youtube.com/playlist?list=PLx&index=3"
        );
        assert_eq!(
            canon("https://www.youtube.com/@channel/videos?si=x"),
            "https://www.youtube.com/@channel/videos"
        );
    }

    #[test]
    fn streaming_services() {
        assert_eq!(
            canon("https://open.spotify.com/intl-de/track/4cOdK2wGLETKBW3PvgPWqT?si=x&nd=1"),
            "https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT"
        );
        assert_eq!(
            canon("https://open.spotify.com/embed/album/4aawyAB9vmqN3uQ7FjRGTy"),
            "https://open.spotify.com/album/4aawyAB9vmqN3uQ7FjRGTy"
        );
        assert_eq!(
            canon(
                "https://music.apple.com/us/album/never-gonna-give-you-up/1559885420?i=1559885421&uo=4&ls=1"
            ),
            "https://music.apple.com/us/album/never-gonna-give-you-up/1559885420?i=1559885421"
        );
        assert_eq!(
            canon("https://itunes.apple.com/us/album/x/123?app=itunes"),
            "https://music.apple.com/us/album/x/123"
        );
        assert_eq!(
            canon(
                "https://www.deezer.com/de/playlist/13957557901?host=677491421&utm_campaign=clipboard-generic&deferredFl=1"
            ),
            "https://www.deezer.com/playlist/13957557901"
        );
        assert_eq!(
            canon("https://deezer.com/en/track/3135556/"),
            "https://www.deezer.com/track/3135556"
        );
        assert_eq!(
            canon("https://listen.tidal.com/browse/track/1781887?u"),
            "https://tidal.com/track/1781887"
        );
        assert_eq!(
            canon("https://tidal.com/browse/album/1781800"),
            "https://tidal.com/album/1781800"
        );
        assert_eq!(
            canon(
                "https://music.amazon.com/albums/B000TDBNDG?do=play&trackAsin=B000TDFHJW&ref=dm_sh_x"
            ),
            "https://music.amazon.com/albums/B000TDBNDG?trackAsin=B000TDFHJW"
        );
        assert_eq!(
            canon(
                "https://m.soundcloud.com/forss/flickermood?ref=clipboard&p=a&c=1&si=0e&utm_source=clipboard"
            ),
            "https://soundcloud.com/forss/flickermood"
        );
        assert_eq!(
            canon("https://artist.bandcamp.com/track/song?from=embed"),
            "https://artist.bandcamp.com/track/song"
        );
        assert_eq!(
            canon("https://m.mixcloud.com/spartacus/party-time/?utm_source=x"),
            "https://www.mixcloud.com/spartacus/party-time"
        );
        assert_eq!(
            canon("https://player.vimeo.com/video/76979871?h=abc"),
            "https://vimeo.com/76979871"
        );
        assert_eq!(
            canon("https://m.twitch.tv/videos/123?t=1h2m3s"),
            "https://www.twitch.tv/videos/123?t=1h2m3s"
        );
    }

    #[test]
    fn wrappers_and_shorteners() {
        assert_eq!(
            canon(
                "https://l.instagram.com/?u=https%3A%2F%2Fopen.spotify.com%2Ftrack%2Fabc%3Fsi%3D1&e=AT0"
            ),
            "https://open.spotify.com/track/abc"
        );
        assert_eq!(
            canon("https://spotify.link/6tpneu0iVIb"),
            "https://spotify.link/6tpneu0iVIb"
        );
        assert_eq!(
            canon("https://on.soundcloud.com/9BWtqT2nIcIiAubq3X/"),
            "https://on.soundcloud.com/9BWtqT2nIcIiAubq3X"
        );
        assert_eq!(
            canon("https://vm.tiktok.com/ZMRxd5VLN/"),
            "https://vm.tiktok.com/ZMRxd5VLN"
        );
        let shortener = |u: &str| is_shortener(&Url::parse(u).unwrap());
        assert!(shortener("https://link.deezer.com/s/323PqDbPY7fERrDJhrypG"));
        assert!(shortener("https://www.tiktok.com/t/ZTabc123/"));
        assert!(shortener("https://www.reddit.com/r/music/s/AbCdEf"));
        assert!(!shortener("https://www.reddit.com/r/music/comments/abc/x"));
        assert!(!shortener("https://youtu.be/dQw4w9WgXcQ"));
    }

    #[test]
    fn generic_sites_keep_scheme_and_port() {
        assert_eq!(
            canon("http://example.com:8080/a/b/?utm_source=x&id=3#frag"),
            "http://example.com:8080/a/b?id=3"
        );
        assert_eq!(
            canon("HTTPS://Example.COM/Track.mp3"),
            "https://example.com/Track.mp3"
        );
        assert_eq!(canon("https://example.com"), "https://example.com/");
        assert_eq!(canon("http://[::1]:8000/x"), "http://[::1]:8000/x");
        assert_eq!(canon("example.com/song"), "https://example.com/song");
    }

    #[test]
    fn rejects_non_http() {
        for bad in [
            "",
            "   ",
            "ftp://example.com/x",
            "file:///tmp/a.mp3",
            "/tmp/file.mp4",
            "mailto:a@b.c",
            "spotify:track:abc",
            "not a link",
        ] {
            assert!(canonicalize(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn url_detection() {
        assert!(looks_like_url("https://youtube.com/watch?v=abc"));
        assert!(!looks_like_url("/tmp/file.mp4"));
        assert!(looks_like_url("  https://example.com/a  \n"));
        assert!(looks_like_url("youtu.be/abc"));
        assert!(looks_like_url(
            "open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT"
        ));
        assert!(looks_like_url("example.com"));
        assert!(looks_like_url("www.example.zone"));
        assert!(looks_like_url("youtu.be"));
        assert!(!looks_like_url("song.mp3"));
        assert!(!looks_like_url("Mr.Brightside"));
        assert!(!looks_like_url("notes.txt"));
        assert!(!looks_like_url("hello world"));
        assert!(!looks_like_url("me@example.com"));
        assert!(!looks_like_url("e.g."));
        assert!(!looks_like_url("file:///tmp/a.mp3"));
        assert!(!looks_like_url(""));
    }

    #[test]
    fn extracts_first_url_from_shared_text() {
        assert_eq!(
            extract_url("Check this out https://open.spotify.com/track/abc?si=1 via @app")
                .as_deref(),
            Some("https://open.spotify.com/track/abc?si=1")
        );
        assert_eq!(
            extract_url("(see https://example.com/a_(b)).").as_deref(),
            Some("https://example.com/a_(b)")
        );
        assert_eq!(
            extract_url("“https://example.com/x”").as_deref(),
            Some("https://example.com/x")
        );
        assert_eq!(
            extract_url("listen: youtu.be/dQw4w9WgXcQ!").as_deref(),
            Some("https://youtu.be/dQw4w9WgXcQ")
        );
        assert_eq!(
            extract_url("first http://a.example/1 then https://b.example/2").as_deref(),
            Some("http://a.example/1")
        );
        assert_eq!(extract_url("Visit example.com today"), None);
        assert_eq!(extract_url("xhttps://example.com"), None);
        assert_eq!(
            extract_url("http://127.0.0.1/admin").as_deref(),
            Some("http://127.0.0.1/admin")
        );
    }
}
