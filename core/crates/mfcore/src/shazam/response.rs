//! Parsing of Shazam `discovery/v5` responses into a [`ShazamMatch`].
//!
//! Every field is optional on the wire; anything missing or of the wrong type
//! is skipped. Only a first match plus a track title are required.

use std::collections::HashMap;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde_json::Value;

use crate::model::{Service, ServiceLink, Track};
use crate::shazam::client::ShazamMatch;

/// Characters kept as-is in search URL paths and queries.
const QUERY: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Separators between credited artists in Shazam's `subtitle`.
const ARTIST_SEPARATORS: [&str; 7] = [", ", " & ", " feat. ", " ft. ", " featuring ", " x ", " × "];

/// Artwork edge length requested from Apple's image CDN.
const ARTWORK_SIZE: u32 = 1000;

/// `None` when the response has no match or no usable track.
pub(crate) fn parse_match(body: &Value, country: &str) -> Option<ShazamMatch> {
    let first = body.get("matches")?.as_array()?.first()?;
    let track = body.get("track")?;
    let title = text(track, "title")?;
    let artist = text(track, "subtitle").unwrap_or_default();

    let actions = track
        .pointer("/hub/actions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let apple_id = actions
        .iter()
        .find(|a| a.get("type").and_then(Value::as_str) == Some("applemusicplay"))
        .and_then(|a| text(a, "id"))
        .filter(|id| id.bytes().all(|b| b.is_ascii_digit()));
    let preview_url = actions
        .iter()
        .filter(|a| a.get("type").and_then(Value::as_str) == Some("uri"))
        .filter_map(|a| text(a, "uri"))
        .find(|uri| {
            is_https(uri)
                && uri
                    .split(['?', '#'])
                    .next()
                    .is_some_and(|p| p.ends_with(".m4a"))
        });

    let mut ids = HashMap::new();
    if let Some(key) = text(track, "key") {
        ids.insert("shazam".to_string(), key);
    }
    if let Some(id) = &apple_id {
        ids.insert("apple".to_string(), id.clone());
    }

    let lead = lead_artist(track, &title, &artist);
    let track = Track {
        artists: split_artists(&artist, lead.as_deref()),
        album: song_metadata(track, "Album"),
        isrc: text(track, "isrc").map(|s| s.to_ascii_uppercase()),
        duration_ms: None,
        release_year: song_metadata(track, "Released").and_then(|s| year(&s)),
        genre: track
            .pointer("/genres/primary")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from),
        artwork_url: ["coverarthq", "coverart"]
            .iter()
            .find_map(|k| track.get("images").and_then(|i| text(i, k)))
            .filter(|u| is_https(u))
            .map(|u| upgrade_artwork(&u)),
        preview_url,
        links: links(track, apple_id.as_deref(), country),
        ids,
        identity_key: String::new(),
        title,
        artist,
    };
    Some(ShazamMatch {
        track,
        offset_ms: first
            .get("offset")
            .and_then(Value::as_f64)
            .filter(|s| s.is_finite())
            .map_or(0, |s| (s * 1000.0).round() as i64),
        timeskew: first.get("timeskew").and_then(Value::as_f64).unwrap_or(0.0) as f32,
        frequency_skew: first
            .get("frequencyskew")
            .and_then(Value::as_f64)
            .unwrap_or(0.0) as f32,
    })
}

/// Non-empty trimmed string (numbers are accepted and formatted).
fn text(value: &Value, key: &str) -> Option<String> {
    match value.get(key)? {
        Value::String(s) => Some(s.trim()).filter(|s| !s.is_empty()).map(String::from),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn is_https(url: &str) -> bool {
    url.starts_with("https://")
}

/// `text` of the `SONG` section's metadata entry titled `title` ("Album", "Released", ...).
fn song_metadata(track: &Value, title: &str) -> Option<String> {
    track
        .get("sections")?
        .as_array()?
        .iter()
        .filter(|s| s.get("type").and_then(Value::as_str) == Some("SONG"))
        .filter_map(|s| s.get("metadata")?.as_array())
        .flatten()
        .find(|m| m.get("title").and_then(Value::as_str) == Some(title))
        .and_then(|m| text(m, "text"))
}

/// First four-digit run in `text`, e.g. "2013" or "Jun 3, 2013" → 2013.
fn year(text: &str) -> Option<u16> {
    let bytes = text.as_bytes();
    bytes.windows(4).enumerate().find_map(|(i, w)| {
        let before = i
            .checked_sub(1)
            .map(|j| bytes[j].is_ascii_digit())
            .unwrap_or(false);
        let after = bytes.get(i + 4).is_some_and(u8::is_ascii_digit);
        if before || after || !w.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(w)
            .ok()?
            .parse::<u16>()
            .ok()
            .filter(|y| (1000..=2999).contains(y))
    })
}

/// The first credited artist, from the `SONG` section's page captions, when the
/// subtitle starts with it at an artist boundary.
fn lead_artist(track: &Value, title: &str, subtitle: &str) -> Option<String> {
    track
        .get("sections")?
        .as_array()?
        .iter()
        .filter(|s| s.get("type").and_then(Value::as_str) == Some("SONG"))
        .filter_map(|s| s.get("metapages")?.as_array())
        .flatten()
        .filter_map(|p| text(p, "caption"))
        .find(|caption| {
            caption != title
                && subtitle.strip_prefix(caption.as_str()).is_some_and(|rest| {
                    rest.is_empty() || ARTIST_SEPARATORS.iter().any(|sep| rest.starts_with(sep))
                })
        })
}

/// Individual artists. The subtitle is only split after a known lead artist,
/// so names like "Earth, Wind & Fire" stay whole.
fn split_artists(subtitle: &str, lead: Option<&str>) -> Vec<String> {
    if subtitle.is_empty() {
        return Vec::new();
    }
    let Some(lead) = lead else {
        return vec![subtitle.to_string()];
    };
    let mut rest = subtitle[lead.len()..].to_string();
    for sep in ARTIST_SEPARATORS {
        rest = rest.replace(sep, "\u{0}");
    }
    let mut artists = vec![lead.to_string()];
    for name in rest.split('\u{0}').map(str::trim).filter(|s| !s.is_empty()) {
        if !artists.iter().any(|a| a == name) {
            artists.push(name.to_string());
        }
    }
    artists
}

/// Apple CDN artwork URLs end in `{w}x{h}{crop}.{ext}`; ask for 1000 px.
pub(crate) fn upgrade_artwork(url: &str) -> String {
    let Some((base, last)) = url.rsplit_once('/') else {
        return url.to_string();
    };
    let host_ok = url
        .strip_prefix("https://")
        .and_then(|r| r.split('/').next())
        .is_some_and(|h| h.ends_with(".mzstatic.com"));
    let Some((size, ext)) = last.split_once('.') else {
        return url.to_string();
    };
    let Some((w, rest)) = size.split_once('x') else {
        return url.to_string();
    };
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let (h, crop) = rest.split_at(digits);
    let valid = host_ok
        && !w.is_empty()
        && w.bytes().all(|b| b.is_ascii_digit())
        && !h.is_empty()
        && crop.len() <= 3
        && crop.bytes().all(|b| b.is_ascii_lowercase())
        && matches!(ext, "jpg" | "jpeg" | "png" | "webp");
    let small = w.parse::<u32>().is_ok_and(|w| w < ARTWORK_SIZE);
    if valid && small {
        format!("{base}/{ARTWORK_SIZE}x{ARTWORK_SIZE}{crop}.{ext}")
    } else {
        url.to_string()
    }
}

fn links(track: &Value, apple_id: Option<&str>, country: &str) -> Vec<ServiceLink> {
    let mut links = Vec::new();
    let shazam_url =
        text(track, "url").or_else(|| track.get("share").and_then(|s| text(s, "href")));
    if let Some(url) = shazam_url.filter(|u| is_https(u)) {
        links.push(ServiceLink {
            service: Service::Shazam,
            url,
            is_search: false,
        });
    }
    if let Some(id) = apple_id {
        let url = format!(
            "https://music.apple.com/{}/song/{id}",
            country.to_ascii_lowercase()
        );
        links.push(ServiceLink {
            service: Service::AppleMusic,
            url,
            is_search: false,
        });
    }
    let providers = track
        .pointer("/hub/providers")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for provider in providers {
        let service = match provider.get("type").and_then(Value::as_str) {
            Some("SPOTIFY") => Service::Spotify,
            Some("YOUTUBEMUSIC") => Service::YouTubeMusic,
            Some("DEEZER") => Service::Deezer,
            _ => continue,
        };
        if links.iter().any(|l: &ServiceLink| l.service == service) {
            continue;
        }
        let actions = provider
            .get("actions")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Some(link) = actions
            .iter()
            .filter_map(|a| text(a, "uri"))
            .find_map(|uri| provider_link(service, &uri))
        {
            links.push(link);
        }
    }
    links
}

/// Convert a hub provider action URI into a web link.
pub(crate) fn provider_link(service: Service, uri: &str) -> Option<ServiceLink> {
    if is_https(uri) {
        let is_search = uri.contains("/search");
        return Some(ServiceLink {
            service,
            url: uri.to_string(),
            is_search,
        });
    }
    let query = match service {
        Service::Spotify => decode_component(uri.strip_prefix("spotify:search:")?),
        Service::Deezer => deezer_query(uri)?,
        _ => return None,
    };
    let query = collapse_whitespace(&query);
    if query.is_empty() {
        return None;
    }
    let encoded = utf8_percent_encode(&query, QUERY);
    let url = match service {
        Service::Spotify => format!("https://open.spotify.com/search/{encoded}"),
        _ => format!("https://www.deezer.com/search/{encoded}"),
    };
    Some(ServiceLink {
        service,
        url,
        is_search: true,
    })
}

/// `deezer-query://www.deezer.com/play?query={track:'T' artist:'A'}` → "T A".
fn deezer_query(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("deezer-query://")?;
    let raw = rest
        .split_once('?')?
        .1
        .split('&')
        .find_map(|kv| kv.strip_prefix("query="))?;
    let query = decode_component(raw);
    let field = |name: &str| {
        let start = query.find(&format!("{name}:'"))? + name.len() + 2;
        let end = query[start..].find('\'')? + start;
        Some(query[start..end].to_string())
    };
    match (field("track"), field("artist")) {
        (Some(track), Some(artist)) => Some(format!("{track} {artist}")),
        (Some(track), None) => Some(track),
        _ => Some(query.trim_matches(['{', '}']).to_string()),
    }
}

/// Percent-decode a query component, treating `+` as a space.
fn decode_component(raw: &str) -> String {
    percent_decode_str(&raw.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const FIXTURE: &str = include_str!("testdata/match_get_lucky.json");

    #[test]
    fn parses_captured_response() {
        let body: Value = serde_json::from_str(FIXTURE).unwrap();
        let m = parse_match(&body, "US").unwrap();
        let t = &m.track;
        assert_eq!(t.title, "Get Lucky");
        assert_eq!(t.artist, "Daft Punk, Pharrell Williams & Nile Rodgers");
        assert_eq!(
            t.artists,
            ["Daft Punk", "Pharrell Williams", "Nile Rodgers"]
        );
        assert_eq!(t.album.as_deref(), Some("Random Access Memories"));
        assert_eq!(t.isrc.as_deref(), Some("USQX91300108"));
        assert_eq!(t.genre.as_deref(), Some("Pop"));
        assert_eq!(t.release_year, Some(2013));
        assert_eq!(t.duration_ms, None);
        assert_eq!(
            t.artwork_url.as_deref(),
            Some(
                "https://is1-ssl.mzstatic.com/image/thumb/Music115/v4/e8/43/5f/e8435ffa-b6b9-b171-40ab-4ff3959ab661/886443919266.jpg/1000x1000cc.jpg"
            )
        );
        assert!(
            t.preview_url
                .as_deref()
                .unwrap()
                .ends_with("mzaf_2937358213989499437.plus.aac.ep.m4a")
        );
        assert_eq!(t.ids.get("shazam").map(String::as_str), Some("105842472"));
        assert_eq!(t.ids.get("apple").map(String::as_str), Some("617154366"));

        let link = |s: Service| t.links.iter().find(|l| l.service == s).unwrap();
        assert_eq!(
            link(Service::Shazam).url,
            "https://www.shazam.com/track/105842472/get-lucky"
        );
        assert!(!link(Service::Shazam).is_search);
        assert_eq!(
            link(Service::AppleMusic).url,
            "https://music.apple.com/us/song/617154366"
        );
        assert!(!link(Service::AppleMusic).is_search);
        assert_eq!(
            link(Service::Spotify).url,
            "https://open.spotify.com/search/Get%20Lucky%20Daft%20Punk"
        );
        assert!(link(Service::Spotify).is_search);
        assert!(
            link(Service::YouTubeMusic)
                .url
                .starts_with("https://music.youtube.com/search?q=Get+Lucky")
        );
        assert!(link(Service::YouTubeMusic).is_search);
        assert_eq!(
            link(Service::Deezer).url,
            "https://www.deezer.com/search/Get%20Lucky%20Daft%20Punk%2C%20Pharrell%20Williams%20Nile%20Rodgers"
        );
        assert!(link(Service::Deezer).is_search);
        assert_eq!(t.links.len(), 5);

        assert_eq!(m.offset_ms, 47_996);
        assert!((m.timeskew - -5.483_627_3e-6).abs() < 1e-9);
        assert_eq!(m.frequency_skew, 0.0);
    }

    #[test]
    fn apple_music_link_uses_storefront() {
        let body: Value = serde_json::from_str(FIXTURE).unwrap();
        let m = parse_match(&body, "DE").unwrap();
        assert!(
            m.track
                .links
                .iter()
                .any(|l| l.url == "https://music.apple.com/de/song/617154366")
        );
    }

    #[test]
    fn no_match_is_none() {
        let body = json!({"matches": [], "timestamp": 1, "timezone": "Europe/Paris", "tagid": "x"});
        assert!(parse_match(&body, "US").is_none());
        assert!(parse_match(&json!({}), "US").is_none());
        assert!(parse_match(&json!([]), "US").is_none());
        // A match without a track, or a track without a title, is not usable.
        assert!(parse_match(&json!({"matches": [{"offset": 1.0}]}), "US").is_none());
        assert!(parse_match(&json!({"matches": [{}], "track": {"title": "  "}}), "US").is_none());
    }

    #[test]
    fn tolerates_minimal_and_mistyped_fields() {
        let body = json!({
            "matches": [{"offset": "soon", "timeskew": null}],
            "track": {
                "title": "Song", "key": 123, "subtitle": ["x"], "isrc": "usabc1234567",
                "images": {"coverart": "http://insecure.example/a.jpg"},
                "genres": "Pop", "sections": {"type": "SONG"},
                "hub": {"actions": "nope", "providers": [{"type": "SPOTIFY", "actions": [{"uri": "spotify:search:"}]}]}
            }
        });
        let m = parse_match(&body, "US").unwrap();
        assert_eq!(m.track.title, "Song");
        assert_eq!(m.track.artist, "");
        assert!(m.track.artists.is_empty());
        assert_eq!(m.track.ids.get("shazam").map(String::as_str), Some("123"));
        assert_eq!(m.track.isrc.as_deref(), Some("USABC1234567"));
        assert_eq!(m.track.artwork_url, None);
        assert_eq!(m.track.genre, None);
        assert!(m.track.links.is_empty());
        assert_eq!((m.offset_ms, m.timeskew, m.frequency_skew), (0, 0.0, 0.0));
    }

    #[test]
    fn album_comes_from_the_album_entry_not_the_first() {
        let body = json!({
            "matches": [{"offset": 1.5}],
            "track": {"title": "T", "sections": [
                {"type": "LYRICS", "metadata": [{"title": "Album", "text": "Wrong"}]},
                {"type": "SONG", "metadata": [{"title": "Label", "text": "Label X"}, {"title": "Album", "text": "Right"},
                                              {"title": "Released", "text": "Jun 3, 1999"}]}
            ]}
        });
        let m = parse_match(&body, "US").unwrap();
        assert_eq!(m.track.album.as_deref(), Some("Right"));
        assert_eq!(m.track.release_year, Some(1999));
        assert_eq!(m.offset_ms, 1500);
    }

    #[test]
    fn artists_split_only_after_a_known_lead() {
        assert_eq!(split_artists("Adele", Some("Adele")), ["Adele"]);
        assert_eq!(
            split_artists("Earth, Wind & Fire", None),
            ["Earth, Wind & Fire"]
        );
        assert_eq!(
            split_artists("Earth, Wind & Fire", Some("Earth, Wind & Fire")),
            ["Earth, Wind & Fire"]
        );
        assert_eq!(
            split_artists(
                "Tyler, The Creator & Kali Uchis",
                Some("Tyler, The Creator")
            ),
            ["Tyler, The Creator", "Kali Uchis"]
        );
        assert_eq!(
            split_artists("Calvin Harris feat. Rihanna", Some("Calvin Harris")),
            ["Calvin Harris", "Rihanna"]
        );
        assert!(split_artists("", Some("x")).is_empty());

        let track = json!({"sections": [{"type": "SONG", "metapages": [{"caption": "Daft"}, {"caption": "Song"}]}]});
        assert_eq!(
            lead_artist(&track, "Song", "Daft Punk"),
            None,
            "prefix must end at an artist boundary"
        );
    }

    #[test]
    fn artwork_upgrade_only_for_apple_cdn_size_tokens() {
        assert_eq!(
            upgrade_artwork("https://is1-ssl.mzstatic.com/image/thumb/Music/x.jpg/400x400cc.jpg"),
            "https://is1-ssl.mzstatic.com/image/thumb/Music/x.jpg/1000x1000cc.jpg"
        );
        assert_eq!(
            upgrade_artwork("https://is2-ssl.mzstatic.com/image/thumb/a/b.png/100x100bb.webp"),
            "https://is2-ssl.mzstatic.com/image/thumb/a/b.png/1000x1000bb.webp"
        );
        for unchanged in [
            "https://is1-ssl.mzstatic.com/image/thumb/a/3000x3000cc.jpg",
            "https://example.com/image/400x400cc.jpg",
            "https://is1-ssl.mzstatic.com/image/thumb/a/cover.jpg",
            "https://is1-ssl.mzstatic.com/image/thumb/a/400x400-999.jpg",
            "https://is1-ssl.mzstatic.com/image/thumb/a/{w}x{h}bb.jpg",
            "not a url",
        ] {
            assert_eq!(upgrade_artwork(unchanged), unchanged);
        }
    }

    #[test]
    fn provider_uris_become_web_links() {
        let l = provider_link(Service::Spotify, "spotify:search:Hello%20Adele").unwrap();
        assert_eq!(
            (l.url.as_str(), l.is_search),
            ("https://open.spotify.com/search/Hello%20Adele", true)
        );
        let l = provider_link(Service::Deezer, "deezer-query://www.deezer.com/play?query=%7Btrack%3A%27Hello%27%20artist%3A%27Adele%27%7D").unwrap();
        assert_eq!(
            (l.url.as_str(), l.is_search),
            ("https://www.deezer.com/search/Hello%20Adele", true)
        );
        let l = provider_link(
            Service::Spotify,
            "https://open.spotify.com/track/4aebBr4JAihzJQR0CiIZJv",
        )
        .unwrap();
        assert!(!l.is_search);
        assert!(
            provider_link(
                Service::YouTubeMusic,
                "intent://music.youtube.com#Intent;end"
            )
            .is_none()
        );
        assert!(provider_link(Service::Deezer, "deezer-query://www.deezer.com/play").is_none());
        assert!(provider_link(Service::Spotify, "spotify:search:%20").is_none());
    }

    #[test]
    fn years() {
        assert_eq!(year("2013"), Some(2013));
        assert_eq!(year("Released 1987-05-01"), Some(1987));
        assert_eq!(year("12345"), None);
        assert_eq!(year("n/a"), None);
    }
}
