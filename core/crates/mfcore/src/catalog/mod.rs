//! Cross-matching and enrichment: fill ISRC, artwork, previews, duration and
//! service links using keyless public APIs (Deezer, iTunes).
//!
//! Deezer is the primary source (ISRC lookup, generous rate limit); iTunes adds
//! the Apple Music link, genre and a second artwork/preview source but allows only
//! about 20 calls a minute, so enrichment skips it when its queue is long.
//! Nothing here fails: errors leave the track as it was.

mod cache;
pub(crate) mod deezer;
pub(crate) mod itunes;
mod matching;

use std::sync::LazyLock;
use std::time::Duration;

use url::Url;

use self::cache::TtlCache;
use self::matching::Query;
use crate::error::Result;
use crate::identity::{self, normalize_isrc};
use crate::model::{Service, ServiceLink, Track};
use crate::util::cancel::CancelToken;

/// iTunes calls `enrich` may queue behind before skipping Apple Music.
const ENRICH_ITUNES_QUEUE: usize = 2;
/// iTunes calls `find` may queue behind (it has no other fallback).
const FIND_ITUNES_QUEUE: usize = 4;

/// Services that get a link on every enriched track, in display order.
pub const LINK_SERVICES: [Service; 8] = [
    Service::Spotify,
    Service::AppleMusic,
    Service::YouTubeMusic,
    Service::YouTube,
    Service::Deezer,
    Service::Tidal,
    Service::AmazonMusic,
    Service::SoundCloud,
];

// Match results per query. Deezer entries expire before its signed preview URLs do.
static DEEZER_MATCHES: LazyLock<TtlCache<Option<Track>>> =
    LazyLock::new(|| TtlCache::new(512, Duration::from_secs(600)));
static APPLE_MATCHES: LazyLock<TtlCache<Option<Track>>> =
    LazyLock::new(|| TtlCache::new(512, Duration::from_secs(6 * 3600)));

/// Best-effort enrichment; never fails, returns the input on errors.
///
/// Fills ISRC, duration, album, release year, genre, artwork and preview where
/// missing, adds direct Deezer and Apple Music links when a confident match is
/// found and search links for every other service.
pub async fn enrich(http: &reqwest::Client, track: Track, country: &str, cancel: &CancelToken) -> Track {
    let mut track = crate::links::finish_track(track);
    if track.title.is_empty() {
        return finish(track);
    }
    if let Some(found) = deezer_match(http, &track, cancel).await {
        merge(&mut track, &found);
    }
    if !cancel.is_canceled()
        && let Some(found) = apple_match(http, &track, country, ENRICH_ITUNES_QUEUE, cancel).await
    {
        merge(&mut track, &found);
    }
    finish(track)
}

/// Enrichment for many tracks at once (playlists and albums of streaming
/// services): Deezer only, so a whole collection finishes in seconds. iTunes
/// is asked only for tracks Deezer does not know, and only while nothing else
/// waits for it. Tracks that already carry Deezer's metadata are only finished.
pub async fn enrich_quick(http: &reqwest::Client, track: Track, country: &str, cancel: &CancelToken) -> Track {
    let mut track = crate::links::finish_track(track);
    if track.title.is_empty() || has_deezer_metadata(&track) {
        return finish(track);
    }
    match deezer_match(http, &track, cancel).await {
        Some(found) => merge(&mut track, &found),
        None if !cancel.is_canceled() => {
            if let Some(found) = apple_match(http, &track, country, 1, cancel).await {
                merge(&mut track, &found);
            }
        }
        None => {}
    }
    finish(track)
}

/// A track read from Deezer itself (id, ISRC, artwork and duration present).
fn has_deezer_metadata(track: &Track) -> bool {
    track.ids.contains_key("deezer")
        && track.isrc.is_some()
        && track.artwork_url.is_some()
        && track.duration_ms.is_some()
}

/// The track as it is, with search links for every service and a refreshed
/// identity: what a collection shows before enrichment arrives.
pub fn with_search_links(track: Track) -> Track {
    finish(crate::links::finish_track(track))
}

/// Best catalog match for a title/artist (and optional duration), if confident.
///
/// Returns the catalog's own metadata (Deezer first, then iTunes) with its
/// direct link and search links for the other services; pass it to `enrich`
/// for the second catalog.
pub async fn find(
    http: &reqwest::Client,
    title: &str,
    artist: &str,
    duration_ms: Option<u64>,
    country: &str,
    cancel: &CancelToken,
) -> Option<Track> {
    let probe = crate::links::finish_track(Track {
        title: title.into(),
        artist: artist.into(),
        duration_ms,
        ..Track::default()
    });
    if probe.title.is_empty() {
        return None;
    }
    if let Some(found) = deezer_match(http, &probe, cancel).await {
        return Some(finish(found));
    }
    if cancel.is_canceled() {
        return None;
    }
    apple_match(http, &probe, country, FIND_ITUNES_QUEUE, cancel).await.map(finish)
}

/// Search links for every major service (`LINK_SERVICES`), in display order.
pub fn search_links(track: &Track) -> Vec<ServiceLink> {
    let artist = primary_artist(track);
    let query = format!("{artist} {}", track.title.trim()).trim().to_string();
    if track.title.trim().is_empty() {
        return Vec::new();
    }
    LINK_SERVICES
        .iter()
        .filter_map(|&service| {
            let url = match service {
                Service::Spotify => path_search("https://open.spotify.com/search", &query),
                Service::AppleMusic => query_search("https://music.apple.com/search", "term", &query),
                Service::YouTubeMusic => query_search("https://music.youtube.com/search", "q", &query),
                Service::YouTube => query_search("https://www.youtube.com/results", "search_query", &query),
                Service::Deezer => path_search("https://www.deezer.com/search", &query),
                Service::Tidal => query_search("https://tidal.com/search", "q", &query),
                Service::AmazonMusic => path_search("https://music.amazon.com/search", &query),
                Service::SoundCloud => query_search("https://soundcloud.com/search", "q", &query),
                _ => None,
            }?;
            Some(ServiceLink { service, url, is_search: true })
        })
        .collect()
}

fn path_search(base: &str, query: &str) -> Option<String> {
    let mut url = Url::parse(base).ok()?;
    url.path_segments_mut().ok()?.push(query);
    Some(url.to_string())
}

fn query_search(base: &str, key: &str, query: &str) -> Option<String> {
    Url::parse_with_params(base, &[(key, query)]).ok().map(|u| u.to_string())
}

/// First credited artist, used for searching.
fn primary_artist(track: &Track) -> String {
    track.artists.first().cloned().unwrap_or_else(|| track.artist.clone()).trim().to_string()
}

fn query_key(track: &Track) -> String {
    let seconds = track.duration_ms.map(|d| (d / 1000).to_string()).unwrap_or_default();
    format!("{}|{seconds}", identity::fuzzy_key(&track.title, &primary_artist(track)))
}

async fn deezer_match(http: &reqwest::Client, track: &Track, cancel: &CancelToken) -> Option<Track> {
    let key = match (track.ids.get("deezer"), track.isrc.as_deref().and_then(normalize_isrc)) {
        (Some(id), _) => format!("id:{id}"),
        (None, Some(isrc)) => format!("isrc:{isrc}|{}", query_key(track)),
        (None, None) => format!("q:{}", query_key(track)),
    };
    if let Some(hit) = DEEZER_MATCHES.get(&key) {
        return hit;
    }
    match deezer_lookup(http, track, cancel).await {
        Ok(found) => {
            DEEZER_MATCHES.put(key, found.clone());
            found
        }
        Err(e) => {
            tracing::debug!("deezer enrichment skipped: {e}");
            None
        }
    }
}

async fn deezer_lookup(http: &reqwest::Client, track: &Track, cancel: &CancelToken) -> Result<Option<Track>> {
    if let Some(id) = track.ids.get("deezer").filter(|id| !id.trim().is_empty()) {
        return deezer::track(http, id.trim(), cancel).await.map(Some);
    }
    if let Some(isrc) = track.isrc.as_deref().and_then(normalize_isrc)
        && let Some(found) = deezer::track_by_isrc(http, &isrc, cancel).await?
        && plausible_title(track, &found)
    {
        return Ok(Some(found));
    }
    let query = Query::of(track);
    let artist = primary_artist(track);
    let candidates = deezer::search(http, &track.title, &artist, cancel).await?;
    if let Some(best) = matching::best(&query, &candidates) {
        return Ok(Some(best.clone()));
    }
    let plain = deezer::search_plain(http, &track.title, &artist, cancel).await?;
    Ok(matching::best(&query, &plain).cloned())
}

/// ISRC data is sometimes wrong; reject lookups whose title has nothing in common.
fn plausible_title(track: &Track, found: &Track) -> bool {
    let a = identity::normalize_title(&track.title);
    let b = identity::normalize_title(&found.title);
    a.is_empty() || a.contains(&b) || b.contains(&a) || strsim::normalized_levenshtein(&a, &b) >= 0.5
}

async fn apple_match(
    http: &reqwest::Client,
    track: &Track,
    country: &str,
    max_queue: usize,
    cancel: &CancelToken,
) -> Option<Track> {
    let cc = crate::links::storefront(country);
    let key = match track.ids.get("apple") {
        Some(id) => format!("{cc}:id:{id}"),
        None => format!("{cc}:q:{}", query_key(track)),
    };
    if let Some(hit) = APPLE_MATCHES.get(&key) {
        return hit;
    }
    match apple_lookup(http, track, &cc, max_queue, cancel).await {
        Ok(found) => {
            APPLE_MATCHES.put(key, found.clone());
            found
        }
        Err(e) => {
            tracing::debug!("apple music enrichment skipped: {e}");
            None
        }
    }
}

async fn apple_lookup(
    http: &reqwest::Client,
    track: &Track,
    country: &str,
    max_queue: usize,
    cancel: &CancelToken,
) -> Result<Option<Track>> {
    if let Some(id) = track.ids.get("apple").map(|id| id.trim()).filter(|id| !id.is_empty()) {
        let items = itunes::lookup(http, id, country, false, max_queue, cancel).await?;
        return Ok(items.iter().find_map(itunes::track_from_json));
    }
    let term = format!("{} {}", primary_artist(track), track.title);
    let candidates = itunes::search(http, &term, country, max_queue, cancel).await?;
    Ok(matching::best(&Query::of(track), &candidates).cloned())
}

fn fill<T: Clone>(slot: &mut Option<T>, value: &Option<T>) {
    if slot.is_none() {
        slot.clone_from(value);
    }
}

/// Adds what `found` knows and `target` lacks; never overwrites existing values.
fn merge(target: &mut Track, found: &Track) {
    if target.title.trim().is_empty() {
        target.title.clone_from(&found.title);
    }
    if target.artist.trim().is_empty() {
        target.artist.clone_from(&found.artist);
    }
    if target.artists.is_empty() {
        target.artists.clone_from(&found.artists);
    }
    fill(&mut target.album, &found.album);
    fill(&mut target.isrc, &found.isrc);
    fill(&mut target.duration_ms, &found.duration_ms);
    fill(&mut target.release_year, &found.release_year);
    fill(&mut target.genre, &found.genre);
    fill(&mut target.artwork_url, &found.artwork_url);
    fill(&mut target.preview_url, &found.preview_url);
    for (key, value) in &found.ids {
        target.ids.entry(key.clone()).or_insert_with(|| value.clone());
    }
    for link in &found.links {
        add_link(&mut target.links, link.clone());
    }
}

/// Adds `link` unless a direct link for its service exists; a direct link
/// replaces a search link.
fn add_link(links: &mut Vec<ServiceLink>, link: ServiceLink) {
    match links.iter_mut().find(|l| l.service == link.service) {
        Some(existing) if existing.is_search && !link.is_search => *existing = link,
        Some(_) => {}
        None => links.push(link),
    }
}

fn service_rank(service: Service) -> usize {
    LINK_SERVICES.iter().position(|s| *s == service).unwrap_or(LINK_SERVICES.len())
}

fn finish(mut track: Track) -> Track {
    for link in search_links(&track) {
        add_link(&mut track.links, link);
    }
    track.links.sort_by_key(|l| service_rank(l.service));
    identity::refresh_identity(&mut track);
    track
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(title: &str, artist: &str) -> Track {
        crate::links::finish_track(Track { title: title.into(), artist: artist.into(), ..Track::default() })
    }

    #[test]
    fn search_links_cover_every_service() {
        let links = search_links(&track("Harder, Better, Faster, Stronger", "Daft Punk"));
        assert_eq!(links.len(), LINK_SERVICES.len());
        assert!(links.iter().all(|l| l.is_search));
        let url = |s: Service| links.iter().find(|l| l.service == s).map(|l| l.url.clone()).unwrap();
        assert_eq!(
            url(Service::Spotify),
            "https://open.spotify.com/search/Daft%20Punk%20Harder,%20Better,%20Faster,%20Stronger"
        );
        assert_eq!(
            url(Service::YouTubeMusic),
            "https://music.youtube.com/search?q=Daft+Punk+Harder%2C+Better%2C+Faster%2C+Stronger"
        );
        assert_eq!(
            url(Service::YouTube),
            "https://www.youtube.com/results?search_query=Daft+Punk+Harder%2C+Better%2C+Faster%2C+Stronger"
        );
        assert_eq!(
            url(Service::AppleMusic),
            "https://music.apple.com/search?term=Daft+Punk+Harder%2C+Better%2C+Faster%2C+Stronger"
        );
        assert!(url(Service::Deezer).starts_with("https://www.deezer.com/search/Daft%20Punk"));
        assert!(url(Service::AmazonMusic).starts_with("https://music.amazon.com/search/Daft%20Punk"));
        assert!(url(Service::Tidal).starts_with("https://tidal.com/search?q=Daft+Punk"));
        assert!(url(Service::SoundCloud).starts_with("https://soundcloud.com/search?q=Daft+Punk"));
        assert!(search_links(&track("", "Daft Punk")).is_empty());
        assert!(search_links(&track("Song/With?Odd#Chars", "")).iter().all(|l| Url::parse(&l.url).is_ok()));
    }

    #[test]
    fn merge_fills_without_overwriting() {
        let mut target = track("Song", "Artist");
        target.album = Some("Mine".into());
        target.links.push(ServiceLink {
            service: Service::Deezer,
            url: "https://www.deezer.com/search/x".into(),
            is_search: true,
        });
        target.links.push(ServiceLink {
            service: Service::Spotify,
            url: "https://open.spotify.com/track/a".into(),
            is_search: false,
        });
        let mut found = track("Song (Remastered)", "Artist");
        found.album = Some("Theirs".into());
        found.isrc = Some("GBX".into());
        found.duration_ms = Some(1000);
        found.ids.insert("deezer".into(), "1".into());
        found.links = vec![
            ServiceLink { service: Service::Deezer, url: "https://www.deezer.com/track/1".into(), is_search: false },
            ServiceLink { service: Service::Spotify, url: "https://open.spotify.com/search/x".into(), is_search: true },
        ];
        merge(&mut target, &found);
        assert_eq!(target.title, "Song");
        assert_eq!(target.album.as_deref(), Some("Mine"));
        assert_eq!(target.isrc.as_deref(), Some("GBX"));
        assert_eq!(target.duration_ms, Some(1000));
        assert_eq!(target.ids.get("deezer").map(String::as_str), Some("1"));
        let deezer = target.links.iter().find(|l| l.service == Service::Deezer).unwrap();
        assert!(!deezer.is_search);
        let spotify = target.links.iter().find(|l| l.service == Service::Spotify).unwrap();
        assert_eq!(spotify.url, "https://open.spotify.com/track/a");
    }

    #[test]
    fn finish_orders_links_and_refreshes_identity() {
        let mut t = track("Song", "Artist");
        t.isrc = Some("usx1".into());
        t.links.push(ServiceLink { service: Service::Web, url: "https://example.com".into(), is_search: false });
        let t = finish(t);
        assert_eq!(t.links.len(), LINK_SERVICES.len() + 1);
        assert_eq!(t.links[0].service, Service::Spotify);
        assert_eq!(t.links.last().map(|l| l.service), Some(Service::Web));
        assert_eq!(t.identity_key, "isrc::USX1");
    }

    #[test]
    fn plausible_isrc_titles() {
        assert!(plausible_title(&track("Billie Jean", "MJ"), &track("Billie Jean (Single Version)", "MJ")));
        assert!(!plausible_title(&track("Billie Jean", "MJ"), &track("Thriller", "MJ")));
    }

    #[tokio::test]
    async fn never_fails_and_respects_cancellation() {
        let http = crate::links::fetch::test_client();
        let cancel = CancelToken::new();
        cancel.cancel();
        let input = track("Some Song", "Some Artist");
        let out = enrich(&http, input.clone(), "us", &cancel).await;
        assert_eq!(out.title, "Some Song");
        assert_eq!(out.links.len(), LINK_SERVICES.len());
        assert!(find(&http, "Some Song", "Some Artist", None, "us", &cancel).await.is_none());
        let empty = enrich(&http, Track::default(), "us", &cancel).await;
        assert!(empty.links.is_empty());
    }
}

/// See `crate::custom_provider_link`.
pub fn custom_link(track: &Track, template: &str) -> Option<String> {
    fn enc(value: &str) -> String {
        percent_encoding::utf8_percent_encode(value.trim(), percent_encoding::NON_ALPHANUMERIC).to_string()
    }
    let artist = track.artists.first().cloned().unwrap_or_else(|| track.artist.clone());
    let query = format!("{} {}", artist.trim(), track.title.trim()).trim().to_string();
    let url = template
        .trim()
        .replace("{artist}", &enc(&artist))
        .replace("{title}", &enc(&track.title))
        .replace("{query}", &enc(&query))
        .replace("{isrc}", &enc(track.isrc.as_deref().unwrap_or("")));
    let parsed = url::Url::parse(&url).ok()?;
    matches!(parsed.scheme(), "http" | "https").then_some(url)
}

#[cfg(test)]
mod custom_link_tests {
    use super::*;

    #[test]
    fn fills_placeholders_and_rejects_other_schemes() {
        let track = Track { title: "Take On Me".into(), artist: "a-ha".into(), ..Default::default() };
        assert_eq!(
            custom_link(&track, "https://example.com/search?q={query}").as_deref(),
            Some("https://example.com/search?q=a%2Dha%20Take%20On%20Me")
        );
        assert_eq!(custom_link(&track, "javascript:alert({title})"), None);
        assert_eq!(custom_link(&track, "not a url {title}"), None);
    }
}
