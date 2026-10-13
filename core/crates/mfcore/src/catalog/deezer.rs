//! Deezer public API (keyless): tracks, albums, playlists, ISRC lookup, search.
//!
//! Requests are spaced to stay under Deezer's limit (about 50 per 5 s per IP;
//! we allow 40) and responses are cached for 10 minutes, shorter than the
//! lifetime of the signed preview URLs they contain.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;

use super::cache::TtlCache;
use crate::error::{MfError, Result};
use crate::links::fetch;
use crate::model::{Service, SourceInfo, Track};
use crate::util::cancel::CancelToken;
use crate::util::ratelimit::RateLimiter;

const API: &str = "https://api.deezer.com";
const SITE: &str = "Deezer";
/// Most tracks read from one playlist.
pub(crate) const MAX_PLAYLIST_TRACKS: usize = 500;

static LIMITER: LazyLock<RateLimiter> =
    LazyLock::new(|| RateLimiter::new(Duration::from_millis(125), Duration::from_millis(25)));
static CACHE: LazyLock<TtlCache<Value>> =
    LazyLock::new(|| TtlCache::new(256, Duration::from_secs(600)));
/// Set once `artist:"…" track:"…"` searches return nothing where a plain search
/// finds the track (Deezer's advanced search has been broken at times).
static ADVANCED_SEARCH_BROKEN: AtomicBool = AtomicBool::new(false);

/// GET `path` (with query) from the API: rate limited, cached, Deezer's
/// in-body errors mapped.
pub(crate) async fn get(http: &reqwest::Client, path: &str, cancel: &CancelToken) -> Result<Value> {
    let url = format!("{API}{path}");
    if let Some(hit) = CACHE.get(&url) {
        return Ok(hit);
    }
    LIMITER.acquire(cancel, Duration::from_secs(15)).await?;
    let value = match fetch::get_json(http, &url, SITE, cancel).await {
        Err(e @ MfError::RateLimited { .. }) => {
            LIMITER
                .throttled(Duration::from_secs(5), Duration::from_secs(60), None)
                .await;
            return Err(e);
        }
        other => other?,
    };
    if let Some(error) = value.get("error") {
        let code = error.get("code").and_then(Value::as_u64).unwrap_or(0);
        return Err(match code {
            4 => {
                let wait = LIMITER
                    .throttled(Duration::from_secs(5), Duration::from_secs(60), None)
                    .await;
                MfError::RateLimited {
                    retry_in_ms: wait.as_millis() as u64,
                }
            }
            800 => MfError::MediaUnavailable {
                message: "This Deezer item doesn't exist or isn't available.".into(),
            },
            _ => MfError::network("Deezer couldn't answer the request"),
        });
    }
    LIMITER.success().await;
    CACHE.put(url, value.clone());
    Ok(value)
}

fn encode(text: &str) -> String {
    url::form_urlencoded::byte_serialize(text.as_bytes()).collect()
}

fn artwork(item: &Value, album: Option<&Value>) -> Option<String> {
    let from = |v: &Value| {
        v.get("cover_xl")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    item.get("album")
        .and_then(from)
        .or_else(|| album.and_then(from))
        .or_else(|| {
            item.get("md5_image")
                .and_then(Value::as_str)
                .filter(|md5| !md5.is_empty() && md5.chars().all(|c| c.is_ascii_hexdigit()))
                .map(|md5| format!("https://cdn-images.dzcdn.net/images/cover/{md5}/1000x1000-000000-80-0-0.jpg"))
        })
}

/// Maps a Deezer track object. `album` supplies album fields for album track lists.
pub(crate) fn track_from_json(item: &Value, album: Option<&Value>) -> Option<Track> {
    let id = item.get("id").and_then(Value::as_u64)?;
    let title = item
        .get("title")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())?;
    let mut artists: Vec<String> = item
        .get("contributors")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|c| c.get("name").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if artists.is_empty()
        && let Some(name) = item.pointer("/artist/name").and_then(Value::as_str)
    {
        artists.push(name.to_string());
    }
    let album_title = item
        .pointer("/album/title")
        .or_else(|| album.and_then(|a| a.get("title")))
        .and_then(Value::as_str);
    let release_date = item
        .get("release_date")
        .or_else(|| item.pointer("/album/release_date"))
        .or_else(|| album.and_then(|a| a.get("release_date")))
        .and_then(Value::as_str);
    let mut track = Track {
        title: title.to_string(),
        artists,
        album: album_title.map(str::to_string),
        isrc: item
            .get("isrc")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        duration_ms: item
            .get("duration")
            .and_then(Value::as_u64)
            .filter(|d| *d > 0)
            .map(|d| d * 1000),
        release_year: release_date.and_then(crate::links::year_of),
        artwork_url: artwork(item, album),
        preview_url: item
            .get("preview")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        links: vec![crate::links::service_link(
            Service::Deezer,
            format!("https://www.deezer.com/track/{id}"),
        )],
        ..Track::default()
    };
    track.ids.insert("deezer".into(), id.to_string());
    Some(crate::links::finish_track(track))
}

/// `/track/{id}`.
pub(crate) async fn track(http: &reqwest::Client, id: &str, cancel: &CancelToken) -> Result<Track> {
    let value = get(http, &format!("/track/{}", encode(id)), cancel).await?;
    track_from_json(&value, None).ok_or_else(|| MfError::network("Deezer sent an unreadable track"))
}

/// `/track/isrc:{ISRC}`; None when Deezer does not know the ISRC.
pub(crate) async fn track_by_isrc(
    http: &reqwest::Client,
    isrc: &str,
    cancel: &CancelToken,
) -> Result<Option<Track>> {
    match get(http, &format!("/track/isrc:{}", encode(isrc)), cancel).await {
        Ok(value) => Ok(track_from_json(&value, None)),
        Err(MfError::MediaUnavailable { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

/// `/search?q=`; the advanced `artist:"…" track:"…"` form first, then a plain query.
pub(crate) async fn search(
    http: &reqwest::Client,
    title: &str,
    artist: &str,
    cancel: &CancelToken,
) -> Result<Vec<Track>> {
    let quoted = |s: &str| s.replace('"', " ");
    let plain = format!("{artist} {title}").trim().to_string();
    if !artist.trim().is_empty() && !ADVANCED_SEARCH_BROKEN.load(Ordering::Relaxed) {
        let advanced = format!("artist:\"{}\" track:\"{}\"", quoted(artist), quoted(title));
        let found = search_query(http, &advanced, cancel).await?;
        if !found.is_empty() {
            return Ok(found);
        }
        let found = search_query(http, &plain, cancel).await?;
        if !found.is_empty() {
            ADVANCED_SEARCH_BROKEN.store(true, Ordering::Relaxed);
        }
        return Ok(found);
    }
    search_query(http, &plain, cancel).await
}

/// Plain free-text `/search?q=artist title`.
pub(crate) async fn search_plain(
    http: &reqwest::Client,
    title: &str,
    artist: &str,
    cancel: &CancelToken,
) -> Result<Vec<Track>> {
    search_query(http, format!("{artist} {title}").trim(), cancel).await
}

async fn search_query(
    http: &reqwest::Client,
    query: &str,
    cancel: &CancelToken,
) -> Result<Vec<Track>> {
    let value = get(
        http,
        &format!("/search?q={}&limit=15", encode(query)),
        cancel,
    )
    .await?;
    Ok(value
        .get("data")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| track_from_json(item, None))
                .collect()
        })
        .unwrap_or_default())
}

fn collection_source(value: &Value, kind: &str, id: &str) -> SourceInfo {
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .map(str::to_string);
    let uploader = value
        .pointer("/artist/name")
        .or_else(|| value.pointer("/creator/name"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let thumbnail = ["cover_xl", "picture_xl"]
        .iter()
        .find_map(|k| value.get(*k).and_then(Value::as_str))
        .map(str::to_string);
    SourceInfo {
        url: Some(format!("https://www.deezer.com/{kind}/{id}")),
        title,
        uploader,
        thumbnail_url: thumbnail,
        duration_ms: value
            .get("duration")
            .and_then(Value::as_u64)
            .filter(|d| *d > 0)
            .map(|d| d * 1000),
        service: Some(Service::Deezer),
        site: Some("Deezer".into()),
        item_count: 0,
        hints: Vec::new(),
    }
}

/// `/album/{id}`: album info and its tracks.
pub(crate) async fn album(
    http: &reqwest::Client,
    id: &str,
    cancel: &CancelToken,
) -> Result<(SourceInfo, Vec<Track>)> {
    let value = get(http, &format!("/album/{}", encode(id)), cancel).await?;
    let source = collection_source(&value, "album", id);
    let tracks: Vec<Track> = value
        .pointer("/tracks/data")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| track_from_json(item, Some(&value)))
                .collect()
        })
        .unwrap_or_default();
    Ok((source, tracks))
}

/// `/playlist/{id}` plus `/playlist/{id}/tracks` pages, up to `MAX_PLAYLIST_TRACKS`.
pub(crate) async fn playlist(
    http: &reqwest::Client,
    id: &str,
    cancel: &CancelToken,
) -> Result<(SourceInfo, Vec<Track>)> {
    let value = get(http, &format!("/playlist/{}", encode(id)), cancel).await?;
    let source = collection_source(&value, "playlist", id);
    let mut tracks: Vec<Track> = value
        .pointer("/tracks/data")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| track_from_json(item, None))
                .collect()
        })
        .unwrap_or_default();
    let total = value.get("nb_tracks").and_then(Value::as_u64).unwrap_or(0) as usize;
    let mut index = value
        .pointer("/tracks/data")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    while index < total.min(MAX_PLAYLIST_TRACKS) {
        let page = get(
            http,
            &format!("/playlist/{}/tracks?index={index}&limit=100", encode(id)),
            cancel,
        )
        .await?;
        let Some(items) = page
            .get("data")
            .and_then(Value::as_array)
            .filter(|items| !items.is_empty())
        else {
            break;
        };
        index += items.len();
        tracks.extend(items.iter().filter_map(|item| track_from_json(item, None)));
    }
    tracks.truncate(MAX_PLAYLIST_TRACKS);
    Ok((source, tracks))
}

/// Finds an album by title and artist (for services whose album pages list no tracks).
pub(crate) async fn find_album(
    http: &reqwest::Client,
    title: &str,
    artist: &str,
    cancel: &CancelToken,
) -> Result<Option<String>> {
    let query = format!("{artist} {title}");
    let value = get(
        http,
        &format!("/search/album?q={}&limit=10", encode(query.trim())),
        cancel,
    )
    .await?;
    let want_title = crate::identity::normalize_title(title);
    let want_artist = crate::identity::normalize_artists(artist);
    let found = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().find(|item| {
                let t = item
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let a = item
                    .pointer("/artist/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                crate::identity::normalize_title(t) == want_title
                    && crate::identity::normalize_artists(a) == want_artist
            })
        });
    Ok(found
        .and_then(|item| item.get("id"))
        .and_then(Value::as_u64)
        .map(|id| id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Value {
        let path = format!("{}/tests/fixtures/links/{name}", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json")
    }

    #[test]
    fn maps_track() {
        let t = track_from_json(&fixture("deezer_track.json"), None).expect("track");
        assert_eq!(t.title, "Harder, Better, Faster, Stronger");
        assert_eq!(t.artist, "Daft Punk");
        assert_eq!(t.album.as_deref(), Some("Discovery"));
        assert_eq!(t.isrc.as_deref(), Some("GBDUW0000059"));
        assert_eq!(t.duration_ms, Some(226_000));
        assert_eq!(t.release_year, Some(2001));
        assert!(
            t.artwork_url
                .as_deref()
                .is_some_and(|u| u.contains("1000x1000"))
        );
        assert!(t.preview_url.is_some());
        assert_eq!(t.ids.get("deezer").map(String::as_str), Some("3135556"));
        assert_eq!(t.links[0].url, "https://www.deezer.com/track/3135556");
        assert_eq!(t.identity_key, "isrc::GBDUW0000059");
    }

    #[test]
    fn maps_album_tracks_with_album_context() {
        let album = fixture("deezer_album.json");
        let source = collection_source(&album, "album", "302127");
        assert_eq!(source.title.as_deref(), Some("Discovery"));
        assert_eq!(source.uploader.as_deref(), Some("Daft Punk"));
        let items = album
            .pointer("/tracks/data")
            .and_then(Value::as_array)
            .unwrap();
        let tracks: Vec<Track> = items
            .iter()
            .filter_map(|i| track_from_json(i, Some(&album)))
            .collect();
        assert_eq!(tracks.len(), items.len());
        assert_eq!(tracks[0].title, "One More Time");
        assert_eq!(tracks[0].album.as_deref(), Some("Discovery"));
        assert_eq!(tracks[0].release_year, Some(2001));
        assert!(tracks[0].artwork_url.is_some());
    }

    #[test]
    fn maps_playlist_tracks_with_isrc() {
        let playlist = fixture("deezer_playlist.json");
        let items = playlist
            .pointer("/tracks/data")
            .and_then(Value::as_array)
            .unwrap();
        let first = track_from_json(&items[0], None).unwrap();
        assert!(first.isrc.is_some());
        assert!(first.album.is_some());
        let source = collection_source(&playlist, "playlist", "3155776842");
        assert_eq!(
            source.url.as_deref(),
            Some("https://www.deezer.com/playlist/3155776842")
        );
    }

    #[test]
    fn rejects_malformed_items() {
        assert!(track_from_json(&serde_json::json!({"title": "x"}), None).is_none());
        assert!(track_from_json(&serde_json::json!({"id": 1, "title": " "}), None).is_none());
        assert!(track_from_json(&serde_json::json!({"id": "1", "title": "x"}), None).is_none());
        let minimal = track_from_json(
            &serde_json::json!({"id": 1, "title": "x", "duration": 0, "md5_image": "../x"}),
            None,
        )
        .unwrap();
        assert_eq!(minimal.duration_ms, None);
        assert_eq!(minimal.artwork_url, None);
    }
}
