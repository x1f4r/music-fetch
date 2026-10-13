//! iTunes Search API (keyless): lookup by id and song search.
//!
//! Apple allows roughly 20 calls per minute, so calls are spaced about 3 s apart
//! and callers that only want extras skip iTunes when the queue is long.
//! Results are cached for six hours.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::Value;

use super::cache::TtlCache;
use crate::error::{MfError, Result};
use crate::links::fetch;
use crate::model::{Service, Track};
use crate::util::cancel::CancelToken;
use crate::util::ratelimit::RateLimiter;

const SITE: &str = "Apple Music";

static LIMITER: LazyLock<RateLimiter> =
    LazyLock::new(|| RateLimiter::new(Duration::from_millis(3000), Duration::from_millis(400)));
static CACHE: LazyLock<TtlCache<Value>> =
    LazyLock::new(|| TtlCache::new(256, Duration::from_secs(6 * 3600)));
static WAITING: AtomicUsize = AtomicUsize::new(0);

/// Decrements the queue counter when a request finishes or is dropped.
struct QueueSlot;

impl Drop for QueueSlot {
    fn drop(&mut self) {
        WAITING.fetch_sub(1, Ordering::SeqCst);
    }
}

/// GET `url`: cached, rate limited. Fails fast with `RateLimited` when more than
/// `max_queue` calls are already waiting.
async fn get(
    http: &reqwest::Client,
    url: &str,
    max_queue: usize,
    cancel: &CancelToken,
) -> Result<Value> {
    if let Some(hit) = CACHE.get(url) {
        return Ok(hit);
    }
    if WAITING.fetch_add(1, Ordering::SeqCst) >= max_queue {
        WAITING.fetch_sub(1, Ordering::SeqCst);
        return Err(MfError::RateLimited {
            retry_in_ms: 3000 * max_queue as u64,
        });
    }
    let _slot = QueueSlot;
    LIMITER.acquire(cancel, Duration::from_secs(10)).await?;
    let response = fetch::fetch_json(http, url, SITE, cancel).await?;
    match response.status.as_u16() {
        200..=299 => {
            let value = response
                .value
                .ok_or_else(|| MfError::network("Apple Music sent an unreadable response"))?;
            LIMITER.success().await;
            CACHE.put(url, value.clone());
            Ok(value)
        }
        // Apple answers 403 (and sometimes 429) when throttling.
        403 | 429 => {
            let wait = LIMITER
                .throttled(
                    Duration::from_secs(30),
                    Duration::from_secs(300),
                    response.retry_after,
                )
                .await;
            Err(MfError::RateLimited {
                retry_in_ms: wait.as_millis() as u64,
            })
        }
        _ => Err(fetch::status_error(
            response.status,
            response.retry_after,
            SITE,
        )),
    }
}

fn results(value: &Value) -> Vec<Value> {
    value
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// `artworkUrl100` scaled to 1000 px.
pub(crate) fn large_artwork(url: &str) -> String {
    match url.rfind("/100x100") {
        Some(i) => format!("{}/1000x1000bb.jpg", &url[..i]),
        None => url.to_string(),
    }
}

/// `trackViewUrl` without Apple's tracking parameters.
fn clean_view_url(url: &str) -> String {
    crate::links::canonicalize(url).unwrap_or_else(|_| url.to_string())
}

/// Maps an iTunes `song` result.
pub(crate) fn track_from_json(item: &Value) -> Option<Track> {
    if item.get("kind").and_then(Value::as_str) != Some("song") {
        return None;
    }
    let id = item.get("trackId").and_then(Value::as_u64)?;
    let title = item
        .get("trackName")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())?;
    let artist = item
        .get("artistName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let str_field = |key: &str| {
        item.get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    let link = str_field("trackViewUrl")
        .map(|u| clean_view_url(&u))
        .unwrap_or_else(|| format!("https://music.apple.com/song/{id}"));
    let mut track = Track {
        title: title.to_string(),
        artist: artist.to_string(),
        album: str_field("collectionName"),
        duration_ms: item
            .get("trackTimeMillis")
            .and_then(Value::as_u64)
            .filter(|d| *d > 0),
        release_year: str_field("releaseDate")
            .as_deref()
            .and_then(crate::links::year_of),
        genre: str_field("primaryGenreName"),
        artwork_url: str_field("artworkUrl100").map(|u| large_artwork(&u)),
        preview_url: str_field("previewUrl"),
        links: vec![crate::links::service_link(Service::AppleMusic, link)],
        ..Track::default()
    };
    track.ids.insert("apple".into(), id.to_string());
    Some(crate::links::finish_track(track))
}

fn country_code(country: &str) -> String {
    crate::links::storefront(country)
}

/// `/lookup?id=…`: the song itself, or with `songs = true` a collection's songs.
pub(crate) async fn lookup(
    http: &reqwest::Client,
    id: &str,
    country: &str,
    songs: bool,
    max_queue: usize,
    cancel: &CancelToken,
) -> Result<Vec<Value>> {
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
        return Err(MfError::invalid("That Apple Music id isn't valid."));
    }
    let entity = if songs { "&entity=song&limit=200" } else { "" };
    let url = format!(
        "https://itunes.apple.com/lookup?id={id}&country={}{entity}",
        country_code(country)
    );
    Ok(results(&get(http, &url, max_queue, cancel).await?))
}

/// `/search?term=…&entity=song`.
pub(crate) async fn search(
    http: &reqwest::Client,
    term: &str,
    country: &str,
    max_queue: usize,
    cancel: &CancelToken,
) -> Result<Vec<Track>> {
    let term: String = url::form_urlencoded::byte_serialize(term.trim().as_bytes()).collect();
    let url = format!(
        "https://itunes.apple.com/search?term={term}&entity=song&limit=15&country={}",
        country_code(country)
    );
    Ok(results(&get(http, &url, max_queue, cancel).await?)
        .iter()
        .filter_map(track_from_json)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_song_results() {
        let path = format!(
            "{}/tests/fixtures/links/itunes_search.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let value: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let tracks: Vec<Track> = results(&value).iter().filter_map(track_from_json).collect();
        let t = &tracks[0];
        assert_eq!(t.title, "Never Gonna Give You Up");
        assert_eq!(t.artist, "Rick Astley");
        assert_eq!(t.album.as_deref(), Some("Whenever You Need Somebody"));
        assert_eq!(t.duration_ms, Some(213_573));
        assert_eq!(t.release_year, Some(1987));
        assert_eq!(t.genre.as_deref(), Some("Pop"));
        assert!(
            t.artwork_url
                .as_deref()
                .is_some_and(|u| u.ends_with("/1000x1000bb.jpg"))
        );
        assert!(t.preview_url.is_some());
        assert_eq!(
            t.links[0].url,
            "https://music.apple.com/us/album/never-gonna-give-you-up/1559885420?i=1559885421"
        );
        assert_eq!(t.ids.get("apple").map(String::as_str), Some("1559885421"));
    }

    #[test]
    fn ignores_non_songs() {
        assert!(
            track_from_json(
                &serde_json::json!({"kind": "music-video", "trackId": 1, "trackName": "x"})
            )
            .is_none()
        );
        assert!(
            track_from_json(&serde_json::json!({"wrapperType": "collection", "collectionId": 1}))
                .is_none()
        );
        assert_eq!(
            large_artwork("https://a/b.jpg/100x100bb.jpg"),
            "https://a/b.jpg/1000x1000bb.jpg"
        );
        assert_eq!(large_artwork("https://a/b.jpg"), "https://a/b.jpg");
    }
}
