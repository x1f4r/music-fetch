//! Spotify tracks, albums and playlists from the public embed page
//! (`__NEXT_DATA__`), with the crawler-facing track page as a fallback.
//! The embed lists at most 100 playlist tracks and carries no ISRC.

use serde_json::Value;
use url::Url;

use super::fetch::{self, Agent, PageRequest};
use super::html::{Document, str_at, u64_at, year_of};
use super::{Ctx, Resolution, Site, canonical, finish_track, service_link, source_info};
use crate::error::{MfError, Result};
use crate::model::{Service, Track};

const SITE: &str = "Spotify";

/// Supported Spotify entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Track,
    Album,
    Playlist,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Track => "track",
            Kind::Album => "album",
            Kind::Playlist => "playlist",
        }
    }
}

/// `(kind, id)` of a canonical Spotify link.
pub(super) fn entity(url: &Url) -> Option<(Kind, String)> {
    let parts = canonical::segments(url);
    let kind = match *parts.first()? {
        "track" => Kind::Track,
        "album" => Kind::Album,
        "playlist" => Kind::Playlist,
        _ => return None,
    };
    let id = parts
        .get(1)
        .filter(|id| !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric()))?;
    Some((kind, (*id).to_string()))
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url) -> Result<Resolution> {
    let (kind, id) = entity(url).ok_or_else(|| MfError::UnsupportedLink {
        message: "Only Spotify tracks, albums and playlists are supported.".into(),
    })?;
    let canonical = format!("https://open.spotify.com/{}/{id}", kind.as_str());
    let canonical_url = Url::parse(&canonical).map_err(|_| MfError::internal("spotify url"))?;
    let embed = format!("https://open.spotify.com/embed/{}/{id}", kind.as_str());
    match fetch::get_page(ctx.http, &embed, &PageRequest::new(SITE), ctx.cancel).await {
        Ok(page) if page.status.is_success() => {
            if let Some(resolution) = parse_embed(&page.body, &canonical_url) {
                return Ok(resolution);
            }
        }
        Ok(page) if page.status.as_u16() == 404 => {
            return Err(fetch::status_error(page.status, None, SITE));
        }
        Ok(_) => {}
        Err(MfError::Canceled) => return Err(MfError::Canceled),
        Err(e) => tracing::debug!("spotify embed failed: {e}"),
    }
    let page = fetch::get_page(
        ctx.http,
        &canonical,
        &PageRequest::new(SITE).agent(Agent::Crawler),
        ctx.cancel,
    )
    .await?;
    let page = fetch::require_success(page, SITE)?;
    parse_crawler_page(&page.body, kind, &canonical_url).ok_or_else(|| MfError::MediaUnavailable {
        message: "Couldn't read this Spotify page.".into(),
    })
}

/// Largest image of an embed entity (`visualIdentity.image` or `coverArt.sources`).
fn best_image(entity: &Value) -> Option<String> {
    let images = entity
        .pointer("/visualIdentity/image")
        .or_else(|| entity.pointer("/coverArt/sources"))
        .and_then(Value::as_array)?;
    images
        .iter()
        .max_by_key(|img| {
            img.get("maxWidth")
                .or_else(|| img.get("width"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
        })
        .and_then(|img| img.get("url"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn id_from_uri(uri: &str) -> Option<&str> {
    uri.strip_prefix("spotify:track:")
        .filter(|id| !id.is_empty())
}

fn spotify_track(title: String, artists: Vec<String>, id: &str) -> Track {
    let mut track = Track {
        title,
        artists,
        links: vec![service_link(
            Service::Spotify,
            format!("https://open.spotify.com/track/{id}"),
        )],
        ..Track::default()
    };
    track.ids.insert("spotify".into(), id.to_string());
    track
}

/// Parses an embed page into tracks.
pub(super) fn parse_embed(html: &str, canonical: &Url) -> Option<Resolution> {
    let doc = Document::parse(html);
    let data = doc.script_json("__NEXT_DATA__")?;
    let entity = data.pointer("/props/pageProps/state/data/entity")?;
    let kind = entity.get("type").and_then(Value::as_str)?;
    let name = str_at(entity, "/name").or_else(|| str_at(entity, "/title"));
    let image = best_image(entity);
    let mut source = source_info(canonical, Site::Spotify);
    source.title.clone_from(&name);
    source.thumbnail_url.clone_from(&image);
    let tracks: Vec<Track> = match kind {
        "track" => {
            let id = str_at(entity, "/id").or_else(|| {
                str_at(entity, "/uri").and_then(|u| id_from_uri(&u).map(str::to_string))
            })?;
            let artists: Vec<String> = entity
                .get("artists")
                .and_then(Value::as_array)
                .map(|list| list.iter().filter_map(|a| str_at(a, "/name")).collect())
                .unwrap_or_default();
            let mut track = spotify_track(name.clone()?, artists, &id);
            track.duration_ms = u64_at(entity, "/duration").filter(|d| *d > 0);
            track.release_year = str_at(entity, "/releaseDate/isoString")
                .as_deref()
                .and_then(year_of);
            track.preview_url = str_at(entity, "/audioPreview/url");
            track.artwork_url = image;
            let track = finish_track(track);
            source.uploader = Some(track.artist.clone()).filter(|a| !a.is_empty());
            source.duration_ms = track.duration_ms;
            vec![track]
        }
        "album" | "playlist" => {
            let is_album = kind == "album";
            source.uploader = str_at(entity, "/subtitle");
            let list = entity.get("trackList").and_then(Value::as_array)?;
            list.iter()
                .filter_map(|item| {
                    let id =
                        str_at(item, "/uri").and_then(|u| id_from_uri(&u).map(str::to_string))?;
                    let title = str_at(item, "/title")?;
                    // Artists are joined with ",\u{a0}"; a plain comma can be part of a name.
                    let artists = str_at(item, "/subtitle")
                        .map(|s| s.split(",\u{a0}").map(|a| a.trim().to_string()).collect())
                        .unwrap_or_default();
                    let mut track = spotify_track(title, artists, &id);
                    track.duration_ms = u64_at(item, "/duration").filter(|d| *d > 0);
                    track.preview_url = str_at(item, "/audioPreview/url");
                    if is_album {
                        track.album.clone_from(&name);
                        track.artwork_url.clone_from(&source.thumbnail_url);
                    }
                    Some(finish_track(track))
                })
                .collect()
        }
        _ => return None,
    };
    if tracks.is_empty() {
        return None;
    }
    source.item_count = tracks.len() as u32;
    if source.duration_ms.is_none() {
        source.duration_ms =
            Some(tracks.iter().filter_map(|t| t.duration_ms).sum()).filter(|d| *d > 0);
    }
    Some(Resolution::Tracks { source, tracks })
}

/// Fallback for tracks: Open Graph and `music:*` tags served to crawlers.
/// `og:description` reads "Artist · Album · Song · Year".
pub(super) fn parse_crawler_page(html: &str, kind: Kind, canonical: &Url) -> Option<Resolution> {
    if kind != Kind::Track {
        return None;
    }
    let doc = Document::parse(html);
    let title = doc.meta("og:title")?.to_string();
    let id = canonical::segments(canonical).get(1)?.to_string();
    let description = doc.meta("og:description").unwrap_or_default();
    let parts: Vec<&str> = description.split(" · ").map(str::trim).collect();
    let artist = doc
        .meta("music:musician_description")
        .map(str::to_string)
        .or_else(|| parts.first().map(|s| (*s).to_string()))
        .unwrap_or_default();
    let mut track = spotify_track(title, Vec::new(), &id);
    track.artist = artist;
    if parts.len() >= 4 && parts[2] == "Song" {
        track.album = Some(parts[1].to_string());
    }
    track.duration_ms = doc
        .meta("music:duration")
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|d| *d > 0)
        .map(|s| s * 1000);
    track.release_year = doc.meta("music:release_date").and_then(year_of);
    track.preview_url = doc.meta("og:audio").map(str::to_string);
    track.artwork_url = doc.meta("og:image").map(str::to_string);
    let track = finish_track(track);
    let mut source = source_info(canonical, Site::Spotify);
    source.title = Some(track.title.clone());
    source.uploader = Some(track.artist.clone()).filter(|a| !a.is_empty());
    source.thumbnail_url.clone_from(&track.artwork_url);
    source.duration_ms = track.duration_ms;
    Some(Resolution::Tracks {
        source,
        tracks: vec![track],
    })
}

/// Turns a resolution into a single hint track (used for TikTok's Spotify ids).
pub(super) fn first_track(resolution: Resolution) -> Option<Track> {
    match resolution {
        Resolution::Tracks { tracks, .. } => tracks.into_iter().next(),
        Resolution::Media { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!(
            "{}/tests/fixtures/links/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .expect("fixture")
    }

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn parses_entities() {
        assert_eq!(
            entity(&url(
                "https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT"
            )),
            Some((Kind::Track, "4cOdK2wGLETKBW3PvgPWqT".into()))
        );
        assert_eq!(
            entity(&url(
                "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
            ))
            .map(|e| e.0),
            Some(Kind::Playlist)
        );
        assert_eq!(
            entity(&url(
                "https://open.spotify.com/artist/0gxyHStUsqpMadRV0Di1Qt"
            )),
            None
        );
        assert_eq!(entity(&url("https://open.spotify.com/track/")), None);
        assert_eq!(entity(&url("https://open.spotify.com/track/abc$")), None);
    }

    #[test]
    fn parses_track_embed() {
        let canonical = url("https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT");
        let Some(Resolution::Tracks { source, tracks }) =
            parse_embed(&fixture("spotify_track_embed.html"), &canonical)
        else {
            panic!("expected tracks");
        };
        assert_eq!(tracks.len(), 1);
        let t = &tracks[0];
        assert_eq!(t.title, "Never Gonna Give You Up");
        assert_eq!(t.artist, "Rick Astley");
        assert_eq!(t.duration_ms, Some(213_573));
        assert_eq!(t.release_year, Some(1987));
        assert!(
            t.preview_url
                .as_deref()
                .is_some_and(|u| u.starts_with("https://p.scdn.co/mp3-preview/"))
        );
        assert!(
            t.artwork_url
                .as_deref()
                .is_some_and(|u| u.contains("ab67616d0000b273"))
        );
        assert_eq!(
            t.ids.get("spotify").map(String::as_str),
            Some("4cOdK2wGLETKBW3PvgPWqT")
        );
        assert_eq!(
            t.links[0].url,
            "https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT"
        );
        assert_eq!(source.item_count, 1);
        assert_eq!(source.service, Some(Service::Spotify));
        assert_eq!(source.uploader.as_deref(), Some("Rick Astley"));
    }

    #[test]
    fn parses_album_embed() {
        let canonical = url("https://open.spotify.com/album/4aawyAB9vmqN3uQ7FjRGTy");
        let Some(Resolution::Tracks { source, tracks }) =
            parse_embed(&fixture("spotify_album_embed.html"), &canonical)
        else {
            panic!("expected tracks");
        };
        assert_eq!(source.title.as_deref(), Some("Global Warming"));
        assert_eq!(source.uploader.as_deref(), Some("Pitbull"));
        assert_eq!(source.item_count as usize, tracks.len());
        let t = &tracks[0];
        assert_eq!(t.title, "Global Warming (feat. Sensato)");
        assert_eq!(t.artists, vec!["Pitbull", "Sensato"]);
        assert_eq!(t.artist, "Pitbull & Sensato");
        assert_eq!(t.album.as_deref(), Some("Global Warming"));
        assert!(t.artwork_url.is_some());
        assert!(t.preview_url.is_some());
    }

    #[test]
    fn parses_playlist_embed() {
        let canonical = url("https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M");
        let Some(Resolution::Tracks { source, tracks }) =
            parse_embed(&fixture("spotify_playlist_embed.html"), &canonical)
        else {
            panic!("expected tracks");
        };
        assert_eq!(source.title.as_deref(), Some("Today’s Top Hits"));
        assert!(source.thumbnail_url.is_some());
        assert!(tracks.len() >= 2);
        assert!(
            tracks
                .iter()
                .all(|t| t.album.is_none() && !t.title.is_empty() && !t.artist.is_empty())
        );
    }

    #[test]
    fn parses_crawler_fallback() {
        let canonical = url("https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT");
        let Some(Resolution::Tracks { tracks, .. }) =
            parse_crawler_page(&fixture("spotify_track_og.html"), Kind::Track, &canonical)
        else {
            panic!("expected tracks");
        };
        let t = &tracks[0];
        assert_eq!(t.title, "Never Gonna Give You Up");
        assert_eq!(t.artist, "Rick Astley");
        assert_eq!(t.album.as_deref(), Some("Whenever You Need Somebody"));
        assert_eq!(t.duration_ms, Some(214_000));
        assert_eq!(t.release_year, Some(1987));
        assert!(t.preview_url.is_some());
        assert!(
            parse_crawler_page(&fixture("spotify_track_og.html"), Kind::Album, &canonical)
                .is_none()
        );
    }

    #[test]
    fn garbage_is_not_a_resolution() {
        let canonical = url("https://open.spotify.com/track/x");
        assert!(parse_embed("<html></html>", &canonical).is_none());
        assert!(
            parse_embed(
                r#"<script id="__NEXT_DATA__" type="application/json">{"props":{}}</script>"#,
                &canonical
            )
            .is_none()
        );
        assert!(parse_crawler_page("", Kind::Track, &canonical).is_none());
    }
}
