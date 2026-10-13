//! TIDAL tracks from the page's JSON-LD (`MusicRecording`, including the ISRC)
//! or Open Graph tags. Album pages list no tracks without an account, so albums
//! are matched to Deezer by title and artist.

use serde_json::Value;
use url::Url;

use super::fetch::{self, PageRequest};
use super::html::{Document, parse_iso_duration, str_at, year_of};
use super::{Ctx, Resolution, Site, canonical, deezer, finish_track, service_link, source_info};
use crate::error::{MfError, Result};
use crate::model::{Service, Track};

const SITE: &str = "TIDAL";

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url) -> Result<Resolution> {
    let parts = canonical::segments(url);
    let kind = parts.first().copied().unwrap_or_default();
    let has_id = parts
        .get(1)
        .is_some_and(|id| id.chars().all(|c| c.is_ascii_digit()));
    if !matches!(kind, "track" | "album") || !has_id {
        return Err(MfError::UnsupportedLink {
            message: "Only TIDAL tracks and albums are supported.".into(),
        });
    }
    let page = fetch::get_page(ctx.http, url.as_str(), &PageRequest::new(SITE), ctx.cancel).await?;
    let page = fetch::require_success(page, SITE)?;
    let unreadable = || MfError::MediaUnavailable {
        message: "Couldn't read this TIDAL page.".into(),
    };
    if kind == "track" {
        return parse_track_page(&page.body, url).ok_or_else(unreadable);
    }
    let album = parse_album_page(&page.body).ok_or_else(unreadable)?;
    let mut source = source_info(url, Site::Tidal);
    source.title = Some(album.title.clone());
    source.uploader = Some(album.artist.clone());
    source.thumbnail_url = album.image;
    deezer::album_via_deezer(ctx, &album.title, &album.artist, source).await
}

/// Names from a JSON-LD `byArtist` (object, array of objects, or string).
pub(super) fn ld_names(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items.iter().flat_map(|v| ld_names(Some(v))).collect(),
        Some(Value::Object(_)) => value.and_then(|v| str_at(v, "/name")).into_iter().collect(),
        Some(Value::String(s)) if !s.trim().is_empty() => vec![s.trim().to_string()],
        _ => Vec::new(),
    }
}

/// Parses a track page: JSON-LD first, then `og:title` ("Artist - Title") with
/// the title taken from `og:description` ("Listen to Title on TIDAL").
pub(super) fn parse_track_page(html: &str, url: &Url) -> Option<Resolution> {
    let doc = Document::parse(html);
    let mut track = if let Some(ld) = doc.json_ld_of(&["MusicRecording"]).next() {
        Track {
            title: str_at(ld, "/name")?,
            artists: ld_names(ld.get("byArtist")),
            album: ld
                .pointer("/inAlbum/name")
                .and_then(Value::as_str)
                .map(str::to_string),
            isrc: str_at(ld, "/isrcCode"),
            duration_ms: str_at(ld, "/duration")
                .as_deref()
                .and_then(parse_iso_duration),
            artwork_url: str_at(ld, "/image"),
            ..Track::default()
        }
    } else {
        let og_title = doc.meta("og:title")?;
        let title = doc
            .meta("og:description")
            .and_then(|d| d.strip_prefix("Listen to"))
            .and_then(|d| d.strip_suffix("on TIDAL"))
            .map(str::trim)
            .filter(|t| !t.is_empty())?
            .to_string();
        let artist = og_title
            .strip_suffix(&title)
            .map(|a| a.trim().trim_end_matches('-').trim().to_string())
            .unwrap_or_default();
        Track {
            title,
            artist,
            artwork_url: doc.meta("og:image").map(str::to_string),
            ..Track::default()
        }
    };
    track.release_year = doc.meta("music:release_date").and_then(year_of);
    track.links = vec![service_link(Service::Tidal, url.as_str())];
    let track = finish_track(track);
    let mut source = source_info(url, Site::Tidal);
    source.title = Some(track.title.clone());
    source.uploader = Some(track.artist.clone()).filter(|a| !a.is_empty());
    source.thumbnail_url.clone_from(&track.artwork_url);
    source.duration_ms = track.duration_ms;
    Some(Resolution::Tracks {
        source,
        tracks: vec![track],
    })
}

/// Album title, artist and image.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct AlbumPage {
    pub title: String,
    pub artist: String,
    pub image: Option<String>,
}

pub(super) fn parse_album_page(html: &str) -> Option<AlbumPage> {
    let doc = Document::parse(html);
    if let Some(ld) = doc.json_ld_of(&["MusicAlbum"]).next() {
        let title = str_at(ld, "/name")?;
        let artist = super::join_artists(&ld_names(ld.get("byArtist")));
        return Some(AlbumPage {
            title,
            artist,
            image: str_at(ld, "/image"),
        });
    }
    let og_title = doc.meta("og:title")?;
    let title = doc
        .meta("og:description")?
        .strip_prefix("Listen to")?
        .strip_suffix("on TIDAL")?
        .trim()
        .to_string();
    let artist = og_title
        .strip_suffix(&title)?
        .trim()
        .trim_end_matches('-')
        .trim()
        .to_string();
    Some(AlbumPage {
        title,
        artist,
        image: doc.meta("og:image").map(str::to_string),
    })
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

    #[test]
    fn parses_track_json_ld() {
        let url = Url::parse("https://tidal.com/track/1781887").unwrap();
        let Some(Resolution::Tracks { tracks, source }) =
            parse_track_page(&fixture("tidal_track.html"), &url)
        else {
            panic!("expected tracks");
        };
        let t = &tracks[0];
        assert_eq!(t.title, "Billie Jean");
        assert_eq!(t.artist, "Michael Jackson");
        assert_eq!(t.album.as_deref(), Some("Thriller"));
        assert_eq!(t.isrc.as_deref(), Some("USSM19902991"));
        assert_eq!(t.duration_ms, Some(294_000));
        assert_eq!(t.release_year, Some(1982));
        assert_eq!(t.identity_key, "isrc::USSM19902991");
        assert_eq!(source.service, Some(Service::Tidal));
    }

    #[test]
    fn falls_back_to_open_graph() {
        let html = r#"<meta property="og:title" content="Michael Jackson - Billie Jean">
            <meta property="og:description" content="Listen to Billie Jean on TIDAL">"#;
        let url = Url::parse("https://tidal.com/track/1781887").unwrap();
        let Some(Resolution::Tracks { tracks, .. }) = parse_track_page(html, &url) else {
            panic!("expected tracks")
        };
        assert_eq!(tracks[0].title, "Billie Jean");
        assert_eq!(tracks[0].artist, "Michael Jackson");
    }

    #[test]
    fn parses_album() {
        let album = parse_album_page(&fixture("tidal_album.html")).expect("album");
        assert_eq!(album.title, "Thriller");
        assert_eq!(album.artist, "Michael Jackson");
        assert!(album.image.is_some());
        let og = r#"<meta property="og:title" content="A - B"><meta property="og:description" content="Listen to B on TIDAL">"#;
        assert_eq!(
            parse_album_page(og).map(|a| (a.title, a.artist)),
            Some(("B".into(), "A".into()))
        );
        assert!(parse_album_page("<html></html>").is_none());
    }
}
