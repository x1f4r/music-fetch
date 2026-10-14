//! Amazon Music songs from the Open Graph and `music:*` tags served to crawlers.
//! A track page names only the title; the artist and album come from its album
//! page ("Album – Artist"). Album track lists hold ASINs only, so albums are
//! matched to Deezer.

use url::Url;

use super::fetch::{self, Agent, PageRequest};
use super::html::{Document, year_of};
use super::{Ctx, Resolution, Site, canonical, deezer, finish_track, service_link, source_info};
use crate::error::{MfError, Result};
use crate::model::{Service, Track};

const SITE: &str = "Amazon Music";

fn is_asin(s: &str) -> bool {
    s.len() == 10 && s.chars().all(|c| c.is_ascii_alphanumeric())
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url) -> Result<Resolution> {
    let host = canonical::host_of(url);
    let parts = canonical::segments(url);
    let track_asin = url.query_pairs().find(|(k, v)| k == "trackAsin" && is_asin(v)).map(|(_, v)| v.into_owned());
    match parts.as_slice() {
        ["tracks", asin] if is_asin(asin) => track(ctx, &host, asin).await,
        ["albums", _] if track_asin.is_some() => track(ctx, &host, track_asin.as_deref().unwrap_or_default()).await,
        ["albums", asin] if is_asin(asin) => album(ctx, url, asin).await,
        _ => Err(MfError::UnsupportedLink { message: "Only Amazon Music songs and albums are supported.".into() }),
    }
}

async fn crawler_page(ctx: &Ctx<'_>, url: &str) -> Result<String> {
    let page = fetch::get_page(ctx.http, url, &PageRequest::new(SITE).agent(Agent::Crawler), ctx.cancel).await?;
    Ok(fetch::require_success(page, SITE)?.body)
}

fn unreadable() -> MfError {
    MfError::MediaUnavailable { message: "Couldn't read this Amazon Music page.".into() }
}

async fn track(ctx: &Ctx<'_>, host: &str, asin: &str) -> Result<Resolution> {
    let track_url = format!("https://{host}/tracks/{asin}");
    let page = parse_track_page(&crawler_page(ctx, &track_url).await?).ok_or_else(unreadable)?;
    let mut track = Track {
        title: page.title,
        duration_ms: page.duration_ms,
        artwork_url: page.image,
        links: vec![service_link(Service::AmazonMusic, track_url.clone())],
        ..Track::default()
    };
    // The album page names the artist; it lives on the same Amazon host.
    if let Some(album_url) = page.album_url.filter(|u| Url::parse(u).is_ok_and(|p| canonical::host_of(&p) == host)) {
        match crawler_page(ctx, &album_url).await.map(|html| parse_album_page(&html)) {
            Ok(Some(album)) => {
                track.artist = album.artist;
                track.album = Some(album.title);
                track.release_year = album.year;
                track.artwork_url = track.artwork_url.or(album.image);
            }
            Ok(None) => {}
            Err(MfError::Canceled) => return Err(MfError::Canceled),
            Err(e) => tracing::debug!("amazon album page failed: {e}"),
        }
    }
    let track = finish_track(track);
    let source_url = Url::parse(&track_url).map_err(|_| unreadable())?;
    let mut source = source_info(&source_url, Site::AmazonMusic);
    source.title = Some(track.title.clone());
    source.uploader = Some(track.artist.clone()).filter(|a| !a.is_empty());
    source.thumbnail_url.clone_from(&track.artwork_url);
    source.duration_ms = track.duration_ms;
    Ok(Resolution::Tracks { source, tracks: vec![track] })
}

async fn album(ctx: &Ctx<'_>, url: &Url, asin: &str) -> Result<Resolution> {
    let album_url = format!("https://{}/albums/{asin}", canonical::host_of(url));
    let album = parse_album_page(&crawler_page(ctx, &album_url).await?).ok_or_else(unreadable)?;
    let mut source = source_info(url, Site::AmazonMusic);
    source.title = Some(album.title.clone());
    source.uploader = Some(album.artist.clone());
    source.thumbnail_url = album.image;
    deezer::album_via_deezer(ctx, &album.title, &album.artist, source).await
}

/// Amazon's og:image carries a logo sticker; the bare image id has none.
fn clean_image(url: &str) -> String {
    let Some(start) = url.find("/images/I/").map(|i| i + "/images/I/".len()) else {
        return url.to_string();
    };
    let id: String = url[start..].chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '+').collect();
    if id.is_empty() { url.to_string() } else { format!("https://m.media-amazon.com/images/I/{id}.jpg") }
}

/// Fields of a track page.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TrackPage {
    pub title: String,
    pub duration_ms: Option<u64>,
    pub album_url: Option<String>,
    pub image: Option<String>,
}

pub(super) fn parse_track_page(html: &str) -> Option<TrackPage> {
    let doc = Document::parse(html);
    if doc.meta("og:type") != Some("music.song") {
        return None;
    }
    Some(TrackPage {
        title: doc.meta("og:title")?.to_string(),
        duration_ms: doc
            .meta("music:duration")
            .and_then(|d| d.parse::<u64>().ok())
            .filter(|d| *d > 0)
            .map(|d| d * 1000),
        album_url: doc.meta("music:album").map(str::to_string),
        image: doc.meta("og:image").map(clean_image),
    })
}

/// Fields of an album page; `og:title` reads "Album – Artist".
#[derive(Debug, Clone, PartialEq)]
pub(super) struct AlbumPage {
    pub title: String,
    pub artist: String,
    pub year: Option<u16>,
    pub image: Option<String>,
}

pub(super) fn parse_album_page(html: &str) -> Option<AlbumPage> {
    let doc = Document::parse(html);
    if doc.meta("og:type") != Some("music.album") {
        return None;
    }
    let (title, artist) = doc.meta("og:title")?.rsplit_once(" – ")?;
    Some(AlbumPage {
        title: title.trim().to_string(),
        artist: artist.trim().to_string(),
        year: doc.meta("music:release_date").and_then(year_of),
        image: doc.meta("og:image").map(clean_image),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("{}/tests/fixtures/links/{name}", env!("CARGO_MANIFEST_DIR"))).expect("fixture")
    }

    #[test]
    fn parses_track_and_album_pages() {
        let track = parse_track_page(&fixture("amazon_track.html")).expect("track");
        assert_eq!(track.title, "Alive");
        assert_eq!(track.duration_ms, Some(316_000));
        assert_eq!(track.album_url.as_deref(), Some("https://music.amazon.com/albums/B000TDBNDG"));
        assert_eq!(track.image.as_deref(), Some("https://m.media-amazon.com/images/I/61EBamxG3SL.jpg"));
        let album = parse_album_page(&fixture("amazon_album.html")).expect("album");
        assert_eq!(album.title, "Musique, Vol. 1");
        assert_eq!(album.artist, "Daft Punk");
        assert_eq!(album.year, Some(2006));
    }

    #[test]
    fn rejects_generic_pages() {
        let generic = r#"<meta property="og:title" content="Amazon Music"><meta property="og:type" content="website">"#;
        assert!(parse_track_page(generic).is_none());
        assert!(parse_album_page(generic).is_none());
        assert!(is_asin("B000TDFHJW"));
        assert!(!is_asin("B000TDFHJ"));
        assert_eq!(clean_image("https://example.com/x.jpg"), "https://example.com/x.jpg");
    }
}
