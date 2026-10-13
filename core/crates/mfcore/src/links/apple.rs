//! Apple Music albums, songs and playlists from the page's
//! `serialized-server-data` (plus JSON-LD), with the iTunes lookup API as a
//! fallback. An album link with `?i=<id>` means that one song.

use serde_json::Value;
use url::Url;

use super::fetch::{self, PageRequest};
use super::html::{Document, parse_iso_duration, str_at, u64_at, year_of};
use super::{Ctx, Resolution, Site, canonical, finish_track, service_link, source_info};
use crate::catalog::itunes;
use crate::error::{MfError, Result};
use crate::model::{Service, SourceInfo, Track};

const SITE: &str = "Apple Music";
/// iTunes calls a link may queue behind.
const ITUNES_QUEUE: usize = 6;

/// What an Apple Music link points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AppleRef {
    Album {
        storefront: String,
        id: String,
        track: Option<String>,
    },
    Song {
        storefront: String,
        id: String,
    },
    Playlist {
        storefront: String,
        id: String,
    },
}

fn is_numeric(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}

/// Parses `/{cc}/album/{slug}/{id}?i={track}`, `/{cc}/song/{slug}/{id}` and
/// `/{cc}/playlist/{slug}/pl.{id}` (storefront and slug optional).
pub(super) fn parse_ref(url: &Url, default_storefront: &str) -> Option<AppleRef> {
    let parts = canonical::segments(url);
    let (storefront, rest) = match parts.first() {
        Some(cc) if cc.len() == 2 && cc.chars().all(|c| c.is_ascii_alphabetic()) => {
            (cc.to_ascii_lowercase(), &parts[1..])
        }
        _ => (default_storefront.to_string(), &parts[..]),
    };
    if rest.len() < 2 {
        return None;
    }
    let id = rest[rest.len() - 1].to_string();
    match rest[0] {
        "album" if is_numeric(&id) => {
            let track = url
                .query_pairs()
                .find(|(k, v)| k == "i" && is_numeric(v))
                .map(|(_, v)| v.into_owned());
            Some(AppleRef::Album {
                storefront,
                id,
                track,
            })
        }
        "song" if is_numeric(&id) => Some(AppleRef::Song { storefront, id }),
        "playlist" if id.starts_with("pl.") => Some(AppleRef::Playlist { storefront, id }),
        _ => None,
    }
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url) -> Result<Resolution> {
    let apple = parse_ref(url, &ctx.country).ok_or_else(|| MfError::UnsupportedLink {
        message: "Only Apple Music songs, albums and playlists are supported.".into(),
    })?;
    match fetch::get_page(ctx.http, url.as_str(), &PageRequest::new(SITE), ctx.cancel).await {
        Ok(page) if page.status.is_success() => {
            if let Some(resolution) = parse_page(&page.body, &apple, url) {
                return Ok(resolution);
            }
        }
        Ok(page) if page.status.as_u16() == 404 => {
            return Err(fetch::status_error(page.status, None, SITE));
        }
        Ok(_) => {}
        Err(MfError::Canceled) => return Err(MfError::Canceled),
        Err(e) => tracing::debug!("apple music page failed: {e}"),
    }
    itunes_fallback(ctx, &apple, url).await
}

/// `{w}x{h}bb.{f}` artwork templates at 1000 px.
fn artwork(value: &Value) -> Option<String> {
    let template = value
        .pointer("/artwork/dictionary/url")
        .and_then(Value::as_str)?;
    Some(
        template
            .replace("{w}", "1000")
            .replace("{h}", "1000")
            .replace("{c}", "bb")
            .replace("{f}", "jpg"),
    )
}

/// Last plausible four-digit year in free text ("Pop · 1987", "July 27, 1987").
fn year_in_text(text: &str) -> Option<u16> {
    text.split(|c: char| !c.is_ascii_digit())
        .filter(|t| t.len() == 4)
        .filter_map(|t| t.parse::<u16>().ok())
        .rfind(|y| (1900..=2100).contains(y))
}

fn store_id(item: &Value) -> Option<String> {
    str_at(item, "/contentDescriptor/identifiers/storeAdamID")
}

fn apple_track(item: &Value, storefront: &str) -> Option<Track> {
    let id = store_id(item)?;
    let title = str_at(item, "/title")?;
    let link = str_at(item, "/contentDescriptor/url")
        .and_then(|u| canonical::canonicalize(&u).ok())
        .unwrap_or_else(|| format!("https://music.apple.com/{storefront}/song/{id}"));
    let mut track = Track {
        title,
        artist: str_at(item, "/artistName").unwrap_or_default(),
        duration_ms: u64_at(item, "/duration").filter(|d| *d > 0),
        preview_url: str_at(item, "/previewUrl"),
        artwork_url: artwork(item),
        album: item
            .pointer("/tertiaryLinks/0/title")
            .and_then(Value::as_str)
            .map(str::to_string),
        links: vec![service_link(Service::AppleMusic, link)],
        ..Track::default()
    };
    track.ids.insert("apple".into(), id);
    Some(track)
}

/// Items of every section of `kind` ("trackLockup", "songDetailHeader", ...).
fn items_of<'a>(sections: &'a [Value], kind: &str) -> Vec<&'a Value> {
    sections
        .iter()
        .filter(|s| s.get("itemKind").and_then(Value::as_str) == Some(kind))
        .filter_map(|s| s.get("items").and_then(Value::as_array))
        .flatten()
        .collect()
}

fn song_tracks(
    sections: &[Value],
    song_ld: Option<&Value>,
    storefront: &str,
) -> Option<Vec<Track>> {
    let header = *items_of(sections, "songDetailHeader").first()?;
    let mut track = apple_track(header, storefront)?;
    track.artist = str_at(header, "/artists").unwrap_or_default();
    track.album = str_at(header, "/album");
    track.release_year = str_at(header, "/releaseDate")
        .as_deref()
        .and_then(year_in_text);
    if let Some(ld) = song_ld {
        track.duration_ms = str_at(ld, "/audio/duration")
            .or_else(|| str_at(ld, "/timeRequired"))
            .as_deref()
            .and_then(parse_iso_duration);
        track.release_year = track
            .release_year
            .or_else(|| str_at(ld, "/datePublished").as_deref().and_then(year_of));
    }
    Some(vec![finish_track(track)])
}

/// Album or playlist tracks; `wanted` keeps only that song (album `?i=`).
fn collection_tracks(
    sections: &[Value],
    storefront: &str,
    wanted: Option<&str>,
    is_album: bool,
    source: &mut SourceInfo,
) -> Vec<Track> {
    let header = items_of(sections, "containerDetailHeaderLockup")
        .first()
        .copied();
    let header_title = header.and_then(|h| str_at(h, "/title"));
    let header_artist = header.and_then(|h| str_at(h, "/subtitleLinks/0/title"));
    let header_art = header.and_then(artwork);
    let header_year = header
        .and_then(|h| str_at(h, "/quaternaryTitle"))
        .as_deref()
        .and_then(year_in_text);
    source.title.clone_from(&header_title);
    source.uploader.clone_from(&header_artist);
    source.thumbnail_url.clone_from(&header_art);
    let mut tracks: Vec<Track> = items_of(sections, "trackLockup")
        .into_iter()
        .filter(|item| wanted.is_none_or(|w| store_id(item).as_deref() == Some(w)))
        .filter_map(|item| {
            let mut track = apple_track(item, storefront)?;
            if track.artist.is_empty() {
                track.artist = header_artist.clone().unwrap_or_default();
            }
            if is_album {
                track.album.clone_from(&header_title);
                track.artwork_url = track.artwork_url.or_else(|| header_art.clone());
                track.release_year = header_year;
            }
            Some(finish_track(track))
        })
        .collect();
    if wanted.is_some() {
        tracks.truncate(1);
    }
    tracks
}

/// Parses an Apple Music page for `apple`.
pub(super) fn parse_page(html: &str, apple: &AppleRef, canonical_url: &Url) -> Option<Resolution> {
    let doc = Document::parse(html);
    let data = doc.script_json("serialized-server-data")?;
    let sections = data
        .pointer("/data/0/data/sections")
        .or_else(|| data.pointer("/0/data/sections"))
        .and_then(Value::as_array)?;
    let mut source = source_info(canonical_url, Site::AppleMusic);
    let tracks = match apple {
        AppleRef::Song { storefront, .. } => song_tracks(
            sections,
            doc.script_json("schema:song").as_ref(),
            storefront,
        )?,
        AppleRef::Album {
            storefront, track, ..
        } => collection_tracks(sections, storefront, track.as_deref(), true, &mut source),
        AppleRef::Playlist { storefront, .. } => {
            collection_tracks(sections, storefront, None, false, &mut source)
        }
    };
    if tracks.is_empty() {
        return None;
    }
    finish_source(&mut source, &tracks);
    Some(Resolution::Tracks { source, tracks })
}

/// Single-track links describe the track; collections keep their own title.
fn finish_source(source: &mut SourceInfo, tracks: &[Track]) {
    if let [track] = tracks {
        source.title = Some(track.title.clone());
        source.uploader = Some(track.artist.clone()).filter(|a| !a.is_empty());
        source.thumbnail_url = track.artwork_url.clone().or(source.thumbnail_url.take());
        source.duration_ms = track.duration_ms;
    } else {
        source.duration_ms =
            Some(tracks.iter().filter_map(|t| t.duration_ms).sum()).filter(|d| *d > 0);
    }
    source.item_count = tracks.len() as u32;
}

async fn itunes_fallback(ctx: &Ctx<'_>, apple: &AppleRef, url: &Url) -> Result<Resolution> {
    let unreadable = || MfError::MediaUnavailable {
        message: "Couldn't read this Apple Music link.".into(),
    };
    let (storefront, id, songs, wanted) = match apple {
        AppleRef::Album {
            storefront,
            id,
            track,
        } => (storefront, id, true, track.as_deref()),
        AppleRef::Song { storefront, id } => (storefront, id, false, None),
        AppleRef::Playlist { .. } => return Err(unreadable()),
    };
    let lookup_id = match (songs, wanted) {
        (true, Some(track)) => track,
        _ => id.as_str(),
    };
    let results = itunes::lookup(
        ctx.http,
        lookup_id,
        storefront,
        songs && wanted.is_none(),
        ITUNES_QUEUE,
        ctx.cancel,
    )
    .await?;
    let tracks: Vec<Track> = results.iter().filter_map(itunes::track_from_json).collect();
    if tracks.is_empty() {
        return Err(unreadable());
    }
    let mut source = source_info(url, Site::AppleMusic);
    if let Some(collection) = results
        .iter()
        .find(|r| r.get("wrapperType").and_then(Value::as_str) == Some("collection"))
    {
        source.title = str_at(collection, "/collectionName");
        source.uploader = str_at(collection, "/artistName");
        source.thumbnail_url =
            str_at(collection, "/artworkUrl100").map(|u| itunes::large_artwork(&u));
    }
    finish_source(&mut source, &tracks);
    Ok(Resolution::Tracks { source, tracks })
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
    fn parses_refs() {
        assert_eq!(
            parse_ref(
                &url(
                    "https://music.apple.com/us/album/never-gonna-give-you-up/1559885420?i=1559885421"
                ),
                "de"
            ),
            Some(AppleRef::Album {
                storefront: "us".into(),
                id: "1559885420".into(),
                track: Some("1559885421".into())
            })
        );
        assert_eq!(
            parse_ref(&url("https://music.apple.com/album/1559885420"), "de"),
            Some(AppleRef::Album {
                storefront: "de".into(),
                id: "1559885420".into(),
                track: None
            })
        );
        assert_eq!(
            parse_ref(
                &url("https://music.apple.com/us/song/never-gonna-give-you-up/1559885421"),
                "de"
            ),
            Some(AppleRef::Song {
                storefront: "us".into(),
                id: "1559885421".into()
            })
        );
        assert!(matches!(
            parse_ref(
                &url(
                    "https://music.apple.com/us/playlist/todays-hits/pl.f4d106fed2bd41149aaacabb233eb5eb"
                ),
                "us"
            ),
            Some(AppleRef::Playlist { .. })
        ));
        assert_eq!(
            parse_ref(
                &url("https://music.apple.com/us/artist/rick-astley/669771"),
                "us"
            ),
            None
        );
        assert_eq!(
            parse_ref(&url("https://music.apple.com/us/browse"), "us"),
            None
        );
    }

    #[test]
    fn album_with_track_id_is_that_song() {
        let canonical =
            url("https://music.apple.com/us/album/never-gonna-give-you-up/1559885420?i=1559885421");
        let apple = parse_ref(&canonical, "us").unwrap();
        let Some(Resolution::Tracks { source, tracks }) =
            parse_page(&fixture("apple_album_i.html"), &apple, &canonical)
        else {
            panic!("expected tracks");
        };
        assert_eq!(tracks.len(), 1);
        let t = &tracks[0];
        assert_eq!(t.title, "Never Gonna Give You Up");
        assert_eq!(t.artist, "Rick Astley");
        assert_eq!(t.album.as_deref(), Some("Whenever You Need Somebody"));
        assert_eq!(t.duration_ms, Some(213_573));
        assert_eq!(t.release_year, Some(1987));
        assert!(
            t.preview_url
                .as_deref()
                .is_some_and(|u| u.ends_with(".m4a"))
        );
        assert!(
            t.artwork_url
                .as_deref()
                .is_some_and(|u| u.ends_with("/1000x1000bb.jpg"))
        );
        assert_eq!(t.ids.get("apple").map(String::as_str), Some("1559885421"));
        assert_eq!(
            t.links[0].url,
            "https://music.apple.com/us/album/never-gonna-give-you-up/1559885420?i=1559885421"
        );
        assert_eq!(source.title.as_deref(), Some("Never Gonna Give You Up"));
        assert_eq!(source.item_count, 1);
    }

    #[test]
    fn whole_album() {
        let canonical =
            url("https://music.apple.com/us/album/whenever-you-need-somebody/1559885420");
        let apple = parse_ref(&canonical, "us").unwrap();
        let Some(Resolution::Tracks { source, tracks }) =
            parse_page(&fixture("apple_album_i.html"), &apple, &canonical)
        else {
            panic!("expected tracks");
        };
        assert!(tracks.len() >= 2);
        assert_eq!(source.title.as_deref(), Some("Whenever You Need Somebody"));
        assert_eq!(source.uploader.as_deref(), Some("Rick Astley"));
        assert!(
            tracks
                .iter()
                .all(|t| t.album.as_deref() == Some("Whenever You Need Somebody"))
        );
    }

    #[test]
    fn song_page() {
        let canonical = url("https://music.apple.com/us/song/never-gonna-give-you-up/1559885421");
        let apple = parse_ref(&canonical, "us").unwrap();
        let Some(Resolution::Tracks { tracks, .. }) =
            parse_page(&fixture("apple_song.html"), &apple, &canonical)
        else {
            panic!("expected tracks");
        };
        let t = &tracks[0];
        assert_eq!(t.title, "Never Gonna Give You Up");
        assert_eq!(t.artist, "Rick Astley");
        assert_eq!(t.album.as_deref(), Some("Whenever You Need Somebody"));
        assert_eq!(t.duration_ms, Some(213_000));
        assert_eq!(t.release_year, Some(1987));
    }

    #[test]
    fn playlist_page() {
        let canonical = url(
            "https://music.apple.com/us/playlist/todays-hits/pl.f4d106fed2bd41149aaacabb233eb5eb",
        );
        let apple = parse_ref(&canonical, "us").unwrap();
        let Some(Resolution::Tracks { source, tracks }) =
            parse_page(&fixture("apple_playlist.html"), &apple, &canonical)
        else {
            panic!("expected tracks");
        };
        assert_eq!(source.title.as_deref(), Some("Today’s Hits"));
        assert!(tracks.len() >= 2);
        let t = &tracks[0];
        assert_eq!(t.title, "Nicole Kidman");
        assert_eq!(t.artist, "ADÉLA");
        assert_eq!(t.album.as_deref(), Some("PRIMA"));
        assert!(t.artwork_url.is_some());
    }

    #[test]
    fn years_in_text() {
        assert_eq!(year_in_text("Pop · 1987"), Some(1987));
        assert_eq!(year_in_text("July 27, 1987"), Some(1987));
        assert_eq!(year_in_text("Updated Friday"), None);
    }
}
