//! TikTok videos: page info and the sound attribution from the rehydration JSON
//! (`itemStruct.music`). TikTok's own song match (`tt2dsp`) carries Apple Music
//! (platform 1) and Spotify (platform 3) track ids, even for "original sound".
//! The video itself goes to the media backend.

use serde_json::Value;
use url::Url;

use super::fetch::{self, PageRequest};
use super::html::{Document, str_at, u64_at};
use super::{Ctx, Resolution, Site, canonical, finish_track, service_link, source_info};
use crate::catalog::itunes;
use crate::error::{MfError, Result};
use crate::model::{Service, Track};

const SITE: &str = "TikTok";
/// iTunes calls a hint lookup may queue behind.
const ITUNES_QUEUE: usize = 3;

/// The sound a video uses.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct Sound {
    pub title: Option<String>,
    pub author: Option<String>,
    pub album: Option<String>,
    /// TikTok marks the creator's own audio as original.
    pub original: bool,
    pub apple_id: Option<String>,
    pub spotify_id: Option<String>,
}

/// Page info plus the sound.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct VideoPage {
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub thumbnail_url: Option<String>,
    pub duration_ms: Option<u64>,
    pub sound: Option<Sound>,
    /// TikTok says the video does not exist (or is private).
    pub missing: bool,
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url) -> Result<Resolution> {
    let parts = canonical::segments(url);
    let is_post = parts.len() == 3 && parts[0].starts_with('@') && matches!(parts[1], "video" | "photo");
    if !is_post {
        return Err(MfError::UnsupportedLink { message: "This TikTok link doesn't point to a video.".into() });
    }
    let mut source = source_info(url, Site::TikTok);
    match fetch::get_page(ctx.http, url.as_str(), &PageRequest::new(SITE), ctx.cancel).await {
        Ok(page) if page.status.is_success() => {
            let video = parse_page(&page.body);
            if video.missing {
                return Err(MfError::MediaUnavailable {
                    message: "This TikTok video doesn't exist or is private.".into(),
                });
            }
            source.title = video.title;
            source.uploader = video.uploader;
            source.thumbnail_url = video.thumbnail_url;
            source.duration_ms = video.duration_ms;
            if let Some(sound) = video.sound {
                source.hints = hints(ctx, &sound).await?;
            }
        }
        Ok(page) => tracing::debug!("tiktok page answered {}", page.status),
        Err(MfError::Canceled) => return Err(MfError::Canceled),
        Err(e) => tracing::debug!("tiktok page failed: {e}"),
    }
    Ok(Resolution::Media { source, url: url.to_string(), direct: false })
}

fn truncate(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((i, _)) => format!("{}…", text[..i].trim_end()),
        None => text.to_string(),
    }
}

/// Parses `__UNIVERSAL_DATA_FOR_REHYDRATION__` → `webapp.video-detail`.
pub(super) fn parse_page(html: &str) -> VideoPage {
    let doc = Document::parse(html);
    let Some(data) = doc.script_json("__UNIVERSAL_DATA_FOR_REHYDRATION__") else {
        return VideoPage::default();
    };
    let Some(detail) = data.pointer("/__DEFAULT_SCOPE__/webapp.video-detail") else {
        return VideoPage::default();
    };
    let status = detail.get("statusCode").and_then(Value::as_i64).unwrap_or(0);
    let Some(item) = detail.pointer("/itemInfo/itemStruct") else {
        return VideoPage { missing: status != 0, ..VideoPage::default() };
    };
    let music = item.get("music");
    let sound = music.map(|m| {
        let dsp = m.pointer("/tt2dsp/tt_to_dsp_song_infos").and_then(Value::as_array);
        let dsp_id = |platform: u64| {
            dsp.and_then(|list| list.iter().find(|s| s.get("Platform").and_then(Value::as_u64) == Some(platform)))
                .and_then(|s| str_at(s, "/SongId"))
        };
        Sound {
            title: str_at(m, "/title"),
            author: str_at(m, "/authorName"),
            album: str_at(m, "/album"),
            original: m.get("original").and_then(Value::as_bool).unwrap_or(false),
            apple_id: dsp_id(1).filter(|id| id.chars().all(|c| c.is_ascii_digit())),
            spotify_id: dsp_id(3).filter(|id| id.chars().all(|c| c.is_ascii_alphanumeric())),
        }
    });
    VideoPage {
        title: str_at(item, "/desc").map(|d| truncate(&d, 200)),
        uploader: str_at(item, "/author/nickname").or_else(|| str_at(item, "/author/uniqueId")),
        thumbnail_url: str_at(item, "/video/cover"),
        duration_ms: u64_at(item, "/video/duration").filter(|d| *d > 0).map(|d| d * 1000),
        sound,
        missing: false,
    }
}

/// The attribution as a track: TikTok's labels when the sound is a released
/// song, plus Apple Music and Spotify ids and links when TikTok matched it.
pub(super) fn sound_track(sound: &Sound) -> Option<Track> {
    if sound.original && sound.apple_id.is_none() && sound.spotify_id.is_none() {
        return None;
    }
    let mut track = Track::default();
    if !sound.original {
        track.title = sound.title.clone().unwrap_or_default();
        track.artist = sound.author.clone().unwrap_or_default();
        track.album.clone_from(&sound.album);
    }
    if let Some(id) = &sound.apple_id {
        track.ids.insert("apple".into(), id.clone());
        track.links.push(service_link(Service::AppleMusic, format!("https://music.apple.com/song/{id}")));
    }
    if let Some(id) = &sound.spotify_id {
        track.ids.insert("spotify".into(), id.clone());
        track.links.push(service_link(Service::Spotify, format!("https://open.spotify.com/track/{id}")));
    }
    Some(finish_track(track))
}

/// Hints for a sound. "Original sound" labels say nothing about the song, so
/// TikTok's matched ids are looked up for title and artist.
async fn hints(ctx: &Ctx<'_>, sound: &Sound) -> Result<Vec<Track>> {
    let Some(mut track) = sound_track(sound) else {
        return Ok(Vec::new());
    };
    if track.title.is_empty() {
        match lookup_title(ctx, sound).await {
            Ok(Some(found)) => {
                track.title = found.title;
                track.artist = found.artist;
                track.artists = found.artists;
                track.album = found.album;
                track.duration_ms = found.duration_ms;
                track.artwork_url = found.artwork_url;
            }
            Ok(None) => {}
            Err(MfError::Canceled) => return Err(MfError::Canceled),
            Err(e) => tracing::debug!("tiktok sound lookup failed: {e}"),
        }
    }
    if track.title.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![finish_track(track)])
}

async fn lookup_title(ctx: &Ctx<'_>, sound: &Sound) -> Result<Option<Track>> {
    if let Some(id) = &sound.spotify_id {
        let url = Url::parse(&format!("https://open.spotify.com/track/{id}"))
            .map_err(|_| MfError::internal("spotify url"))?;
        if let Ok(resolution) = super::spotify::resolve(ctx, &url).await {
            return Ok(super::spotify::first_track(resolution));
        }
    }
    if let Some(id) = &sound.apple_id {
        let results = itunes::lookup(ctx.http, id, &ctx.country, false, ITUNES_QUEUE, ctx.cancel).await?;
        return Ok(results.iter().find_map(itunes::track_from_json));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("{}/tests/fixtures/links/{name}", env!("CARGO_MANIFEST_DIR"))).expect("fixture")
    }

    #[test]
    fn parses_song_attribution() {
        let page = parse_page(&fixture("tiktok_video.html"));
        assert!(!page.missing);
        assert_eq!(page.uploader.as_deref(), Some("KATSEYE"));
        assert_eq!(page.duration_ms, Some(29_000));
        let sound = page.sound.expect("sound");
        assert_eq!(sound.title.as_deref(), Some("Touch"));
        assert_eq!(sound.author.as_deref(), Some("KATSEYE"));
        assert!(!sound.original);
        assert_eq!(sound.apple_id.as_deref(), Some("1753359801"));
        assert_eq!(sound.spotify_id.as_deref(), Some("6aJn7Cst74cj4lNIiPRgav"));
        let track = sound_track(&sound).expect("hint");
        assert_eq!(track.title, "Touch");
        assert_eq!(track.artist, "KATSEYE");
        assert_eq!(track.ids.get("spotify").map(String::as_str), Some("6aJn7Cst74cj4lNIiPRgav"));
        assert_eq!(track.links.len(), 2);
        assert!(track.identity_key.starts_with("am::"));
    }

    #[test]
    fn original_sound_without_match_has_no_hint() {
        let page = parse_page(&fixture("tiktok_original.html"));
        let sound = page.sound.expect("sound");
        assert!(sound.original);
        assert!(sound_track(&sound).is_none());
        // An original sound TikTok matched keeps only the ids until looked up.
        let matched = Sound {
            original: true,
            title: Some("original sound - someone".into()),
            apple_id: Some("1".into()),
            ..Sound::default()
        };
        let track = sound_track(&matched).unwrap();
        assert!(track.title.is_empty());
        assert_eq!(track.ids.get("apple").map(String::as_str), Some("1"));
    }

    #[test]
    fn missing_videos() {
        let html = r#"<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">{"__DEFAULT_SCOPE__":{"webapp.video-detail":{"statusCode":10204,"statusMsg":"item doesn't exist"}}}</script>"#;
        assert!(parse_page(html).missing);
        assert_eq!(parse_page("<html></html>"), VideoPage::default());
    }

    #[test]
    fn truncates_long_captions() {
        assert_eq!(truncate("abc", 5), "abc");
        assert_eq!(truncate("abcdef", 3), "abc…");
    }
}
