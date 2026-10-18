//! YouTube and YouTube Music videos: page info plus attribution hints from the
//! watch page (the description's "Music" cards, the Shorts sound and art tracks
//! from "Artist - Topic" channels). The audio goes to the media backend.
//!
//! Requests ask for English (`hl=en`) so section titles are predictable and send
//! the consent cookies EU visitors otherwise get a consent wall for (the
//! `youtube` header rule).

use serde_json::Value;
use url::Url;

use super::fetch::{self, PageRequest};
use super::html::{json_after, str_at, text_of, u64_at};
use super::media_sites::{self, OEmbed};
use super::{Ctx, Resolution, Site, canonical, dedup_tracks, finish_track, service_link, source_info};
use crate::error::{MfError, Result};
use crate::model::{Service, SourceInfo, Track};
use crate::util::http::HeaderRuleId;

const SITE: &str = "YouTube";
const TOPIC_SUFFIX: &str = " - Topic";

/// The Shorts "sound" button.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ShortsSound {
    pub title: String,
    /// Video of the sound's source track (usually a Topic art track).
    pub video_id: Option<String>,
}

/// What a watch page says.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct WatchPage {
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub thumbnail_url: Option<String>,
    pub duration_ms: Option<u64>,
    /// Set when YouTube reports the video as unavailable.
    pub unavailable: Option<String>,
    /// Songs from the description's "Music" section.
    pub cards: Vec<Track>,
    /// The video itself when it is an auto-generated art track.
    pub art_track: Option<Track>,
    pub shorts_sound: Option<ShortsSound>,
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url, site: Site, shorts: bool) -> Result<Resolution> {
    let mut source = source_info(url, site);
    let Some(id) = canonical::youtube_video_id(url) else {
        // Playlists and channels: the media backend lists entries.
        source.item_count = 0;
        apply_oembed(&mut source, media_sites::youtube_oembed(ctx, url.as_str()).await?);
        return Ok(Resolution::Media { source, url: url.to_string(), direct: false });
    };
    let page_url = if shorts {
        format!("https://www.youtube.com/shorts/{id}?hl=en")
    } else {
        format!("https://www.youtube.com/watch?v={id}&hl=en&persist_hl=1")
    };
    let req = PageRequest::new(SITE).rule(HeaderRuleId::Youtube);
    match fetch::get_page(ctx.http, &page_url, &req, ctx.cancel).await {
        Ok(page) if page.status.is_success() => {
            let watch = parse_watch_page(&page.body);
            if let Some(reason) = watch.unavailable {
                return Err(MfError::MediaUnavailable { message: reason });
            }
            source.title = watch.title;
            source.uploader = watch.uploader;
            source.thumbnail_url = watch.thumbnail_url;
            source.duration_ms = watch.duration_ms;
            let mut hints = Vec::new();
            hints.extend(watch.art_track);
            hints.extend(watch.cards);
            if let Some(sound) = &watch.shorts_sound
                && let Some(hint) = shorts_hint(ctx, sound).await?
            {
                hints.push(hint);
            }
            source.hints = dedup_tracks(hints);
        }
        Ok(page) => tracing::debug!("youtube page answered {}", page.status),
        Err(MfError::Canceled) => return Err(MfError::Canceled),
        Err(e) => tracing::debug!("youtube page failed: {e}"),
    }
    if source.title.is_none() {
        apply_oembed(&mut source, media_sites::youtube_oembed(ctx, url.as_str()).await?);
    }
    Ok(Resolution::Media { source, url: url.to_string(), direct: false })
}

fn apply_oembed(source: &mut SourceInfo, oembed: Option<OEmbed>) {
    if let Some(info) = oembed {
        source.title = source.title.take().or(info.title);
        source.uploader = source.uploader.take().or(info.author);
        source.thumbnail_url = source.thumbnail_url.take().or(info.thumbnail_url);
    }
}

/// Depth-first search for the first value under `key`.
fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => map.get(key).or_else(|| map.values().find_map(|v| find_key(v, key))),
        Value::Array(items) => items.iter().find_map(|v| find_key(v, key)),
        _ => None,
    }
}

fn largest_thumbnail(details: &Value) -> Option<String> {
    details
        .pointer("/thumbnail/thumbnails")
        .and_then(Value::as_array)?
        .iter()
        .max_by_key(|t| t.get("width").and_then(Value::as_u64).unwrap_or(0))
        .and_then(|t| str_at(t, "/url"))
}

fn youtube_music_link(video_id: &str) -> crate::model::ServiceLink {
    service_link(Service::YouTubeMusic, format!("https://music.youtube.com/watch?v={video_id}"))
}

/// Songs in the description's "Music" section
/// (`engagementPanels → structuredDescriptionContentRenderer → horizontalCardListRenderer`).
fn music_cards(data: &Value) -> Vec<Track> {
    let mut tracks = Vec::new();
    let panels = data.get("engagementPanels").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
    for panel in panels {
        let items = panel
            .pointer("/engagementPanelSectionListRenderer/content/structuredDescriptionContentRenderer/items")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for list in items.iter().filter_map(|item| item.get("horizontalCardListRenderer")) {
            let header = list.pointer("/header/richListHeaderRenderer/title").and_then(text_of);
            let icon = list.pointer("/footerButton/buttonViewModel/iconName").and_then(Value::as_str);
            if !(header.as_deref().is_some_and(|h| h.eq_ignore_ascii_case("music")) || icon == Some("MUSIC")) {
                continue;
            }
            let cards = list.get("cards").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
            for card in cards.iter().filter_map(|c| c.get("videoAttributeViewModel")) {
                let Some(title) = card.get("title").and_then(text_of) else {
                    continue;
                };
                let track = Track {
                    title,
                    artist: card.get("subtitle").and_then(text_of).unwrap_or_default(),
                    album: card.get("secondarySubtitle").and_then(text_of),
                    artwork_url: str_at(card, "/image/sources/0/url"),
                    ..Track::default()
                };
                tracks.push(finish_track(track));
            }
        }
    }
    tracks
}

fn shorts_sound(data: &Value) -> Option<ShortsSound> {
    let title = data.get("overlay").and_then(|o| find_key(o, "soundAttributionTitle")).and_then(text_of)?;
    let video_id = data
        .get("engagementPanels")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(|p| p.get("engagementPanelSectionListRenderer"))
        .find(|p| p.get("targetId").and_then(Value::as_str) == Some("engagement-panel-shorts-audio-pivot"))
        .and_then(|p| p.pointer("/content/richGridRenderer/header"))
        .and_then(|h| find_key(h, "watchEndpoint"))
        .and_then(|w| str_at(w, "/videoId"));
    Some(ShortsSound { title, video_id })
}

/// Parses `ytInitialPlayerResponse` and `ytInitialData` from a watch or Shorts page.
pub(super) fn parse_watch_page(html: &str) -> WatchPage {
    let player =
        json_after(html, "var ytInitialPlayerResponse =").or_else(|| json_after(html, "ytInitialPlayerResponse ="));
    let data = json_after(html, "var ytInitialData =").or_else(|| json_after(html, "window[\"ytInitialData\"] ="));
    let mut page = WatchPage::default();
    if let Some(player) = &player {
        if player.pointer("/playabilityStatus/status").and_then(Value::as_str) == Some("ERROR") {
            let reason = str_at(player, "/playabilityStatus/reason").unwrap_or_else(|| "Video unavailable".into());
            page.unavailable = Some(format!("{}.", reason.trim_end_matches('.')));
            return page;
        }
        if let Some(details) = player.get("videoDetails") {
            page.title = str_at(details, "/title");
            page.uploader = str_at(details, "/author");
            page.thumbnail_url = largest_thumbnail(details);
            page.duration_ms = u64_at(details, "/lengthSeconds").filter(|s| *s > 0).map(|s| s * 1000);
            if let (Some(title), Some(author), Some(video_id)) =
                (&page.title, &page.uploader, str_at(details, "/videoId"))
                && let Some(artist) = author.strip_suffix(TOPIC_SUFFIX)
            {
                let mut track = Track {
                    title: title.clone(),
                    artist: artist.to_string(),
                    duration_ms: page.duration_ms,
                    artwork_url: page.thumbnail_url.clone(),
                    links: vec![youtube_music_link(&video_id)],
                    ..Track::default()
                };
                track.ids.insert("youtube".into(), video_id);
                page.art_track = Some(finish_track(track));
            }
        }
    }
    if let Some(data) = &data {
        page.cards = music_cards(data);
        page.shorts_sound = shorts_sound(data);
    }
    page
}

/// "Original sound" carries no song; a "Song · Artist" label carries both.
fn sound_label(title: &str) -> Option<(String, Option<String>)> {
    if title.to_lowercase().contains("original sound") {
        return None;
    }
    match title.split_once(" · ") {
        Some((song, artist)) => Some((song.trim().to_string(), Some(artist.trim().to_string()))),
        None => Some((title.trim().to_string(), None)),
    }
}

/// The Shorts sound as a hint; the artist comes from the source video's channel
/// ("Artist - Topic") when the label does not name one.
async fn shorts_hint(ctx: &Ctx<'_>, sound: &ShortsSound) -> Result<Option<Track>> {
    let Some((title, mut artist)) = sound_label(&sound.title) else {
        return Ok(None);
    };
    let mut track = Track { title, ..Track::default() };
    if let Some(video_id) = &sound.video_id {
        let watch = format!("https://www.youtube.com/watch?v={video_id}");
        if let Some(info) = media_sites::youtube_oembed(ctx, &watch).await? {
            let topic = info.author.as_deref().and_then(|a| a.strip_suffix(TOPIC_SUFFIX)).map(str::to_string);
            if artist.is_none() {
                artist = topic.clone();
            }
            if topic.is_some() {
                track.links.push(youtube_music_link(video_id));
                track.ids.insert("youtube".into(), video_id.clone());
            }
        }
    }
    let Some(artist) = artist else {
        return Ok(None);
    };
    track.artist = artist;
    Ok(Some(finish_track(track)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("{}/tests/fixtures/links/{name}", env!("CARGO_MANIFEST_DIR"))).expect("fixture")
    }

    #[test]
    fn official_video_has_one_music_card() {
        let page = parse_watch_page(&fixture("youtube_watch_card.html"));
        assert_eq!(page.title.as_deref(), Some("Rick Astley - Never Gonna Give You Up (Official Video) (4K Remaster)"));
        assert_eq!(page.uploader.as_deref(), Some("Rick Astley"));
        assert_eq!(page.duration_ms, Some(213_000));
        assert!(page.thumbnail_url.is_some());
        assert_eq!(page.cards.len(), 1);
        let card = &page.cards[0];
        assert_eq!(card.title, "Never Gonna Give You Up (7\" Mix)");
        assert_eq!(card.artist, "Rick Astley");
        assert_eq!(card.album.as_deref(), Some("Whenever You Need Somebody"));
        assert!(page.art_track.is_none());
        assert!(page.shorts_sound.is_none());
    }

    #[test]
    fn mix_lists_several_cards() {
        let page = parse_watch_page(&fixture("youtube_watch_mix.html"));
        assert!(page.cards.len() >= 5, "got {}", page.cards.len());
        assert!(page.cards.iter().any(|t| t.artist == "MEDUZA"));
        assert!(page.cards.iter().all(|t| !t.title.is_empty()));
    }

    #[test]
    fn shorts_sound_and_pivot_video() {
        let page = parse_watch_page(&fixture("youtube_shorts.html"));
        let sound = page.shorts_sound.expect("sound");
        assert_eq!(sound.title, "Hootie Frutti");
        assert_eq!(sound.video_id.as_deref(), Some("HY397w59MT4"));
        assert_eq!(page.duration_ms, Some(14_000));
    }

    #[test]
    fn art_tracks_and_errors() {
        let html = r#"<script>var ytInitialPlayerResponse = {"playabilityStatus":{"status":"OK"},"videoDetails":{"videoId":"HY397w59MT4","title":"Hootie Frutti","author":"KATSEYE - Topic","lengthSeconds":"150","thumbnail":{"thumbnails":[{"url":"https://i.ytimg.com/a.jpg","width":120},{"url":"https://i.ytimg.com/b.jpg","width":480}]}}};</script>"#;
        let page = parse_watch_page(html);
        let art = page.art_track.expect("art track");
        assert_eq!(art.artist, "KATSEYE");
        assert_eq!(art.links[0].url, "https://music.youtube.com/watch?v=HY397w59MT4");
        assert_eq!(page.thumbnail_url.as_deref(), Some("https://i.ytimg.com/b.jpg"));
        let gone = r#"<script>var ytInitialPlayerResponse = {"playabilityStatus":{"status":"ERROR","reason":"Video unavailable"}};</script>"#;
        assert_eq!(parse_watch_page(gone).unavailable.as_deref(), Some("Video unavailable."));
        assert_eq!(parse_watch_page("<html></html>"), WatchPage::default());
    }

    #[test]
    fn sound_labels() {
        assert_eq!(sound_label("original sound - someone"), None);
        assert_eq!(sound_label("Original Sound"), None);
        assert_eq!(sound_label("Song · Artist"), Some(("Song".into(), Some("Artist".into()))));
        assert_eq!(sound_label("Hootie Frutti"), Some(("Hootie Frutti".into(), None)));
    }
}
