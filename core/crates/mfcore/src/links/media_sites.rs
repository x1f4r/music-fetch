//! Sites the media backend downloads from (SoundCloud, Vimeo, Mixcloud,
//! Bandcamp). Only cheap page info is fetched here; failures leave it out so the
//! backend still gets its chance.

use serde_json::Value;
use url::Url;

use super::fetch::{self, PageRequest};
use super::html::{Document, parse_iso_duration, str_at, u64_at};
use super::tidal::ld_names;
use super::{Ctx, Resolution, Site, canonical, finish_track, join_artists, source_info};
use crate::error::{MfError, Result};
use crate::model::{SourceInfo, Track};

/// The useful part of an oEmbed response.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct OEmbed {
    pub title: Option<String>,
    pub author: Option<String>,
    pub thumbnail_url: Option<String>,
    pub duration_ms: Option<u64>,
}

pub(super) fn parse_oembed(value: &Value) -> OEmbed {
    OEmbed {
        title: str_at(value, "/title"),
        author: str_at(value, "/author_name"),
        thumbnail_url: str_at(value, "/thumbnail_url"),
        duration_ms: u64_at(value, "/duration")
            .filter(|d| *d > 0)
            .map(|d| d * 1000),
    }
}

fn encode(text: &str) -> String {
    url::form_urlencoded::byte_serialize(text.as_bytes()).collect()
}

/// GETs an oEmbed endpoint (`endpoint` ends with the `url=` parameter). Only
/// cancellation is an error; anything else means no info.
async fn oembed(ctx: &Ctx<'_>, endpoint: &str, target: &str, site: &str) -> Result<Option<OEmbed>> {
    match fetch::fetch_json(
        ctx.http,
        &format!("{endpoint}{}", encode(target)),
        site,
        ctx.cancel,
    )
    .await
    {
        Ok(response) if response.status.is_success() => {
            Ok(response.value.as_ref().map(parse_oembed))
        }
        Ok(_) => Ok(None),
        Err(MfError::Canceled) => Err(MfError::Canceled),
        Err(e) => {
            tracing::debug!("{site} oEmbed failed: {e}");
            Ok(None)
        }
    }
}

/// YouTube oEmbed (videos and playlists).
pub(super) async fn youtube_oembed(ctx: &Ctx<'_>, target: &str) -> Result<Option<OEmbed>> {
    oembed(
        ctx,
        "https://www.youtube.com/oembed?format=json&url=",
        target,
        "YouTube",
    )
    .await
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url, site: Site) -> Result<Resolution> {
    let mut source = source_info(url, site);
    let result = match site {
        Site::SoundCloud => soundcloud(ctx, url, &mut source).await,
        Site::Vimeo => vimeo(ctx, url, &mut source).await,
        Site::Mixcloud => mixcloud(ctx, url, &mut source).await,
        Site::Bandcamp => bandcamp(ctx, url, &mut source).await,
        _ => Ok(()),
    };
    match result {
        Err(MfError::Canceled) => return Err(MfError::Canceled),
        Err(e) => tracing::debug!("page info for {site:?} failed: {e}"),
        Ok(()) => {}
    }
    Ok(Resolution::Media {
        source,
        url: url.to_string(),
        direct: false,
    })
}

fn apply(source: &mut SourceInfo, info: OEmbed) {
    source.title = info.title;
    source.uploader = info.author;
    source.thumbnail_url = info.thumbnail_url;
    source.duration_ms = info.duration_ms;
}

async fn soundcloud(ctx: &Ctx<'_>, url: &Url, source: &mut SourceInfo) -> Result<()> {
    // Profiles and sets hold many tracks.
    let parts = canonical::segments(url);
    if parts.len() == 1 || parts.get(1) == Some(&"sets") {
        source.item_count = 0;
    }
    if let Some(mut info) = oembed(
        ctx,
        "https://soundcloud.com/oembed?format=json&url=",
        url.as_str(),
        "SoundCloud",
    )
    .await?
    {
        // Titles read "<track> by <uploader>".
        if let (Some(title), Some(author)) = (&info.title, &info.author)
            && let Some(stripped) = title.strip_suffix(&format!(" by {author}"))
        {
            info.title = Some(stripped.to_string());
        }
        apply(source, info);
    }
    Ok(())
}

async fn vimeo(ctx: &Ctx<'_>, url: &Url, source: &mut SourceInfo) -> Result<()> {
    if let Some(info) = oembed(
        ctx,
        "https://vimeo.com/api/oembed.json?url=",
        url.as_str(),
        "Vimeo",
    )
    .await?
    {
        apply(source, info);
    }
    Ok(())
}

/// Mixcloud's public API: show info and, when the uploader added one, the tracklist.
async fn mixcloud(ctx: &Ctx<'_>, url: &Url, source: &mut SourceInfo) -> Result<()> {
    let parts = canonical::segments(url);
    if parts.len() != 2 {
        return Ok(());
    }
    let api = format!("https://api.mixcloud.com/{}/{}/", parts[0], parts[1]);
    let value = fetch::get_json(ctx.http, &api, "Mixcloud", ctx.cancel).await?;
    apply_mixcloud(&value, source);
    Ok(())
}

pub(super) fn apply_mixcloud(value: &Value, source: &mut SourceInfo) {
    source.title = str_at(value, "/name");
    source.uploader = str_at(value, "/user/name");
    source.thumbnail_url =
        str_at(value, "/pictures/extra_large").or_else(|| str_at(value, "/pictures/large"));
    source.duration_ms = u64_at(value, "/audio_length")
        .filter(|d| *d > 0)
        .map(|d| d * 1000);
    source.hints = value
        .get("sections")
        .and_then(Value::as_array)
        .map(|sections| {
            sections
                .iter()
                .filter_map(|s| {
                    let title = str_at(s, "/track/name")?;
                    let artist = str_at(s, "/track/artist/name").unwrap_or_default();
                    Some(finish_track(Track {
                        title,
                        artist,
                        ..Track::default()
                    }))
                })
                .collect()
        })
        .unwrap_or_default();
}

async fn bandcamp(ctx: &Ctx<'_>, url: &Url, source: &mut SourceInfo) -> Result<()> {
    let page = fetch::get_page(
        ctx.http,
        url.as_str(),
        &PageRequest::new("Bandcamp"),
        ctx.cancel,
    )
    .await?;
    let page = fetch::require_success(page, "Bandcamp")?;
    apply_bandcamp(&page.body, source);
    Ok(())
}

/// Bandcamp pages: JSON-LD `MusicRecording`/`MusicAlbum`, Open Graph as fallback.
/// A track page's song becomes a hint.
pub(super) fn apply_bandcamp(html: &str, source: &mut SourceInfo) {
    let doc = Document::parse(html);
    source.thumbnail_url = doc.meta("og:image").map(str::to_string);
    if let Some(ld) = doc.json_ld_of(&["MusicRecording"]).next() {
        let artists = ld_names(ld.get("byArtist"));
        let track = finish_track(Track {
            title: str_at(ld, "/name").unwrap_or_default(),
            artists,
            album: ld
                .pointer("/inAlbum/name")
                .and_then(Value::as_str)
                .map(str::to_string),
            duration_ms: str_at(ld, "/duration")
                .as_deref()
                .and_then(parse_iso_duration),
            artwork_url: source.thumbnail_url.clone(),
            ..Track::default()
        });
        source.title = Some(track.title.clone()).filter(|t| !t.is_empty());
        source.uploader = Some(track.artist.clone()).filter(|a| !a.is_empty());
        source.duration_ms = track.duration_ms;
        if !track.title.is_empty() && !track.artist.is_empty() {
            source.hints = vec![track];
        }
    } else if let Some(ld) = doc.json_ld_of(&["MusicAlbum"]).next() {
        source.title = str_at(ld, "/name");
        source.uploader =
            Some(join_artists(&ld_names(ld.get("byArtist")))).filter(|a| !a.is_empty());
        source.item_count = u64_at(ld, "/numTracks").map_or(0, |n| n as u32);
    } else {
        source.title = doc.meta("og:title").map(str::to_string);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oembed_fields() {
        let v = serde_json::json!({"title": "Flickermood by Forss", "author_name": "Forss", "thumbnail_url": "https://i1.sndcdn.com/a.jpg", "duration": 120});
        let info = parse_oembed(&v);
        assert_eq!(info.author.as_deref(), Some("Forss"));
        assert_eq!(info.duration_ms, Some(120_000));
        assert_eq!(parse_oembed(&serde_json::json!({})), OEmbed::default());
    }

    #[test]
    fn mixcloud_tracklist_becomes_hints() {
        let v = serde_json::json!({
            "name": "Party Time", "user": {"name": "Spartacus"}, "audio_length": 3361,
            "pictures": {"extra_large": "https://thumbnailer.mixcloud.com/x.png"},
            "sections": [{"track": {"name": "Song A", "artist": {"name": "Artist A"}}, "start_time": 0}, {"section_type": "chapter"}]
        });
        let mut source = SourceInfo::default();
        apply_mixcloud(&v, &mut source);
        assert_eq!(source.title.as_deref(), Some("Party Time"));
        assert_eq!(source.duration_ms, Some(3_361_000));
        assert_eq!(source.hints.len(), 1);
        assert_eq!(source.hints[0].artist, "Artist A");
    }

    #[test]
    fn bandcamp_track_page() {
        let html = r#"<meta property="og:image" content="https://f4.bcbits.com/img/a10_10.jpg">
            <script type="application/ld+json">{"@type":"MusicRecording","name":"Song","byArtist":{"@type":"MusicGroup","name":"Band"},"duration":"P00H03M05S","inAlbum":{"name":"LP"}}</script>"#;
        let mut source = SourceInfo::default();
        apply_bandcamp(html, &mut source);
        assert_eq!(source.title.as_deref(), Some("Song"));
        assert_eq!(source.uploader.as_deref(), Some("Band"));
        assert_eq!(source.duration_ms, Some(185_000));
        assert_eq!(source.hints[0].album.as_deref(), Some("LP"));
    }
}
