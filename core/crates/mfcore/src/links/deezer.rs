//! Deezer tracks, albums and playlists through the public API (with ISRCs).

use url::Url;

use super::{Ctx, Resolution, canonical};
use crate::catalog::deezer;
use crate::error::{MfError, Result};
use crate::model::SourceInfo;

/// `(kind, id)` of a canonical Deezer link.
pub(super) fn entity(url: &Url) -> Option<(&'static str, String)> {
    let parts = canonical::segments(url);
    let kind = match *parts.first()? {
        "track" => "track",
        "album" => "album",
        "playlist" => "playlist",
        _ => return None,
    };
    let id = parts.get(1).filter(|id| !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()))?;
    Some((kind, (*id).to_string()))
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url) -> Result<Resolution> {
    let (kind, id) = entity(url).ok_or_else(|| MfError::UnsupportedLink {
        message: "Only Deezer tracks, albums and playlists are supported.".into(),
    })?;
    let (mut source, tracks) = match kind {
        "track" => {
            let track = deezer::track(ctx.http, &id, ctx.cancel).await?;
            let source = SourceInfo {
                url: Some(format!("https://www.deezer.com/track/{id}")),
                title: Some(track.title.clone()),
                uploader: Some(track.artist.clone()).filter(|a| !a.is_empty()),
                thumbnail_url: track.artwork_url.clone(),
                duration_ms: track.duration_ms,
                service: Some(crate::model::Service::Deezer),
                site: Some("Deezer".into()),
                ..SourceInfo::default()
            };
            (source, vec![track])
        }
        "album" => deezer::album(ctx.http, &id, ctx.cancel).await?,
        _ => deezer::playlist(ctx.http, &id, ctx.cancel).await?,
    };
    if tracks.is_empty() {
        return Err(MfError::MediaUnavailable { message: "This Deezer link has no playable tracks.".into() });
    }
    source.item_count = tracks.len() as u32;
    if source.duration_ms.is_none() {
        source.duration_ms = Some(tracks.iter().filter_map(|t| t.duration_ms).sum()).filter(|d| *d > 0);
    }
    Ok(Resolution::Tracks { source, tracks })
}

/// Tracks of an album found on Deezer by title and artist, for services whose
/// album pages list no tracks. `source` stays the original link.
pub(super) async fn album_via_deezer(
    ctx: &Ctx<'_>,
    title: &str,
    artist: &str,
    mut source: SourceInfo,
) -> Result<Resolution> {
    let not_found = || MfError::MediaUnavailable { message: "Couldn't find the tracks on this album.".into() };
    let id = deezer::find_album(ctx.http, title, artist, ctx.cancel).await?.ok_or_else(not_found)?;
    let (album, tracks) = deezer::album(ctx.http, &id, ctx.cancel).await?;
    if tracks.is_empty() {
        return Err(not_found());
    }
    source.title = source.title.or(album.title);
    source.uploader = source.uploader.or(album.uploader);
    source.thumbnail_url = source.thumbnail_url.or(album.thumbnail_url);
    source.item_count = tracks.len() as u32;
    source.duration_ms = Some(tracks.iter().filter_map(|t| t.duration_ms).sum()).filter(|d| *d > 0);
    Ok(Resolution::Tracks { source, tracks })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_entities() {
        let e = |s: &str| entity(&Url::parse(s).unwrap());
        assert_eq!(e("https://www.deezer.com/track/3135556"), Some(("track", "3135556".into())));
        assert_eq!(e("https://www.deezer.com/playlist/13957557901"), Some(("playlist", "13957557901".into())));
        assert_eq!(e("https://www.deezer.com/artist/27"), None);
        assert_eq!(e("https://www.deezer.com/track/abc"), None);
    }
}
