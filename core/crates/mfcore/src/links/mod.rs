//! Links: canonicalization, shortener resolution with SSRF guard, and
//! site-specific resolvers.
//!
//! `resolve` turns whatever the user pasted into either track metadata
//! (streaming services) or a media URL for the media backend, plus whatever
//! cheap page information exists (title, uploader, thumbnail, attribution hints).

mod amazon;
mod apple;
mod canonical;
mod deezer;
pub(crate) mod fetch;
mod generic;
mod html;
mod media_sites;
mod site;
mod spotify;
mod ssrf;
mod tidal;
mod tiktok;
mod youtube;

use std::future::Future;
use std::pin::Pin;

use serde::Serialize;
use url::Url;

pub use canonical::{
    SHORTENER_HOSTS, TRACKING_QUERY_KEYS, canonicalize, extract_url, looks_like_url,
};
pub use fetch::http_client;
pub(crate) use html::year_of;
pub use site::Site;
pub use ssrf::{assert_public_url, ensure_public_url, is_public_ip};

use crate::error::{MfError, Result};
use crate::identity;
use crate::model::{Service, ServiceLink, SourceInfo, Track};
use crate::util::cancel::CancelToken;

/// What a link resolves to.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Resolution {
    /// Tracks known from service metadata, no audio needed (Spotify, Apple Music,
    /// Deezer, Tidal, Amazon Music track/album/playlist pages).
    Tracks {
        source: SourceInfo,
        tracks: Vec<Track>,
    },
    /// Media to fetch: through the media backend, or directly when `direct` is
    /// true (a plain audio/video file URL).
    Media {
        source: SourceInfo,
        url: String,
        direct: bool,
    },
}

impl Resolution {
    /// What the link turned out to be.
    pub fn source(&self) -> &SourceInfo {
        match self {
            Resolution::Tracks { source, .. } | Resolution::Media { source, .. } => source,
        }
    }
}

/// Everything a resolver needs.
pub(crate) struct Ctx<'a> {
    pub http: &'a reqwest::Client,
    /// Lowercase ISO 3166-1 alpha-2 storefront.
    pub country: String,
    pub cancel: &'a CancelToken,
}

/// Canonicalize, expand shorteners, then route to a resolver.
///
/// `url` may be shared text containing a link ("Check this out https://…").
/// `country` is the storefront used where a link does not name one.
pub async fn resolve(
    http: &reqwest::Client,
    url: &str,
    country: &str,
    cancel: &CancelToken,
) -> Result<Resolution> {
    let text =
        extract_url(url).ok_or_else(|| MfError::invalid("That doesn't look like a link."))?;
    let parsed = canonical::parse_http_url(&text)?;
    let ctx = Ctx {
        http,
        country: storefront(country),
        cancel,
    };
    resolve_url(&ctx, parsed, 0).await
}

/// Lowercase two-letter storefront, "us" when `country` is not one.
pub(crate) fn storefront(country: &str) -> String {
    let cc = country.trim().to_ascii_lowercase();
    if cc.len() == 2 && cc.chars().all(|c| c.is_ascii_lowercase()) {
        cc
    } else {
        "us".to_string()
    }
}

type BoxedResolution<'a> = Pin<Box<dyn Future<Output = Result<Resolution>> + Send + 'a>>;

fn resolve_url<'a>(ctx: &'a Ctx<'a>, url: Url, depth: u8) -> BoxedResolution<'a> {
    Box::pin(async move {
        ctx.cancel.check()?;
        let mut url = canonical::unwrap_wrapper(&url).unwrap_or(url);
        if canonical::is_shortener(&url) {
            url = fetch::expand_short_url(&url, ctx.cancel).await?;
            url = canonical::unwrap_wrapper(&url).unwrap_or(url);
        }
        let shorts = is_youtube_shorts(&url);
        let canonical_text = canonical::canonical_url(&url);
        let canonical =
            Url::parse(&canonical_text).map_err(|_| MfError::invalid("That link isn't valid."))?;
        fetch::cancellable(ctx.cancel, ssrf::ensure_public(&canonical)).await??;
        let site = Site::of_host(&canonical::host_of(&canonical));
        match site {
            Site::Spotify => spotify::resolve(ctx, &canonical).await,
            Site::AppleMusic => apple::resolve(ctx, &canonical).await,
            Site::Deezer => deezer::resolve(ctx, &canonical).await,
            Site::Tidal => tidal::resolve(ctx, &canonical).await,
            Site::AmazonMusic => amazon::resolve(ctx, &canonical).await,
            Site::TikTok => tiktok::resolve(ctx, &canonical).await,
            Site::YouTube | Site::YouTubeMusic => {
                youtube::resolve(ctx, &canonical, site, shorts).await
            }
            Site::SoundCloud | Site::Vimeo | Site::Mixcloud | Site::Bandcamp => {
                media_sites::resolve(ctx, &canonical, site).await
            }
            Site::Instagram | Site::X | Site::Reddit | Site::Twitch | Site::Facebook => {
                Ok(Resolution::Media {
                    source: source_info(&canonical, site),
                    url: canonical_text,
                    direct: false,
                })
            }
            Site::Other => match generic::resolve(ctx, &canonical).await? {
                generic::Outcome::Done(resolution) => Ok(resolution),
                generic::Outcome::Embed { target, fallback } if depth == 0 => {
                    match resolve_url(ctx, target, depth + 1).await {
                        Ok(resolution) => Ok(resolution),
                        Err(e @ (MfError::Canceled | MfError::InvalidInput { .. })) => Err(e),
                        Err(e) => {
                            tracing::debug!("embedded player could not be resolved: {e}");
                            Ok(fallback)
                        }
                    }
                }
                generic::Outcome::Embed { fallback, .. } => Ok(fallback),
            },
        }
    })
}

fn is_youtube_shorts(url: &Url) -> bool {
    let site = Site::of_host(&canonical::host_of(url));
    matches!(site, Site::YouTube | Site::YouTubeMusic)
        && canonical::segments(url).first() == Some(&"shorts")
}

/// Base `SourceInfo` for a canonical link.
pub(crate) fn source_info(url: &Url, site: Site) -> SourceInfo {
    SourceInfo {
        url: Some(url.to_string()),
        service: Some(site.service()),
        site: Some(site.display_name(&canonical::host_of(url))),
        item_count: 1,
        ..SourceInfo::default()
    }
}

/// A direct link to a track on a service.
pub(crate) fn service_link(service: Service, url: impl Into<String>) -> ServiceLink {
    ServiceLink {
        service,
        url: url.into(),
        is_search: false,
    }
}

/// Display string for credited artists: "A", "A & B", "A, B & C".
pub(crate) fn join_artists(artists: &[String]) -> String {
    match artists {
        [] => String::new(),
        [one] => one.clone(),
        [init @ .., last] => format!("{} & {last}", init.join(", ")),
    }
}

/// Trims text fields, fills `artist`/`artists` from each other and sets the identity key.
pub(crate) fn finish_track(mut track: Track) -> Track {
    track.title = track.title.trim().to_string();
    track.artists.retain(|a| !a.trim().is_empty());
    for artist in &mut track.artists {
        *artist = artist.trim().to_string();
    }
    track.artists.dedup();
    track.artist = track.artist.trim().to_string();
    if track.artist.is_empty() {
        track.artist = join_artists(&track.artists);
    }
    if track.artists.is_empty() && !track.artist.is_empty() {
        track.artists.push(track.artist.clone());
    }
    track.album = track
        .album
        .take()
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty());
    identity::refresh_identity(&mut track);
    track
}

/// Removes hints that describe the same recording as an earlier one.
pub(crate) fn dedup_tracks(tracks: Vec<Track>) -> Vec<Track> {
    let mut out: Vec<Track> = Vec::with_capacity(tracks.len());
    for track in tracks {
        if !out.iter().any(|t| identity::same_track(t, &track)) {
            out.push(track);
        }
    }
    out
}

/// A link to `at_ms` within the source, when the site supports it
/// (YouTube `&t=`, SoundCloud `#t=m:ss`).
pub fn deep_link(url: &str, at_ms: u64) -> Option<String> {
    let parsed = canonical::parse_http_url(url).ok()?;
    let canonical = Url::parse(&canonical::canonical_url(&parsed)).ok()?;
    let seconds = at_ms / 1000;
    match Site::of_host(&canonical::host_of(&canonical)) {
        Site::YouTube | Site::YouTubeMusic => {
            let id = canonical::youtube_video_id(&canonical)?;
            Some(format!("https://www.youtube.com/watch?v={id}&t={seconds}s"))
        }
        Site::SoundCloud if canonical::host_of(&canonical) == "soundcloud.com" => {
            // Tracks only: /<user>/<track>, not profiles or /<user>/sets/<playlist>.
            let parts = canonical::segments(&canonical);
            (parts.len() == 2 && parts[1] != "sets").then(|| {
                format!(
                    "https://soundcloud.com{}#t={}:{:02}",
                    canonical.path(),
                    seconds / 60,
                    seconds % 60
                )
            })
        }
        // Mixcloud has no documented way to open a show at a time.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_links() {
        assert_eq!(
            deep_link("https://youtu.be/dQw4w9WgXcQ?si=x", 83_900).as_deref(),
            Some("https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=83s")
        );
        assert_eq!(
            deep_link("https://music.youtube.com/watch?v=lYBUbBu4W08&list=RD", 0).as_deref(),
            Some("https://www.youtube.com/watch?v=lYBUbBu4W08&t=0s")
        );
        assert_eq!(
            deep_link(
                "https://m.soundcloud.com/forss/flickermood?ref=x",
                3_723_000
            )
            .as_deref(),
            Some("https://soundcloud.com/forss/flickermood#t=62:03")
        );
        assert_eq!(deep_link("https://soundcloud.com/forss", 1000), None);
        assert_eq!(
            deep_link("https://www.youtube.com/playlist?list=PL1", 1000),
            None
        );
        assert_eq!(
            deep_link("https://www.mixcloud.com/spartacus/party-time/", 1000),
            None
        );
        assert_eq!(deep_link("https://example.com/a.mp3", 1000), None);
        assert_eq!(deep_link("not a url", 1000), None);
    }

    #[test]
    fn artists_display() {
        assert_eq!(join_artists(&[]), "");
        assert_eq!(join_artists(&["A".into()]), "A");
        assert_eq!(join_artists(&["A".into(), "B".into()]), "A & B");
        assert_eq!(
            join_artists(&["A".into(), "B".into(), "C".into()]),
            "A, B & C"
        );
    }

    #[test]
    fn finish_fills_fields() {
        let t = finish_track(Track {
            title: " T ".into(),
            artists: vec!["A".into(), " ".into(), "B".into()],
            album: Some(" ".into()),
            ..Track::default()
        });
        assert_eq!(t.title, "T");
        assert_eq!(t.artist, "A & B");
        assert_eq!(t.artists, vec!["A", "B"]);
        assert_eq!(t.album, None);
        assert_eq!(t.identity_key, "fuzzy::a;b::t");
        let t = finish_track(Track {
            title: "T".into(),
            artist: "Solo".into(),
            ..Track::default()
        });
        assert_eq!(t.artists, vec!["Solo"]);
    }

    #[test]
    fn storefronts() {
        assert_eq!(storefront("DE"), "de");
        assert_eq!(storefront(" us "), "us");
        assert_eq!(storefront("deu"), "us");
        assert_eq!(storefront(""), "us");
    }

    #[test]
    fn resolution_serializes_with_kind() {
        let r = Resolution::Media {
            source: SourceInfo::default(),
            url: "https://a.example/x.mp3".into(),
            direct: true,
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["kind"], "media");
        assert_eq!(json["direct"], true);
    }

    #[tokio::test]
    async fn rejects_non_links_and_private_targets() {
        let http = fetch::test_client();
        let cancel = CancelToken::new();
        assert!(matches!(
            resolve(&http, "hello there", "us", &cancel).await,
            Err(MfError::InvalidInput { .. })
        ));
        assert!(matches!(
            resolve(&http, "ftp://example.com/a", "us", &cancel).await,
            Err(MfError::InvalidInput { .. })
        ));
        assert_eq!(
            resolve(&http, "http://127.0.0.1/admin", "us", &cancel).await,
            Err(ssrf::private_address_error())
        );
        assert_eq!(
            resolve(
                &http,
                "see http://169.254.169.254/latest/meta-data/",
                "us",
                &cancel
            )
            .await,
            Err(ssrf::private_address_error())
        );
        cancel.cancel();
        assert_eq!(
            resolve(&http, "https://example.com/", "us", &cancel).await,
            Err(MfError::Canceled)
        );
    }
}
