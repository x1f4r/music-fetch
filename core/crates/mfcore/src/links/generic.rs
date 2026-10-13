//! Any other web page. In order: a media file (by extension or HEAD
//! content type), media the page declares (Open Graph, Twitter player stream,
//! JSON-LD, `<audio>`/`<video>`/`<source>`), then a player of a known site
//! embedded in the page (followed once by the router). Pages with none of these
//! go to the media backend's generic extractor.
//!
//! Every request uses the guarded client: manual redirects with the SSRF check
//! on each hop.

use serde_json::Value;
use url::Url;

use super::fetch::{self, Agent, Page, PageRequest};
use super::html::{Document, parse_iso_duration, str_at};
use super::tidal::ld_names;
use super::{
    Ctx, Resolution, Site, apple, canonical, deezer, finish_track, source_info, spotify, ssrf,
};
use crate::error::{MfError, Result};
use crate::model::{SourceInfo, Track};

const SITE: &str = "the website";

/// Extensions of files the core can download and decode directly.
const MEDIA_EXTENSIONS: &[&str] = &[
    "mp3", "m4a", "aac", "wav", "flac", "ogg", "oga", "opus", "weba", "webm", "mp4", "m4v", "mov",
    "mkv", "mka", "aif", "aiff", "caf", "wma",
];
/// Adaptive streams the media backend handles.
const STREAM_EXTENSIONS: &[&str] = &["m3u8", "mpd"];

/// How a media URL is fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MediaKind {
    /// A plain file, downloaded directly.
    File,
    /// HLS/DASH, handed to the media backend.
    Stream,
}

/// What a generic page led to.
#[derive(Debug)]
pub(super) enum Outcome {
    Done(Resolution),
    /// A known site's player is embedded; resolve `target`, or use `fallback`.
    Embed {
        target: Url,
        fallback: Resolution,
    },
}

/// Something on the page that may be the media.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Candidate {
    Media(Url, MediaKind),
    /// A page on a site with its own resolver.
    Site(Url),
}

/// What a page offers.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct Discovery {
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub thumbnail_url: Option<String>,
    pub duration_ms: Option<u64>,
    pub site_name: Option<String>,
    pub hints: Vec<Track>,
    pub candidates: Vec<Candidate>,
}

pub(super) fn kind_of_path(path: &str) -> Option<MediaKind> {
    let file = path.rsplit('/').next().unwrap_or_default();
    let (_, ext) = file.rsplit_once('.')?;
    let ext = ext.to_ascii_lowercase();
    if MEDIA_EXTENSIONS.contains(&ext.as_str()) {
        Some(MediaKind::File)
    } else if STREAM_EXTENSIONS.contains(&ext.as_str()) {
        Some(MediaKind::Stream)
    } else {
        None
    }
}

pub(super) fn kind_of_content_type(content_type: &str) -> Option<MediaKind> {
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if matches!(
        mime.as_str(),
        "application/vnd.apple.mpegurl"
            | "application/x-mpegurl"
            | "audio/mpegurl"
            | "audio/x-mpegurl"
            | "application/dash+xml"
    ) {
        Some(MediaKind::Stream)
    } else if mime.starts_with("audio/") || mime.starts_with("video/") {
        Some(MediaKind::File)
    } else {
        None
    }
}

fn media(source: SourceInfo, url: &Url, kind: MediaKind) -> Resolution {
    Resolution::Media {
        source,
        url: url.to_string(),
        direct: kind == MediaKind::File,
    }
}

/// Last path segment, as a stand-in title for bare media files.
fn file_name(url: &Url) -> Option<String> {
    canonical::segments(url)
        .last()
        .map(|s| s.replace("%20", " "))
}

pub(super) async fn resolve(ctx: &Ctx<'_>, url: &Url) -> Result<Outcome> {
    let mut source = source_info(url, Site::Other);
    if let Some(kind) = kind_of_path(url.path()) {
        source.title = file_name(url);
        return Ok(Outcome::Done(media(source, url, kind)));
    }
    match fetch::guarded_head(url, ctx.cancel).await {
        Ok(probe) if probe.status.is_success() => {
            if let Some(kind) = probe.content_type.as_deref().and_then(kind_of_content_type) {
                source.title = file_name(url);
                return Ok(Outcome::Done(media(source, url, kind)));
            }
        }
        Ok(_) => {}
        Err(e @ (MfError::Canceled | MfError::InvalidInput { .. })) => return Err(e),
        Err(e) => tracing::debug!("HEAD probe failed: {e}"),
    }
    let backend = |source: SourceInfo| {
        Outcome::Done(Resolution::Media {
            source,
            url: url.to_string(),
            direct: false,
        })
    };
    let page = match fetch_page(ctx, url).await {
        Ok(page) => page,
        Err(e @ (MfError::Canceled | MfError::InvalidInput { .. })) => return Err(e),
        Err(e) => {
            tracing::debug!("page fetch failed: {e}");
            return Ok(backend(source));
        }
    };
    if matches!(page.status.as_u16(), 404 | 410) {
        return Err(fetch::status_error(page.status, None, SITE));
    }
    if !page.status.is_success() {
        return Ok(backend(source));
    }
    if let Some(kind) = page.content_type.as_deref().and_then(kind_of_content_type) {
        source.title = file_name(url);
        return Ok(Outcome::Done(media(source, url, kind)));
    }
    if !page.is_html() {
        return Ok(backend(source));
    }
    let found = discover(&page.body, &page.url);
    source.title = found.title;
    source.uploader = found.uploader;
    source.thumbnail_url = found.thumbnail_url;
    source.duration_ms = found.duration_ms;
    source.site = found.site_name.or(source.site);
    source.hints = found.hints;
    for candidate in found.candidates {
        match candidate {
            Candidate::Media(media_url, kind) => {
                match fetch::cancellable(ctx.cancel, ssrf::ensure_public(&media_url)).await? {
                    Ok(()) => return Ok(Outcome::Done(media(source, &media_url, kind))),
                    Err(e) => tracing::debug!("skipping page media: {e}"),
                }
            }
            Candidate::Site(target) => {
                let fallback = Resolution::Media {
                    source,
                    url: url.to_string(),
                    direct: false,
                };
                return Ok(Outcome::Embed { target, fallback });
            }
        }
    }
    Ok(backend(source))
}

/// GET with the neutral agent, once more as a browser when refused.
async fn fetch_page(ctx: &Ctx<'_>, url: &Url) -> Result<Page> {
    let req = PageRequest::new(SITE);
    let page = fetch::guarded_get(url, &req, ctx.cancel).await?;
    if matches!(page.status.as_u16(), 403 | 406 | 429) {
        return fetch::guarded_get(url, &req.agent(Agent::Browser), ctx.cancel).await;
    }
    Ok(page)
}

/// A known site's content page for `url` (embedded players mapped to their page).
pub(super) fn site_target(url: &Url) -> Option<Url> {
    let host = canonical::host_of(url);
    let parts = canonical::segments(url);
    let mapped = match host.as_str() {
        "w.soundcloud.com" => url
            .query_pairs()
            .find(|(k, _)| k == "url")
            .and_then(|(_, v)| Url::parse(&v).ok()),
        "widget.deezer.com" => {
            let i = parts
                .iter()
                .position(|p| matches!(*p, "track" | "album" | "playlist"))?;
            Url::parse(&format!(
                "https://www.deezer.com/{}/{}",
                parts[i],
                parts.get(i + 1)?
            ))
            .ok()
        }
        "embed.tidal.com" => match parts.as_slice() {
            [kind, id, ..] => Url::parse(&format!(
                "https://tidal.com/{}/{id}",
                kind.trim_end_matches('s')
            ))
            .ok(),
            _ => None,
        },
        "www.mixcloud.com" | "mixcloud.com" if parts.first() == Some(&"widget") => url
            .query_pairs()
            .find(|(k, _)| k == "feed")
            .and_then(|(_, feed)| {
                Url::parse("https://www.mixcloud.com/")
                    .ok()?
                    .join(feed.trim_start_matches('/'))
                    .ok()
            }),
        _ => None,
    };
    let candidate = mapped.unwrap_or_else(|| url.clone());
    let canonical = Url::parse(&canonical::canonical_url(&candidate)).ok()?;
    let parts = canonical::segments(&canonical);
    let is_content = match Site::of_host(&canonical::host_of(&canonical)) {
        Site::YouTube | Site::YouTubeMusic => {
            canonical::youtube_video_id(&canonical).is_some() || canonical.path() == "/playlist"
        }
        Site::Spotify => spotify::entity(&canonical).is_some(),
        Site::AppleMusic => apple::parse_ref(&canonical, "us").is_some(),
        Site::Deezer => deezer::entity(&canonical).is_some(),
        Site::Tidal => matches!(parts.first(), Some(&("track" | "album"))) && parts.len() >= 2,
        Site::SoundCloud | Site::Mixcloud => parts.len() >= 2,
        Site::Vimeo => parts
            .first()
            .is_some_and(|p| p.chars().all(|c| c.is_ascii_digit())),
        Site::TikTok => parts.len() == 3 && parts[1] == "video",
        _ => false,
    };
    is_content.then_some(canonical)
}

fn absolute(base: &Url, raw: &str) -> Option<Url> {
    let url = base.join(raw.trim()).ok()?;
    matches!(url.scheme(), "http" | "https").then_some(url)
}

/// Classifies a URL the page declares as its media.
fn declared(base: &Url, raw: &str, declared_type: Option<&str>) -> Option<Candidate> {
    let url = absolute(base, raw)?;
    if let Some(target) = site_target(&url) {
        return Some(Candidate::Site(target));
    }
    let typed = declared_type.and_then(kind_of_content_type);
    if declared_type.is_some_and(|t| t.contains("html")) {
        return None;
    }
    let kind = kind_of_path(url.path())
        .or(typed)
        .unwrap_or(MediaKind::File);
    Some(Candidate::Media(url, kind))
}

fn ld_media_nodes(doc: &Document) -> Vec<&Value> {
    let mut nodes: Vec<&Value> = doc
        .json_ld_of(&["AudioObject", "VideoObject", "MusicRecording"])
        .collect();
    // MusicRecording.audio may hold the AudioObject.
    let nested: Vec<&Value> = nodes
        .iter()
        .filter_map(|n| n.get("audio"))
        .filter(|a| a.is_object())
        .collect();
    nodes.extend(nested);
    nodes
}

/// Everything a page says about its media.
pub(super) fn discover(html: &str, base: &Url) -> Discovery {
    let doc = Document::parse(html);
    let mut found = Discovery::default();
    let ld_nodes = ld_media_nodes(&doc);
    found.title = doc
        .meta_any(&["og:title", "twitter:title"])
        .map(str::to_string)
        .or_else(|| ld_nodes.iter().find_map(|n| str_at(n, "/name")))
        .or_else(|| doc.title.clone());
    found.uploader = doc
        .meta("music:musician_description")
        .map(str::to_string)
        .or_else(|| {
            ld_nodes
                .iter()
                .map(|n| {
                    super::join_artists(&ld_names(n.get("byArtist").or_else(|| n.get("author"))))
                })
                .find(|a| !a.is_empty())
        });
    found.thumbnail_url = doc
        .meta_any(&["og:image:secure_url", "og:image", "twitter:image"])
        .and_then(|u| absolute(base, u))
        .map(|u| u.to_string())
        .or_else(|| ld_nodes.iter().find_map(|n| str_at(n, "/thumbnailUrl")));
    found.duration_ms = ld_nodes
        .iter()
        .find_map(|n| {
            str_at(n, "/duration")
                .as_deref()
                .and_then(parse_iso_duration)
        })
        .or_else(|| {
            doc.meta("music:duration")
                .and_then(|d| d.parse::<u64>().ok())
                .filter(|d| *d > 0)
                .map(|d| d * 1000)
        });
    found.site_name = doc.meta("og:site_name").map(str::to_string);
    found.hints = doc
        .json_ld_of(&["MusicRecording"])
        .filter_map(|n| {
            let artists = ld_names(n.get("byArtist"));
            if artists.is_empty() {
                return None;
            }
            Some(finish_track(Track {
                title: str_at(n, "/name")?,
                artists,
                album: n
                    .pointer("/inAlbum/name")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                isrc: str_at(n, "/isrcCode"),
                duration_ms: str_at(n, "/duration")
                    .as_deref()
                    .and_then(parse_iso_duration),
                ..Track::default()
            }))
        })
        .collect();

    let mut candidates = Vec::new();
    let audio_type = doc.meta("og:audio:type");
    for key in ["og:audio:secure_url", "og:audio", "og:audio:url"] {
        candidates.extend(
            doc.meta_all(key)
                .filter_map(|u| declared(base, u, audio_type)),
        );
    }
    let video_type = doc.meta("og:video:type");
    for key in ["og:video:secure_url", "og:video", "og:video:url"] {
        candidates.extend(
            doc.meta_all(key)
                .filter_map(|u| declared(base, u, video_type)),
        );
    }
    candidates.extend(
        doc.meta_all("twitter:player:stream")
            .filter_map(|u| declared(base, u, doc.meta("twitter:player:stream:content_type"))),
    );
    for node in &ld_nodes {
        if let Some(content) = str_at(node, "/contentUrl") {
            candidates.extend(declared(
                base,
                &content,
                str_at(node, "/encodingFormat").as_deref(),
            ));
        }
        if let Some(embed) = str_at(node, "/embedUrl")
            && let Some(target) = absolute(base, &embed).and_then(|u| site_target(&u))
        {
            candidates.push(Candidate::Site(target));
        }
    }
    candidates.extend(
        doc.media_sources
            .iter()
            .filter_map(|src| declared(base, src, None)),
    );
    let players = doc
        .iframes
        .iter()
        .map(String::as_str)
        .chain(doc.meta_all("twitter:player"));
    candidates.extend(
        players
            .filter_map(|src| absolute(base, src))
            .filter_map(|u| site_target(&u))
            .map(Candidate::Site),
    );
    let mut unique: Vec<Candidate> = Vec::new();
    for candidate in candidates {
        if !unique.contains(&candidate) {
            unique.push(candidate);
        }
    }
    // Media the page serves itself beats players of other sites.
    unique.sort_by_key(|c| matches!(c, Candidate::Site(_)));
    found.candidates = unique;
    found
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
    fn media_kinds() {
        assert_eq!(kind_of_path("/file.mp4"), Some(MediaKind::File));
        assert_eq!(kind_of_path("/a/b/Track.MP3"), Some(MediaKind::File));
        assert_eq!(kind_of_path("/live/index.m3u8"), Some(MediaKind::Stream));
        assert_eq!(kind_of_path("/watch"), None);
        assert_eq!(kind_of_path("/v1.2/page"), None);
        assert_eq!(kind_of_content_type("audio/mpeg"), Some(MediaKind::File));
        assert_eq!(
            kind_of_content_type("video/mp4; codecs=avc1"),
            Some(MediaKind::File)
        );
        assert_eq!(
            kind_of_content_type("application/vnd.apple.mpegurl"),
            Some(MediaKind::Stream)
        );
        assert_eq!(kind_of_content_type("text/html; charset=utf-8"), None);
    }

    #[test]
    fn og_audio_page() {
        let base = url("https://www.jamendo.com/track/1886257/alone");
        let found = discover(&fixture("generic_og_audio.html"), &base);
        assert_eq!(
            found.title.as_deref(),
            Some("Color Out - Alone | Jamendo Music | Free music downloads")
        );
        assert_eq!(found.site_name.as_deref(), Some("Jamendo Music"));
        assert!(found.thumbnail_url.is_some());
        match found.candidates.first() {
            Some(Candidate::Media(u, MediaKind::File)) => {
                assert_eq!(u.scheme(), "https");
                assert_eq!(u.host_str(), Some("prod-1.storage.jamendo.com"));
            }
            other => panic!("expected direct media, got {other:?}"),
        }
    }

    #[test]
    fn og_video_mp3_page() {
        let base = url("https://archive.org/details/gd1977-05-08.shure57.stevenson.29303.flac16");
        let found = discover(&fixture("generic_og_video.html"), &base);
        assert!(
            matches!(found.candidates.first(), Some(Candidate::Media(u, MediaKind::File)) if u.path().ends_with(".mp3"))
        );
    }

    #[test]
    fn embedded_players_and_elements() {
        let base = url("https://blog.example/post/1");
        let html = r#"<html><head><title>Post</title>
            <script type="application/ld+json">{"@type":"MusicRecording","name":"Song","byArtist":{"name":"Band"},"isrcCode":"USX1"}</script></head>
            <body><iframe src="https://open.spotify.com/embed/track/4cOdK2wGLETKBW3PvgPWqT?utm_source=generator"></iframe>
            <iframe src="https://www.youtube.com/embed/dQw4w9WgXcQ"></iframe>
            <iframe src="https://w.soundcloud.com/player/?url=https%3A%2F%2Fapi.soundcloud.com%2Ftracks%2F293&color=ff5500"></iframe>
            <iframe src="https://widget.deezer.com/widget/dark/track/3135556"></iframe>
            <iframe src="https://ads.example/frame"></iframe>
            <video><source src="/media/clip.webm" type="video/webm"></video></body></html>"#;
        let found = discover(html, &base);
        // Structured data names the media more precisely than <title>.
        assert_eq!(found.title.as_deref(), Some("Song"));
        assert_eq!(found.uploader.as_deref(), Some("Band"));
        assert_eq!(found.hints.len(), 1);
        assert_eq!(found.hints[0].identity_key, "isrc::USX1");
        let expected = vec![
            Candidate::Media(url("https://blog.example/media/clip.webm"), MediaKind::File),
            Candidate::Site(url("https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT")),
            Candidate::Site(url("https://www.youtube.com/watch?v=dQw4w9WgXcQ")),
            Candidate::Site(url("https://api.soundcloud.com/tracks/293")),
            Candidate::Site(url("https://www.deezer.com/track/3135556")),
        ];
        assert_eq!(found.candidates, expected);
    }

    #[test]
    fn site_targets() {
        assert_eq!(
            site_target(&url(
                "https://embed.music.apple.com/us/album/x/1559885420?i=1559885421"
            ))
            .map(|u| u.to_string())
            .as_deref(),
            Some("https://music.apple.com/us/album/x/1559885420?i=1559885421")
        );
        assert_eq!(
            site_target(&url("https://embed.tidal.com/tracks/1781887"))
                .map(|u| u.to_string())
                .as_deref(),
            Some("https://tidal.com/track/1781887")
        );
        assert_eq!(
            site_target(&url(
                "https://www.mixcloud.com/widget/iframe/?feed=%2Fspartacus%2Fparty-time%2F"
            ))
            .map(|u| u.to_string())
            .as_deref(),
            Some("https://www.mixcloud.com/spartacus/party-time")
        );
        assert_eq!(site_target(&url("https://www.youtube.com/")), None);
        assert_eq!(site_target(&url("https://www.facebook.com/tr?id=1")), None);
        assert_eq!(
            site_target(&url("https://player.vimeo.com/video/76979871"))
                .map(|u| u.to_string())
                .as_deref(),
            Some("https://vimeo.com/76979871")
        );
    }

    fn respond(_: &str, path: &str, port: u16) -> (String, String, String) {
        let html = |body: String| {
            (
                "200 OK".to_string(),
                "Content-Type: text/html\r\n".to_string(),
                body,
            )
        };
        match path {
            "/stream" => (
                "200 OK".into(),
                "Content-Type: audio/mpeg\r\n".into(),
                String::new(),
            ),
            "/page" => html(format!(
                r#"<meta property="og:title" content="A page"><meta property="og:audio" content="http://127.0.0.1:{port}/song.mp3">"#
            )),
            "/private" => {
                html(r#"<meta property="og:audio" content="http://192.168.0.10/a.mp3">"#.into())
            }
            "/embed" => {
                html(r#"<iframe src="https://www.youtube.com/embed/dQw4w9WgXcQ"></iframe>"#.into())
            }
            "/gone" => ("404 Not Found".into(), String::new(), String::new()),
            _ => html("<html><title>Plain</title></html>".into()),
        }
    }

    #[tokio::test]
    async fn resolves_pages_through_the_guard() {
        let port = fetch::mock_server(respond).await;
        let http = fetch::test_client();
        let cancel = crate::util::cancel::CancelToken::new();
        let ctx = Ctx {
            http: &http,
            country: "us".into(),
            cancel: &cancel,
        };
        let at = |path: &str| url(&format!("http://127.0.0.1:{port}{path}"));
        let direct = |o: Outcome| match o {
            Outcome::Done(Resolution::Media {
                url,
                direct,
                source,
            }) => (url, direct, source.title),
            other => panic!("unexpected {other:?}"),
        };

        let (u, d, title) = direct(resolve(&ctx, &at("/song.mp3")).await.unwrap());
        assert_eq!(
            (u.as_str(), d, title.as_deref()),
            (at("/song.mp3").as_str(), true, Some("song.mp3"))
        );
        let (u, d, _) = direct(resolve(&ctx, &at("/stream")).await.unwrap());
        assert_eq!((u, d), (at("/stream").to_string(), true));
        let (u, d, title) = direct(resolve(&ctx, &at("/page")).await.unwrap());
        assert_eq!(
            (u, d, title.as_deref()),
            (at("/song.mp3").to_string(), true, Some("A page"))
        );
        // Media on a private address is ignored; the page goes to the media backend.
        let (u, d, _) = direct(resolve(&ctx, &at("/private")).await.unwrap());
        assert_eq!((u, d), (at("/private").to_string(), false));
        let (u, d, title) = direct(resolve(&ctx, &at("/plain")).await.unwrap());
        assert_eq!(
            (u, d, title.as_deref()),
            (at("/plain").to_string(), false, Some("Plain"))
        );
        match resolve(&ctx, &at("/embed")).await.unwrap() {
            Outcome::Embed { target, .. } => assert_eq!(
                target.as_str(),
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            ),
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(
            resolve(&ctx, &at("/gone")).await,
            Err(MfError::MediaUnavailable { .. })
        ));
    }

    #[test]
    fn empty_pages_have_no_candidates() {
        let found = discover(
            "<html><head><title>Nothing</title></head></html>",
            &url("https://example.com/"),
        );
        assert!(found.candidates.is_empty());
        assert_eq!(found.title.as_deref(), Some("Nothing"));
        let found = discover(
            r#"<meta property="og:video" content="javascript:alert(1)"><audio src="blob:https://x/y"></audio>"#,
            &url("https://example.com/"),
        );
        assert!(found.candidates.is_empty());
    }
}
