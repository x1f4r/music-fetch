//! YouTube without yt-dlp: the media information `MediaBackend::probe` reports
//! for YouTube (title, channel, duration, description, chapters, the Music
//! card's song, top comments with the pinned and the uploader's first, playlist
//! entries), read from the watch and playlist pages and InnerTube's `next` and
//! `browse` endpoints as the web client (`WEB`, `hl=en`) uses them. The browser
//! engine uses it instead of yt-dlp; nothing here touches the media itself.
//!
//! Requests per probe: the page, then for a video with comments up to three
//! `next` pages (40 top-level comments, as the yt-dlp probe asks for), for a
//! playlist up to two `browse` continuations (300 entries, the pipeline's
//! cap). Pages carry the `youtube` header rule, InnerTube calls the
//! `innertube` one (the static consent cookie, English, the site's origin);
//! nothing of the user's is ever sent. Requests are spaced and a bot check or
//! HTTP 429 stops them for a while (process-wide).

use std::sync::LazyLock;
use std::time::Duration;

use regex::Regex;
use serde_json::{Value, json};
use url::Url;

use super::canonical;
use super::fetch::{self, PageRequest};
use super::html::{json_after, str_at, text_of, u64_at};
use super::youtube::{largest_thumbnail, music_cards};
use crate::error::{MfError, Result};
use crate::media::probe::{Chapter, Comment, MediaEntry, MediaInfo};
use crate::util::cancel::CancelToken;
use crate::util::http::HeaderRuleId;
use crate::util::ratelimit::RateLimiter;

const SITE: &str = "YouTube";
const NEXT_URL: &str = "https://www.youtube.com/youtubei/v1/next?prettyPrint=false";
const BROWSE_URL: &str = "https://www.youtube.com/youtubei/v1/browse?prettyPrint=false";
/// Top-level comments read for tracklists (yt-dlp `max_comments=40`).
pub const MAX_COMMENTS: usize = 40;
const MAX_COMMENT_PAGES: usize = 3;
/// Playlist entries read (`pipeline::playlist::MAX_ENTRIES`).
pub const MAX_ENTRIES: usize = 300;
const MAX_BROWSE_PAGES: usize = 2;
const TOPIC_SUFFIX: &str = " - Topic";

/// One request at a time, a second apart; a bot check or 429 opens the breaker
/// (10 min, doubling up to 6 h).
static LIMITER: LazyLock<RateLimiter> =
    LazyLock::new(|| RateLimiter::new(Duration::from_millis(1000), Duration::from_millis(250)));
const BREAKER_BASE: Duration = Duration::from_secs(600);
const BREAKER_MAX: Duration = Duration::from_secs(6 * 3600);
const MAX_WAIT: Duration = Duration::from_secs(20);

/// What one request and its answer looked like (for [`ProbeHooks`]).
#[derive(Debug)]
pub struct Exchange<'a> {
    /// "watch", "playlist", "next" or "browse".
    pub kind: &'a str,
    pub url: &'a str,
    pub request: Option<&'a Value>,
    pub status: u16,
    pub body: &'a str,
}

/// Observes a probe's requests (fixture recording, request budgets).
pub trait ProbeHooks: Send + Sync {
    /// Before each request; an error stops the probe.
    fn before(&self, _kind: &str, _url: &str) -> Result<()> {
        Ok(())
    }
    /// After each answer.
    fn after(&self, _exchange: &Exchange<'_>) {}
}

struct NoHooks;
impl ProbeHooks for NoHooks {}

/// True when `url` is a YouTube video or playlist this module can probe.
pub fn handles(url: &str) -> bool {
    let Ok(url) = Url::parse(url.trim()) else { return false };
    canonical::youtube_video_id(&url).is_some() || playlist_id(&url).is_some()
}

/// `MediaBackend::probe` for YouTube: a video (with up to [`MAX_COMMENTS`]
/// top-level comments when `with_comments`) or a playlist's entries.
pub async fn probe(http: &reqwest::Client, url: &str, with_comments: bool, cancel: &CancelToken) -> Result<MediaInfo> {
    probe_with(http, url, with_comments, cancel, &NoHooks).await
}

/// [`probe`] with hooks around every request.
pub async fn probe_with(
    http: &reqwest::Client,
    url: &str,
    with_comments: bool,
    cancel: &CancelToken,
    hooks: &dyn ProbeHooks,
) -> Result<MediaInfo> {
    let parsed = Url::parse(url.trim()).map_err(|_| MfError::invalid("That link isn't valid."))?;
    if let Some(id) = canonical::youtube_video_id(&parsed) {
        return video(http, &id, with_comments, cancel, hooks).await;
    }
    if let Some(list) = playlist_id(&parsed) {
        return playlist(http, &list, cancel, hooks).await;
    }
    Err(MfError::UnsupportedLink {
        message: "Only YouTube videos and playlists can be read without the media tools.".into(),
    })
}

fn playlist_id(url: &Url) -> Option<String> {
    let host = canonical::host_of(url);
    if !(host == "www.youtube.com" || host == "m.youtube.com" || host == "music.youtube.com" || host == "youtube.com") {
        return None;
    }
    url.query_pairs()
        .find(|(k, _)| k == "list")
        .map(|(_, v)| v.into_owned())
        .filter(|id| !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'))
}

// ------------------------------------------------------------------ requests

async fn get_page(
    http: &reqwest::Client,
    kind: &str,
    url: &str,
    cancel: &CancelToken,
    hooks: &dyn ProbeHooks,
) -> Result<String> {
    hooks.before(kind, url)?;
    LIMITER.acquire(cancel, MAX_WAIT).await?;
    let page = fetch::get_page(http, url, &PageRequest::new(SITE).rule(HeaderRuleId::Youtube), cancel).await?;
    hooks.after(&Exchange { kind, url, request: None, status: page.status.as_u16(), body: &page.body });
    if page.status.as_u16() == 429 || is_bot_check(&page.body) || is_consent_wall(page.url.as_str()) {
        throttled();
        return Err(blocked());
    }
    let page = fetch::require_success(page, SITE)?;
    LIMITER.success();
    Ok(page.body)
}

async fn innertube(
    http: &reqwest::Client,
    kind: &str,
    client: &Client,
    mut body: Value,
    cancel: &CancelToken,
    hooks: &dyn ProbeHooks,
) -> Result<Value> {
    let url = if kind == "browse" { BROWSE_URL } else { NEXT_URL };
    body["context"] = client.context();
    hooks.before(kind, url)?;
    LIMITER.acquire(cancel, MAX_WAIT).await?;
    let headers = [("x-youtube-client-name", "1"), ("x-youtube-client-version", client.version.as_str())];
    let response = fetch::post_json(http, url, &body, HeaderRuleId::Innertube, &headers, SITE, cancel).await?;
    let text = response.value.as_ref().map(Value::to_string).unwrap_or_default();
    hooks.after(&Exchange { kind, url, request: Some(&body), status: response.status.as_u16(), body: &text });
    if response.status.as_u16() == 429 {
        throttled();
        return Err(blocked());
    }
    if !response.status.is_success() {
        return Err(fetch::status_error(response.status, response.retry_after, SITE));
    }
    LIMITER.success();
    response.value.ok_or_else(|| MfError::network("YouTube sent an unreadable response"))
}

fn throttled() {
    let wait = LIMITER.throttled(BREAKER_BASE, BREAKER_MAX, None);
    tracing::warn!(cooldown_s = wait.as_secs(), "youtube asked for a bot check; pausing requests");
}

fn blocked() -> MfError {
    MfError::RateLimited { retry_in_ms: LIMITER.cooldown().unwrap_or(BREAKER_BASE).as_millis() as u64 }
}

/// YouTube's "confirm you're not a bot" answer (a sign-in wall instead of the video).
pub fn is_bot_check(body: &str) -> bool {
    body.contains("confirm you\u{2019}re not a bot")
        || body.contains("confirm you're not a bot")
        || body.contains("\"reason\":\"Sign in to confirm")
        || body.contains("www.google.com/sorry/")
        || body.contains("/recaptcha/")
}

fn is_consent_wall(final_url: &str) -> bool {
    final_url.starts_with("https://consent.youtube.com/")
}

// ------------------------------------------------------------------ video

async fn video(
    http: &reqwest::Client,
    id: &str,
    with_comments: bool,
    cancel: &CancelToken,
    hooks: &dyn ProbeHooks,
) -> Result<MediaInfo> {
    let url = format!("https://www.youtube.com/watch?v={id}&hl=en&persist_hl=1");
    let html = get_page(http, "watch", &url, cancel, hooks).await?;
    let page = WatchData::parse(&html);
    let mut info = video_info(id, &page)?;
    if with_comments && let Some(token) = page.initial.as_ref().and_then(comments_token) {
        let client = page.client.clone().unwrap_or_else(Client::fallback);
        info.comments = comments(http, &client, token, cancel, hooks).await?;
    }
    Ok(info)
}

/// Top-level comments of up to [`MAX_COMMENT_PAGES`] pages, pinned first, then
/// the uploader's, then by likes. A page that fails ends the list (comments are
/// a bonus); a bot check is an error.
async fn comments(
    http: &reqwest::Client,
    client: &Client,
    first: String,
    cancel: &CancelToken,
    hooks: &dyn ProbeHooks,
) -> Result<Vec<Comment>> {
    let mut out = Vec::new();
    let mut token = Some(first);
    for _ in 0..MAX_COMMENT_PAGES {
        let Some(t) = token.take() else { break };
        let page = match innertube(http, "next", client, json!({"continuation": t}), cancel, hooks).await {
            Ok(page) => page,
            Err(e @ (MfError::Canceled | MfError::RateLimited { .. })) => return Err(e),
            Err(e) => {
                tracing::debug!("youtube comments stopped: {e}");
                break;
            }
        };
        let (found, next) = comments_page(&page);
        out.extend(found);
        token = next;
        if out.len() >= MAX_COMMENTS {
            break;
        }
    }
    Ok(sort_comments(out))
}

fn sort_comments(mut comments: Vec<Comment>) -> Vec<Comment> {
    // Stable: YouTube's own "top" order breaks ties.
    comments.sort_by_key(|c| (!c.is_pinned, !c.author_is_uploader, std::cmp::Reverse(c.like_count)));
    comments.truncate(MAX_COMMENTS);
    comments
}

/// The page's `ytInitialPlayerResponse`, `ytInitialData` and InnerTube client.
#[derive(Debug, Default)]
pub(crate) struct WatchData {
    pub player: Option<Value>,
    pub initial: Option<Value>,
    pub client: Option<Client>,
}

impl WatchData {
    pub(crate) fn parse(html: &str) -> Self {
        let player =
            json_after(html, "var ytInitialPlayerResponse =").or_else(|| json_after(html, "ytInitialPlayerResponse ="));
        let initial =
            json_after(html, "var ytInitialData =").or_else(|| json_after(html, "window[\"ytInitialData\"] ="));
        Self { player, initial, client: Client::from_page(html) }
    }
}

/// A video's [`MediaInfo`] (no comments yet).
pub(crate) fn video_info(id: &str, page: &WatchData) -> Result<MediaInfo> {
    let player = page.player.as_ref().ok_or_else(|| MfError::network("YouTube sent a page without the video"))?;
    let status = str_at(player, "/playabilityStatus/status").unwrap_or_default();
    if status != "OK" && status != "LIVE_STREAM_OFFLINE" && !status.is_empty() {
        let reason = str_at(player, "/playabilityStatus/reason")
            .or_else(|| {
                player
                    .pointer("/playabilityStatus/errorScreen")
                    .and_then(|e| super::youtube::find_key(e, "reason"))
                    .and_then(text_of)
            })
            .unwrap_or_else(|| "This video is unavailable".into());
        return Err(MfError::MediaUnavailable { message: format!("{}.", reason.trim_end_matches('.')) });
    }
    let details = player.get("videoDetails").cloned().unwrap_or(Value::Null);
    let title = str_at(&details, "/title");
    let uploader = str_at(&details, "/author");
    let duration_ms = u64_at(&details, "/lengthSeconds").filter(|s| *s > 0).map(|s| s * 1000);
    let description = details.get("shortDescription").and_then(Value::as_str).map(str::trim).filter(|d| !d.is_empty());
    let mut info = MediaInfo {
        id: Some(str_at(&details, "/videoId").unwrap_or_else(|| id.to_string())),
        title: title.clone(),
        uploader: uploader.clone(),
        duration_ms,
        webpage_url: Some(format!("https://www.youtube.com/watch?v={id}")),
        extractor: Some("Youtube".into()),
        thumbnail: largest_thumbnail(&details),
        description: description.map(str::to_string),
        is_live: details.get("isLive").and_then(Value::as_bool).unwrap_or(false),
        ..MediaInfo::default()
    };
    if let Some(initial) = &page.initial {
        info.chapters = chapters(initial, duration_ms);
        // One song in the "Music" section names the video's song, as yt-dlp reports it.
        let cards = music_cards(initial);
        if let [card] = cards.as_slice() {
            info.track = Some(card.title.clone());
            info.artist = Some(card.artist.clone()).filter(|a| !a.is_empty());
            info.album = card.album.clone();
        }
    }
    // Auto-generated "Artist - Topic" art tracks: "Provided to YouTube by …\n\nTitle · Artist\n\nAlbum".
    if info.track.is_none()
        && let (Some(title), Some(artist)) = (&title, uploader.as_deref().and_then(|u| u.strip_suffix(TOPIC_SUFFIX)))
    {
        info.track = Some(title.clone());
        info.artist = Some(artist.to_string());
        info.album = description.and_then(topic_album);
    }
    Ok(info)
}

/// The album paragraph of an auto-generated art track's description.
fn topic_album(description: &str) -> Option<String> {
    let paragraphs: Vec<&str> = description.split("\n\n").map(str::trim).filter(|p| !p.is_empty()).collect();
    if !paragraphs.first()?.starts_with("Provided to YouTube by") || !paragraphs.get(1)?.contains(" · ") {
        return None;
    }
    let album = paragraphs.get(2)?;
    (!album.starts_with('℗') && !album.contains('\n')).then(|| album.to_string())
}

/// Chapters from the player bar's markers, else the chapters engagement panel
/// (the description's chapters before automatic ones). Each ends where the next
/// starts, the last at the end of the video.
pub(crate) fn chapters(initial: &Value, duration_ms: Option<u64>) -> Vec<Chapter> {
    let mut found: Vec<(u64, String)> = Vec::new();
    let markers = initial
        .pointer("/playerOverlays/playerOverlayRenderer/decoratedPlayerBarRenderer/decoratedPlayerBarRenderer/playerBar/multiMarkersPlayerBarRenderer/markersMap")
        .and_then(Value::as_array);
    for key in ["DESCRIPTION_CHAPTERS", "AUTO_CHAPTERS"] {
        let Some(map) = markers.and_then(|m| m.iter().find(|e| e.get("key").and_then(Value::as_str) == Some(key)))
        else {
            continue;
        };
        for chapter in map.pointer("/value/chapters").and_then(Value::as_array).into_iter().flatten() {
            let r = chapter.get("chapterRenderer").unwrap_or(chapter);
            if let Some(start) = u64_at(r, "/timeRangeStartMillis") {
                found.push((start, r.get("title").and_then(text_of).unwrap_or_default()));
            }
        }
        if !found.is_empty() {
            break;
        }
    }
    if found.is_empty() {
        let panels = initial.get("engagementPanels").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
        for target in
            ["engagement-panel-macro-markers-description-chapters", "engagement-panel-macro-markers-auto-chapters"]
        {
            let Some(panel) = panels.iter().filter_map(|p| p.get("engagementPanelSectionListRenderer")).find(|p| {
                p.get("targetId").and_then(Value::as_str) == Some(target)
                    || p.get("panelIdentifier").and_then(Value::as_str) == Some(target)
            }) else {
                continue;
            };
            let items = panel.pointer("/content/macroMarkersListRenderer/contents").and_then(Value::as_array);
            for item in items.into_iter().flatten().filter_map(|i| i.get("macroMarkersListItemRenderer")) {
                let start = u64_at(item, "/onTap/watchEndpoint/startTimeSeconds")
                    .map(|s| s * 1000)
                    .or_else(|| item.get("timeDescription").and_then(text_of).and_then(|t| clock_ms(&t)));
                if let Some(start) = start {
                    found.push((start, item.get("title").and_then(text_of).unwrap_or_default()));
                }
            }
            if !found.is_empty() {
                break;
            }
        }
    }
    found.sort_by_key(|(start, _)| *start);
    found.dedup_by_key(|(start, _)| *start);
    let ends: Vec<Option<u64>> =
        found.iter().skip(1).map(|(s, _)| Some(*s)).chain(std::iter::once(duration_ms)).collect();
    found.into_iter().zip(ends).map(|((start_ms, title), end_ms)| Chapter { start_ms, end_ms, title }).collect()
}

/// "1:02:03" / "4:05" in milliseconds.
fn clock_ms(text: &str) -> Option<u64> {
    let mut total = 0u64;
    for part in text.trim().split(':') {
        total = total * 60 + part.trim().parse::<u64>().ok()?;
    }
    Some(total * 1000)
}

/// The continuation token of the comments section (default sort: top).
pub(crate) fn comments_token(initial: &Value) -> Option<String> {
    let contents = initial.pointer("/contents/twoColumnWatchNextResults/results/results/contents")?.as_array()?;
    contents
        .iter()
        .filter_map(|c| c.get("itemSectionRenderer"))
        .filter(|s| s.get("sectionIdentifier").and_then(Value::as_str) == Some("comment-item-section"))
        .find_map(|s| s.get("contents")?.as_array()?.iter().find_map(continuation_token))
}

/// `continuationItemRenderer`'s token, however it is wrapped.
fn continuation_token(item: &Value) -> Option<String> {
    let renderer = item.get("continuationItemRenderer")?;
    ["/continuationEndpoint/continuationCommand/token", "/button/buttonRenderer/command/continuationCommand/token"]
        .iter()
        .find_map(|p| str_at(renderer, p))
        .or_else(|| super::youtube::find_key(renderer, "token").and_then(Value::as_str).map(str::to_string))
}

/// One `next` answer: its top-level comments and the next page's token.
///
/// Two shapes: the older `commentThreadRenderer.comment.commentRenderer`, and
/// `commentViewModel`s whose text, author and counts live in
/// `frameworkUpdates.entityBatchUpdate.mutations[].payload.commentEntityPayload`.
pub(crate) fn comments_page(page: &Value) -> (Vec<Comment>, Option<String>) {
    let payloads: Vec<&Value> = page
        .pointer("/frameworkUpdates/entityBatchUpdate/mutations")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|m| m.pointer("/payload/commentEntityPayload"))
        .collect();
    let mut comments = Vec::new();
    let mut next = None;
    for item in continuation_items(page) {
        if let Some(token) = continuation_token(item) {
            next = Some(token);
            continue;
        }
        let Some(thread) = item.get("commentThreadRenderer") else { continue };
        if let Some(c) = thread.pointer("/comment/commentRenderer").and_then(legacy_comment) {
            comments.push(c);
        } else if let Some(view) = thread.pointer("/commentViewModel/commentViewModel")
            && let Some(c) = view_model_comment(view, &payloads)
        {
            comments.push(c);
        }
    }
    (comments, next)
}

/// Items of every `reloadContinuationItemsCommand` / `appendContinuationItemsAction`.
fn continuation_items(page: &Value) -> impl Iterator<Item = &Value> {
    ["onResponseReceivedEndpoints", "onResponseReceivedActions"]
        .into_iter()
        .filter_map(|k| page.get(k).and_then(Value::as_array))
        .flatten()
        .filter_map(|e| {
            e.pointer("/reloadContinuationItemsCommand/continuationItems")
                .or_else(|| e.pointer("/appendContinuationItemsAction/continuationItems"))
                .and_then(Value::as_array)
        })
        .flatten()
}

fn legacy_comment(r: &Value) -> Option<Comment> {
    let text = r.get("contentText").and_then(text_of)?;
    Some(Comment {
        author: r.get("authorText").and_then(text_of),
        is_pinned: r.get("pinnedCommentBadge").is_some(),
        author_is_uploader: r.get("authorIsChannelOwner").and_then(Value::as_bool).unwrap_or(false),
        text,
        like_count: r.get("voteCount").and_then(text_of).as_deref().map_or(0, parse_count),
    })
}

fn view_model_comment(view: &Value, payloads: &[&Value]) -> Option<Comment> {
    let key = str_at(view, "/commentKey");
    let id = str_at(view, "/commentId");
    let payload = payloads.iter().find(|p| {
        key.as_deref().is_some_and(|k| str_at(p, "/key").as_deref() == Some(k))
            || id.as_deref().is_some_and(|i| str_at(p, "/properties/commentId").as_deref() == Some(i))
    })?;
    let text = payload.pointer("/properties/content").and_then(text_of)?;
    let likes = str_at(payload, "/toolbar/likeCountNotliked")
        .or_else(|| str_at(payload, "/toolbar/likeCountA11y"))
        .as_deref()
        .map_or(0, parse_count);
    Some(Comment {
        author: str_at(payload, "/author/displayName"),
        is_pinned: view.get("pinnedText").is_some_and(|p| !p.is_null()),
        author_is_uploader: payload.pointer("/author/isCreator").and_then(Value::as_bool).unwrap_or(false),
        text,
        like_count: likes,
    })
}

/// "1.2K", "3M", "12", "1,234 likes" → a count.
pub(crate) fn parse_count(text: &str) -> u64 {
    let t = text.trim().to_ascii_lowercase();
    let number: String =
        t.chars().take_while(|c| c.is_ascii_digit() || *c == '.' || *c == ',').filter(|c| *c != ',').collect();
    let Ok(value) = number.parse::<f64>() else { return 0 };
    let rest = t[t.find(|c: char| !(c.is_ascii_digit() || c == '.' || c == ',')).unwrap_or(t.len())..].trim_start();
    let factor = match rest.chars().next() {
        Some('k') => 1e3,
        Some('m') => 1e6,
        Some('b') => 1e9,
        _ => 1.0,
    };
    (value * factor).round() as u64
}

// ------------------------------------------------------------------ playlists

async fn playlist(
    http: &reqwest::Client,
    list: &str,
    cancel: &CancelToken,
    hooks: &dyn ProbeHooks,
) -> Result<MediaInfo> {
    let url = format!("https://www.youtube.com/playlist?list={list}&hl=en&persist_hl=1");
    let html = get_page(http, "playlist", &url, cancel, hooks).await?;
    let page = WatchData::parse(&html);
    let initial =
        page.initial.as_ref().ok_or_else(|| MfError::network("YouTube sent a playlist page without its entries"))?;
    if let Some(alert) = playlist_alert(initial) {
        return Err(MfError::MediaUnavailable { message: alert });
    }
    let first = playlist_page(initial);
    let mut entries = first.entries;
    let mut token = first.continuation;
    let client = page.client.clone().unwrap_or_else(Client::fallback);
    for _ in 0..MAX_BROWSE_PAGES {
        if entries.len() >= MAX_ENTRIES {
            break;
        }
        let Some(t) = token.take() else { break };
        let more = innertube(http, "browse", &client, json!({"continuation": t}), cancel, hooks).await?;
        let (found, next) = playlist_continuation(&more);
        if found.is_empty() {
            break;
        }
        entries.extend(found);
        token = next;
    }
    entries.truncate(MAX_ENTRIES);
    for (i, e) in entries.iter_mut().enumerate() {
        e.index = i as u32;
    }
    Ok(MediaInfo {
        id: Some(list.to_string()),
        title: first.title,
        uploader: first.uploader,
        webpage_url: Some(format!("https://www.youtube.com/playlist?list={list}")),
        extractor: Some("YoutubeTab".into()),
        thumbnail: first.thumbnail,
        description: first.description,
        is_playlist: true,
        entries,
        ..MediaInfo::default()
    })
}

/// What a playlist page (or a `browse` answer for `VL<id>`) holds.
#[derive(Debug, Default)]
pub(crate) struct PlaylistPage {
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub description: Option<String>,
    pub thumbnail: Option<String>,
    pub entries: Vec<MediaEntry>,
    pub continuation: Option<String>,
}

/// "This playlist does not exist." and similar.
fn playlist_alert(initial: &Value) -> Option<String> {
    let alerts = initial.get("alerts")?.as_array()?;
    alerts.iter().find_map(|a| {
        let r = a.get("alertRenderer").or_else(|| a.get("alertWithButtonRenderer"))?;
        (str_at(r, "/type").as_deref() == Some("ERROR")).then(|| r.get("text").and_then(text_of)).flatten()
    })
}

pub(crate) fn playlist_page(initial: &Value) -> PlaylistPage {
    let mut page = PlaylistPage {
        title: str_at(initial, "/metadata/playlistMetadataRenderer/title")
            .or_else(|| initial.pointer("/header/playlistHeaderRenderer/title").and_then(text_of))
            .or_else(|| initial.pointer("/header/pageHeaderRenderer/pageTitle").and_then(text_of)),
        uploader: playlist_owner(initial),
        description: str_at(initial, "/metadata/playlistMetadataRenderer/description"),
        thumbnail: initial
            .pointer("/microformat/microformatDataRenderer/thumbnail")
            .and_then(|t| largest_thumbnail(&json!({"thumbnail": t}))),
        ..PlaylistPage::default()
    };
    let tabs = initial.pointer("/contents/twoColumnBrowseResultsRenderer/tabs").and_then(Value::as_array);
    let list = tabs.into_iter().flatten().find_map(|t| super::youtube::find_key(t, "playlistVideoListRenderer"));
    if let Some(items) = list.and_then(|l| l.get("contents")).and_then(Value::as_array) {
        let (entries, next) = entries_of(items);
        page.entries = entries;
        page.continuation = next;
    }
    page
}

fn playlist_owner(initial: &Value) -> Option<String> {
    initial
        .pointer("/header/playlistHeaderRenderer/ownerText")
        .and_then(text_of)
        .or_else(|| {
            initial.pointer("/sidebar/playlistSidebarRenderer/items").and_then(Value::as_array)?.iter().find_map(|i| {
                i.pointer("/playlistSidebarSecondaryInfoRenderer/videoOwner/videoOwnerRenderer/title").and_then(text_of)
            })
        })
        .or_else(|| {
            // pageHeaderViewModel: the first metadata row names the owner ("by X" or "X").
            let rows = initial
                .pointer("/header/pageHeaderRenderer/content/pageHeaderViewModel/metadata/contentMetadataViewModel/metadataRows")?
                .as_array()?;
            rows.iter().find_map(|r| {
                let part = r.get("metadataParts")?.as_array()?.first()?;
                let text = part.pointer("/avatarStack/avatarStackViewModel/text").or_else(|| part.get("text")).and_then(text_of)?;
                Some(text.strip_prefix("by ").unwrap_or(&text).to_string())
            })
        })
}

/// A `browse` continuation: more entries and the next token.
pub(crate) fn playlist_continuation(page: &Value) -> (Vec<MediaEntry>, Option<String>) {
    let items: Vec<Value> = continuation_items(page).cloned().collect();
    entries_of(&items)
}

fn entries_of(items: &[Value]) -> (Vec<MediaEntry>, Option<String>) {
    let mut entries = Vec::new();
    let mut next = None;
    for item in items {
        if let Some(token) = continuation_token(item) {
            next = Some(token);
            continue;
        }
        let Some(r) = item.get("playlistVideoRenderer") else { continue };
        let Some(id) = str_at(r, "/videoId") else { continue };
        let title = r.get("title").and_then(text_of);
        // Removed videos stay in playlists as placeholders.
        if r.get("isPlayable").and_then(Value::as_bool) == Some(false)
            || matches!(title.as_deref(), Some("[Private video]" | "[Deleted video]" | "[Unavailable video]"))
        {
            continue;
        }
        entries.push(MediaEntry {
            index: entries.len() as u32,
            url: format!("https://www.youtube.com/watch?v={id}"),
            id: Some(id),
            title,
            uploader: r.get("shortBylineText").and_then(text_of),
            duration_ms: u64_at(r, "/lengthSeconds").filter(|s| *s > 0).map(|s| s * 1000),
        });
    }
    (entries, next)
}

// ------------------------------------------------------------------ client

/// The InnerTube web client: its version, from the page's `ytcfg` (else a
/// recent one), and the anonymous visitor id the page was served under.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Client {
    pub version: String,
    pub visitor_data: Option<String>,
}

impl Client {
    pub(crate) fn from_page(html: &str) -> Option<Self> {
        static VERSION: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r#""INNERTUBE_CLIENT_VERSION"\s*:\s*"([0-9.]+)""#).expect("regex"));
        static VISITOR: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r#""VISITOR_DATA"\s*:\s*"([A-Za-z0-9_%=-]+)""#).expect("regex"));
        let version = VERSION.captures(html)?.get(1)?.as_str().to_string();
        let visitor_data = VISITOR.captures(html).and_then(|c| c.get(1)).map(|m| m.as_str().to_string());
        Some(Self { version, visitor_data })
    }

    /// `2.YYYYMMDD.00.00` of a week ago: InnerTube accepts recent web versions.
    pub(crate) fn fallback() -> Self {
        let days = crate::util::now_ms().div_euclid(86_400_000) - 7;
        let (y, m, d) = civil_date(days);
        Self { version: format!("2.{y:04}{m:02}{d:02}.00.00"), visitor_data: None }
    }

    fn context(&self) -> Value {
        let mut client = json!({
            "clientName": "WEB",
            "clientVersion": self.version,
            "hl": "en",
            "timeZone": "UTC",
            "utcOffsetMinutes": 0,
        });
        if let Some(v) = &self.visitor_data {
            client["visitorData"] = Value::String(v.clone());
        }
        json!({"client": client})
    }
}

/// Gregorian date of a day count since 1970-01-01.
fn civil_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests;
