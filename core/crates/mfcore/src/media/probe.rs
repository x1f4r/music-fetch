//! Parse yt-dlp `--dump-single-json --flat-playlist` output.
//!
//! Field coverage follows what yt-dlp emits for YouTube (videos, playlists,
//! channel tabs, YouTube Music), SoundCloud, Bandcamp, Mixcloud, TikTok and the
//! generic extractor. Values are read leniently: numbers may be floats or
//! strings, any field may be missing or null.

use serde_json::{Map, Value};

use crate::error::{MfError, Result};

/// Nesting depth followed when flattening playlists of playlists.
const MAX_DEPTH: usize = 8;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Chapter {
    pub start_ms: u64,
    pub end_ms: Option<u64>,
    pub title: String,
}

/// One playable item of a playlist.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MediaEntry {
    /// 0-based position in the flattened entry list.
    pub index: u32,
    /// Page URL to download (http/https).
    pub url: String,
    pub id: Option<String>,
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MediaInfo {
    pub id: Option<String>,
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub duration_ms: Option<u64>,
    pub webpage_url: Option<String>,
    pub extractor: Option<String>,
    pub thumbnail: Option<String>,
    pub description: Option<String>,
    pub chapters: Vec<Chapter>,
    /// Track/artist fields some extractors fill (YouTube Music, SoundCloud, Bandcamp).
    pub track: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    /// True when yt-dlp returned a playlist (even an empty one).
    pub is_playlist: bool,
    /// A live stream that is still running (cannot be analyzed as a file).
    pub is_live: bool,
    /// Non-empty for playlists.
    pub entries: Vec<MediaEntry>,
}

/// Parse probe JSON. Fails only when the text is not a JSON object.
pub fn parse_probe(json: &str) -> Result<MediaInfo> {
    let value: Value = serde_json::from_str(json.trim())
        .map_err(|_| MfError::Platform { message: "yt-dlp returned unreadable media information.".into() })?;
    let Some(obj) = value.as_object() else {
        return Err(MfError::Platform { message: "yt-dlp returned unexpected media information.".into() });
    };

    let kind = text(obj, "_type");
    let is_playlist =
        matches!(kind.as_deref(), Some("playlist" | "multi_video")) || obj.get("entries").is_some_and(Value::is_array);
    let mut webpage_url = first_http(obj, &["webpage_url", "original_url"]);
    if webpage_url.is_none() && matches!(kind.as_deref(), Some("url" | "url_transparent")) {
        webpage_url = first_http(obj, &["url"]);
    }

    let context = EntryContext { youtube_music: is_youtube_music(obj) };
    let mut entries = Vec::new();
    if let Some(list) = obj.get("entries").and_then(Value::as_array) {
        collect_entries(list, &context, 0, &mut entries);
    }

    Ok(MediaInfo {
        id: text(obj, "id"),
        title: text(obj, "title").or_else(|| text(obj, "fulltitle")),
        uploader: first_text(obj, &["uploader", "channel", "creator", "uploader_id"]),
        duration_ms: seconds_to_ms(obj.get("duration")),
        webpage_url,
        extractor: first_text(obj, &["extractor_key", "extractor", "ie_key"]),
        thumbnail: thumbnail(obj),
        description: text(obj, "description"),
        chapters: chapters(obj),
        track: text(obj, "track"),
        artist: artist(obj),
        album: text(obj, "album"),
        is_playlist,
        is_live: obj.get("is_live").and_then(Value::as_bool).unwrap_or(false)
            || text(obj, "live_status").as_deref() == Some("is_live"),
        entries,
    })
}

/// Timestamped lines in a description ("12:34 Artist - Title"), as (ms, text).
///
/// Ports the 0.4 rule: the first match per line of
/// `(?<!\d)(?:(\d{1,2}):)?([0-5]?\d):([0-5]\d)(?!\d)`. The text is the line
/// without its timestamps (ranges like "00:00 - 03:30" lose both) and the
/// separators around them.
pub fn description_timestamps(description: &str) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    for line in description.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(first) = find_timestamp(line.as_bytes()) else { continue };
        let mut label = strip_stamp(line, &first);
        for _ in 0..8 {
            let Some(m) = find_timestamp(label.as_bytes()) else { break };
            label = strip_stamp(&label, &m);
        }
        out.push((first.ms, label));
    }
    out
}

/// `line` without the matched timestamp and the separators hugging it:
/// "Title (3:45)" → "Title", "[00:00] Intro" → "Intro", "▶ 01:15 — Song" → "▶ Song".
fn strip_stamp(line: &str, m: &TimestampMatch) -> String {
    const BEFORE: &[char] = &[' ', '\t', '-', '–', '—', '|', '•', '·', ':', '~', '>', '»', '/', ',', '(', '['];
    const AFTER: &[char] = &[' ', '\t', '-', '–', '—', '|', '•', '·', ':', '~', '>', '»', '/', ',', '.', ')', ']'];
    let before = line[..m.start].trim_end_matches(BEFORE).trim();
    let after = line[m.end..].trim_start_matches(AFTER).trim();
    match (before.is_empty(), after.is_empty()) {
        (true, _) => after.to_string(),
        (_, true) => before.to_string(),
        _ => format!("{before} {after}"),
    }
}

struct TimestampMatch {
    start: usize,
    end: usize,
    ms: u64,
}

/// Leftmost match of the timestamp pattern, with the regex's backtracking
/// order: hours present (two digits, then one) before hours absent; two-digit
/// minutes before one-digit minutes.
fn find_timestamp(s: &[u8]) -> Option<TimestampMatch> {
    (0..s.len()).find_map(|start| {
        if start > 0 && s[start - 1].is_ascii_digit() {
            return None;
        }
        match_at(s, start).map(|(end, ms)| TimestampMatch { start, end, ms })
    })
}

fn match_at(s: &[u8], start: usize) -> Option<(usize, u64)> {
    let digit = |i: usize| s.get(i).filter(|b| b.is_ascii_digit()).map(|b| (b - b'0') as u64);
    let below6 = |i: usize| digit(i).filter(|d| *d <= 5);
    let colon = |i: usize| s.get(i) == Some(&b':');

    // `(\d{1,2}):` greedy, then the group skipped.
    let mut hour_options: Vec<(u64, usize)> = Vec::with_capacity(3);
    if let (Some(a), Some(b)) = (digit(start), digit(start + 1))
        && colon(start + 2)
    {
        hour_options.push((a * 10 + b, start + 3));
    }
    if let Some(a) = digit(start)
        && colon(start + 1)
    {
        hour_options.push((a, start + 2));
    }

    let rest = |pos: usize| -> Option<(usize, u64, u64)> {
        // `([0-5]?\d)` greedy.
        let mut minute_options = Vec::with_capacity(2);
        if let (Some(a), Some(b)) = (below6(pos), digit(pos + 1)) {
            minute_options.push((a * 10 + b, pos + 2));
        }
        if let Some(a) = digit(pos) {
            minute_options.push((a, pos + 1));
        }
        minute_options.into_iter().find_map(|(minutes, p)| {
            if !colon(p) {
                return None;
            }
            let (a, b) = (below6(p + 1)?, digit(p + 2)?);
            // `(?!\d)`
            if digit(p + 3).is_some() {
                return None;
            }
            Some((p + 3, minutes, a * 10 + b))
        })
    };

    hour_options.into_iter().map(|(h, pos)| (Some(h), pos)).chain(std::iter::once((None, start))).find_map(
        |(hours, pos)| {
            let (end, minutes, seconds) = rest(pos)?;
            Some((end, ((hours.unwrap_or(0) * 60 + minutes) * 60 + seconds) * 1000))
        },
    )
}

struct EntryContext {
    youtube_music: bool,
}

fn collect_entries(list: &[Value], context: &EntryContext, depth: usize, out: &mut Vec<MediaEntry>) {
    for value in list {
        let Some(obj) = value.as_object() else { continue };
        if let Some(nested) = obj.get("entries").and_then(Value::as_array) {
            if depth < MAX_DEPTH {
                collect_entries(nested, context, depth + 1, out);
            }
            continue;
        }
        let title = text(obj, "title").or_else(|| text(obj, "track"));
        // YouTube keeps removed items in playlists as placeholders.
        if matches!(title.as_deref(), Some("[Private video]" | "[Deleted video]" | "[Unavailable video]")) {
            continue;
        }
        let Some(url) = entry_url(obj, context) else { continue };
        out.push(MediaEntry {
            index: out.len() as u32,
            url,
            id: text(obj, "id"),
            title,
            uploader: first_text(obj, &["uploader", "channel", "artist", "creator"]),
            duration_ms: seconds_to_ms(obj.get("duration")),
        });
    }
}

/// Downloadable page URL of a flat entry, rebuilt from id + extractor when
/// yt-dlp gave no http(s) URL.
fn entry_url(obj: &Map<String, Value>, context: &EntryContext) -> Option<String> {
    if let Some(url) = first_http(obj, &["webpage_url", "original_url", "url"]) {
        return Some(url);
    }
    let id = text(obj, "id")?;
    let extractor = first_text(obj, &["ie_key", "extractor_key", "extractor"])?.to_ascii_lowercase();
    let id_ok = |extra: &[char]| id.chars().all(|c| c.is_ascii_alphanumeric() || extra.contains(&c));
    if extractor.starts_with("youtube") && id_ok(&['-', '_']) {
        if extractor == "youtubetab" {
            return Some(if id.starts_with("UC") && id.len() == 24 {
                format!("https://www.youtube.com/channel/{id}")
            } else {
                format!("https://www.youtube.com/playlist?list={id}")
            });
        }
        let host =
            if context.youtube_music || extractor.contains("music") { "music.youtube.com" } else { "www.youtube.com" };
        return Some(format!("https://{host}/watch?v={id}"));
    }
    if extractor.starts_with("vimeo") && id.chars().all(|c| c.is_ascii_digit()) {
        return Some(format!("https://vimeo.com/{id}"));
    }
    if extractor.starts_with("dailymotion") && id_ok(&[]) {
        return Some(format!("https://www.dailymotion.com/video/{id}"));
    }
    None
}

fn is_youtube_music(obj: &Map<String, Value>) -> bool {
    ["original_url", "webpage_url"]
        .iter()
        .filter_map(|k| text(obj, k))
        .any(|u| host_of(&u).is_some_and(|h| h == "music.youtube.com"))
        || first_text(obj, &["extractor", "extractor_key"])
            .is_some_and(|e| e.to_ascii_lowercase().contains("youtube:music"))
}

fn host_of(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host = authority.rsplit('@').next()?.split(':').next()?;
    Some(host.to_ascii_lowercase())
}

fn chapters(obj: &Map<String, Value>) -> Vec<Chapter> {
    let Some(list) = obj.get("chapters").and_then(Value::as_array) else { return Vec::new() };
    let mut out: Vec<Chapter> = list
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|c| {
            Some(Chapter {
                start_ms: seconds_to_ms(c.get("start_time"))?,
                end_ms: seconds_to_ms(c.get("end_time")),
                title: text(c, "title").unwrap_or_default(),
            })
        })
        .collect();
    out.sort_by_key(|c| c.start_ms);
    out
}

fn thumbnail(obj: &Map<String, Value>) -> Option<String> {
    first_http(obj, &["thumbnail"]).or_else(|| {
        // yt-dlp sorts thumbnails worst to best.
        obj.get("thumbnails")?
            .as_array()?
            .iter()
            .rev()
            .filter_map(Value::as_object)
            .find_map(|t| first_http(t, &["url"]))
    })
}

fn artist(obj: &Map<String, Value>) -> Option<String> {
    text(obj, "artist")
        .or_else(|| joined(obj, "artists"))
        .or_else(|| text(obj, "creator"))
        .or_else(|| joined(obj, "creators"))
}

fn joined(obj: &Map<String, Value>, key: &str) -> Option<String> {
    let names: Vec<String> = obj
        .get(key)?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    (!names.is_empty()).then(|| names.join(", "))
}

/// Non-empty trimmed string (numbers are formatted, e.g. numeric ids).
fn text(obj: &Map<String, Value>, key: &str) -> Option<String> {
    match obj.get(key)? {
        Value::String(s) => Some(s.trim()).filter(|s| !s.is_empty()).map(str::to_string),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn first_text(obj: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| text(obj, k))
}

fn first_http(obj: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().filter_map(|k| text(obj, k)).find(|u| {
        let lower = u.to_ascii_lowercase();
        (lower.starts_with("https://") || lower.starts_with("http://")) && !u.contains(char::is_whitespace)
    })
}

fn seconds_to_ms(value: Option<&Value>) -> Option<u64> {
    let secs = match value? {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    (0.0..1e9).contains(&secs).then(|| (secs * 1000.0).round() as u64)
}

#[cfg(test)]
mod tests;
