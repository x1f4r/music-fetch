//! Hints collected before any recognizer call: timestamped tracklists from
//! chapters, the description and top comments, page attribution cards and
//! extractor metadata. See docs/PIPELINES.md ("Hints").
//!
//! Tracklist lines come in many shapes; the parser handles `00:00 Artist - Title`,
//! `[0:00] Artist – Title (Remix)`, `1. Artist - Title [Label] 03:45`,
//! `01 | 12:30 | Artist - Title`, en/em dashes, `ID - ID`, `w/` mashups,
//! `vs.`/`x` collaborations, labels in brackets and leading emoji or bullets.
//! Version qualifiers (remix, edit, live, VIP, …) stay in the title: they name
//! the exact recording.

use std::sync::LazyLock;

use regex::Regex;

use crate::identity;
use crate::media::{Comment, MediaInfo};
use crate::model::Track;

/// Fewest entries for a list to count as a tracklist.
pub const MIN_ENTRIES: usize = 3;

/// Where a tracklist came from, most trusted first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub enum HintSource {
    Chapters,
    Description,
    PinnedComment,
    UploaderComment,
    Comment,
}

/// One tracklist entry.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackHint {
    /// Start in the media.
    pub start_ms: u64,
    /// Artist credit as written (may be empty when the line has no separator).
    pub artist: String,
    /// Title including its version qualifier, e.g. "Opus (Four Tet Remix)".
    pub title: String,
    /// True for unreleased/unknown entries ("ID - ID").
    pub is_id: bool,
    /// The line as written, without its timestamp.
    pub raw: String,
}

impl TrackHint {
    /// The entry as a track (title and artist only).
    pub fn to_track(&self) -> Track {
        let artists = split_artists(&self.artist);
        let mut track = Track { title: self.title.clone(), artist: self.artist.clone(), artists, ..Track::default() };
        identity::refresh_identity(&mut track);
        track
    }
}

/// A timestamped tracklist chosen from one source.
#[derive(Debug, Clone, PartialEq)]
pub struct Tracklist {
    pub source: HintSource,
    pub entries: Vec<TrackHint>,
}

/// Everything the page and the extractor say about the media.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Hints {
    /// The best timestamped tracklist, if any source has one.
    pub tracklist: Option<Tracklist>,
    /// Songs without times (YouTube "Music" cards, Shorts or TikTok sound).
    pub untimed: Vec<Track>,
    /// The extractor's own track/artist fields (YouTube Music, Topic channels,
    /// SoundCloud, Bandcamp): the media is that one recording.
    pub extractor_track: Option<Track>,
    /// "Artist - Title" parsed from the media title, noise like "(Official Video)" removed.
    pub title_track: Option<Track>,
    /// How much the title reads like a mix, set, show or compilation.
    pub mix_words: MixWords,
    /// The description announces a tracklist (with or without times).
    pub mentions_tracklist: bool,
    /// The title marks a music video ("Official Video"), so intros and outros may be long.
    pub music_video: bool,
}

/// Collect hints from a probe and the link resolver's page hints.
pub fn collect(info: &MediaInfo, page_hints: &[Track], duration_ms: Option<u64>) -> Hints {
    let duration = duration_ms.or(info.duration_ms);
    let mut lists: Vec<Tracklist> = Vec::new();
    if !info.chapters.is_empty() {
        let lines: Vec<(u64, String)> = info.chapters.iter().map(|c| (c.start_ms, c.title.clone())).collect();
        if let Some(entries) = accept(entries_from_timed(&lines), duration) {
            lists.push(Tracklist { source: HintSource::Chapters, entries });
        }
    }
    if let Some(description) = info.description.as_deref()
        && let Some(entries) = parse_tracklist(description, duration)
    {
        lists.push(Tracklist { source: HintSource::Description, entries });
    }
    for comment in &info.comments {
        if let Some(entries) = parse_tracklist(&comment.text, duration) {
            lists.push(Tracklist { source: comment_source(comment), entries });
        }
    }
    // Most valid entries wins; ties go to the more trusted source.
    let mut tracklist =
        lists.into_iter().max_by(|a, b| a.entries.len().cmp(&b.entries.len()).then_with(|| b.source.cmp(&a.source)));
    // Titles without artists (a live set, an album): the performer from the media title.
    if let Some(list) = tracklist.as_mut() {
        let bare = list.entries.iter().filter(|e| e.artist.is_empty() && !e.is_id).count();
        if bare * 10 >= list.entries.len() * 6
            && let Some(artist) = info.title.as_deref().and_then(performer)
        {
            for entry in list.entries.iter_mut().filter(|e| e.artist.is_empty() && !e.is_id) {
                entry.artist = artist.clone();
            }
        }
    }

    let extractor_track = info.track.as_deref().filter(|t| !t.trim().is_empty()).and_then(|title| {
        let artist = info.artist.clone().or_else(|| info.uploader.as_deref().and_then(topic_artist))?;
        let mut track = Track {
            title: title.trim().to_string(),
            artists: split_artists(&artist),
            artist,
            album: info.album.clone(),
            duration_ms: duration,
            ..Track::default()
        };
        identity::refresh_identity(&mut track);
        Some(track)
    });
    let title = info.title.as_deref().unwrap_or_default();
    let title_track = parse_media_title(title, info.uploader.as_deref()).map(|(artist, title)| {
        let mut track =
            Track { title, artists: split_artists(&artist), artist, duration_ms: duration, ..Track::default() };
        identity::refresh_identity(&mut track);
        track
    });
    Hints {
        tracklist,
        untimed: page_hints.to_vec(),
        extractor_track,
        title_track,
        mix_words: mix_words(title),
        mentions_tracklist: info.description.as_deref().is_some_and(description_mix_words),
        music_video: is_music_video_title(title),
    }
}

fn comment_source(comment: &Comment) -> HintSource {
    if comment.is_pinned {
        HintSource::PinnedComment
    } else if comment.author_is_uploader {
        HintSource::UploaderComment
    } else {
        HintSource::Comment
    }
}

/// The performer named at the start of a media title: "Kiasmos live for
/// Cercle …" → "Kiasmos", "ATEEZ(에이티즈) "THE WORLD" || FULL ALBUM" → "ATEEZ".
pub fn performer(title: &str) -> Option<String> {
    // ASCII lowercase keeps byte offsets valid for `title`.
    let lower = title.to_ascii_lowercase();
    let cuts = [
        " live ",
        " - ",
        " – ",
        " — ",
        " | ",
        "||",
        " @ ",
        " at ",
        " for ",
        " b2b ",
        " presents ",
        " pres. ",
        "(",
        "[",
        "\"",
        "“",
        ":",
    ];
    let end = cuts.iter().filter_map(|c| lower.find(c)).min().unwrap_or(title.len());
    let name = title[..end].trim().trim_matches(|c: char| !c.is_alphanumeric()).trim();
    let plausible = !name.is_empty()
        && name.chars().count() <= 40
        && name.split_whitespace().count() <= 4
        && mix_words(name) == MixWords::None
        && !name.chars().any(|c| c.is_ascii_digit());
    plausible.then(|| name.to_string())
}

/// "Artist - Topic" → "Artist".
fn topic_artist(uploader: &str) -> Option<String> {
    uploader.strip_suffix(" - Topic").map(|a| a.trim().to_string()).filter(|a| !a.is_empty())
}

// ---------------------------------------------------------------------------
// Tracklist parsing

static TIMESTAMP: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?:^|[^\d:])((?:(\d{1,2}):)?([0-5]?\d):([0-5]\d))(?:$|[^\d:])"));
static LEADING_NUMBER: LazyLock<Regex> =
    LazyLock::new(|| regex(r"^(?:#?\d{1,3}\s*[\.\):|/]|#\d{1,3}\s+|\d{1,3}\s+[|/]|\d{1,3}\s+[-–—]\s+)\s*"));
static TRACK_NUMBER: LazyLock<Regex> = LazyLock::new(|| regex(r"(?i)^(?:track|tr\.?|no\.?|#)\s*\d{1,3}$"));
static SEPARATOR: LazyLock<Regex> = LazyLock::new(|| regex(r"\s+[-–—‒~]{1,2}\s+|\s+[-–—]\s*|\s*[–—]\s+"));
static BRACKET_GROUP: LazyLock<Regex> = LazyLock::new(|| regex(r"[\(\[\{]([^\(\)\[\]\{\}]*)[\)\]\}]"));
static MASHUP: LazyLock<Regex> = LazyLock::new(|| regex(r"(?i)\s+(?:w/|with\s+/|vs\.?\s+.*\s+-\s+).*$"));
static ARTIST_SPLIT: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?i)\s*(?:,|&|\band\b|\bfeat\b\.?|\bft\b\.?|\bfeaturing\b|\bvs\b\.?|\bx\b|×|\+|/)\s*"));
static SECTION_WORDS: LazyLock<Regex> = LazyLock::new(|| {
    regex(
        r"(?i)^(?:intro|outro|verse|pre[- ]?chorus|chorus|hook|bridge|drop|build[- ]?up|break(?:down)?|interlude|solo|refrain|credits|end|ending|start|beginning|part\s*\w+|act\s*\w+|chapter\s*\w+|speech|talk|q\s*&\s*a)\b",
    )
});
static NOISE_GROUP: LazyLock<Regex> = LazyLock::new(|| {
    regex(
        r"(?i)^\s*(?:official\s+)?(?:music\s+|lyrics?\s+|audio\s+|hd\s+|4k\s+|visuali[sz]er\s+|animated\s+)*(?:video|audio|lyrics?|visuali[sz]er|clip|mv|m/v|hd|hq|4k|explicit|clean|out now|free download|premiere|full song|official|remaster(?:ed)?(?:\s+\d{4})?|\d{4}\s+remaster(?:ed)?|with lyrics|lyric video|official (?:music )?video|official audio|official lyric video)\s*$",
    )
});
static VERSION_WORDS: LazyLock<Regex> = LazyLock::new(|| {
    regex(
        r"(?i)\b(?:re-?mix|rmx|edit|extended|live|acoustic|unplugged|vip|bootleg|rework|re-work|flip|sped\s*up|speed\s*up|slowed|reverb|nightcore|instrumental|a\s*cappella|acapella|dub|mix|version|demo|mashup|cover|remaster(?:ed)?|orchestral|piano)\b",
    )
});
/// Words that never distinguish one recording from another.
const FILLER: &[&str] = &[
    "extended",
    "radio",
    "edit",
    "mix",
    "version",
    "original",
    "club",
    "the",
    "a",
    "official",
    "short",
    "long",
    "full",
    "remaster",
    "remastered",
    "single",
    "album",
    "main",
    "clean",
    "explicit",
    "edited",
    "intro",
    "outro",
    "vocal",
    "length",
    "cut",
    "lp",
    "ep",
    "of",
    "and",
];
/// Words that make a group a distinct recording (remix, live, acoustic, ...).
const DISTINCT: &[&str] = &[
    "remix",
    "rmx",
    "live",
    "acoustic",
    "unplugged",
    "vip",
    "bootleg",
    "rework",
    "flip",
    "sped",
    "slowed",
    "reverb",
    "nightcore",
    "instrumental",
    "acapella",
    "cappella",
    "dub",
    "mashup",
    "cover",
    "demo",
    "orchestral",
    "piano",
    "remake",
    "rerecorded",
    "re-recorded",
    "taylors",
];

fn regex(pattern: &str) -> Regex {
    // Patterns are compile-time constants covered by the tests below.
    Regex::new(pattern).expect("hint regex is valid")
}

/// Parse a timestamped tracklist out of free text (a description or comment).
///
/// Returns the entries when at least [`MIN_ENTRIES`] have strictly increasing
/// start times inside `duration_ms` (when known) and the list reads like songs
/// rather than sections of one song ("Intro", "Chorus", ...). Times written as
/// track lengths (`1. Song 03:45`) are accumulated into start times.
pub fn parse_tracklist(text: &str, duration_ms: Option<u64>) -> Option<Vec<TrackHint>> {
    let lines: Vec<(u64, String)> = text
        .lines()
        .filter_map(|line| {
            let (ms, label) = split_timestamp(line)?;
            Some((ms, label))
        })
        .collect();
    if lines.len() < MIN_ENTRIES {
        return None;
    }
    if let Some(entries) = accept(entries_from_timed(&lines), duration_ms) {
        return Some(entries);
    }
    // Lengths instead of start times: accumulate.
    let duration = duration_ms?;
    let total: u64 = lines.iter().map(|(ms, _)| *ms).sum();
    let plausible = lines.iter().all(|(ms, _)| (20_000..=20 * 60_000).contains(ms))
        && total as f64 >= 0.6 * duration as f64
        && total as f64 <= 1.05 * duration as f64;
    if !plausible {
        return None;
    }
    let mut start = 0;
    let starts: Vec<(u64, String)> = lines
        .iter()
        .map(|(len, label)| {
            let entry = (start, label.clone());
            start += len;
            entry
        })
        .collect();
    accept(entries_from_timed(&starts), duration_ms)
}

/// Keep the longest run of strictly increasing entries; reject section lists.
fn accept(entries: Vec<TrackHint>, duration_ms: Option<u64>) -> Option<Vec<TrackHint>> {
    let mut kept: Vec<TrackHint> = Vec::with_capacity(entries.len());
    for entry in entries {
        if duration_ms.is_some_and(|d| entry.start_ms >= d) {
            continue;
        }
        if kept.last().is_some_and(|last| entry.start_ms <= last.start_ms) {
            // A restart (a second list, or a typo): keep whichever run is longer.
            if entry.start_ms == 0 && kept.len() < 3 {
                kept.clear();
            } else {
                continue;
            }
        }
        kept.push(entry);
    }
    if kept.len() < MIN_ENTRIES {
        return None;
    }
    let sections = kept.iter().filter(|e| e.artist.is_empty() && SECTION_WORDS.is_match(&e.title)).count();
    let songs = kept.iter().filter(|e| !e.artist.is_empty() || e.is_id).count();
    if sections * 2 >= kept.len() || (songs == 0 && sections > 0) {
        return None;
    }
    Some(kept)
}

fn entries_from_timed(lines: &[(u64, String)]) -> Vec<TrackHint> {
    lines
        .iter()
        .filter_map(|(ms, label)| parse_entry(label).map(|(artist, title, is_id)| (ms, artist, title, is_id, label)))
        .map(|(ms, artist, title, is_id, label)| TrackHint {
            start_ms: *ms,
            artist,
            title,
            is_id,
            raw: label.trim().to_string(),
        })
        .collect()
}

/// First timestamp in `line` and the line without it (and without the
/// brackets or separators hugging it). Later timestamps (ranges, lengths) are
/// dropped too.
pub fn split_timestamp(line: &str) -> Option<(u64, String)> {
    let caps = TIMESTAMP.captures(line)?;
    let whole = caps.get(1)?;
    let hours: u64 = caps.get(2).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
    let minutes: u64 = caps.get(3)?.as_str().parse().ok()?;
    let seconds: u64 = caps.get(4)?.as_str().parse().ok()?;
    let ms = ((hours * 60 + minutes) * 60 + seconds) * 1000;
    let mut rest = format!("{} {}", &line[..whole.start()], &line[whole.end()..]);
    // Drop further timestamps ("00:00 - 03:30", trailing lengths).
    while let Some(c) = TIMESTAMP.captures(&rest) {
        let Some(m) = c.get(1) else { break };
        rest = format!("{} {}", &rest[..m.start()], &rest[m.end()..]);
    }
    // Empty brackets left behind: "[]", "()".
    let rest = rest.replace("[ ]", " ").replace("( )", " ").replace("[]", " ").replace("()", " ");
    Some((ms, rest))
}

/// Artist, title (with version) and whether the entry is an unknown "ID".
/// `None` for lines without letters.
pub fn parse_entry(label: &str) -> Option<(String, String, bool)> {
    // Invisible marks (LRM, zero-width spaces) hide separators.
    let label = label.replace(['\u{200e}', '\u{200f}', '\u{200b}', '\u{2060}', '\u{feff}'], "");
    let mut text = clean_edges(&label);
    // Leading track numbers: "01.", "1)", "#1", "01 |", "1 - ".
    for _ in 0..2 {
        let stripped = LEADING_NUMBER.replace(&text, "").to_string();
        if stripped == text {
            break;
        }
        text = clean_edges(&stripped);
    }
    // Labels in square brackets that are not version qualifiers: "[Armada]".
    text = BRACKET_GROUP
        .replace_all(&text, |caps: &regex::Captures<'_>| {
            let whole = caps.get(0).map_or("", |m| m.as_str());
            let inner = caps.get(1).map_or("", |m| m.as_str());
            if whole.starts_with('[') && !VERSION_WORDS.is_match(inner) && !inner.to_lowercase().contains("feat") {
                String::new()
            } else {
                whole.to_string()
            }
        })
        .to_string();
    text = collapse_spaces(&text);
    text = clean_edges(&text);
    if !text.chars().any(char::is_alphanumeric) {
        return None;
    }
    let (mut artist, mut title) = match SEPARATOR.find(&text) {
        Some(m) if m.start() > 0 && m.end() < text.len() => {
            (text[..m.start()].trim().to_string(), text[m.end()..].trim().to_string())
        }
        _ => (String::new(), text.clone()),
    };
    // "Track 01 - Title": the number is not an artist.
    if TRACK_NUMBER.is_match(&artist) {
        artist.clear();
    }
    // "Title (Original Mix) - Artist": the version sits on the left.
    let bracket_version =
        |s: &str| BRACKET_GROUP.captures_iter(s).any(|c| c.get(1).is_some_and(|g| VERSION_WORDS.is_match(g.as_str())));
    if bracket_version(&artist) && !bracket_version(&title) {
        std::mem::swap(&mut artist, &mut title);
    }
    // "Title - Extended Mix" → "Title (Extended Mix)"; a second separator in
    // the title otherwise belongs to the title.
    let title = match SEPARATOR.find(&title) {
        Some(m) if VERSION_WORDS.is_match(&title[m.end()..]) && !title[m.end()..].contains('(') => {
            format!("{} ({})", title[..m.start()].trim(), title[m.end()..].trim())
        }
        _ => title,
    };
    // Mashups: keep the first song ("A - B w/ C - D").
    let title = MASHUP.replace(&title, "").trim().to_string();
    let title = clean_edges(&title);
    let artist = clean_edges(&artist);
    let is_id = is_unknown(&title) && (artist.is_empty() || is_unknown(&artist));
    if title.is_empty() {
        return None;
    }
    Some((artist, title, is_id))
}

fn is_unknown(text: &str) -> bool {
    let t = text.trim().trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase();
    matches!(t.as_str(), "id" | "unknown" | "tba" | "untitled" | "unreleased" | "") || text.trim().starts_with("ID (")
}

/// Trim whitespace, bullets, emoji and dangling separators from both ends.
fn clean_edges(text: &str) -> String {
    let keep_start = |c: char| c.is_alphanumeric() || "([{\"'¡¿#".contains(c);
    let keep_end = |c: char| c.is_alphanumeric() || ")]}\"'!?.".contains(c);
    let start = text.find(keep_start).unwrap_or(text.len());
    let trimmed = &text[start..];
    let end = trimmed.rfind(keep_end).map(|i| i + trimmed[i..].chars().next().map_or(1, char::len_utf8)).unwrap_or(0);
    trimmed[..end].trim().to_string()
}

fn collapse_spaces(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Individual artists of a credit: "A & B feat. C" → ["A", "B", "C"].
pub fn split_artists(artist: &str) -> Vec<String> {
    ARTIST_SPLIT
        .split(artist)
        .map(|s| s.trim().trim_matches(|c: char| c == '(' || c == ')').trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// ---------------------------------------------------------------------------
// Versions

/// The part of a title that names a distinct recording, normalized: "" for the
/// original, "tiesto remix" for "(Tiësto Extended Remix)", "live" for
/// "(Live)". Length variants (radio edit, extended mix, original mix,
/// remaster) map to "" so they compare equal to the original.
pub fn version_key(title: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut consider = |group: &str| {
        let lower = group.to_lowercase();
        if lower.contains("feat") || lower.starts_with("ft.") || lower.starts_with("with ") {
            return;
        }
        let words: Vec<String> = clean_words(&lower);
        if !words.iter().any(|w| DISTINCT.contains(&w.as_str())) {
            return;
        }
        let kept: Vec<&str> = words
            .iter()
            .filter(|w| !FILLER.contains(&w.as_str()))
            .map(|w| match w.as_str() {
                "rmx" => "remix",
                "acapella" => "cappella",
                other => other,
            })
            .collect();
        if !kept.is_empty() {
            parts.push(kept.join(" "));
        }
    };
    for caps in BRACKET_GROUP.captures_iter(title) {
        if let Some(inner) = caps.get(1) {
            consider(inner.as_str());
        }
    }
    // "Title - Live at X" style clauses.
    let outside = BRACKET_GROUP.replace_all(title, " ");
    if let Some(m) = SEPARATOR.find(&outside)
        && m.start() > 0
    {
        consider(&outside[m.end()..]);
    }
    parts.sort();
    parts.dedup();
    parts.join(" | ")
}

fn clean_words(text: &str) -> Vec<String> {
    use unicode_normalization::UnicodeNormalization;
    use unicode_normalization::char::canonical_combining_class;
    let folded: String = text
        .nfkd()
        .filter(|c| canonical_combining_class(*c) == 0)
        .map(|c| if c.is_alphanumeric() || c == '\'' { c } else { ' ' })
        .collect();
    folded.split_whitespace().map(|w| w.replace('\'', "")).filter(|w| !w.is_empty()).collect()
}

/// Same song in the same version (remix vs original are different).
pub fn same_version(a: &Track, b: &Track) -> bool {
    versions_compatible(&version_key(&a.title), &version_key(&b.title))
}

/// Two version keys name the same recording: both originals, or both share a
/// version word ("live", "remix") and, when both name people or places, at
/// least one of those ("tiesto remix" vs "afrojack remix" differ; "live" and
/// "live at wembley 1986" agree).
pub fn versions_compatible(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    if a.is_empty() || b.is_empty() {
        return false;
    }
    let split = |key: &str| -> (Vec<String>, Vec<String>) {
        key.split([' ', '|'])
            .filter(|w| !w.is_empty())
            .map(str::to_string)
            .partition(|w| DISTINCT.contains(&w.as_str()) || w == "up" || w == "a")
    };
    let (kinds_a, names_a) = split(a);
    let (kinds_b, names_b) = split(b);
    let kinds_shared = kinds_a.iter().any(|k| k != "up" && k != "a" && kinds_b.contains(k));
    let names_ok = names_a.is_empty() || names_b.is_empty() || names_a.iter().any(|n| names_b.contains(n));
    kinds_shared && names_ok
}

/// Same recording for stitching and deduplication: different ISRCs never
/// are, shared provider ids decide, otherwise the same song in the same
/// version (a remix is not its original; "Original Mix" and "Radio Edit" are).
pub fn same_recording(a: &Track, b: &Track) -> bool {
    let isrc = |t: &Track| t.isrc.as_deref().and_then(identity::normalize_isrc);
    if let (Some(x), Some(y)) = (isrc(a), isrc(b)) {
        return x == y;
    }
    let mut shared = false;
    for (provider, _) in identity::PROVIDER_ID_PREFIXES {
        let id = |t: &Track| t.ids.get(provider).map(|v| v.trim().to_lowercase()).filter(|v| !v.is_empty());
        if let (Some(x), Some(y)) = (id(a), id(b)) {
            if x == y {
                return true;
            }
            shared = true;
        }
    }
    !shared && same_song(a, b) && same_version(a, b)
}

/// The bare song title: no bracketed groups, no " - Version" clause, no
/// featured artists; normalized like identity keys.
fn song_title(title: &str) -> String {
    let mut text = title.to_string();
    for _ in 0..3 {
        let next = BRACKET_GROUP.replace_all(&text, " ").to_string();
        if next == text {
            break;
        }
        text = next;
    }
    if let Some(m) = SEPARATOR.find(&text)
        && m.start() > 0
    {
        let tail = &text[m.end()..];
        if VERSION_WORDS.is_match(tail) || tail.to_lowercase().contains("feat") {
            text.truncate(m.start());
        }
    }
    let bare = identity::normalize_title(&text);
    if bare.is_empty() { identity::normalize_title(title) } else { bare }
}

/// Same song, version ignored (for "is the hint the song we hear?").
pub fn same_song(a: &Track, b: &Track) -> bool {
    if identity::same_track(a, b) {
        return true;
    }
    let ta = song_title(&a.title);
    let tb = song_title(&b.title);
    if ta.is_empty() || tb.is_empty() {
        return false;
    }
    let similarity = strsim::normalized_levenshtein(&ta, &tb);
    let same_artists = {
        let (x, y) = (identity::normalize_artists(&a.artist), identity::normalize_artists(&b.artist));
        !x.is_empty() && x == y
    };
    // Spelling drift in tracklists ("Marido Dance" for "Marido Dansa") passes
    // when the artists are exactly the same.
    let title_ok = ta == tb
        || similarity >= 0.85
        || (same_artists && similarity >= 0.75)
        || (ta.len() >= 6 && tb.len() >= 6 && (ta.contains(&tb) || tb.contains(&ta)));
    title_ok && artists_overlap(&a.artist, &b.artist)
}

/// Any credited artist in common (fuzzy), or one side has no artist.
pub fn artists_overlap(a: &str, b: &str) -> bool {
    let norm = |s: &str| -> Vec<String> {
        split_artists(s).iter().map(|x| identity::normalize_artists(x)).filter(|x| !x.is_empty()).collect()
    };
    let (xa, xb) = (norm(a), norm(b));
    if xa.is_empty() || xb.is_empty() {
        return true;
    }
    xa.iter().any(|x| {
        xb.iter().any(|y| {
            x == y
                || strsim::normalized_levenshtein(x, y) >= 0.8
                || (x.len() >= 4 && y.contains(x.as_str()))
                || (y.len() >= 4 && x.contains(y.as_str()))
        })
    })
}

// ---------------------------------------------------------------------------
// Media titles

/// "Artist - Title" from a video title, without noise such as
/// "(Official Video)", "[HD]" or "| Lyrics". Version qualifiers stay. Uses a
/// Topic channel ("Artist - Topic") or a "VEVO" channel as the artist when the
/// title has no separator.
pub fn parse_media_title(title: &str, uploader: Option<&str>) -> Option<(String, String)> {
    let mut text = title.to_string();
    // "Song | Official Video" → "Song".
    if let Some(i) = text.find(" | ") {
        let tail = text[i + 3..].to_lowercase();
        if tail.contains("official") || tail.contains("lyric") || tail.contains("video") || tail.contains("audio") {
            text.truncate(i);
        }
    }
    text = BRACKET_GROUP
        .replace_all(&text, |caps: &regex::Captures<'_>| {
            let inner = caps.get(1).map_or("", |m| m.as_str());
            if is_noise_group(inner) { String::new() } else { caps.get(0).map_or("", |m| m.as_str()).to_string() }
        })
        .to_string();
    text = collapse_spaces(&text);
    let (artist, song, _) = parse_entry(&text)?;
    if !artist.is_empty() {
        return Some((artist, song));
    }
    let channel = uploader?.trim();
    let artist = topic_artist(channel)
        .or_else(|| channel.strip_suffix("VEVO").map(|a| a.trim().to_string()))
        .or_else(|| channel.strip_suffix(" Official").map(|a| a.trim().to_string()))?;
    (!artist.is_empty()).then_some((artist, song))
}

/// A bracket group about the upload rather than the recording: "Official
/// Video", "HD", "Lyrics", "Official Video - Shortened Version". Groups that
/// name a version ("Official Live Video") stay.
fn is_noise_group(inner: &str) -> bool {
    if NOISE_GROUP.is_match(inner) {
        return true;
    }
    let words = clean_words(&inner.to_lowercase());
    let uploadish = words.iter().any(|w| {
        matches!(
            w.as_str(),
            "official"
                | "video"
                | "lyric"
                | "lyrics"
                | "visualizer"
                | "visualiser"
                | "audio"
                | "hd"
                | "hq"
                | "4k"
                | "mv"
        )
    });
    uploadish && !words.iter().any(|w| DISTINCT.contains(&w.as_str()))
}

/// Title marks a music video, which may carry long intros, outros or skits.
pub fn is_music_video_title(title: &str) -> bool {
    let lower = title.to_lowercase();
    lower.contains("official video")
        || lower.contains("official music video")
        || lower.contains("music video")
        || lower.contains("(video)")
        || lower.contains("short film")
}

/// How strongly the words of a title suggest a mix, set, show or compilation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum MixWords {
    #[default]
    None,
    /// "mix" or "set" on their own: songs use these words too ("Little Mix").
    Weak,
    /// Phrases songs practically never use ("DJ set", "Boiler Room", "full album").
    Strong,
}

/// Mix words in a title. Version qualifiers ("(Extended Mix)", "Radio Edit") do not count.
pub fn mix_words(title: &str) -> MixWords {
    let outside = BRACKET_GROUP.replace_all(title, |caps: &regex::Captures<'_>| {
        let inner = caps.get(1).map_or("", |m| m.as_str()).to_lowercase();
        // Keep brackets about the upload rather than a version ("[Full Set]", "(DJ Mix)").
        if ["full", "set", "dj mix", "live stream", "tracklist", "continuous", "mixed"]
            .iter()
            .any(|w| inner.contains(w))
        {
            format!(" {inner} ")
        } else {
            " ".to_string()
        }
    });
    let lower = outside.to_lowercase().replace(['|', '-', '–', '—', ':', ',', '/', '#', '@', '!', '.'], " ");
    let joined = format!(" {} ", lower.split_whitespace().collect::<Vec<_>>().join(" "));
    const STRONG: &[&str] = &[
        " dj set ",
        " live set ",
        " full set ",
        " boiler room ",
        " tracklist ",
        " compilation ",
        " playlist ",
        " radio show ",
        " full album ",
        " full ep ",
        " album stream ",
        " b2b ",
        " nonstop ",
        " non stop ",
        " continuous mix ",
        " mixed by ",
        " essential mix ",
        " megamix ",
        " mixtape ",
        " minimix ",
        " mini mix ",
        " year mix ",
        " dj mix ",
        " best of 20",
        " top 50 ",
        " top 100 ",
        " top 40 ",
        " top 20 ",
        " hour mix ",
        " hours mix ",
        " 1 hour ",
        " 2 hours ",
        " 3 hours ",
        " 1h ",
        " 2h ",
        " a state of trance ",
        " asot ",
        " cercle ",
        " episode ",
        " podcast ",
        " medley ",
        " live stream ",
        " guest mix ",
        " mixmag ",
        " essentials ",
    ];
    if STRONG.iter().any(|p| joined.contains(p)) {
        return MixWords::Strong;
    }
    let weak = [" mix ", " set ", " session ", " sessions "].iter().any(|p| joined.contains(p))
        && !(joined.contains(" little mix ") || joined.contains(" set fire ") || joined.contains(" set me "));
    if weak { MixWords::Weak } else { MixWords::None }
}

fn description_mix_words(description: &str) -> bool {
    let lower = description.to_lowercase();
    lower.contains("tracklist") || lower.contains("track list") || lower.contains("tracklisting")
}

#[cfg(test)]
mod tests;
