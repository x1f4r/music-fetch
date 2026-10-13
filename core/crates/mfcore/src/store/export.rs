//! Session exports: plain-text tracklist, CSV, JSON and CUE sheet.

use std::fmt::Write as _;
use std::path::Path;

use super::codec::DbText;
use crate::error::{MfError, Result};
use crate::model::{ExportFormat, Segment, SegmentKind, SessionDetail, SessionKind, Track};

/// Placeholder for an unknown artist or title (tracklist convention).
const UNKNOWN: &str = "ID";

const CSV_HEADER: &str = "index,start,end,kind,tier,confidence,title,artist,album,isrc,links";

/// CUE sheets count 75 frames per second.
const CUE_FPS: u64 = 75;

/// Renders a session in `format`.
///
/// - `Text`: one line per music segment, `[mm:ss] Artist - Title` (`[h:mm:ss]`
///   from one hour on). Collections (albums, playlists, multi-item sessions)
///   number their lines instead (`1. Artist - Title`). Unidentified music reads
///   `ID - ID`. Sessions without segments list their track or candidates.
/// - `Csv`: RFC 4180 with a header row; every segment, or the track or
///   candidates (kind `candidate`) when there are none.
/// - `Json`: the pretty-printed `SessionDetail`.
/// - `Cue`: one `TRACK` per music segment; fails for sessions without a timeline.
pub fn render(detail: &SessionDetail, format: ExportFormat) -> Result<String> {
    match format {
        ExportFormat::Text => Ok(text(detail)),
        ExportFormat::Csv => Ok(csv(detail)),
        ExportFormat::Json => json(detail),
        ExportFormat::Cue => cue(detail),
    }
}

fn sorted_segments(detail: &SessionDetail) -> Vec<&Segment> {
    let mut segments: Vec<&Segment> = detail.segments.iter().collect();
    segments.sort_by_key(|s| (s.item_index, s.start_ms, s.end_ms));
    segments
}

fn music_segments(detail: &SessionDetail) -> Vec<&Segment> {
    let mut segments = sorted_segments(detail);
    segments.retain(|s| matches!(s.kind, SegmentKind::Track | SegmentKind::Unidentified));
    segments
}

/// True for one recording whose segments carry positions on its timeline.
fn has_timeline(detail: &SessionDetail, segments: &[&Segment]) -> bool {
    let single_item = detail.source.as_ref().is_none_or(|s| s.item_count <= 1)
        && segments
            .windows(2)
            .all(|w| w[0].item_index == w[1].item_index);
    single_item && segments.iter().any(|s| s.end_ms > 0)
}

/// Collapses control characters so untrusted metadata cannot break lines.
fn one_line(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for word in text.split(char::is_control).flat_map(str::split_whitespace) {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    out
}

fn or_unknown(text: String) -> String {
    if text.is_empty() {
        UNKNOWN.to_string()
    } else {
        text
    }
}

fn artist_of(track: &Track) -> String {
    let artist = one_line(&track.artist);
    if artist.is_empty() {
        one_line(&track.artists.join(", "))
    } else {
        artist
    }
}

fn artist_title(track: Option<&Track>) -> (String, String) {
    match track {
        Some(t) => (or_unknown(artist_of(t)), or_unknown(one_line(&t.title))),
        None => (UNKNOWN.to_string(), UNKNOWN.to_string()),
    }
}

fn segment_track(segment: &Segment) -> Option<&Track> {
    if segment.kind == SegmentKind::Track {
        segment.track.as_ref()
    } else {
        None
    }
}

fn display_name(track: Option<&Track>) -> String {
    let (artist, title) = artist_title(track);
    format!("{artist} - {title}")
}

/// `mm:ss` below one hour, `h:mm:ss` from one hour on.
fn clock(ms: u64) -> String {
    let secs = ms / 1000;
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m:02}:{s:02}")
    }
}

/// `hh:mm:ss`, unambiguous for spreadsheets.
fn clock_hms(ms: u64) -> String {
    let secs = ms / 1000;
    format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

fn text(detail: &SessionDetail) -> String {
    let mut out = String::new();
    let music = music_segments(detail);
    if music.is_empty() {
        if let Some(track) = &detail.entry.track {
            let _ = writeln!(out, "{}", display_name(Some(track)));
        } else {
            for (n, candidate) in detail.candidates.iter().enumerate() {
                let _ = writeln!(out, "{}. {}", n + 1, display_name(Some(&candidate.track)));
            }
        }
        return out;
    }
    let timeline = has_timeline(detail, &music);
    for (n, segment) in music.iter().enumerate() {
        let name = display_name(segment_track(segment));
        let _ = if timeline {
            writeln!(out, "[{}] {name}", clock(segment.start_ms))
        } else {
            writeln!(out, "{}. {name}", n + 1)
        };
    }
    out
}

fn csv_field(out: &mut String, value: &str) {
    if value.contains([',', '"', '\n', '\r']) || value.starts_with(' ') || value.ends_with(' ') {
        out.push('"');
        out.push_str(&value.replace('"', "\"\""));
        out.push('"');
    } else {
        out.push_str(value);
    }
}

/// Text from remote metadata; a leading formula character is neutralized so
/// spreadsheets do not evaluate it.
fn csv_text(out: &mut String, value: &str) {
    if value.starts_with(['=', '+', '-', '@', '\t', '\r']) {
        csv_field(out, &format!("'{value}"));
    } else {
        csv_field(out, value);
    }
}

struct CsvRow<'a> {
    start: String,
    end: String,
    kind: &'static str,
    tier: &'static str,
    confidence: Option<f32>,
    track: Option<&'a Track>,
}

fn csv_row(out: &mut String, index: usize, row: &CsvRow<'_>) {
    let _ = write!(
        out,
        "{index},{},{},{},{},",
        row.start, row.end, row.kind, row.tier
    );
    if let Some(confidence) = row.confidence {
        let _ = write!(out, "{confidence:.2}");
    }
    let track = row.track;
    let artist = track.map(artist_of).unwrap_or_default();
    for value in [
        track.map(|t| t.title.as_str()).unwrap_or(""),
        artist.as_str(),
        track.and_then(|t| t.album.as_deref()).unwrap_or(""),
        track.and_then(|t| t.isrc.as_deref()).unwrap_or(""),
    ] {
        out.push(',');
        csv_text(out, value);
    }
    let links: Vec<&str> = track
        .map(|t| {
            t.links
                .iter()
                .filter(|l| !l.is_search)
                .map(|l| l.url.as_str())
                .collect()
        })
        .unwrap_or_default();
    out.push(',');
    csv_text(out, &links.join(" | "));
    out.push_str("\r\n");
}

fn csv(detail: &SessionDetail) -> String {
    let mut out = String::from(CSV_HEADER);
    out.push_str("\r\n");
    let segments = sorted_segments(detail);
    if segments.is_empty() {
        if let Some(track) = &detail.entry.track {
            let row = CsvRow {
                start: String::new(),
                end: String::new(),
                kind: SegmentKind::Track.as_text(),
                tier: "",
                confidence: None,
                track: Some(track),
            };
            csv_row(&mut out, 1, &row);
        } else {
            for (n, candidate) in detail.candidates.iter().enumerate() {
                let row = CsvRow {
                    start: String::new(),
                    end: String::new(),
                    kind: "candidate",
                    tier: candidate.tier.as_text(),
                    confidence: Some(candidate.score),
                    track: Some(&candidate.track),
                };
                csv_row(&mut out, n + 1, &row);
            }
        }
        return out;
    }
    let timed = segments.iter().any(|s| s.start_ms > 0 || s.end_ms > 0);
    for (n, segment) in segments.iter().enumerate() {
        let time = |ms| if timed { clock_hms(ms) } else { String::new() };
        let row = CsvRow {
            start: time(segment.start_ms),
            end: time(segment.end_ms),
            kind: segment.kind.as_text(),
            tier: segment.tier.as_text(),
            confidence: Some(segment.confidence),
            track: segment.track.as_ref(),
        };
        csv_row(&mut out, n + 1, &row);
    }
    out
}

fn json(detail: &SessionDetail) -> Result<String> {
    let mut out = serde_json::to_string_pretty(detail).map_err(|e| {
        tracing::error!(error = %e, "store: json export failed");
        MfError::internal("Could not export the session.")
    })?;
    out.push('\n');
    Ok(out)
}

/// CUE strings cannot escape quotes.
fn cue_quote(text: &str) -> String {
    format!("\"{}\"", one_line(text).replace('"', "'"))
}

fn cue_time(frames: u64) -> String {
    let (m, s, f) = (
        frames / (CUE_FPS * 60),
        frames / CUE_FPS % 60,
        frames % CUE_FPS,
    );
    format!("{m:02}:{s:02}:{f:02}")
}

/// File name and CUE file type: the local file's name for file sessions,
/// otherwise the source title (with `.mp3` unless it already has an extension).
fn cue_file(detail: &SessionDetail, title: &str) -> (String, &'static str) {
    let local = match detail.entry.kind {
        SessionKind::File => detail
            .entry
            .source_url
            .as_deref()
            .and_then(|p| Path::new(p).file_name()),
        _ => None,
    }
    .and_then(|name| name.to_str())
    .map(str::to_string);
    let name = local.unwrap_or_else(|| one_line(title).replace(['/', '\\'], "-"));
    let ext = Path::new(&name)
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    match ext.as_deref() {
        Some("mp3") => (name, "MP3"),
        Some("aif" | "aiff") => (name, "AIFF"),
        Some(
            "wav" | "flac" | "m4a" | "aac" | "alac" | "ogg" | "opus" | "webm" | "mp4" | "mkv"
            | "mov",
        ) => (name, "WAVE"),
        _ => (format!("{name}.mp3"), "MP3"),
    }
}

fn cue(detail: &SessionDetail) -> Result<String> {
    let music = music_segments(detail);
    if !has_timeline(detail, &music) {
        return Err(MfError::invalid(
            "Only a recording with a timeline can be exported as a cue sheet.",
        ));
    }
    let source = detail.source.as_ref();
    let title = source
        .and_then(|s| s.title.as_deref())
        .filter(|t| !t.trim().is_empty())
        .unwrap_or(&detail.entry.title);
    let mut out = String::new();
    if let Some(uploader) = source
        .and_then(|s| s.uploader.as_deref())
        .filter(|u| !u.trim().is_empty())
    {
        let _ = writeln!(out, "PERFORMER {}", cue_quote(uploader));
    }
    let _ = writeln!(out, "TITLE {}", cue_quote(title));
    let (file, file_type) = cue_file(detail, title);
    let _ = writeln!(out, "FILE {} {file_type}", cue_quote(&file));
    let mut previous: Option<u64> = None;
    for (n, segment) in music.iter().enumerate() {
        let (artist, title) = artist_title(segment_track(segment));
        // INDEX times must increase strictly.
        let mut frame = segment.start_ms.saturating_mul(CUE_FPS) / 1000;
        if let Some(prev) = previous
            && frame <= prev
        {
            frame = prev + 1;
        }
        previous = Some(frame);
        let _ = writeln!(out, "  TRACK {:02} AUDIO", n + 1);
        let _ = writeln!(out, "    TITLE {}", cue_quote(&title));
        let _ = writeln!(out, "    PERFORMER {}", cue_quote(&artist));
        let _ = writeln!(out, "    INDEX 01 {}", cue_time(frame));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Candidate, CandidateTier, LibraryEntry, Service, ServiceLink, SessionStatus, SourceInfo,
        Tier,
    };

    fn track(artist: &str, title: &str) -> Track {
        Track {
            title: title.into(),
            artist: artist.into(),
            ..Default::default()
        }
    }

    fn segment(
        id: &str,
        item: u32,
        start: u64,
        end: u64,
        kind: SegmentKind,
        track: Option<Track>,
    ) -> Segment {
        let confidence = if track.is_some() { 0.88 } else { 0.0 };
        Segment {
            id: id.into(),
            item_index: item,
            start_ms: start,
            end_ms: end,
            kind,
            track,
            confidence,
            tier: if confidence > 0.0 {
                Tier::Likely
            } else {
                Tier::Unknown
            },
            evidence: vec![],
            alternates: vec![],
            source_url: None,
        }
    }

    fn detail(
        kind: SessionKind,
        segments: Vec<Segment>,
        source: Option<SourceInfo>,
    ) -> SessionDetail {
        SessionDetail {
            entry: LibraryEntry {
                id: "s1".into(),
                kind,
                status: SessionStatus::Completed,
                created_at_ms: 0,
                title: "Session".into(),
                subtitle: None,
                artwork_url: None,
                track: None,
                track_count: 0,
                duration_ms: None,
                source_url: None,
                pinned: false,
            },
            source,
            segments,
            candidates: vec![],
            transcript: None,
        }
    }

    fn mix() -> SessionDetail {
        let source = SourceInfo {
            title: Some("Boiler Room: Berlin \"Live\"".into()),
            uploader: Some("Boiler Room".into()),
            item_count: 1,
            ..Default::default()
        };
        let mut first = track("Daft Punk", "One More Time");
        first.album = Some("Discovery".into());
        first.isrc = Some("GBDUW0000059".into());
        first.links = vec![
            ServiceLink {
                service: Service::Spotify,
                url: "https://open.spotify.com/track/abc".into(),
                is_search: false,
            },
            ServiceLink {
                service: Service::Deezer,
                url: "https://www.deezer.com/search/x".into(),
                is_search: true,
            },
            ServiceLink {
                service: Service::Deezer,
                url: "https://www.deezer.com/track/1".into(),
                is_search: false,
            },
        ];
        detail(
            SessionKind::Link,
            vec![
                // Out of order on purpose: exports sort by position.
                segment(
                    "c",
                    0,
                    3_725_400,
                    3_900_000,
                    SegmentKind::Track,
                    Some(track("Artist, Jr.", "=Title \"Q\"")),
                ),
                segment("a", 0, 0, 195_000, SegmentKind::Track, Some(first)),
                segment("b", 0, 195_000, 3_570_500, SegmentKind::Unidentified, None),
                segment("s", 0, 3_570_500, 3_725_400, SegmentKind::Speech, None),
            ],
            Some(source),
        )
    }

    #[test]
    fn text_mix_golden() {
        let expected = "\
[00:00] Daft Punk - One More Time
[03:15] ID - ID
[1:02:05] Artist, Jr. - =Title \"Q\"
";
        assert_eq!(render(&mix(), ExportFormat::Text).unwrap(), expected);
    }

    #[test]
    fn text_collection_is_numbered() {
        let playlist = detail(
            SessionKind::Link,
            vec![
                segment("1", 1, 0, 0, SegmentKind::Track, Some(track("B", "Second"))),
                segment("0", 0, 0, 0, SegmentKind::Track, Some(track("A", "First"))),
                segment(
                    "2",
                    2,
                    0,
                    0,
                    SegmentKind::Track,
                    Some(track("", "No Artist")),
                ),
            ],
            Some(SourceInfo {
                item_count: 3,
                ..Default::default()
            }),
        );
        assert_eq!(
            render(&playlist, ExportFormat::Text).unwrap(),
            "1. A - First\n2. B - Second\n3. ID - No Artist\n"
        );
    }

    #[test]
    fn text_multi_item_media_is_numbered() {
        let playlist = detail(
            SessionKind::Link,
            vec![
                segment(
                    "a",
                    0,
                    10_000,
                    50_000,
                    SegmentKind::Track,
                    Some(track("A", "One")),
                ),
                segment(
                    "b",
                    1,
                    5_000,
                    40_000,
                    SegmentKind::Track,
                    Some(track("B", "Two")),
                ),
            ],
            None,
        );
        assert_eq!(
            render(&playlist, ExportFormat::Text).unwrap(),
            "1. A - One\n2. B - Two\n"
        );
    }

    #[test]
    fn text_without_segments_lists_answer_or_candidates() {
        let mut listen = detail(SessionKind::Listen, vec![], None);
        let mut answer = track("", "Song");
        answer.artists = vec!["X".into(), "Y".into()];
        listen.entry.track = Some(answer);
        assert_eq!(
            render(&listen, ExportFormat::Text).unwrap(),
            "X, Y - Song\n"
        );

        let mut hum = detail(SessionKind::Hum, vec![], None);
        hum.candidates = vec![
            Candidate {
                track: track("A", "One"),
                score: 0.8,
                tier: CandidateTier::Strong,
                sources: vec![],
                alignment: None,
            },
            Candidate {
                track: track("B", "Two"),
                score: 0.3,
                tier: CandidateTier::LongShot,
                sources: vec![],
                alignment: None,
            },
        ];
        assert_eq!(
            render(&hum, ExportFormat::Text).unwrap(),
            "1. A - One\n2. B - Two\n"
        );
        assert_eq!(
            render(&detail(SessionKind::Hum, vec![], None), ExportFormat::Text).unwrap(),
            ""
        );
    }

    #[test]
    fn text_strips_line_breaks_from_metadata() {
        let d = detail(
            SessionKind::Link,
            vec![segment(
                "a",
                0,
                0,
                1000,
                SegmentKind::Track,
                Some(track("A\nB", "T\r\n2")),
            )],
            None,
        );
        assert_eq!(
            render(&d, ExportFormat::Text).unwrap(),
            "[00:00] A B - T 2\n"
        );
    }

    #[test]
    fn csv_mix_golden() {
        let expected = "index,start,end,kind,tier,confidence,title,artist,album,isrc,links\r\n\
1,00:00:00,00:03:15,track,likely,0.88,One More Time,Daft Punk,Discovery,GBDUW0000059,https://open.spotify.com/track/abc | https://www.deezer.com/track/1\r\n\
2,00:03:15,00:59:30,unidentified,unknown,0.00,,,,,\r\n\
3,00:59:30,01:02:05,speech,unknown,0.00,,,,,\r\n\
4,01:02:05,01:05:00,track,likely,0.88,\"'=Title \"\"Q\"\"\",\"Artist, Jr.\",,,\r\n";
        assert_eq!(render(&mix(), ExportFormat::Csv).unwrap(), expected);
    }

    #[test]
    fn csv_collection_has_no_times() {
        let playlist = detail(
            SessionKind::Link,
            vec![segment(
                "0",
                0,
                0,
                0,
                SegmentKind::Track,
                Some(track("A", "Line\nBreak")),
            )],
            None,
        );
        let expected = "index,start,end,kind,tier,confidence,title,artist,album,isrc,links\r\n\
1,,,track,likely,0.88,\"Line\nBreak\",A,,,\r\n";
        assert_eq!(render(&playlist, ExportFormat::Csv).unwrap(), expected);
    }

    #[test]
    fn csv_candidates_and_answer() {
        let mut hum = detail(SessionKind::Hum, vec![], None);
        hum.candidates = vec![Candidate {
            track: track("A", "One"),
            score: 0.456,
            tier: CandidateTier::LongShot,
            sources: vec![],
            alignment: None,
        }];
        assert_eq!(
            render(&hum, ExportFormat::Csv).unwrap(),
            "index,start,end,kind,tier,confidence,title,artist,album,isrc,links\r\n1,,,candidate,long_shot,0.46,One,A,,,\r\n"
        );
        let mut listen = detail(SessionKind::Listen, vec![], None);
        listen.entry.track = Some(track("-A", "@B"));
        assert_eq!(
            render(&listen, ExportFormat::Csv).unwrap(),
            "index,start,end,kind,tier,confidence,title,artist,album,isrc,links\r\n1,,,track,,,'@B,'-A,,,\r\n"
        );
    }

    #[test]
    fn json_round_trips() {
        let d = mix();
        let out = render(&d, ExportFormat::Json).unwrap();
        assert!(out.starts_with("{\n  \"entry\": {"));
        assert!(out.ends_with("}\n"));
        let back: SessionDetail = serde_json::from_str(&out).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn cue_mix_golden() {
        let expected = "\
PERFORMER \"Boiler Room\"
TITLE \"Boiler Room: Berlin 'Live'\"
FILE \"Boiler Room: Berlin 'Live'.mp3\" MP3
  TRACK 01 AUDIO
    TITLE \"One More Time\"
    PERFORMER \"Daft Punk\"
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    TITLE \"ID\"
    PERFORMER \"ID\"
    INDEX 01 03:15:00
  TRACK 03 AUDIO
    TITLE \"=Title 'Q'\"
    PERFORMER \"Artist, Jr.\"
    INDEX 01 62:05:30
";
        assert_eq!(render(&mix(), ExportFormat::Cue).unwrap(), expected);
    }

    #[test]
    fn cue_frames_and_strictly_increasing_indexes() {
        assert_eq!(cue_time(0), "00:00:00");
        assert_eq!(cue_time(75 * 61 + 74), "01:01:74");
        let d = detail(
            SessionKind::File,
            vec![
                segment(
                    "a",
                    0,
                    1_013,
                    5_000,
                    SegmentKind::Track,
                    Some(track("A", "One")),
                ),
                segment(
                    "b",
                    0,
                    1_013,
                    5_000,
                    SegmentKind::Track,
                    Some(track("B", "Two")),
                ),
            ],
            Some(SourceInfo {
                title: Some("ignored".into()),
                ..Default::default()
            }),
        );
        let mut d = d;
        d.entry.source_url = Some("/Users/me/Music/Set 01.flac".into());
        let out = render(&d, ExportFormat::Cue).unwrap();
        assert!(out.contains("FILE \"Set 01.flac\" WAVE\n"), "{out}");
        assert!(out.contains("INDEX 01 00:01:00\n"), "{out}");
        assert!(out.contains("INDEX 01 00:01:01\n"), "{out}");
    }

    #[test]
    fn cue_requires_a_timeline() {
        let playlist = detail(
            SessionKind::Link,
            vec![segment(
                "0",
                0,
                0,
                0,
                SegmentKind::Track,
                Some(track("A", "One")),
            )],
            None,
        );
        assert!(matches!(
            render(&playlist, ExportFormat::Cue),
            Err(MfError::InvalidInput { .. })
        ));
        assert!(
            render(
                &detail(SessionKind::Listen, vec![], None),
                ExportFormat::Cue
            )
            .is_err()
        );
    }

    #[test]
    fn clock_formats() {
        assert_eq!(clock(0), "00:00");
        assert_eq!(clock(59_999), "00:59");
        assert_eq!(clock(3_599_999), "59:59");
        assert_eq!(clock(3_600_000), "1:00:00");
        assert_eq!(clock(36_000_000), "10:00:00");
        assert_eq!(clock_hms(3_725_400), "01:02:05");
    }
}
