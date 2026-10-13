//! Playlists. See docs/PIPELINES.md ("Playlists").
//!
//! Entries are handled in order. Metadata comes first: an entry whose title
//! reads "Artist - Title" (or whose channel names the artist) without a
//! qualifier that marks another version, and that the catalog knows with a
//! matching duration, needs no audio at all. Everything else is downloaded and
//! surveyed like a single link (an entry of 15 minutes or more becomes a nested
//! mix). Up to two entries download at a time; recognizer calls go through the
//! shared budget.

use futures_util::StreamExt;

use crate::error::{MfError, Result};
use crate::media::{MediaEntry, MediaInfo};
use crate::model::{AnalyzeMode, Evidence, EvidenceSource, Outcome, Segment, SegmentKind, SourceInfo, Tier, Track};
use crate::pipeline::analyze::{self, AnalyzeOptions, Report, Services, Strategy};
use crate::pipeline::ctx::SessionCtx;
use crate::pipeline::hints;
use crate::store::SessionUpdate;

/// Most entries analyzed.
pub const MAX_ENTRIES: usize = 300;
/// Duration difference accepted for a metadata match.
const DURATION_TOLERANCE_MS: u64 = 8_000;
/// Music videos carry intros and outros.
const VIDEO_TOLERANCE_MS: u64 = 25_000;
/// Confidence of a metadata-only entry.
const METADATA_SCORE: f32 = 0.9;
/// Entries downloaded at the same time.
const PARALLEL: usize = 2;

/// Words that make the title name a different version than the catalog's.
const RISKY: &[&str] = &[
    "live",
    "cover",
    "remix",
    "rmx",
    "sped up",
    "speed up",
    "slowed",
    "nightcore",
    "karaoke",
    "instrumental",
    "8d",
    "reverb",
    "mashup",
    "bootleg",
    "acoustic",
    "unplugged",
    "edit",
    "vip",
    "flip",
    "rework",
    "tribute",
    "parody",
];

/// One entry's result.
struct EntryResult {
    segments: Vec<Segment>,
    probes: u32,
    paid: u32,
    log: Vec<String>,
}

/// Analyze a playlist's entries.
pub async fn run<S: Services>(
    ctx: &SessionCtx,
    services: &S,
    source: &SourceInfo,
    info: &MediaInfo,
    options: &AnalyzeOptions,
) -> Result<Report> {
    let total_entries = info.entries.len();
    let entries: Vec<&MediaEntry> = info.entries.iter().take(MAX_ENTRIES).collect();
    let count = entries.len() as u32;
    let mut log = Vec::new();
    if total_entries > MAX_ENTRIES {
        log.push(format!("playlist truncated to {MAX_ENTRIES} of {total_entries} entries"));
    }
    let results: Vec<Result<EntryResult>> = futures_util::stream::iter(entries.iter().copied())
        .map(|entry| entry_result(ctx, services, entry, count, options))
        .buffered(PARALLEL)
        .collect()
        .await;
    let mut segments = Vec::new();
    let mut probes = 0;
    let mut paid = 0;
    for result in results {
        match result {
            Ok(r) => {
                segments.extend(r.segments);
                probes += r.probes;
                paid += r.paid;
                log.extend(r.log);
            }
            Err(MfError::Canceled) => return Err(MfError::Canceled),
            Err(e) => log.push(format!("entry failed: {e}")),
        }
    }
    let identified = segments.iter().filter(|s| s.kind == SegmentKind::Track).count() as u32;
    Ok(Report {
        outcome: Outcome::Completed { identified, total: count.max(identified) },
        strategy: Strategy::Playlist,
        segments,
        duration_ms: source.duration_ms,
        probes,
        paid,
        log,
        update: SessionUpdate { title: source.title.clone(), ..SessionUpdate::default() },
    })
}

async fn entry_result<S: Services>(
    ctx: &SessionCtx,
    services: &S,
    entry: &MediaEntry,
    count: u32,
    options: &AnalyzeOptions,
) -> Result<EntryResult> {
    ctx.cancel.check()?;
    ctx.emitter.item(entry.index, count, entry.title.clone());
    if options.use_hints
        && let Some(track) = metadata_match(services, entry).await
    {
        let track = services.enrich(track).await;
        let segment = Segment {
            id: format!("e{}-meta", entry.index),
            item_index: entry.index,
            start_ms: 0,
            end_ms: entry.duration_ms.unwrap_or(0),
            kind: SegmentKind::Track,
            track: Some(track),
            confidence: METADATA_SCORE,
            tier: Tier::Likely,
            evidence: vec![Evidence {
                source: EvidenceSource::Metadata,
                at_ms: 0,
                score: METADATA_SCORE,
                detail: Some("entry title and duration match the catalog".into()),
            }],
            alternates: Vec::new(),
            source_url: crate::links::deep_link(&entry.url, 0).or_else(|| Some(entry.url.clone())),
        };
        ctx.emitter.segment(segment.clone());
        return Ok(EntryResult {
            segments: vec![segment],
            probes: 0,
            paid: 0,
            log: vec![format!("entry {}: metadata", entry.index)],
        });
    }
    match audio_result(ctx, services, entry, options).await {
        Ok(r) => Ok(r),
        Err(MfError::Canceled) => Err(MfError::Canceled),
        Err(e) => {
            // One broken entry must not sink the playlist.
            let segment = crate::pipeline::stitch::Piece::gap(
                format!("e{}-failed", entry.index),
                0,
                entry.duration_ms.unwrap_or(0),
                SegmentKind::Unidentified,
            );
            let mut segment = segment.to_segment(None);
            segment.item_index = entry.index;
            ctx.emitter.segment(segment.clone());
            Ok(EntryResult {
                segments: vec![segment],
                probes: 0,
                paid: 0,
                log: vec![format!("entry {}: {e}", entry.index)],
            })
        }
    }
}

async fn audio_result<S: Services>(
    ctx: &SessionCtx,
    services: &S,
    entry: &MediaEntry,
    options: &AnalyzeOptions,
) -> Result<EntryResult> {
    let dir = ctx.ensure_work_dir()?.join(format!("entry-{}", entry.index));
    let info = analyze::probe_media(ctx, options, &entry.url, false).await.unwrap_or_default();
    let pcm = analyze::fetch_audio(ctx, options, &entry.url, false, &dir).await?;
    let hints = if options.use_hints {
        let mut info = info;
        if info.title.is_none() {
            info.title.clone_from(&entry.title);
        }
        if info.uploader.is_none() {
            info.uploader.clone_from(&entry.uploader);
        }
        hints::collect(&info, &[], Some(pcm.duration_ms()))
    } else {
        hints::Hints::default()
    };
    let source = SourceInfo { title: entry.title.clone(), url: Some(entry.url.clone()), ..SourceInfo::default() };
    let report = analyze::analyze_pcm(
        ctx,
        services,
        &pcm,
        &hints,
        &source,
        Some(entry.url.clone()),
        AnalyzeMode::Auto,
        entry.index,
        &format!("e{}-", entry.index),
    )
    .await;
    if options.media_cache.is_none() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    let report = report?;
    let mut log = vec![format!("entry {}: {:?}, {} probes", entry.index, report.strategy, report.probes)];
    log.extend(report.log);
    Ok(EntryResult { segments: report.segments, probes: report.probes, paid: report.paid, log })
}

/// The catalog track an entry's title names, when nothing suggests another
/// version and the durations agree.
async fn metadata_match<S: Services>(services: &S, entry: &MediaEntry) -> Option<Track> {
    let title = entry.title.as_deref()?;
    if has_risky_words(title) {
        return None;
    }
    let (artist, song) = hints::parse_media_title(title, entry.uploader.as_deref())?;
    let found = services.find(&song, &artist, None).await?;
    // Official uploads (videos, audio, lyric videos) often add a few seconds of
    // picture before or after the song.
    let official = hints::is_music_video_title(title) || is_official_upload(title);
    let tolerance = if official { VIDEO_TOLERANCE_MS } else { DURATION_TOLERANCE_MS };
    match (entry.duration_ms, found.duration_ms) {
        (Some(a), Some(b)) if a.abs_diff(b) <= tolerance => Some(found),
        (None, _) => Some(found),
        _ => None,
    }
}

/// "(Official Audio)", "(Official Lyric Video)", "[Visualizer]": the artist's own upload.
fn is_official_upload(title: &str) -> bool {
    let lower = title.to_lowercase();
    ["official audio", "official lyric", "lyric video", "visualizer", "visualiser", "official version"]
        .iter()
        .any(|w| lower.contains(w))
}

/// The title names a version other than the original.
pub fn has_risky_words(title: &str) -> bool {
    let lower = title.to_lowercase();
    let words: String = lower.chars().map(|c| if c.is_alphanumeric() { c } else { ' ' }).collect();
    let padded = format!(" {} ", words.split_whitespace().collect::<Vec<_>>().join(" "));
    if RISKY.iter().filter(|w| **w != "live").any(|w| padded.contains(&format!(" {w} "))) {
        return true;
    }
    // "Live" only as a qualifier, not inside a song title ("Live Forever").
    [
        "(live",
        "[live",
        "live at ",
        "live from ",
        "live in ",
        "live version",
        "live session",
        "live performance",
        "live @",
    ]
    .iter()
    .any(|p| lower.contains(p))
        || lower.trim_end().ends_with(" - live")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::testutil::{FakeServices, track};

    fn entry(title: &str, uploader: &str, duration_s: u64) -> MediaEntry {
        MediaEntry {
            index: 0,
            url: "https://www.youtube.com/watch?v=x".into(),
            id: None,
            title: Some(title.into()),
            uploader: Some(uploader.into()),
            duration_ms: Some(duration_s * 1000),
        }
    }

    #[tokio::test]
    async fn metadata_first_for_plain_entries() {
        let mut services = FakeServices::new(Vec::new(), Vec::new());
        services.catalog = vec![
            track("Daft Punk", "Get Lucky (feat. Pharrell Williams & Nile Rodgers)", 248),
            track("Coldplay", "Yellow", 266),
            track("Avicii", "Levels", 200),
        ];
        let found = metadata_match(
            &services,
            &entry("Daft Punk - Get Lucky (Official Audio) ft. Pharrell Williams", "Daft Punk", 249),
        )
        .await;
        assert_eq!(found.map(|t| t.artist), Some("Daft Punk".to_string()));
        // A Topic channel names the artist.
        assert!(metadata_match(&services, &entry("Yellow", "Coldplay - Topic", 268)).await.is_some());
        // Durations must agree (a music video may run 25 s longer).
        assert!(metadata_match(&services, &entry("Coldplay - Yellow", "Coldplay", 290)).await.is_none());
        assert!(
            metadata_match(&services, &entry("Coldplay - Yellow (Official Video)", "Coldplay", 290)).await.is_some()
        );
        // A version the catalog track is not: listen instead.
        assert!(
            metadata_match(&services, &entry("Coldplay - Yellow (Live in Madrid 2011)", "Coldplay", 266))
                .await
                .is_none()
        );
        assert!(metadata_match(&services, &entry("Avicii - Levels (Skrillex Remix)", "Skrillex", 200)).await.is_none());
        assert!(metadata_match(&services, &entry("Some upload", "Random Channel", 200)).await.is_none());
    }

    #[test]
    fn risky_titles() {
        assert!(has_risky_words("Avicii - Levels (Skrillex Remix)"));
        assert!(has_risky_words("Song (Sped Up)"));
        assert!(has_risky_words("Adele - Someone Like You (Live at the BRIT Awards 2011)"));
        assert!(has_risky_words("Artist - Song - Live From Wembley"));
        assert!(has_risky_words("Song [Acoustic]"));
        assert!(!has_risky_words("Oasis - Live Forever"));
        assert!(!has_risky_words("Daft Punk - Get Lucky (Official Video) ft. Pharrell Williams"));
        assert!(!has_risky_words("Rick Astley - Never Gonna Give You Up (Official Music Video)"));
    }
}
