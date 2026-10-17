use super::*;

#[test]
fn validates_links_and_files() {
    assert_eq!(
        validate("  check this https://youtu.be/dQw4w9WgXcQ?si=x out "),
        Ok(AnalyzeInput::Url("https://youtu.be/dQw4w9WgXcQ?si=x".into()))
    );
    assert_eq!(validate("youtu.be/dQw4w9WgXcQ"), Ok(AnalyzeInput::Url("https://youtu.be/dQw4w9WgXcQ".into())));
    assert!(matches!(validate(""), Err(MfError::InvalidInput { .. })));
    assert!(matches!(validate("   "), Err(MfError::InvalidInput { .. })));
    assert!(matches!(validate("just some words"), Err(MfError::InvalidInput { .. })));

    let dir = std::env::temp_dir().join(format!("mf-validate-{}", crate::util::new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("My Song.mp3");
    std::fs::write(&file, b"x").unwrap();
    assert_eq!(validate(file.to_str().unwrap()), Ok(AnalyzeInput::File(file.clone())));
    let url = format!("file://{}", file.to_str().unwrap().replace(' ', "%20"));
    assert_eq!(validate(&url), Ok(AnalyzeInput::File(file.clone())));
    assert!(matches!(validate(dir.to_str().unwrap()), Err(MfError::InvalidInput { .. })));
    assert!(matches!(validate(dir.join("missing.mp3").to_str().unwrap()), Err(MfError::InvalidInput { .. })));
    let _ = std::fs::remove_dir_all(dir);
}

fn temp_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mf-analyze-{}", crate::util::new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread")]
async fn a_song_file_is_one_segment() {
    use crate::pipeline::testutil::{FakeServices, noise, play, session, track};
    let dir = temp_dir();
    let audio = noise(200, 21);
    let path = dir.join("Artist - Song.wav");
    crate::audio::wav::write_wav(&path, &audio, crate::audio::SAMPLE_RATE).unwrap();
    let song = track("Artist", "Song", 200);
    let services = FakeServices::new(audio, vec![play(0, 200, &song, 0)]);
    let (ctx, events) = session(&dir, SessionKind::File);
    let input = validate(path.to_str().unwrap()).unwrap();
    let report = run_with(&ctx, &services, input, AnalyzeMode::Auto, &AnalyzeOptions::default()).await.unwrap();
    assert_eq!(report.strategy, Strategy::Single, "{:?}", report.log);
    assert_eq!(report.segments.len(), 1);
    assert_eq!((report.segments[0].start_ms, report.segments[0].end_ms), (0, 200_000));
    assert_eq!(report.outcome, Outcome::Completed { identified: 1, total: 1 });
    // The file name named the song and the first probe confirmed it.
    assert_eq!(services.calls(), 1);
    assert_eq!(report.segments[0].tier, crate::model::Tier::Confirmed);
    let events = events.0.lock();
    assert!(events.iter().any(|e| matches!(e, crate::model::SessionEvent::Source { .. })));
    assert!(events.iter().any(|e| matches!(e, crate::model::SessionEvent::Segment { .. })));
    ctx.cleanup();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_two_song_file_is_a_mix() {
    use crate::pipeline::testutil::{FakeServices, noise, play, session, track};
    let dir = temp_dir();
    let audio = noise(320, 22);
    let path = dir.join("recording.wav");
    crate::audio::wav::write_wav(&path, &audio, crate::audio::SAMPLE_RATE).unwrap();
    let a = track("A", "First", 160);
    let b = track("B", "Second", 160);
    let services = FakeServices::new(audio, vec![play(0, 160, &a, 0), play(160, 320, &b, 0)]);
    let (ctx, _) = session(&dir, SessionKind::File);
    let input = validate(path.to_str().unwrap()).unwrap();
    let report = run_with(&ctx, &services, input, AnalyzeMode::Auto, &AnalyzeOptions::default()).await.unwrap();
    assert_eq!(report.strategy, Strategy::Mix, "{:?}", report.log);
    let tracks: Vec<_> = report.segments.iter().filter(|s| s.kind == SegmentKind::Track).collect();
    assert_eq!(tracks.len(), 2, "{:#?}", report.segments);
    assert!(tracks[1].start_ms.abs_diff(160_000) <= 3_000);
    ctx.cleanup();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn collections_list_every_track_before_enriching() {
    use crate::model::SessionEvent;
    use crate::pipeline::testutil::{FakeServices, session, track};
    let dir = temp_dir();
    let (ctx, events) = session(&dir, SessionKind::Link);
    let mut services = FakeServices::new(Vec::new(), Vec::new());
    services.catalog = vec![track("A", "One", 200), track("B", "Two", 180), track("C", "Three", 210)];
    let page = |artist: &str, title: &str| Track { title: title.into(), artist: artist.into(), ..Track::default() };
    let tracks = vec![page("A", "One"), page("B", "Two"), page("C", "Three")];
    let options = AnalyzeOptions::default();
    let mut run = Run { ctx: &ctx, services: &services, options: &options, log: Vec::new(), probes: 0, paid: 0 };
    let report = run.tracks(SourceInfo::default(), tracks).await.unwrap();
    assert_eq!(report.outcome, Outcome::Completed { identified: 3, total: 3 });
    assert!(report.segments.iter().all(|s| s.track.as_ref().is_some_and(|t| t.duration_ms.is_some())));
    let segments: Vec<Segment> = events
        .0
        .lock()
        .iter()
        .filter_map(|e| match e {
            SessionEvent::Segment { segment } => Some(segment.clone()),
            _ => None,
        })
        .collect();
    // Every track at once from the page, with search links, before any enrichment...
    let first: Vec<&str> = segments[..3].iter().map(|s| s.id.as_str()).collect();
    assert_eq!(first, ["track-0", "track-1", "track-2"]);
    assert!(
        segments[..3].iter().all(|s| s.track.as_ref().is_some_and(|t| t.duration_ms.is_none() && !t.links.is_empty()))
    );
    // ...then each again under the same id once enriched.
    assert_eq!(segments.len(), 6);
    assert!(segments[3..].iter().all(|s| s.track.as_ref().is_some_and(|t| t.duration_ms.is_some())));
    ctx.cleanup();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn durations_within_tolerance() {
    assert!(within(212_000, 213_000, 0.12));
    assert!(within(240_000, 213_000, 0.13));
    assert!(!within(357_000, 822_000, 0.12));
}

#[test]
fn source_refresh_from_probe() {
    let mut source = SourceInfo { url: Some("https://example.com/v".into()), ..SourceInfo::default() };
    let info = MediaInfo {
        title: Some("T".into()),
        uploader: Some("U".into()),
        duration_ms: Some(1000),
        is_playlist: true,
        entries: vec![crate::media::MediaEntry::default(); 3],
        ..MediaInfo::default()
    };
    refresh_source(&mut source, &info);
    assert_eq!(source.title.as_deref(), Some("T"));
    assert_eq!(source.item_count, 3);
    assert_eq!(source.url.as_deref(), Some("https://example.com/v"));
}
