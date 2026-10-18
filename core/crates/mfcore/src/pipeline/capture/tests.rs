use super::*;
use crate::audio::SAMPLE_RATE;
use crate::model::{Phase, SessionEvent, SessionKind};
use crate::pipeline::testutil::{FakeServices, noise, play, session, track};

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("mf-capture-{}", crate::util::new_id()))
}

fn f32s(samples: &[i16]) -> Vec<f32> {
    samples.iter().map(|&s| f32::from(s) / 32768.0).collect()
}

#[test]
fn page_media_becomes_probe_data() {
    let page = PageMedia {
        url: "https://www.youtube.com/watch?v=abcdefghijk&t=5".into(),
        title: Some("Set".into()),
        duration_ms: Some(600_000),
        chapters: vec![
            PageChapter { start_ms: 300_000, title: "B - Two".into() },
            PageChapter { start_ms: 0, title: "A - One".into() },
            PageChapter { start_ms: 10, title: " ".into() },
        ],
        ..PageMedia::default()
    };
    let info = page.info();
    assert_eq!(info.chapters.len(), 2);
    assert_eq!((info.chapters[0].start_ms, info.chapters[0].end_ms), (0, Some(300_000)));
    assert_eq!(info.chapters[1].end_ms, Some(600_000));
    let source = page.source();
    assert_eq!(source.service, Some(crate::model::Service::YouTube));
    assert_eq!(source.duration_ms, Some(600_000));
    assert_eq!(source.title.as_deref(), Some("Set"));
}

/// A 1 kHz tone played at 4× (pitch not preserved) sounds at 4 kHz in a 48 kHz
/// capture; written at 48 / 4 = 12 kHz it is 1 kHz again, at the media's length.
#[tokio::test]
async fn a_four_times_capture_keeps_the_media_time_base() {
    let dir = temp_dir();
    let (ctx, _) = session(&dir, SessionKind::Link);
    let ctx = Arc::new(ctx);
    let page = PageMedia { url: "https://example.com/v".into(), ..PageMedia::default() };
    let sink = sink(ctx.clone(), &page).expect("sink");
    let media_s = 8.0;
    let captured: Vec<f32> = (0..(48_000.0 * media_s / 4.0) as usize)
        .map(|i| (2.0 * std::f64::consts::PI * 4_000.0 * i as f64 / 48_000.0).sin() as f32 * 0.5)
        .collect();
    for (k, piece) in captured.chunks(4_800).enumerate() {
        sink.push(k as f64 * 400.0, piece, 48_000, 4.0, true);
    }
    sink.finish();
    let pcm = sink.captured().await.expect("pcm");
    assert!(pcm.duration_ms().abs_diff(8_000) <= 50, "{} ms", pcm.duration_ms());
    let samples = pcm.read_ms(1_000, 4_000).unwrap();
    let crossings = samples.windows(2).filter(|w| (w[0] < 0) != (w[1] < 0)).count();
    let hz = crossings as f64 / 2.0 / 4.0;
    assert!((hz - 1_000.0).abs() < 20.0, "{hz} Hz");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_listen_through_is_analyzed_as_a_mix() {
    let dir = temp_dir();
    let (ctx, events) = session(&dir, SessionKind::Link);
    let ctx = Arc::new(ctx);
    let (a, b) = (track("Artist A", "One", 300), track("Artist B", "Two", 300));
    let audio = noise(1_200, 7);
    let services = FakeServices::new(audio.clone(), vec![play(0, 600, &a, 0), play(600, 1_200, &b, 0)]);
    let page =
        PageMedia { url: "https://example.com/set".into(), duration_ms: Some(1_200_000), ..PageMedia::default() };
    let sink = sink(ctx.clone(), &page).expect("sink");
    // 100 ms blocks of media; an ad break starts a new run at 300 s. (At 16 kHz
    // the resampler passes samples through, so the fake recognizer can find them.)
    let block = SAMPLE_RATE as usize / 10;
    for (k, piece) in audio.chunks(block).enumerate() {
        let media_ms = (k * 100) as f64;
        sink.push(media_ms, &f32s(piece), SAMPLE_RATE, 1.0, k != 3_000);
    }
    sink.finish();
    run_with(&ctx, &services, &sink, &page, AnalyzeMode::Auto).await;
    let events = events.0.lock();
    let segments: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            SessionEvent::Segment { segment } => segment.track.as_ref().map(|t| (t.title.clone(), segment.start_ms)),
            _ => None,
        })
        .collect();
    assert!(segments.iter().any(|(t, _)| t == "One"), "{segments:?}");
    // The mix pipeline places the boundary (its own accuracy is tested elsewhere).
    assert!(segments.iter().any(|(t, s)| t == "Two" && s.abs_diff(600_000) <= 30_000), "{segments:?}");
    assert!(events.iter().any(|e| matches!(e, SessionEvent::Phase { phase: Phase::Listening, fraction: Some(_) })));
    assert!(matches!(events.last(), Some(SessionEvent::Finished { .. })), "{:?}", events.last());
    drop(events);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn short_windows_span_the_whole_media() {
    let dir = temp_dir();
    let (ctx, events) = session(&dir, SessionKind::Link);
    let ctx = Arc::new(ctx);
    let a = track("Artist A", "One", 240);
    let audio = noise(240, 3);
    let services = FakeServices::new(audio.clone(), vec![play(0, 240, &a, 0)]);
    let page = PageMedia { url: "https://example.com/song".into(), ..PageMedia::default() };
    let sink = sink(ctx.clone(), &page).expect("sink");
    sink.set_duration(240_000);
    // The survey's first three windows (50 %, 20 %, 80 %), 14 s each.
    for fraction in [0.5, 0.2, 0.8] {
        let start = (240_000.0 * fraction) as usize - 7_000;
        let piece = &audio[start * 16..(start + 14_000) * 16];
        sink.push(start as f64, &f32s(piece), SAMPLE_RATE, 1.0, false);
    }
    sink.finish();
    run_with(&ctx, &services, &sink, &page, AnalyzeMode::Auto).await;
    let events = events.0.lock();
    let tracks: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            SessionEvent::Segment { segment } => segment.track.as_ref().map(|t| t.title.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(tracks, vec!["One".to_string()], "{events:?}");
    drop(events);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_failed_capture_fails_the_session() {
    let dir = temp_dir();
    let (ctx, events) = session(&dir, SessionKind::Link);
    let ctx = Arc::new(ctx);
    let services = FakeServices::new(Vec::new(), Vec::new());
    let page = PageMedia { url: "https://example.com/v".into(), ..PageMedia::default() };
    let sink = sink(ctx.clone(), &page).expect("sink");
    sink.push(0.0, &[0.1; 4_800], 48_000, 1.0, false);
    sink.fail(MfError::MediaUnavailable { message: "The tab was closed.".into() });
    run_with(&ctx, &services, &sink, &page, AnalyzeMode::Auto).await;
    let last = events.0.lock().last().cloned();
    assert!(matches!(last, Some(SessionEvent::Failed { .. })), "{last:?}");
    let _ = std::fs::remove_dir_all(dir);
}
