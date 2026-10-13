use super::*;
use crate::pipeline::hints::{HintSource, TrackHint};
use crate::pipeline::testutil::{FakeServices, TempPcm, emitter, noise, play, track};

fn songs(pieces: &[Piece]) -> Vec<(u64, u64, String)> {
    pieces
        .iter()
        .filter(|p| p.kind == SegmentKind::Track)
        .map(|p| (p.start_ms / 1000, p.end_ms / 1000, p.track.as_ref().map(|t| t.title.clone()).unwrap_or_default()))
        .collect()
}

fn close(a: u64, b: u64, tolerance: u64) -> bool {
    a.abs_diff(b) <= tolerance
}

async fn analyze(services: &FakeServices, pcm: &PcmFile, hints: MixHints) -> MixReport {
    let (emitter, _) = emitter();
    run(services, &emitter, pcm, &hints, Vec::new(), &MixOptions::default()).await.expect("mix")
}

#[tokio::test(flavor = "multi_thread")]
async fn blind_compilation_with_hard_cuts() {
    // Eight full-length tracks of 80–100 s back to back.
    let mut plays = Vec::new();
    let mut at = 0;
    for k in 0..8u64 {
        let len = 80 + (k * 7) % 21;
        plays.push(play(at, at + len, &track(&format!("Artist {k}"), &format!("Song {k}"), len), 0));
        at += len;
    }
    let audio = noise(at, 1);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, plays.clone());
    let report = analyze(&services, &media.pcm, MixHints::default()).await;
    let found = songs(&report.pieces);
    assert_eq!(found.len(), 8, "{found:?}\n{:#?}", report.log);
    for (p, f) in plays.iter().zip(&found) {
        assert_eq!(f.2, p.track.title);
        assert!(close(f.0, p.start_ms / 1000, 3), "{f:?} vs {p:?}");
    }
    // About one probe per track.
    assert!(services.calls() <= 12, "{} probes: {:#?}", services.calls(), report.log);
}

#[tokio::test(flavor = "multi_thread")]
async fn blind_dj_mix_with_partial_plays_and_a_short_track() {
    // Tracks of 6–7 min played for ~3 min each, from their start; one 60 s track in between.
    let a = track("A", "Long One", 400);
    let b = track("B", "Long Two", 380);
    let c = track("C", "Short", 200);
    let d = track("D", "Long Three", 420);
    let plays = vec![play(0, 190, &a, 0), play(190, 380, &b, 0), play(380, 440, &c, 0), play(440, 660, &d, 0)];
    let audio = noise(660, 2);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, plays);
    let report = analyze(&services, &media.pcm, MixHints::default()).await;
    let found = songs(&report.pieces);
    let titles: Vec<&str> = found.iter().map(|f| f.2.as_str()).collect();
    assert_eq!(titles, vec!["Long One", "Long Two", "Short", "Long Three"], "{:#?}", report.log);
    assert!(close(found[1].0, 190, 12) && close(found[2].0, 380, 12) && close(found[3].0, 440, 12), "{found:?}");
    // Never more than the budget: 660 s / 45 s + 10.
    assert!(services.calls() <= 24);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_is_a_separate_segment() {
    let a = track("A", "Again", 120);
    let b = track("B", "Between", 100);
    let plays = vec![play(0, 120, &a, 0), play(120, 220, &b, 0), play(220, 340, &a, 0)];
    let audio = noise(340, 3);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, plays);
    let report = analyze(&services, &media.pcm, MixHints::default()).await;
    let titles: Vec<String> = songs(&report.pieces).into_iter().map(|f| f.2).collect();
    assert_eq!(titles, vec!["Again", "Between", "Again"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn one_long_track_is_one_segment() {
    // A 20-minute track (with a DJ loop: its position jumps back once).
    let t = track("A", "Endless", 1300);
    let mut late = play(600, 1200, &t, 560);
    late.offset_ms = 560_000;
    let plays = vec![play(0, 600, &t, 0), late];
    let audio = noise(1200, 4);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, plays);
    let report = analyze(&services, &media.pcm, MixHints::default()).await;
    let found = songs(&report.pieces);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!((found[0].0, found[0].1), (0, 1200));
}

fn hint(start_s: u64, t: &Track) -> TrackHint {
    TrackHint {
        start_ms: start_s * 1000,
        artist: t.artist.clone(),
        title: t.title.clone(),
        is_id: false,
        raw: String::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hinted_entries_verified_locally_cost_no_probe() {
    let a = track("A", "First", 130);
    let b = track("B", "Second (C Remix)", 120);
    let c = track("D", "Third", 110);
    let plays = vec![play(0, 130, &a, 0), play(130, 250, &b, 0), play(250, 360, &c, 0)];
    let audio = noise(360, 5);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, plays)
        .with_preview_of(&a, 40, 30)
        .with_preview_of(&b, 30, 30)
        .with_preview_of(&c, 50, 30);
    let list = Tracklist { source: HintSource::Description, entries: vec![hint(0, &a), hint(130, &b), hint(250, &c)] };
    let report = analyze(&services, &media.pcm, MixHints { tracklist: Some(list), untimed: Vec::new() }).await;
    assert_eq!(services.calls(), 0, "{:#?}", report.log);
    assert_eq!(report.verified_locally, 3);
    let found = songs(&report.pieces);
    assert_eq!(
        found,
        vec![
            (0, 130, "First".to_string()),
            (130, 250, "Second (C Remix)".to_string()),
            (250, 360, "Third".to_string())
        ]
    );
    let tiers: Vec<_> = report.pieces.iter().map(|p| p.to_segment(None).tier).collect();
    assert!(tiers.iter().all(|t| *t == crate::model::Tier::Confirmed), "{tiers:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_hint_is_overruled_by_the_audio() {
    let a = track("A", "First", 130);
    let actual = track("X", "Actually Played", 120);
    let claimed = track("Y", "Claimed", 120);
    let plays = vec![play(0, 130, &a, 0), play(130, 250, &actual, 0)];
    let audio = noise(250, 6);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, plays).with_preview_of(&a, 40, 30);
    services.catalog.push(claimed.clone());
    let list = Tracklist { source: HintSource::Description, entries: vec![hint(0, &a), hint(130, &claimed)] };
    let report = analyze(&services, &media.pcm, MixHints { tracklist: Some(list), untimed: Vec::new() }).await;
    assert_eq!(services.calls(), 2, "two agreeing probes overrule a hint");
    let found = songs(&report.pieces);
    assert_eq!(found[1].2, "Actually Played", "{:#?}", report.log);
    let second = &report.pieces.iter().find(|p| p.start_ms == 130_000).unwrap();
    assert!(second.alternates.iter().any(|t| t.title == "Claimed"));
}

#[test]
fn next_probe_goes_past_the_end() {
    let anchor = |at_s: u64, end_s: Option<i64>| Anchor {
        id: String::new(),
        at_ms: at_s * 1000,
        len_ms: PROBE_MS,
        track: track("A", "B", 100),
        evidence: Evidence { source: EvidenceSource::Shazam, at_ms: 0, score: 0.88, detail: None },
        start_est: Some(0),
        end_est: end_s.map(|e| e * 1000),
        kind: AnchorKind::Recognizer,
    };
    assert_eq!(next_probe(&anchor(10, Some(100))), 110_000);
    assert_eq!(next_probe(&anchor(10, Some(400))), 160_000);
    assert_eq!(next_probe(&anchor(10, Some(30))), 40_000);
    assert_eq!(next_probe(&anchor(10, None)), 100_000);
    // Heard after its supposed end: the duration is wrong.
    assert_eq!(next_probe(&anchor(200, Some(100))), 290_000);
}

#[test]
fn probe_points_sit_inside_an_entry() {
    assert_eq!(probe_point(0, 300_000), 60_000);
    assert_eq!(probe_point(0, 50_000), 25_000);
    assert_eq!(probe_point(100_000, 110_000), 100_000);
}
