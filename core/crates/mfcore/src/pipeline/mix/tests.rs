use super::*;
use crate::pipeline::hints::{HintSource, TrackHint};
use crate::pipeline::testutil::{FakeServices, Play, TempPcm, emitter, noise, play, track};

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
    // Six full-length tracks of 64–78 s back to back.
    let mut plays = Vec::new();
    let mut at = 0;
    for k in 0..6u64 {
        let len = 64 + (k * 5) % 15;
        plays.push(play(at, at + len, &track(&format!("Artist {k}"), &format!("Song {k}"), len), 0));
        at += len;
    }
    let audio = noise(at, 1);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, plays.clone());
    // Catalog previews let the pipeline confirm each answer without a second probe.
    for p in &plays {
        services = services.with_preview_of(&p.track, 30, 30);
    }
    let report = analyze(&services, &media.pcm, MixHints::default()).await;
    let found = songs(&report.pieces);
    assert_eq!(found.len(), 6, "{found:?}\n{:#?}", report.log);
    for (p, f) in plays.iter().zip(&found) {
        assert_eq!(f.2, p.track.title);
        assert!(close(f.0, p.start_ms / 1000, 3), "{f:?} vs {p:?}");
    }
    // About one probe per track.
    assert!(services.calls() <= 9, "{} probes: {:#?}", services.calls(), report.log);
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
    // Changes land inside the stretch between the last and first hearing.
    assert!(close(found[1].0, 190, 20) && close(found[2].0, 380, 20) && close(found[3].0, 440, 20), "{found:?}");
    // Never more than the budget: 660 s / 45 s + 10.
    assert!(services.calls() <= 24);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lone_false_answer_is_dropped() {
    // An unreleased track the recognizer once mistakes for something else.
    let a = track("A", "Known One", 150);
    let c = track("C", "Known Two", 150);
    let ghost = track("Z", "Misheard", 300);
    let plays = vec![play(0, 150, &a, 0), play(150, 300, &track("ID", "Unreleased", 150), 0), play(300, 450, &c, 0)];
    let audio = noise(450, 7);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, plays).with_preview_of(&a, 30, 30).with_preview_of(&c, 30, 30);
    // The recognizer knows neither the unreleased track nor (mostly) anything there.
    services.overrides.push((150_000, 176_000, Some(ghost)));
    // The misheard answer claims a start long before, while "Known One" surely played.
    services.override_offset_ms = 200_000;
    services.overrides.push((150_000, 300_000, None));
    services.catalog.retain(|t| t.title != "Unreleased");
    let report = analyze(&services, &media.pcm, MixHints::default()).await;
    let titles: Vec<String> = songs(&report.pieces).into_iter().map(|f| f.2).collect();
    assert!(!titles.contains(&"Misheard".to_string()), "{titles:?}\n{:#?}", report.log);
    assert!(titles.contains(&"Known One".to_string()) && titles.contains(&"Known Two".to_string()));
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
async fn a_title_artist_list_is_turned_around() {
    let songs: Vec<Track> =
        [("Tina", "Private Dancer"), ("Abba", "Waterloo"), ("Chic", "Le Freak"), ("Wham", "Freedom")]
            .iter()
            .map(|(artist, title)| track(artist, title, 90))
            .collect();
    let plays: Vec<Play> =
        songs.iter().enumerate().map(|(k, t)| play(k as u64 * 90, k as u64 * 90 + 90, t, 0)).collect();
    let audio = noise(360, 9);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, plays);
    for t in &songs {
        services = services.with_preview_of(t, 20, 30);
    }
    // Written "Title - Artist".
    let entries = songs
        .iter()
        .enumerate()
        .map(|(k, t)| TrackHint {
            start_ms: k as u64 * 90_000,
            artist: t.title.clone(),
            title: t.artist.clone(),
            is_id: false,
            raw: String::new(),
        })
        .collect();
    let list = Tracklist { source: HintSource::Chapters, entries };
    let report = analyze(&services, &media.pcm, MixHints { tracklist: Some(list), untimed: Vec::new() }).await;
    assert_eq!(report.verified_locally, 4, "{:#?}", report.log);
    let found: Vec<(String, String)> =
        report.pieces.iter().filter_map(|p| p.track.as_ref().map(|t| (t.artist.clone(), t.title.clone()))).collect();
    assert_eq!(found[0], ("Tina".to_string(), "Private Dancer".to_string()));
    assert_eq!(services.calls(), 0);
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

#[tokio::test(flavor = "multi_thread")]
async fn a_tracklist_that_stops_matching_is_abandoned() {
    // The first two entries are right; the rest name songs that never play.
    let real: Vec<Track> = (0..5).map(|k| track(&format!("R{k}"), &format!("Real {k}"), 100)).collect();
    let wrong: Vec<Track> = (0..3).map(|k| track(&format!("W{k}"), &format!("Wrong {k}"), 100)).collect();
    let plays: Vec<Play> =
        real.iter().enumerate().map(|(k, t)| play(k as u64 * 100, k as u64 * 100 + 100, t, 0)).collect();
    let audio = noise(500, 8);
    let media = TempPcm::new(&audio);
    let mut services =
        FakeServices::new(audio, plays).with_preview_of(&real[0], 30, 30).with_preview_of(&real[1], 30, 30);
    services.catalog.extend(wrong.iter().cloned());
    let mut entries = vec![hint(0, &real[0]), hint(100, &real[1])];
    entries.extend(wrong.iter().enumerate().map(|(k, t)| hint(200 + k as u64 * 100, t)));
    let list = Tracklist { source: HintSource::Description, entries };
    let report = analyze(&services, &media.pcm, MixHints { tracklist: Some(list), untimed: Vec::new() }).await;
    let titles: Vec<String> = songs(&report.pieces).into_iter().map(|f| f.2).collect();
    assert_eq!(titles, vec!["Real 0", "Real 1", "Real 2", "Real 3", "Real 4"], "{:#?}", report.log);
    assert!(report.log.iter().any(|l| l.contains("does not match")));
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
        odd: false,
        speed: 0.0,
        pitch: 0.0,
    };
    assert_eq!(next_probe(&anchor(10, Some(100))), 110_000);
    assert_eq!(next_probe(&anchor(10, Some(400))), 160_000);
    assert_eq!(next_probe(&anchor(10, Some(220))), 230_000);
    assert_eq!(next_probe(&anchor(10, Some(30))), 40_000);
    assert_eq!(next_probe(&anchor(10, None)), 100_000);
    // Heard after its supposed end: the duration is wrong.
    assert_eq!(next_probe(&anchor(200, Some(100))), 290_000);
}

#[test]
fn plausible_speed_and_pitch() {
    // Values the recognizer reported for real plays in the evaluation mixes...
    assert!(plausible_skew(0.0, 0.0));
    assert!(plausible_skew(-0.0369, -0.0370)); // turntable / pitch fader
    assert!(plausible_skew(0.0532, -0.0006)); // key lock
    // ...and for samples inside other records.
    assert!(!plausible_skew(0.0519, 0.0433));
    assert!(!plausible_skew(-0.0066, 0.0099));
    assert!(!plausible_skew(0.1009, -0.0004)); // beyond any pitch fader
}

#[test]
fn stretching_undoes_a_pitch_fader() {
    // 1 s of 1030 Hz (a 1000 Hz tone played 3 % fast) becomes 1000 Hz.
    let rate = crate::audio::SAMPLE_RATE as f64;
    let fast: Vec<i16> =
        (0..16_000).map(|i| (8000.0 * (2.0 * std::f64::consts::PI * 1030.0 * i as f64 / rate).sin()) as i16).collect();
    let slow = stretch(&fast, 1.03, 1_000);
    assert_eq!(slow.len(), 16_000, "cut to the requested length");
    let crossings = slow.windows(2).filter(|w| (w[0] < 0) != (w[1] < 0)).count();
    assert!((1990..=2010).contains(&crossings), "{crossings}");
}

#[test]
fn unanswered_probes_widen_the_step() {
    let steps: Vec<u64> = (1..=6).map(|k| miss_step(k) / 1000).collect();
    assert_eq!(steps, vec![25, 25, 50, 100, 150, 150]);
}

fn answer(id: &str, at_s: u64, t: &Track, start_s: i64, kind: AnchorKind) -> Anchor {
    Anchor {
        id: id.into(),
        at_ms: at_s * 1000,
        len_ms: if kind == AnchorKind::Recognizer { PROBE_MS } else { 30_000 },
        track: t.clone(),
        evidence: Evidence { source: EvidenceSource::Shazam, at_ms: at_s * 1000, score: 0.72, detail: None },
        start_est: Some(start_s * 1000),
        end_est: t.duration_ms.map(|d| start_s * 1000 + d as i64),
        kind,
        odd: false,
        speed: 0.0,
        pitch: 0.0,
    }
}

#[test]
fn versions_heard_in_one_play_become_one_recording() {
    let mut edit = track("Faithless", "Insomnia (Radio Edit)", 215);
    edit.isrc = Some("GBBKS9600001".into());
    let mut monster = track("Faithless", "Insomnia (Monster Mix)", 520);
    monster.isrc = Some("GBBKS9600002".into());
    let next = track("ATB", "9 PM (Till I Come)", 320);
    let anchors = vec![
        answer("1", 4, &edit, -158, AnchorKind::Recognizer),
        answer("2", 64, &edit, -158, AnchorKind::Recognizer),
        answer("3", 154, &monster, -29, AnchorKind::Recognizer),
        answer("4", 220, &edit, -158, AnchorKind::Recognizer),
        answer("5", 304, &next, 240, AnchorKind::Recognizer),
    ];
    let unified = unify_versions(anchors);
    let titles: Vec<&str> = unified.iter().map(|a| a.track.title.as_str()).collect();
    assert_eq!(titles[..4], ["Insomnia (Radio Edit)"; 4]);
    assert_eq!(unified[2].start_est, Some(-158_000), "the chosen version's timeline");
    assert_eq!(titles[4], "9 PM (Till I Come)");
}

#[test]
fn a_play_heard_past_its_catalog_end_has_no_known_end() {
    // The catalog knows a 3:16 edit; the DJ plays the long version for 6 minutes.
    let t = track("Kerri", "System (Remix)", 196);
    let anchors = vec![answer("1", 100, &t, 0, AnchorKind::Recognizer), answer("2", 300, &t, 0, AnchorKind::Local)];
    let fixed = forget_overrun_ends(anchors);
    assert!(fixed.iter().all(|a| a.end_est.is_none()));
    // Heard inside its length: the end stays.
    let anchors = vec![answer("1", 100, &t, 0, AnchorKind::Recognizer)];
    assert_eq!(forget_overrun_ends(anchors)[0].end_est, Some(196_000));
}

#[test]
fn a_preview_backs_up_only_its_own_play() {
    let t = track("Artist", "Looped", 300);
    let answer_at = answer("1", 200, &t, 150, AnchorKind::Recognizer);
    assert!(within_play(&answer_at, &answer("2", 250, &t, 150, AnchorKind::Local)));
    // A preview match from an earlier play of the same record says nothing about this one.
    assert!(!within_play(&answer_at, &answer("3", 20, &t, 0, AnchorKind::Local)));
}

#[test]
fn a_reprise_after_the_song_stays_separate() {
    let song = track("Band", "Song", 200);
    let reprise = track("Band", "Song (Reprise)", 90);
    // The reprise begins where the song ends and is heard after that.
    let anchors = vec![
        answer("1", 100, &song, 0, AnchorKind::Recognizer),
        answer("2", 215, &reprise, 200, AnchorKind::Recognizer),
    ];
    let unified = unify_versions(anchors);
    assert_eq!(unified[1].track.title, "Song (Reprise)");
}

#[test]
fn probe_points_sit_inside_an_entry() {
    assert_eq!(probe_point(0, 300_000), 60_000);
    assert_eq!(probe_point(0, 50_000), 25_000);
    assert_eq!(probe_point(100_000, 110_000), 100_000);
}
