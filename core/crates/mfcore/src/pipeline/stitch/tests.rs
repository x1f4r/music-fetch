use super::*;

fn track(title: &str, artist: &str) -> Track {
    Track { title: title.into(), artist: artist.into(), ..Track::default() }
}

fn shazam(at_ms: u64, score: f32) -> Evidence {
    Evidence { source: EvidenceSource::Shazam, at_ms, score, detail: None }
}

fn song(id: &str, start_s: u64, end_s: u64, t: Track) -> Piece {
    Piece {
        id: id.into(),
        item_index: 0,
        start_ms: start_s * 1000,
        end_ms: end_s * 1000,
        kind: SegmentKind::Track,
        track: Some(t),
        evidence: vec![shazam(start_s * 1000, 0.72)],
        alternates: Vec::new(),
        track_start_ms: None,
    }
}

fn gap(id: &str, start_s: u64, end_s: u64, kind: SegmentKind) -> Piece {
    Piece::gap(id, start_s * 1000, end_s * 1000, kind)
}

fn spans(s: &Stitched) -> Vec<(u64, u64, SegmentKind)> {
    s.pieces.iter().map(|p| (p.start_ms / 1000, p.end_ms / 1000, p.kind)).collect()
}

// Ported from 0.4: test_stitch_segment_timeline_merges_duplicate_track_segments_and_clamps_overlaps
#[test]
fn merges_duplicates_and_clamps_overlaps() {
    let slow = track("Slow Down", "Chris Linton & CADMIUM");
    let high = track("High", "Vanic");
    let s = stitch(vec![song("a", 5, 53, slow.clone()), song("b", 46, 74, slow), song("c", 67, 98, high)]);
    assert_eq!(spans(&s), vec![(5, 67, SegmentKind::Track), (67, 98, SegmentKind::Track)]);
    assert_eq!(s.pieces[0].track.as_ref().unwrap().title, "Slow Down");
    assert_eq!(s.pieces[1].track.as_ref().unwrap().title, "High");
    assert_eq!(s.removed, vec!["b".to_string()]);
    assert_eq!(s.pieces[0].evidence.len(), 2);
}

// Ported from 0.4: test_stitch_bridges_short_speech_between_same_identity_matches
#[test]
fn bridges_short_speech() {
    let a = track("Slow Down", "CADMIUM, Chris Linton");
    let a2 = track("Slow Down", "Chris Linton & CADMIUM");
    let s = stitch(vec![song("a", 0, 30, a), gap("talk", 30, 33, SegmentKind::Speech), song("b", 33, 60, a2)]);
    assert_eq!(spans(&s), vec![(0, 60, SegmentKind::Track)]);
    assert_eq!(s.removed, vec!["b".to_string(), "talk".to_string()]);
}

// Ported from 0.4: test_stitch_does_not_bridge_across_long_speech_region
#[test]
fn never_bridges_long_speech() {
    let t = track("Repeat Song", "Artist");
    let s = stitch(vec![song("a", 0, 30, t.clone()), gap("talk", 30, 50, SegmentKind::Speech), song("b", 50, 80, t)]);
    assert_eq!(
        spans(&s),
        vec![(0, 30, SegmentKind::Track), (30, 50, SegmentKind::Speech), (50, 80, SegmentKind::Track)]
    );
    assert!(s.removed.is_empty());
}

// Ported from 0.4: test_stitch_does_not_merge_distinct_isrcs_even_with_matching_titles
#[test]
fn never_merges_different_isrcs() {
    let mut left = track("Same Title", "Artist");
    left.isrc = Some("USRC11111111".into());
    let mut right = track("Same Title", "Artist");
    right.isrc = Some("USRC22222222".into());
    let s = stitch(vec![song("a", 0, 10, left), song("b", 10, 20, right)]);
    assert_eq!(s.pieces.len(), 2);
}

#[test]
fn never_merges_a_remix_with_its_original() {
    let s = stitch(vec![
        song("a", 0, 200, track("Opus", "Eric Prydz")),
        song("b", 200, 420, track("Opus (Four Tet Remix)", "Eric Prydz")),
    ]);
    assert_eq!(s.pieces.len(), 2);
    // A length variant is the same recording.
    let s = stitch(vec![
        song("a", 0, 200, track("Opus", "Eric Prydz")),
        song("b", 205, 420, track("Opus (Extended Mix)", "Eric Prydz")),
    ]);
    assert_eq!(spans(&s), vec![(0, 420, SegmentKind::Track)]);
}

#[test]
fn bridge_budget_scales_with_the_longer_neighbour() {
    assert_eq!(bridge_budget_ms(9_000), 2_000);
    assert_eq!(bridge_budget_ms(30_000), 5_000);
    assert_eq!(bridge_budget_ms(300_000), 8_000);
    assert_eq!(bridge_budget_ms(900_000), 19_000);
    assert_eq!(merge_allowance_ms(9_000), 2_000);
    assert_eq!(merge_allowance_ms(30_000), 5_000);
    assert_eq!(merge_allowance_ms(300_000), 12_000);
    assert_eq!(merge_allowance_ms(900_000), 30_000);
    // A long track bridges 8 s of silence...
    let t = track("Long", "Artist");
    let s =
        stitch(vec![song("a", 0, 200, t.clone()), gap("q", 200, 208, SegmentKind::Silence), song("b", 208, 400, t)]);
    assert_eq!(s.pieces.len(), 1);
    // ...but not 9 s.
    let t = track("Long", "Artist");
    let s =
        stitch(vec![song("a", 0, 200, t.clone()), gap("q", 200, 209, SegmentKind::Silence), song("b", 209, 400, t)]);
    assert_eq!(s.pieces.len(), 3);
}

#[test]
fn agreeing_start_estimates_merge_across_a_gap() {
    let t = track("Song", "Artist");
    let mut a = song("a", 0, 100, t.clone());
    let mut b = song("b", 160, 300, t);
    a.track_start_ms = Some(-20_000);
    b.track_start_ms = Some(-18_000);
    let s = stitch(vec![a, b]);
    assert_eq!(spans(&s), vec![(0, 300, SegmentKind::Track)]);
}

#[test]
fn a_replay_stays_separate() {
    let t = track("Song", "Artist");
    let mut first = song("a", 100, 300, t.clone());
    let mut again = song("b", 300, 500, t);
    first.track_start_ms = Some(100_000);
    again.track_start_ms = Some(300_000);
    let s = stitch(vec![first, again]);
    assert_eq!(s.pieces.len(), 2);
    // A DJ loop (start estimate moves, but not past the first piece) is one play.
    let t = track("Song", "Artist");
    let mut first = song("a", 100, 226, t.clone());
    let mut looped = song("b", 226, 400, t);
    first.track_start_ms = Some(100_000);
    looped.track_start_ms = Some(130_000);
    assert_eq!(stitch(vec![first, looped]).pieces.len(), 1);
}

#[test]
fn gaps_merge_by_kind_and_tracks_win_overlaps() {
    let s = stitch(vec![
        gap("u1", 0, 50, SegmentKind::Unidentified),
        gap("u2", 51, 90, SegmentKind::Unidentified),
        gap("s", 90, 100, SegmentKind::Speech),
        song("t", 95, 200, track("A", "B")),
        gap("z", 200, 200, SegmentKind::Silence),
    ]);
    assert_eq!(
        spans(&s),
        vec![(0, 90, SegmentKind::Unidentified), (90, 95, SegmentKind::Speech), (95, 200, SegmentKind::Track)]
    );
    assert!(s.removed.contains(&"u2".to_string()) && s.removed.contains(&"z".to_string()));
    // A gap swallowed by a track disappears.
    let s = stitch(vec![song("t", 0, 100, track("A", "B")), gap("g", 40, 90, SegmentKind::Unidentified)]);
    assert_eq!(spans(&s), vec![(0, 100, SegmentKind::Track)]);
    assert_eq!(s.removed, vec!["g".to_string()]);
}

#[test]
fn grades_from_evidence() {
    let hint = |score| Evidence { source: EvidenceSource::PageHint, at_ms: 0, score, detail: None };
    assert_eq!(grade(&[shazam(0, 0.88), shazam(60_000, 0.72)]), (Tier::Confirmed, 0.95));
    assert_eq!(grade(&[shazam(0, 0.88)]), (Tier::Likely, 0.88));
    assert_eq!(grade(&[shazam(0, 0.72)]), (Tier::Likely, 0.72));
    assert_eq!(grade(&[hint(VERIFIED_HINT)]), (Tier::Confirmed, 0.95));
    assert_eq!(grade(&[hint(UNVERIFIED_HINT)]), (Tier::Possible, 0.55));
    assert_eq!(grade(&[]), (Tier::Unknown, 0.0));
}
