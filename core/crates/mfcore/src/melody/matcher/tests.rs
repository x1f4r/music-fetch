use super::*;
use crate::melody::builder::{PhraseInput, SongInput, build_bytes};
use crate::melody::notes::query_from_frames;
use crate::model::PitchFrame;

/// Deterministic pseudo-random numbers.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f32 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 40) as f32 / (1u64 << 24) as f32
    }
}

/// A random stepwise melody of `n` notes: [pitch, onset, duration].
fn random_melody(rng: &mut Rng, n: usize) -> Vec<[f64; 3]> {
    let mut pitch = 60.0f64 + (rng.next() as f64 * 12.0).floor();
    let mut t = 0.0f64;
    (0..n)
        .map(|_| {
            let step: f64 = [-4.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 5.0, -5.0, 7.0][(rng.next() * 10.0) as usize % 10];
            pitch = (pitch + step).clamp(50.0, 80.0);
            let dur: f64 = [250.0, 250.0, 500.0, 375.0, 750.0][(rng.next() * 5.0) as usize % 5];
            let note = [pitch, t, dur * 0.9];
            t += dur;
            note
        })
        .collect()
}

fn song(title: &str, notes: Vec<[f64; 3]>) -> SongInput {
    SongInput {
        title: title.into(),
        artist: "Test".into(),
        isrc: None,
        deezer_id: None,
        year: None,
        popularity: Some(0.5),
        phrases: vec![PhraseInput { source: Some("preview".into()), start_ms: 0, notes }],
        contours: Vec::new(),
    }
}

/// Frames for notes sung `shift` semitones away, `stretch` times slower, with
/// pitch wobble and short breaths between notes.
fn sing(notes: &[[f64; 3]], shift: f32, stretch: f32, rng: &mut Rng) -> Vec<PitchFrame> {
    let end = notes.iter().map(|n| n[1] + n[2]).fold(0.0, f64::max) as f32 * stretch;
    (0..(end / 16.0) as u64)
        .map(|f| {
            let t = f as f32 * 16.0 / stretch;
            let midi = notes
                .iter()
                .find(|n| (n[1] as f32) <= t && t < (n[1] + n[2]) as f32 - 30.0)
                .map(|n| n[0] as f32 + shift + (rng.next() - 0.5) * 0.4)
                .unwrap_or(f32::NAN);
            PitchFrame { t_ms: f * 16, midi, confidence: if midi.is_finite() { 0.9 } else { 0.2 } }
        })
        .collect()
}

fn index(rng: &mut Rng, target: &[[f64; 3]]) -> MelodyIndex {
    let mut songs: Vec<SongInput> = (0..300).map(|i| song(&format!("Song {i}"), random_melody(rng, 60))).collect();
    songs.insert(137, song("Target", target.to_vec()));
    let (bytes, _) = build_bytes(&songs, "test").expect("build");
    MelodyIndex::from_bytes(bytes).expect("open")
}

#[test]
fn finds_a_transposed_slower_rendition() {
    let mut rng = Rng(1);
    let target = random_melody(&mut rng, 60);
    let idx = index(&mut rng, &target);
    // Hum notes 20..36 a fourth higher and 25% slower.
    let excerpt: Vec<[f64; 3]> = target[20..36].iter().map(|n| [n[0], n[1] - target[20][1], n[2]]).collect();
    let query = query_from_frames(&sing(&excerpt, 5.0, 1.25, &mut rng)).expect("query");
    let matches = search(&idx, &query, 5);
    assert_eq!(matches.first().map(|m| m.song.title.as_str()), Some("Target"));
    let best = &matches[0];
    assert!(best.score > 0.5, "score {}", best.score);
    assert!((best.key_shift - 5.0).abs() < 0.6, "key {}", best.key_shift);
    assert!((best.tempo - 0.8).abs() < 0.1, "tempo {}", best.tempo);
    let a = &best.alignment;
    assert_eq!(a.frame_ms, 50);
    assert_eq!(a.query.len(), a.reference.len());
    let close = a
        .query
        .iter()
        .zip(&a.reference)
        .filter(|(q, r)| q.is_finite() && r.is_finite() && (*q - *r).abs() < 1.0)
        .count();
    assert!(close * 2 > a.query.iter().filter(|q| q.is_finite()).count());
    // Scores are calibrated probabilities, best first.
    assert!(matches.windows(2).all(|w| w[0].score >= w[1].score));
    assert!(matches.iter().all(|m| (0.0..=1.0).contains(&m.score)));
}

#[test]
fn finds_an_excerpt_across_phrase_boundaries() {
    let mut rng = Rng(4);
    let target = random_melody(&mut rng, 80);
    // The reference arrives as three phrases with rests between them.
    let mut songs: Vec<SongInput> = (0..200).map(|i| song(&format!("Song {i}"), random_melody(&mut rng, 60))).collect();
    let mut t = song("Target", Vec::new());
    t.phrases = target
        .chunks(20)
        .enumerate()
        .map(|(i, part)| PhraseInput {
            source: Some("preview".into()),
            start_ms: (part[0][1] + 400.0 * i as f64) as u32,
            notes: part.iter().map(|n| [n[0], n[1] - part[0][1], n[2]]).collect(),
        })
        .collect();
    songs.push(t);
    let (bytes, _) = build_bytes(&songs, "test").expect("build");
    let idx = MelodyIndex::from_bytes(bytes).expect("open");
    // Notes 12..34 span the first phrase boundary.
    let excerpt: Vec<[f64; 3]> = target[12..34].iter().map(|n| [n[0], n[1] - target[12][1], n[2]]).collect();
    let query = query_from_frames(&sing(&excerpt, -2.0, 0.9, &mut rng)).expect("query");
    let matches = search(&idx, &query, 3);
    assert_eq!(matches.first().map(|m| m.song.title.as_str()), Some("Target"));
}

#[test]
fn scores_a_named_song() {
    let mut rng = Rng(2);
    let target = random_melody(&mut rng, 50);
    let idx = index(&mut rng, &target);
    let excerpt: Vec<[f64; 3]> = target[5..22].iter().map(|n| [n[0], n[1] - target[5][1], n[2]]).collect();
    let query = query_from_frames(&sing(&excerpt, -3.0, 0.9, &mut rng)).expect("query");
    let target_id = idx.find_song("Target", "Test").expect("target").id;
    let hit = score_song(&idx, &query, target_id).expect("scored");
    assert_eq!(hit.song.id, target_id);
    let other = score_song(&idx, &query, if target_id == 0 { 1 } else { 0 });
    assert!(other.is_none_or(|o| o.score < hit.score));
    assert!(score_song(&idx, &query, 999_999).is_none());
}

#[test]
fn empty_index_and_thin_queries() {
    let (bytes, _) = build_bytes(&[], "empty").expect("build");
    let empty = MelodyIndex::from_bytes(bytes).expect("open");
    let mut rng = Rng(3);
    let target = random_melody(&mut rng, 30);
    let query = query_from_frames(&sing(&target, 0.0, 1.0, &mut rng)).expect("query");
    assert!(search(&empty, &query, 5).is_empty());
    assert!(score_song(&empty, &query, 0).is_none());
    // A long drone has no melody to search with; it must not panic.
    let drone: Vec<PitchFrame> = (0..400).map(|f| PitchFrame { t_ms: f * 16, midi: 60.0, confidence: 0.9 }).collect();
    let q = query_from_frames(&drone).expect("query");
    let idx = index(&mut rng, &target);
    let _ = search(&idx, &q, 5);
    let _ = score_song(&idx, &q, 0);
}

#[test]
fn votes_pool_into_the_right_placement() {
    let tempos = tempo_grid();
    // Consistent votes for track 3 at tempo index 5, anchor ≈ 40, plus noise.
    let mut votes: Vec<Vote> = (0..20).map(|i| (3, 5, 40.0 + (i % 3) as f32, 1.0)).collect();
    votes.extend((0..10).map(|i| (3, 2, 100.0 + 13.0 * i as f32, 1.0)));
    votes.extend((0..5).map(|i| (7, 5, 10.0 * i as f32, 1.0)));
    let placements = pool_votes(votes, 10);
    let best = placements.iter().max_by(|a, b| a.votes.total_cmp(&b.votes)).expect("placement");
    assert_eq!(best.track, 3);
    assert!((best.tempo - tempos[5]).abs() < 1e-6);
    assert!((best.anchor - 41.0).abs() < 1.0, "{}", best.anchor);
    assert!(placements.iter().filter(|p| p.track == 3).count() <= PLACEMENTS_PER_TRACK);
}

#[test]
fn versions_of_a_work_are_one_answer() {
    let mut rng = Rng(5);
    let target = random_melody(&mut rng, 60);
    let mut songs: Vec<SongInput> = (0..200).map(|i| song(&format!("Song {i}"), random_melody(&mut rng, 60))).collect();
    // The original and a cover of it: same title, other artist, the same melody.
    let mut original = song("Target (Remastered)", target.clone());
    original.artist = "Original".into();
    original.popularity = Some(0.9);
    let mut cover = song("Target", target.clone());
    cover.artist = "Cover Band".into();
    cover.popularity = Some(0.2);
    songs.push(cover);
    songs.push(original);
    let (bytes, _) = build_bytes(&songs, "test").expect("build");
    let idx = MelodyIndex::from_bytes(bytes).expect("open");
    let excerpt: Vec<[f64; 3]> = target[10..30].iter().map(|n| [n[0], n[1] - target[10][1], n[2]]).collect();
    let query = query_from_frames(&sing(&excerpt, 2.0, 1.1, &mut rng)).expect("query");
    let matches = search(&idx, &query, 5);
    let best = matches.first().expect("match");
    // One answer for the work, shown as the more popular recording.
    assert_eq!(best.song.artist, "Original");
    assert_eq!(matches.iter().filter(|m| m.song.title.starts_with("Target")).count(), 1);
    let a = idx.find_song("Target", "Original").expect("original").id;
    let b = idx.find_song("Target", "Cover Band").expect("cover").id;
    assert_eq!(work_key(&idx, a), work_key(&idx, b));
    let other = (0..idx.song_count()).find(|&i| i != a && i != b).expect("other");
    assert_ne!(work_key(&idx, a), work_key(&idx, other));
    let all = candidates(&idx, &query, &MatchOptions::default());
    assert!(all.iter().any(|c| c.song == a || c.song == b));
    // A cover that matches a little better gives way to the more popular original,
    // with the work's best score; one that matches much better does not.
    let shown = group_works(&idx, &[(b, 2.0), (other, 1.8), (a, 1.5)]);
    assert_eq!(shown, vec![(2, 2.0), (1, 1.8)]);
    assert_eq!(group_works(&idx, &[(b, 3.0), (a, 1.5)]), vec![(0, 3.0)]);
}
