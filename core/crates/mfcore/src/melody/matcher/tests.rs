use super::*;
use crate::melody::builder::{PhraseInput, SongInput, build_bytes};
use crate::melody::notes::query_from_frames;
use crate::model::PitchFrame;

/// Deterministic pseudo-random numbers.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 40) as f32 / (1u64 << 24) as f32
    }
}

/// A random stepwise melody of `n` notes: [pitch, onset, duration].
fn random_melody(rng: &mut Rng, n: usize) -> Vec<[f64; 3]> {
    let mut pitch = 60.0f64 + (rng.next() as f64 * 12.0).floor();
    let mut t = 0.0f64;
    (0..n)
        .map(|_| {
            let step: f64 = [-4.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 5.0, -5.0, 7.0]
                [(rng.next() * 10.0) as usize % 10];
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
        phrases: vec![PhraseInput {
            source: Some("preview".into()),
            start_ms: 0,
            notes,
        }],
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
            PitchFrame {
                t_ms: f * 16,
                midi,
                confidence: if midi.is_finite() { 0.9 } else { 0.2 },
            }
        })
        .collect()
}

fn index(rng: &mut Rng, target: &[[f64; 3]]) -> MelodyIndex {
    let mut songs: Vec<SongInput> = (0..300)
        .map(|i| song(&format!("Song {i}"), random_melody(rng, 60)))
        .collect();
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
    let excerpt: Vec<[f64; 3]> = target[20..36]
        .iter()
        .map(|n| [n[0], n[1] - target[20][1], n[2]])
        .collect();
    let query = query_from_frames(&sing(&excerpt, 5.0, 1.25, &mut rng)).expect("query");
    let matches = search(&idx, &query, 5);
    assert_eq!(
        matches.first().map(|m| m.song.title.as_str()),
        Some("Target")
    );
    let best = &matches[0];
    assert!(best.score > 0.5, "score {}", best.score);
    assert!((best.key_shift - 5.0).abs() < 0.6, "key {}", best.key_shift);
    assert!((best.tempo - 0.8).abs() < 0.15, "tempo {}", best.tempo);
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
fn scores_a_named_song() {
    let mut rng = Rng(2);
    let target = random_melody(&mut rng, 50);
    let idx = index(&mut rng, &target);
    let excerpt: Vec<[f64; 3]> = target[5..22]
        .iter()
        .map(|n| [n[0], n[1] - target[5][1], n[2]])
        .collect();
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
    // A long drone has no pitch changes to search with.
    let drone: Vec<PitchFrame> = (0..400)
        .map(|f| PitchFrame {
            t_ms: f * 16,
            midi: 60.0,
            confidence: 0.9,
        })
        .collect();
    let q = query_from_frames(&drone).expect("query");
    let idx = index(&mut rng, &target);
    let _ = search(&idx, &q, 5);
}

#[test]
fn query_keys_cover_sung_errors() {
    // A compressed leap (+8.4 sung for +9) still produces the exact key.
    let steps: Vec<Step> = [60.0, 68.4, 66.1, 64.0, 66.0]
        .iter()
        .enumerate()
        .map(|(i, &p)| Step {
            pitch: p,
            onset_ms: i as u32 * 300,
            end_ms: i as u32 * 300 + 250,
        })
        .collect();
    let keys = query_keys(&steps);
    assert!(
        keys.iter()
            .any(|&(j, k, _)| j == 0 && k == index::interval_key(&[9, -2, -2, 2]))
    );
    assert!(
        keys.iter()
            .any(|&(j, k, _)| j == 0 && k == index::interval_key(&[8, -2, -2, 2]))
    );
}

#[test]
fn note_alignment_survives_an_inserted_note() {
    let shape_of = |p: &[f32]| {
        let steps: Vec<Step> = p
            .iter()
            .enumerate()
            .map(|(i, &x)| Step {
                pitch: x,
                onset_ms: i as u32 * 300,
                end_ms: i as u32 * 300 + 250,
            })
            .collect();
        shape(&steps)
    };
    let reference = shape_of(&[
        55.0, 57.0, 60.0, 62.0, 64.0, 62.0, 60.0, 59.0, 60.0, 67.0, 65.0, 64.0,
    ]);
    // Reference notes 1..9 a fourth up, with a passing 66 inserted.
    let query = shape_of(&[62.0, 65.0, 66.0, 67.0, 69.0, 67.0, 65.0, 64.0, 65.0]);
    let mut table = AlignTable::default();
    let found = align_notes(&query, &reference, &mut table);
    let (cost, pairs) = found.first().expect("alignment");
    assert!(*cost < 0.3, "cost {cost}");
    assert_eq!(pairs.first(), Some(&(0, 1)));
}
