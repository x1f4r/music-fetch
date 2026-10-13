use super::*;

const RATE: f32 = 16_000.0;

/// A synthetic "song": random tonal notes with harmonics and a pulse, so the
/// peak picker sees something music-like.
#[derive(Clone)]
struct Note {
    onset: f32,
    dur: f32,
    hz: f32,
    amp: f32,
}

fn song(seed: u64, seconds: f32) -> Vec<Note> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut notes = Vec::new();
    let mut t = 0.0;
    while t < seconds {
        for _ in 0..1 + rng.usize(..3) {
            let hz = 280.0 * (4000.0f32 / 280.0).powf(rng.f32());
            notes.push(Note { onset: t, dur: 0.12 + rng.f32() * 0.5, hz, amp: 0.15 + rng.f32() * 0.25 });
        }
        t += 0.125 * (1 + rng.usize(..4)) as f32;
    }
    notes
}

/// Render notes from song time `from` for `seconds` of output, played at
/// `tempo` (2 = twice as fast) with frequencies multiplied by `pitch`.
fn render(notes: &[Note], from: f32, seconds: f32, tempo: f32, pitch: f32) -> Vec<f32> {
    let len = (seconds * RATE) as usize;
    let mut out = vec![0.0f32; len];
    for note in notes {
        let start = (note.onset - from) / tempo;
        let dur = note.dur / tempo;
        if start + dur < 0.0 || start > seconds {
            continue;
        }
        let hz = note.hz * pitch;
        let first = (start.max(0.0) * RATE) as usize;
        let last = (((start + dur) * RATE) as usize).min(len);
        for (i, sample) in out.iter_mut().enumerate().take(last).skip(first) {
            let t = i as f32 / RATE - start;
            let env = (t / 0.01).min(1.0) * (-3.0 * t / dur).exp();
            let phase = 2.0 * std::f32::consts::PI * hz * t;
            *sample += note.amp * env * (phase.sin() + 0.4 * (2.0 * phase).sin());
        }
    }
    out
}

fn to_i16(samples: &[f32]) -> Vec<i16> {
    samples.iter().map(|s| (s.clamp(-1.0, 1.0) * 20_000.0) as i16).collect()
}

fn add_at(mix: &mut [f32], part: &[f32], at_s: f32, gain: f32) {
    let at = (at_s * RATE) as usize;
    for (i, s) in part.iter().enumerate() {
        if let Some(m) = mix.get_mut(at + i) {
            *m += gain * s;
        }
    }
}

/// 120 s mix: song B for 0–55 s, song A (from its start, at `tempo`/`pitch`)
/// from 40 s on, light noise everywhere.
fn mix_with(a: &[Note], b: &[Note], tempo: f32, pitch: f32) -> Vec<i16> {
    let mut rng = fastrand::Rng::with_seed(99);
    let mut mix: Vec<f32> = (0..(120.0 * RATE) as usize).map(|_| (rng.f32() - 0.5) * 0.02).collect();
    add_at(&mut mix, &render(b, 0.0, 55.0, 1.0, 1.0), 0.0, 0.8);
    add_at(&mut mix, &render(a, 0.0, 80.0, tempo, pitch), 40.0, 0.8);
    to_i16(&mix)
}

fn frames(seconds: f32) -> f32 {
    seconds * 1000.0 / FRAME_MS as f32
}

/// The preview is song A from 20 s to 50 s.
fn preview_index(a: &[Note]) -> Index {
    let mut index = Index::new();
    index.add(&peaks(&to_i16(&render(a, 20.0, 30.0, 1.0, 1.0))));
    index
}

fn check(tempo: f32, pitch: f32, key_locked: bool) {
    let a = song(1, 120.0);
    let b = song(2, 120.0);
    let index = preview_index(&a);
    let mix = mix_with(&a, &b, tempo, pitch);
    let found = index.search(&peaks(&mix), &warps(0.06, 0.005));
    let best = found.first().expect("a match");
    assert!(best.is_confident(&MatchOptions::default()), "{best:?}");
    assert!((best.warp.time - tempo).abs() <= 0.0051, "{best:?}");
    assert_eq!(best.warp.is_key_locked(), key_locked, "{best:?}");
    // Preview start (song time 20 s) plays at 40 s + 20 s / tempo.
    let expected = frames(40.0 + 20.0 / tempo);
    assert!(
        (best.query_frame(0.0) - expected).abs() < frames(0.3),
        "{} vs {expected}: {best:?}",
        best.query_frame(0.0)
    );
    let (lo, hi) = best.span;
    assert!(lo as f32 >= expected - frames(1.0) && hi as f32 <= expected + frames(30.0 / tempo + 1.0), "{best:?}");
    assert!(hi - lo > frames(15.0) as u32, "{best:?}");
}

#[test]
fn finds_a_preview_inside_a_mix() {
    check(1.0, 1.0, false);
}

#[test]
fn finds_a_preview_sped_up_vinyl_style() {
    check(1.04, 1.04, false);
    check(0.96, 0.96, false);
}

#[test]
fn finds_a_preview_with_key_locked_tempo_change() {
    check(1.04, 1.0, true);
    check(0.965, 1.0, true);
}

#[test]
fn other_songs_do_not_match() {
    let a = song(1, 120.0);
    let index = preview_index(&a);
    for seed in 3..6 {
        let other = to_i16(&render(&song(seed, 120.0), 0.0, 90.0, 1.0, 1.0));
        let found = index.search(&peaks(&other), &warps(0.06, 0.005));
        assert!(found.iter().all(|m| !m.is_confident(&MatchOptions::default())), "seed {seed}: {found:?}");
    }
}

#[test]
fn best_reference_wins_with_a_lead() {
    let a = song(1, 120.0);
    let c = song(7, 120.0);
    let mut index = preview_index(&a);
    let other = index.add(&peaks(&to_i16(&render(&c, 20.0, 30.0, 1.0, 1.0))));
    let mix = mix_with(&a, &song(2, 120.0), 1.0, 1.0);
    let found = index.search(&peaks(&mix), &warps(0.02, 0.01));
    assert_eq!(found[0].reference, 0);
    assert!(found[0].is_confident(&MatchOptions::default()));
    assert!(found.iter().filter(|m| m.reference == other).all(|m| !m.is_confident(&MatchOptions::default())));
}

#[test]
fn warps_start_with_identity_and_cover_both_kinds() {
    let w = warps(0.06, 0.01);
    assert_eq!(w[0], Warp::IDENTITY);
    assert_eq!(w.len(), 1 + 6 * 4);
    assert!(w.contains(&Warp { time: 1.06, freq: 1.06 }));
    assert!(w.iter().any(|x| (x.time - 0.94).abs() < 1e-6 && x.freq == 1.0));
    assert_eq!(warps(0.0, 0.01), vec![Warp::IDENTITY]);
}

#[test]
fn file_peaks_match_memory_peaks() {
    let audio = to_i16(&render(&song(5, 150.0), 0.0, 150.0, 1.0, 1.0));
    let dir = std::env::temp_dir().join(format!("mf-fp-{}", crate::util::new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("a.pcm");
    let mut writer = crate::audio::PcmWriter::create(&path).unwrap();
    writer.push(&audio).unwrap();
    let pcm = writer.finish().unwrap();
    let cancel = crate::util::cancel::CancelToken::new();
    let from_file = peaks_in(&pcm, 10_000, 130_000, &cancel).unwrap();
    let from_memory = peaks(&audio[160_000..160_000 + 130 * 16_000]);
    // Chunk edges may differ by a few peaks; the bulk must agree.
    let set: std::collections::HashSet<(u32, u32)> =
        from_memory.iter().map(|p| (p.frame, (p.log_hz * 1000.0) as u32)).collect();
    let shared = from_file.iter().filter(|p| set.contains(&(p.frame, (p.log_hz * 1000.0) as u32))).count();
    assert!(shared as f32 >= 0.97 * from_memory.len() as f32, "{shared} of {}", from_memory.len());
    assert!((from_file.len() as f32 - from_memory.len() as f32).abs() <= 0.03 * from_memory.len() as f32);
    let _ = std::fs::remove_dir_all(dir);
}
