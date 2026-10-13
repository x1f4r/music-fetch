use std::sync::{Arc, OnceLock};

use super::*;

fn tracker() -> Arc<PitchTracker> {
    static TRACKER: OnceLock<Arc<PitchTracker>> = OnceLock::new();
    TRACKER.get_or_init(|| Arc::new(PitchTracker::new().expect("model loads"))).clone()
}

/// A voice-like tone: fundamental plus decaying harmonics, at `peak` amplitude.
fn tone(hz: f32, seconds: f32, peak: f32) -> Vec<f32> {
    let n = (seconds * SAMPLE_RATE as f32) as usize;
    let mut phase = 0f32;
    let mut out: Vec<f32> = (0..n)
        .map(|_| {
            phase += 2.0 * std::f32::consts::PI * hz / SAMPLE_RATE as f32;
            (1..=6).map(|k| (k as f32 * phase).sin() / k as f32).sum::<f32>()
        })
        .collect();
    let m = out.iter().fold(0f32, |m, x| m.max(x.abs()));
    out.iter_mut().for_each(|x| *x *= peak / m);
    out
}

/// A few notes with short pauses between them.
fn melody() -> Vec<f32> {
    let mut out = Vec::new();
    for midi in [60.0, 62.0, 64.0, 60.0, 67.0] {
        out.extend(tone(midi_to_hz(midi), 0.45, 0.4));
        out.extend(std::iter::repeat_n(0.0, 1200));
    }
    out
}

fn voiced(frames: &[FrameDetail]) -> Vec<f32> {
    frames.iter().filter(|f| f.voiced).map(|f| f.midi()).collect()
}

fn median(mut v: Vec<f32>) -> f32 {
    v.sort_by(f32::total_cmp);
    v[v.len() / 2]
}

#[test]
fn tracks_a_harmonic_tone() {
    let a = tracker().analyze_detailed(&tone(220.0, 2.0, 0.5)).expect("analysis");
    assert_eq!(a.frames.len(), 125);
    assert!(!a.whistle);
    let v = voiced(&a.frames);
    assert!(v.len() > 100, "{} voiced", v.len());
    assert!((median(v) - 57.0).abs() < 0.1);
    assert_eq!(a.frames[3].t_ms, 48);
}

#[test]
fn quiet_input_is_normalized() {
    let loud = voiced(&tracker().analyze_detailed(&tone(330.0, 2.0, 0.5)).expect("analysis").frames).len();
    let quiet = voiced(&tracker().analyze_detailed(&tone(330.0, 2.0, 0.003)).expect("analysis").frames).len();
    assert!(quiet + 5 >= loud, "quiet {quiet} vs loud {loud}");
}

#[test]
fn silence_and_hostile_input() {
    let t = tracker();
    assert!(t.analyze(&[]).is_empty());
    assert!(voiced(&t.analyze_detailed(&vec![0.0; 16_000]).expect("analysis").frames).is_empty());
    let short = t.analyze(&[0.1; 100]);
    assert_eq!(short.len(), 1);
    let mut hostile = tone(200.0, 1.0, 0.5);
    hostile[100] = f32::NAN;
    hostile[200] = f32::INFINITY;
    hostile[300] = 1e30;
    assert_eq!(t.analyze(&hostile).len(), 62);
    let mut s = StreamingPitch::new(t);
    assert!(s.push(&[f32::NAN; 5000]).len() <= 20);
    let _ = s.finish();
    assert!(s.push(&[0.0; 100]).is_empty());
}

#[test]
fn stream_matches_batch() {
    let t = tracker();
    let audio = melody();
    let batch = t.analyze_detailed(&audio).expect("analysis").frames;
    for chunk in [1usize, 7, 256, 333, 4096] {
        let mut s = StreamingPitch::new(t.clone());
        let mut frames = Vec::new();
        for c in audio.chunks(chunk) {
            frames.extend(s.push_detailed(c));
        }
        frames.extend(s.finish_detailed());
        assert_eq!(frames, batch, "chunk {chunk}");
    }
}

#[test]
fn short_streams_match_batch() {
    let t = tracker();
    for len in [1usize, 100, 255, 256, 700, 1023, 1024, 2816, 3000, 5000] {
        let audio: Vec<f32> = tone(180.0, 0.4, 0.4).into_iter().take(len).collect();
        let batch = t.analyze_detailed(&audio).expect("analysis").frames;
        let mut s = StreamingPitch::new(t.clone());
        let mut frames = s.push_detailed(&audio);
        frames.extend(s.finish_detailed());
        assert_eq!(frames, batch, "length {len}");
    }
}

#[test]
fn streaming_latency_is_bounded() {
    let t = tracker();
    let audio = tone(250.0, 3.0, 0.4);
    let mut s = StreamingPitch::new(t);
    let mut pushed = 0usize;
    for c in audio.chunks(160) {
        pushed += c.len();
        for f in s.push_detailed(c) {
            let lag_ms = pushed as u64 * 1000 / SAMPLE_RATE as u64 - f.t_ms;
            assert!(lag_ms <= 250, "frame {} emitted {lag_ms} ms late", f.t_ms);
        }
    }
}

#[test]
fn whistles_above_the_model_ceiling() {
    let mut audio = Vec::new();
    for hz in [2600.0, 2900.0, 2450.0] {
        let n = 8000;
        audio.extend((0..n).map(|i| 0.3 * (2.0 * std::f32::consts::PI * hz * i as f32 / 16_000.0).sin()));
    }
    let t = tracker();
    let a = t.analyze_detailed(&audio).expect("analysis");
    assert!(a.whistle);
    let v = voiced(&a.frames);
    assert!(v.len() > 60, "{} voiced", v.len());
    let first: Vec<f32> = a.frames[5..25].iter().filter(|f| f.voiced).map(|f| f.hz).collect();
    assert!((median(first) - 2600.0).abs() < 30.0);
    // The stream switches to the whistle path and then agrees with the batch.
    let mut s = StreamingPitch::new(t);
    let mut frames = Vec::new();
    for c in audio.chunks(320) {
        frames.extend(s.push_detailed(c));
    }
    frames.extend(s.finish_detailed());
    assert!(s.is_whistle());
    assert_eq!(frames.len(), a.frames.len());
    assert_eq!(frames[40..], a.frames[40..]);
}

#[test]
fn upsampler_interpolates() {
    let x: Vec<f32> = (0..4000).map(|i| (2.0 * std::f32::consts::PI * 1000.0 * i as f32 / 16_000.0).sin()).collect();
    let y = upsample_2x(&x);
    assert_eq!(y.len(), 8000);
    for i in 100..3900 {
        assert_eq!(y[2 * i], x[i]);
        let truth = (2.0 * std::f32::consts::PI * 1000.0 * (i as f32 + 0.5) / 16_000.0).sin();
        assert!((y[2 * i + 1] - truth).abs() < 2e-3, "{i}: {} vs {truth}", y[2 * i + 1]);
    }
}

#[test]
fn reference_frames_follow_the_reference_package() {
    let t = tracker();
    for (len, frames) in [(1usize, 1usize), (255, 1), (256, 1), (511, 1), (512, 2), (16_100, 62)] {
        assert_eq!(t.reference_frames(&vec![0.01; len]).expect("frames").len(), frames, "{len}");
    }
}
