use std::sync::Arc;

use super::*;
use crate::melody::pitch::{PitchTracker, StreamingPitch};

/// Feed audio in 20 ms pushes as a live session would; returns the detector.
fn run(audio: &[f32]) -> ImpressionDetector {
    let tracker = Arc::new(PitchTracker::new().expect("model loads"));
    let mut stream = StreamingPitch::new(tracker);
    let mut detector = ImpressionDetector::new();
    for chunk in audio.chunks(320) {
        let frames = stream.push(chunk);
        detector.push(&frames, chunk);
    }
    detector
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 40) as f32 / (1u64 << 24) as f32 - 0.5
    }
}

/// A hummed tune: one voice with vibrato and breaths between notes.
fn hum() -> Vec<f32> {
    let mut out = Vec::new();
    let mut phase = 0f32;
    for (i, midi) in [
        57.0f32, 59.0, 60.0, 62.0, 64.0, 62.0, 60.0, 59.0, 57.0, 60.0, 64.0, 62.0,
    ]
    .iter()
    .enumerate()
    {
        let hz = 440.0 * 2f32.powf((midi - 69.0) / 12.0);
        for n in 0..7000 {
            let t = n as f32 / 16_000.0;
            let env = (t / 0.05).min(1.0) * ((0.4375 - t) / 0.08).clamp(0.0, 1.0);
            phase += 2.0
                * std::f32::consts::PI
                * hz
                * (1.0 + 0.006 * (2.0 * std::f32::consts::PI * 5.0 * t).sin())
                / 16_000.0;
            out.push(
                0.3 * env * (phase.sin() + 0.4 * (2.0 * phase).sin() + 0.15 * (3.0 * phase).sin()),
            );
        }
        out.extend(std::iter::repeat_n(
            0.0,
            if i % 3 == 2 { 4000 } else { 800 },
        ));
    }
    out
}

/// Recorded-music stand-in: a three-voice chord progression, bass and drums.
fn ensemble() -> Vec<f32> {
    let mut rng = Rng(9);
    let chords = [
        [60.0f32, 64.0, 67.0],
        [57.0, 60.0, 64.0],
        [53.0, 57.0, 60.0],
        [55.0, 59.0, 62.0],
    ];
    (0..16_000 * 6)
        .map(|n| {
            let t = n as f32 / 16_000.0;
            let chord = chords[(t / 1.5) as usize % 4];
            let mut x = 0.0;
            for (k, m) in chord.iter().enumerate() {
                let hz = 440.0 * 2f32.powf((m - 69.0) / 12.0);
                x += 0.12 * (2.0 * std::f32::consts::PI * hz * t + k as f32).sin();
            }
            x += 0.15
                * (2.0
                    * std::f32::consts::PI
                    * 440.0
                    * 2f32.powf((chord[0] - 24.0 - 69.0) / 12.0)
                    * t)
                    .sin();
            let beat = (t * 4.0).fract();
            if beat < 0.04 {
                x += 0.5 * rng.next() * (1.0 - beat / 0.04);
            }
            x
        })
        .collect()
}

#[test]
fn humming_is_an_impression() {
    let d = run(&hum());
    assert_eq!(
        d.verdict(),
        Some(true),
        "p {:?}, features {:?}",
        d.probability(),
        d.features()
    );
    assert!(d.voiced_ms() > 3000);
}

#[test]
fn a_band_is_music() {
    let d = run(&ensemble());
    assert_eq!(
        d.verdict(),
        Some(false),
        "p {:?}, features {:?}",
        d.probability(),
        d.features()
    );
}

#[test]
fn silence_stays_undecided_and_counts_time() {
    let tracker = Arc::new(PitchTracker::new().expect("model loads"));
    let mut stream = StreamingPitch::new(tracker);
    let mut d = ImpressionDetector::new();
    for chunk in vec![0.0f32; 16_000 * 3].chunks(320) {
        let frames = stream.push(chunk);
        d.push(&frames, chunk);
    }
    assert_eq!(d.verdict(), None);
    assert_eq!(d.voiced_ms(), 0);
    assert!(d.silence_ms() > 2000);
    d.push(&[], &[f32::NAN; 4096]);
    assert_eq!(d.sounding_ms(), 0);
}

#[test]
fn harmonicity_separates_one_voice_from_a_chord() {
    let spectrum = |voices: &[f32]| {
        let mut x = vec![0f32; FFT];
        for (i, s) in x.iter_mut().enumerate() {
            for &hz in voices {
                for k in 1..6 {
                    *s += (2.0 * std::f32::consts::PI * hz * k as f32 * i as f32 / SAMPLE_RATE)
                        .sin()
                        / k as f32;
                }
            }
        }
        let fft = RealFftPlanner::<f32>::new().plan_fft_forward(FFT);
        let mut input: Vec<f32> = x
            .iter()
            .enumerate()
            .map(|(i, v)| {
                v * (0.5 - 0.5 * (2.0 * std::f32::consts::PI * i as f32 / FFT as f32).cos())
            })
            .collect();
        let mut out = fft.make_output_vec();
        fft.process(&mut input, &mut out).expect("fft");
        let (lo, hi) = band();
        out[lo..=hi]
            .iter()
            .map(|z| z.norm_sqr() + 1e-12)
            .collect::<Vec<f32>>()
    };
    let one = harmonicity(&spectrum(&[220.0]), 220.0).expect("score");
    let chord = harmonicity(&spectrum(&[220.0, 277.2, 329.6]), 220.0).expect("score");
    assert!(one > 0.8 && chord < one - 0.2, "one {one}, chord {chord}");
    assert!(harmonicity(&spectrum(&[220.0]), f32::NAN).is_none());
}
