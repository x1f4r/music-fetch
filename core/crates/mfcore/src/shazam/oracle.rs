//! Comparison against Apple's `SHSignatureGenerator` (macOS only).
//!
//! Run with `cargo test -p mfcore --lib shazam::oracle -- --ignored --nocapture`.
//! Set `MF_SHAZAM_ORACLE_WAVS` to colon-separated 16 kHz mono 16-bit WAV files
//! (≥ 12 s each) to compare on real music; otherwise synthetic clips are used.
//! Set `MF_SHAZAM_ORACLE_KEEP=1` to keep the clips and both signatures.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::signature::{self, BandPeaks, Options};

const SWIFT_SOURCE: &str = include_str!("testdata/shsig.swift");
const CLIP: usize = signature::MAX_SAMPLES;

struct Overlap {
    apple: usize,
    ours: usize,
    shared: usize,
    /// Shared peaks whose magnitude is identical.
    same_magnitude: usize,
}

impl Overlap {
    fn recall(&self) -> f64 {
        if self.apple == 0 { 1.0 } else { self.shared as f64 / self.apple as f64 }
    }

    fn precision(&self) -> f64 {
        if self.ours == 0 { 1.0 } else { self.shared as f64 / self.ours as f64 }
    }
}

fn overlap(apple: &BandPeaks, ours: &BandPeaks) -> [Overlap; 4] {
    std::array::from_fn(|band| {
        let a: HashMap<(u32, u16), u16> = apple[band].iter().map(|p| ((p.frame, p.fbin), p.magnitude)).collect();
        let o: HashMap<(u32, u16), u16> = ours[band].iter().map(|p| ((p.frame, p.fbin), p.magnitude)).collect();
        let shared: Vec<_> = a.iter().filter_map(|(k, m)| Some((*m, *o.get(k)?))).collect();
        Overlap {
            apple: a.len(),
            ours: o.len(),
            shared: shared.len(),
            same_magnitude: shared.iter().filter(|(a, o)| a == o).count(),
        }
    })
}

fn compile_oracle(dir: &Path) -> PathBuf {
    let source = dir.join("shsig.swift");
    let binary = dir.join("shsig");
    std::fs::write(&source, SWIFT_SOURCE).expect("write swift source");
    let status =
        Command::new("xcrun").args(["swiftc", "-O", "-o"]).arg(&binary).arg(&source).status().expect("run swiftc");
    assert!(status.success(), "swiftc failed");
    binary
}

fn apple_peaks(oracle: &Path, wav: &Path) -> BandPeaks {
    let out = wav.with_extension("apple.bin");
    let status = Command::new(oracle).arg(wav).arg(&out).status().expect("run oracle");
    assert!(status.success(), "SHSignatureGenerator failed on {}", wav.display());
    let bytes = std::fs::read(&out).expect("read oracle output");
    let decoded = signature::decode(&bytes).expect("decode Apple signature");
    assert!(decoded.apple, "expected Apple's magic");
    assert_eq!(decoded.sample_rate, 16_000);
    assert_eq!(decoded.sample_count as usize, CLIP);
    decoded.bands
}

fn write_wav(path: &Path, samples: &[i16]) {
    let data_len = (samples.len() * 2) as u32;
    let mut out = Vec::with_capacity(44 + samples.len() * 2);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_len).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    for v in [16u32, 1 | (1 << 16), 16_000, 32_000, 2 | (16 << 16)] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    for s in samples {
        out.extend_from_slice(&s.to_le_bytes());
    }
    std::fs::write(path, out).expect("write wav");
}

/// Samples of a 16 kHz mono 16-bit PCM WAV.
fn read_wav(path: &Path) -> Vec<i16> {
    let bytes = std::fs::read(path).expect("read wav");
    assert!(bytes.len() > 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WAVE", "not a WAV file");
    let mut pos = 12;
    let mut format_ok = false;
    while pos + 8 <= bytes.len() {
        let id = &bytes[pos..pos + 4];
        let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let body = &bytes[pos + 8..(pos + 8 + len).min(bytes.len())];
        if id == b"fmt " {
            let channels = u16::from_le_bytes([body[2], body[3]]);
            let rate = u32::from_le_bytes(body[4..8].try_into().unwrap());
            let bits = u16::from_le_bytes([body[14], body[15]]);
            assert_eq!((channels, rate, bits), (1, 16_000, 16), "{}: need 16 kHz mono 16-bit", path.display());
            format_ok = true;
        } else if id == b"data" {
            assert!(format_ok, "fmt chunk must precede data");
            return body.as_chunks::<2>().0.iter().map(|c| i16::from_le_bytes(*c)).collect();
        }
        pos += 8 + len + (len & 1);
    }
    panic!("no data chunk in {}", path.display());
}

/// 12 s clips: three windows of every supplied file, or synthetic clips.
fn clips() -> Vec<(String, Vec<i16>)> {
    let Ok(list) = std::env::var("MF_SHAZAM_ORACLE_WAVS") else {
        return (1..=3).map(|seed| (format!("synthetic-{seed}"), signature::tests::test_signal(12.0, seed))).collect();
    };
    let mut clips = Vec::new();
    for path in list.split(':').filter(|p| !p.is_empty()) {
        let samples = read_wav(Path::new(path));
        assert!(samples.len() >= CLIP, "{path} is shorter than 12 s");
        let name = Path::new(path).file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        let last = samples.len() - CLIP;
        for start in [0, last / 2, last] {
            clips.push((format!("{name}@{:.1}s", start as f64 / 16_000.0), samples[start..start + CLIP].to_vec()));
        }
    }
    clips
}

#[test]
#[ignore = "needs macOS with ShazamKit and swiftc; run with --ignored"]
fn reproduces_apples_peaks() {
    let dir = std::env::temp_dir().join(format!("mfcore-shazam-oracle-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let oracle = compile_oracle(&dir);
    let keep = std::env::var_os("MF_SHAZAM_ORACLE_KEEP").is_some();

    let variants = [("reference", Options { pad_tail: false }), ("padded (default)", Options::default())];
    let mut worst = [f64::MAX; 4];
    let mut totals: [Overlap; 4] = std::array::from_fn(|_| Overlap { apple: 0, ours: 0, shared: 0, same_magnitude: 0 });
    for (i, (name, samples)) in clips().into_iter().enumerate() {
        let wav = dir.join(format!("clip{i}.wav"));
        write_wav(&wav, &samples);
        let apple = apple_peaks(&oracle, &wav);
        if keep {
            std::fs::write(wav.with_extension("ours.bin"), signature::generate(&samples).bytes).unwrap();
        }
        println!("\n{name}");
        for (variant, options) in &variants {
            let ours = signature::decode(&signature::generate_with(&samples, options).bytes).unwrap().bands;
            let stats = overlap(&apple, &ours);
            let cells: Vec<String> = stats
                .iter()
                .map(|s| {
                    format!(
                        "{:>3}/{:<3} rec {:5.1}% prec {:5.1}%",
                        s.shared,
                        s.apple,
                        s.recall() * 100.0,
                        s.precision() * 100.0
                    )
                })
                .collect();
            println!("  {variant:<20} {}", cells.join(" | "));
            if *options == Options::default() {
                for ((w, t), s) in worst.iter_mut().zip(totals.iter_mut()).zip(&stats) {
                    *w = w.min(s.recall());
                    t.apple += s.apple;
                    t.ours += s.ours;
                    t.shared += s.shared;
                    t.same_magnitude += s.same_magnitude;
                }
            }
        }
    }
    if keep {
        println!("\nkept {}", dir.display());
    } else {
        let _ = std::fs::remove_dir_all(&dir);
    }
    println!("\npooled, default variant:");
    for (band, t) in totals.iter().enumerate() {
        println!(
            "  band {band}: {}/{} Apple peaks reproduced ({:.1}%), precision {:.1}%, identical magnitude {:.1}%",
            t.shared,
            t.apple,
            t.recall() * 100.0,
            t.precision() * 100.0,
            t.same_magnitude as f64 * 100.0 / t.shared.max(1) as f64
        );
    }
    println!("worst per-band recall of Apple's peaks: {:?}", worst.map(|r| (r * 1000.0).round() / 10.0));
    for (band, recall) in worst.iter().enumerate() {
        assert!(*recall >= 0.90, "band {band}: only {:.1}% of Apple's peaks reproduced", recall * 100.0);
    }
}
