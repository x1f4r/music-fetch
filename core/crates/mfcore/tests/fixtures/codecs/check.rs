// Decoded-PCM checks for the codec fixtures, included by audio/decode.rs (native
// unit tests) and tests/wasm_decode.rs (Node, Chrome, Firefox). The including
// module brings `decode_bytes` and `CancelToken` into scope.
//
// The expectation is the native decoder's output: its length, peak and RMS, and
// the samples of a window in the middle (`expected/<file>.pcm`). Decoders and
// resampler use floating point, so builds whose math differs in rounding
// (aarch64, x86_64, wasm32 SIMD) may differ by one step of 16-bit audio in a
// few samples; nothing more is allowed.

/// (file, hint, bytes, expected window). The MediaRecorder files get the MIME
/// type the browser reports, the others their extension.
#[allow(dead_code)]
const CODEC_FIXTURES: &[(&str, &str, &[u8], &[u8])] = &[
    ("wav-22k-stereo.wav", "wav", include_bytes!("wav-22k-stereo.wav"), include_bytes!("expected/wav-22k-stereo.wav.pcm")),
    ("mp3-44k-stereo.mp3", "mp3", include_bytes!("mp3-44k-stereo.mp3"), include_bytes!("expected/mp3-44k-stereo.mp3.pcm")),
    ("aac-44k-stereo.m4a", "m4a", include_bytes!("aac-44k-stereo.m4a"), include_bytes!("expected/aac-44k-stereo.m4a.pcm")),
    ("flac-44k-mono.flac", "flac", include_bytes!("flac-44k-mono.flac"), include_bytes!("expected/flac-44k-mono.flac.pcm")),
    (
        "vorbis-48k-stereo.ogg",
        "ogg",
        include_bytes!("vorbis-48k-stereo.ogg"),
        include_bytes!("expected/vorbis-48k-stereo.ogg.pcm"),
    ),
    (
        "opus-48k-stereo.opus",
        "opus",
        include_bytes!("opus-48k-stereo.opus"),
        include_bytes!("expected/opus-48k-stereo.opus.pcm"),
    ),
    (
        "opus-48k-stereo.webm",
        "webm",
        include_bytes!("opus-48k-stereo.webm"),
        include_bytes!("expected/opus-48k-stereo.webm.pcm"),
    ),
    (
        "mediarecorder-chrome.webm",
        "audio/webm;codecs=opus",
        include_bytes!("mediarecorder-chrome.webm"),
        include_bytes!("expected/mediarecorder-chrome.webm.pcm"),
    ),
    (
        "mediarecorder-firefox.ogg",
        "audio/ogg",
        include_bytes!("mediarecorder-firefox.ogg"),
        include_bytes!("expected/mediarecorder-firefox.ogg.pcm"),
    ),
];

/// Native decoder output per fixture: samples, peak, RMS and the window start.
#[allow(dead_code)]
const CODEC_EXPECTED: &str = include_str!("expected.json");

/// Samples in the compared window.
#[allow(dead_code)]
const CODEC_WINDOW: usize = 4000;

/// The window compared sample by sample: the middle of the decoded audio.
#[allow(dead_code)]
fn codec_window_start(samples: usize) -> usize {
    samples.saturating_sub(CODEC_WINDOW) / 2
}

/// Length, peak, RMS and window start of decoded audio.
#[allow(dead_code)]
fn codec_summary(samples: &[i16]) -> serde_json::Value {
    let peak = samples.iter().map(|s| s.unsigned_abs()).max().unwrap_or(0);
    let energy: f64 = samples.iter().map(|&s| f64::from(s) * f64::from(s)).sum();
    let rms = (energy / samples.len().max(1) as f64).sqrt();
    serde_json::json!({
        "samples": samples.len(),
        "peak": peak,
        "rms": (rms * 10.0).round() / 10.0,
        "window_start": codec_window_start(samples.len()),
    })
}

/// Per fixture: samples in the window that differ from the native output, and
/// by how much at most.
#[allow(dead_code)]
fn codec_window_diffs() -> Vec<(&'static str, usize, i32)> {
    CODEC_FIXTURES
        .iter()
        .map(|(name, hint, bytes, window)| {
            let samples = decode_bytes(bytes, Some(hint), &CancelToken::new()).unwrap_or_default();
            let start = codec_window_start(samples.len());
            let native = window.as_chunks::<2>().0.iter().map(|b| i16::from_le_bytes(*b));
            let diffs: Vec<i32> =
                samples.iter().skip(start).zip(native).map(|(a, b)| (i32::from(*a) - i32::from(b)).abs()).collect();
            (*name, diffs.iter().filter(|d| **d != 0).count(), diffs.into_iter().max().unwrap_or(0))
        })
        .collect()
}

/// Fixtures whose decoded audio is not the native output, with what differs.
#[allow(dead_code)]
fn codec_mismatches() -> Vec<String> {
    let expected: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(CODEC_EXPECTED).expect("expected.json");
    let mut bad = Vec::new();
    for (name, hint, bytes, window) in CODEC_FIXTURES {
        let samples = match decode_bytes(bytes, Some(hint), &CancelToken::new()) {
            Ok(samples) => samples,
            Err(e) => {
                bad.push(format!("{name}: {e}"));
                continue;
            }
        };
        let want = &expected[*name];
        let got = codec_summary(&samples);
        let num = |v: &serde_json::Value, k: &str| v[k].as_f64().unwrap_or(f64::NAN);
        if got["samples"] != want["samples"]
            || (num(&got, "peak") - num(want, "peak")).abs() > 1.0
            || (num(&got, "rms") - num(want, "rms")).abs() > 0.5
        {
            bad.push(format!("{name}: expected {want}, decoded {got}"));
            continue;
        }
        let start = codec_window_start(samples.len());
        let native: Vec<i16> = window.as_chunks::<2>().0.iter().map(|b| i16::from_le_bytes(*b)).collect();
        let ours = &samples[start..(start + native.len()).min(samples.len())];
        let max_diff = ours.iter().zip(&native).map(|(a, b)| (i32::from(*a) - i32::from(*b)).abs()).max().unwrap_or(0);
        let differing = ours.iter().zip(&native).filter(|(a, b)| a != b).count();
        if ours.len() != native.len() || max_diff > 1 || differing * 50 > native.len() {
            bad.push(format!(
                "{name}: window of {} samples differs in {differing} samples, by up to {max_diff}",
                native.len()
            ));
        }
    }
    bad
}
