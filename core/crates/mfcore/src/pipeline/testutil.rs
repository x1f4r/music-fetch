//! Fakes for pipeline tests: a media file whose every window can be located,
//! a recognizer that answers from a known timeline, a small catalog.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

use crate::audio::{PcmFile, PcmWriter, SAMPLE_RATE};
use crate::error::Result;
use crate::model::{EvidenceSource, SessionEvent, SessionKind, Track};
use crate::pipeline::analyze::Services;
use crate::pipeline::emitter::Emitter;
use crate::pipeline::hints;
use crate::pipeline::recognize::RecognitionHit;
use crate::platform::SessionObserver;
use crate::shazam::Priority;
use crate::store::Store;
use crate::util::cancel::CancelToken;

/// One stretch of the media where a recording plays.
#[derive(Debug, Clone)]
pub struct Play {
    pub start_ms: u64,
    pub end_ms: u64,
    pub track: Track,
    /// Position in the recording at `start_ms`.
    pub offset_ms: i64,
    /// Playback speed (1.04 = 4 % faster).
    pub speed: f32,
}

pub fn track(artist: &str, title: &str, duration_s: u64) -> Track {
    let mut t =
        Track { title: title.into(), artist: artist.into(), duration_ms: Some(duration_s * 1000), ..Track::default() };
    t.ids.insert("shazam".into(), format!("{artist}-{title}").to_lowercase().replace(' ', "-"));
    crate::identity::refresh_identity(&mut t);
    t
}

pub fn play(start_s: u64, end_s: u64, t: &Track, offset_s: i64) -> Play {
    Play { start_ms: start_s * 1000, end_ms: end_s * 1000, track: t.clone(), offset_ms: offset_s * 1000, speed: 1.0 }
}

/// Seeded noise: loud, "musical" for the labeler, and every window is unique.
pub fn noise(seconds: u64, seed: u64) -> Vec<i16> {
    let mut rng = fastrand::Rng::with_seed(seed);
    (0..seconds * SAMPLE_RATE as u64).map(|_| rng.i16(-8000..8000)).collect()
}

pub struct FakeServices {
    pub audio: Vec<i16>,
    pub plays: Vec<Play>,
    /// Tracks `find` and `enrich` know (with durations).
    pub catalog: Vec<Track>,
    /// Previews by identity key.
    pub previews: HashMap<String, Vec<i16>>,
    pub calls: AtomicU32,
    /// Windows the recognizer was asked about (ms).
    pub asked: Mutex<Vec<u64>>,
    /// Answers overriding the timeline for windows starting in a range.
    pub overrides: Vec<(u64, u64, Option<Track>)>,
    pub cancel: CancelToken,
}

impl FakeServices {
    pub fn new(audio: Vec<i16>, plays: Vec<Play>) -> Self {
        let catalog = plays.iter().map(|p| p.track.clone()).collect();
        Self {
            audio,
            plays,
            catalog,
            previews: HashMap::new(),
            calls: AtomicU32::new(0),
            asked: Mutex::new(Vec::new()),
            overrides: Vec::new(),
            cancel: CancelToken::new(),
        }
    }

    /// The media excerpt where `t` plays from its own `from_s` for `len_s`, as a preview.
    pub fn with_preview_of(mut self, t: &Track, from_s: i64, len_s: u64) -> Self {
        let p = self.plays.iter().find(|p| hints::same_recording(&p.track, t)).expect("track plays").clone();
        let at = (p.start_ms as i64 + (from_s * 1000 - p.offset_ms)).max(0) as usize * SAMPLE_RATE as usize / 1000;
        let len = len_s as usize * SAMPLE_RATE as usize;
        self.previews
            .insert(crate::identity::identity_key(t), self.audio[at..(at + len).min(self.audio.len())].to_vec());
        self
    }

    fn locate(&self, samples: &[i16]) -> Option<u64> {
        let probe = samples.get(..32)?;
        let step = SAMPLE_RATE as usize / 1000;
        (0..self.audio.len().saturating_sub(32))
            .step_by(step)
            .find(|&i| &self.audio[i..i + 32] == probe)
            .map(|i| (i / step) as u64)
    }

    pub fn calls(&self) -> u32 {
        self.calls.load(Ordering::Relaxed)
    }
}

impl Services for FakeServices {
    async fn shazam(&self, samples: &[i16], _priority: Priority) -> Result<Option<RecognitionHit>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let Some(at) = self.locate(samples) else { return Ok(None) };
        self.asked.lock().push(at);
        let len = samples.len() as u64 * 1000 / SAMPLE_RATE as u64;
        let mid = at + len / 2;
        if let Some((_, _, answer)) = self.overrides.iter().find(|(lo, hi, _)| *lo <= at && at < *hi) {
            return Ok(answer.clone().map(|track| hit(track, 30_000, 0.0)));
        }
        let Some(p) = self.plays.iter().find(|p| p.start_ms <= mid && mid < p.end_ms) else { return Ok(None) };
        let offset = p.offset_ms + ((at as i64 - p.start_ms as i64) as f32 * p.speed) as i64;
        let mut t = p.track.clone();
        t.duration_ms = None;
        Ok(Some(hit(t, offset, p.speed - 1.0)))
    }

    async fn paid(&self, _samples: &[i16]) -> Result<Option<RecognitionHit>> {
        Ok(None)
    }

    fn has_paid(&self) -> bool {
        false
    }

    async fn find(&self, title: &str, artist: &str, _duration_ms: Option<u64>) -> Option<Track> {
        let probe = Track { title: title.into(), artist: artist.into(), ..Track::default() };
        self.catalog.iter().find(|t| hints::same_song(t, &probe) && hints::same_version(t, &probe)).cloned()
    }

    async fn enrich(&self, track: Track) -> Track {
        match self.catalog.iter().find(|t| hints::same_recording(t, &track)) {
            Some(known) => {
                let mut out = track;
                out.duration_ms = out.duration_ms.or(known.duration_ms);
                out
            }
            None => track,
        }
    }

    async fn preview(&self, track: &Track) -> Option<Vec<i16>> {
        self.previews.get(&crate::identity::identity_key(track)).cloned()
    }

    fn cancel(&self) -> &CancelToken {
        &self.cancel
    }
}

fn hit(track: Track, offset_ms: i64, skew: f32) -> RecognitionHit {
    RecognitionHit {
        track,
        source: EvidenceSource::Shazam,
        score: if skew.abs() <= 0.002 { 0.88 } else { 0.72 },
        offset_ms: Some(offset_ms),
        timeskew: skew,
        frequency_skew: skew,
    }
}

/// Media written to a temporary PCM file (removed on drop).
pub struct TempPcm {
    pub pcm: PcmFile,
    dir: PathBuf,
}

impl TempPcm {
    pub fn new(samples: &[i16]) -> Self {
        let dir = std::env::temp_dir().join(format!("mf-test-{}", crate::util::new_id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let mut writer = PcmWriter::create(&dir.join("audio.pcm")).expect("pcm");
        writer.push(samples).expect("write");
        Self { pcm: writer.finish().expect("finish"), dir }
    }
}

impl Drop for TempPcm {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Collects events.
#[derive(Default)]
pub struct Events(pub Mutex<Vec<SessionEvent>>);

impl SessionObserver for Events {
    fn on_event(&self, event: SessionEvent) {
        self.0.lock().push(event);
    }
}

/// An emitter on an in-memory store that records its events.
pub fn emitter() -> (Emitter, Arc<Events>) {
    let store = Arc::new(Store::open_in_memory().expect("store"));
    store.create_session("s", SessionKind::Link, "test", None, 0).expect("session");
    let events = Arc::new(Events::default());
    let emitter = Emitter::new("s", events.clone(), store);
    emitter.started(SessionKind::Link);
    (emitter, events)
}
