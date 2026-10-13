//! Delivers session events to the app and persists them.
//!
//! Guarantees: `Started` first, exactly one terminal event, nothing after it;
//! `Level` throttled to 30/s and `Pitch` batched to 10/s; every track gets its
//! identity key; store failures are logged, never fatal.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::error::MfError;
use crate::identity;
use crate::model::{
    Candidate, Notice, Outcome, Phase, PitchFrame, Segment, SessionEvent, SessionKind, SessionStatus, SourceInfo, Track,
};
use crate::platform::SessionObserver;
use crate::store::{SessionUpdate, Store};

const LEVEL_INTERVAL: Duration = Duration::from_millis(33);
const PITCH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct Emitter {
    inner: Arc<Inner>,
}

struct Inner {
    session_id: String,
    observer: Arc<dyn SessionObserver>,
    store: Arc<Store>,
    state: Mutex<State>,
}

struct State {
    started: bool,
    terminal: bool,
    last_level: Option<Instant>,
    pitch: Vec<PitchFrame>,
    last_pitch: Option<Instant>,
}

impl Emitter {
    pub fn new(session_id: &str, observer: Arc<dyn SessionObserver>, store: Arc<Store>) -> Self {
        Self {
            inner: Arc::new(Inner {
                session_id: session_id.to_string(),
                observer,
                store,
                state: Mutex::new(State {
                    started: false,
                    terminal: false,
                    last_level: None,
                    pitch: Vec::new(),
                    last_pitch: None,
                }),
            }),
        }
    }

    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.inner.store
    }

    pub fn is_terminal(&self) -> bool {
        self.inner.state.lock().terminal
    }

    /// Send an event unless the session already ended. Never called with the lock held.
    fn send(&self, event: SessionEvent) {
        {
            let state = self.inner.state.lock();
            if state.terminal || !state.started {
                return;
            }
        }
        self.inner.observer.on_event(event);
    }

    fn persist(&self, what: &str, result: crate::error::Result<()>) {
        if let Err(e) = result {
            tracing::warn!(session = %self.inner.session_id, "store {what} failed: {e}");
        }
    }

    pub fn started(&self, kind: SessionKind) {
        {
            let mut state = self.inner.state.lock();
            if state.started {
                return;
            }
            state.started = true;
        }
        self.inner.observer.on_event(SessionEvent::Started { session_id: self.inner.session_id.clone(), kind });
    }

    pub fn phase(&self, phase: Phase, fraction: Option<f32>) {
        self.send(SessionEvent::Phase { phase, fraction: fraction.map(|f| f.clamp(0.0, 1.0)) });
    }

    pub fn progress(&self, analyzed_ms: u64, total_ms: u64, eta_ms: Option<u64>) {
        self.send(SessionEvent::Progress { analyzed_ms: analyzed_ms.min(total_ms), total_ms, eta_ms });
    }

    /// Throttled to ~30 per second; extra calls are dropped.
    pub fn level(&self, rms: f32, peak: f32) {
        {
            let mut state = self.inner.state.lock();
            let now = Instant::now();
            if state.last_level.is_some_and(|t| now.duration_since(t) < LEVEL_INTERVAL) {
                return;
            }
            state.last_level = Some(now);
        }
        self.send(SessionEvent::Level { rms: rms.clamp(0.0, 1.0), peak: peak.clamp(0.0, 1.0) });
    }

    /// Batched to ~10 per second.
    pub fn pitch(&self, frames: &[PitchFrame]) {
        let batch = {
            let mut state = self.inner.state.lock();
            state.pitch.extend_from_slice(frames);
            let now = Instant::now();
            let due = state.last_pitch.is_none_or(|t| now.duration_since(t) >= PITCH_INTERVAL);
            if !due || state.pitch.is_empty() {
                return;
            }
            state.last_pitch = Some(now);
            std::mem::take(&mut state.pitch)
        };
        self.send(SessionEvent::Pitch { frames: batch });
    }

    /// Send any buffered pitch frames now.
    pub fn flush_pitch(&self) {
        let batch = std::mem::take(&mut self.inner.state.lock().pitch);
        if !batch.is_empty() {
            self.send(SessionEvent::Pitch { frames: batch });
        }
    }

    pub fn source(&self, info: SourceInfo) {
        let mut info = info;
        info.hints.iter_mut().for_each(fill_identity);
        let update = SessionUpdate {
            title: info.title.clone(),
            subtitle: info.uploader.clone().or_else(|| info.site.clone()),
            artwork_url: info.thumbnail_url.clone(),
            duration_ms: info.duration_ms,
            source: Some(info.clone()),
            ..Default::default()
        };
        self.persist("source", self.inner.store.update_session(&self.inner.session_id, update));
        self.send(SessionEvent::Source { info });
    }

    pub fn item(&self, index: u32, count: u32, title: Option<String>) {
        self.send(SessionEvent::Item { index, count, title });
    }

    /// Insert or replace a segment (by id).
    pub fn segment(&self, segment: Segment) {
        let mut segment = segment;
        if let Some(track) = segment.track.as_mut() {
            fill_identity(track);
        }
        segment.alternates.iter_mut().for_each(fill_identity);
        self.persist("segment", self.inner.store.upsert_segment(&self.inner.session_id, &segment));
        self.send(SessionEvent::Segment { segment });
    }

    pub fn segment_removed(&self, id: &str) {
        self.persist("remove segment", self.inner.store.remove_segment(&self.inner.session_id, id));
        self.send(SessionEvent::SegmentRemoved { id: id.to_string() });
    }

    pub fn candidates(&self, candidates: Vec<Candidate>, is_final: bool) {
        let mut candidates = candidates;
        candidates.iter_mut().for_each(|c| fill_identity(&mut c.track));
        self.persist("candidates", self.inner.store.set_candidates(&self.inner.session_id, &candidates));
        self.send(SessionEvent::Candidates { candidates, is_final });
    }

    pub fn notice(&self, notice: Notice) {
        self.send(SessionEvent::Notice { notice });
    }

    /// Terminal: the session finished normally. `update` carries extra fields to
    /// persist (title, primary track, kind, transcript).
    pub fn finished(&self, outcome: Outcome, update: SessionUpdate) {
        self.flush_pitch();
        let mut update = update;
        let status = match &outcome {
            Outcome::Matched { track } => {
                let mut track = track.clone();
                fill_identity(&mut track);
                update.artwork_url = update.artwork_url.or_else(|| track.artwork_url.clone());
                update.track = Some(track);
                SessionStatus::Matched
            }
            Outcome::Candidates { .. } => SessionStatus::Candidates,
            Outcome::Completed { .. } => SessionStatus::Completed,
            Outcome::NoMatch { .. } => SessionStatus::NoMatch,
        };
        update.status = Some(status);
        if let Some(track) = update.track.as_mut() {
            fill_identity(track);
        }
        self.persist("finish", self.inner.store.update_session(&self.inner.session_id, update));
        let outcome = match outcome {
            Outcome::Matched { mut track } => {
                fill_identity(&mut track);
                Outcome::Matched { track }
            }
            other => other,
        };
        self.terminal(SessionEvent::Finished { outcome });
    }

    /// Terminal: the session failed.
    pub fn failed(&self, error: &MfError) {
        if matches!(error, MfError::Canceled) {
            self.canceled();
            return;
        }
        self.flush_pitch();
        let update =
            SessionUpdate { status: Some(SessionStatus::Failed), error: Some(error.to_string()), ..Default::default() };
        self.persist("fail", self.inner.store.update_session(&self.inner.session_id, update));
        self.terminal(SessionEvent::Failed { code: error.code(), message: error.to_string() });
    }

    /// Terminal: the app canceled the session.
    pub fn canceled(&self) {
        let update = SessionUpdate { status: Some(SessionStatus::Canceled), ..Default::default() };
        self.persist("cancel", self.inner.store.update_session(&self.inner.session_id, update));
        self.terminal(SessionEvent::Canceled);
    }

    fn terminal(&self, event: SessionEvent) {
        {
            let mut state = self.inner.state.lock();
            if state.terminal || !state.started {
                return;
            }
            state.terminal = true;
        }
        self.inner.observer.on_event(event);
    }
}

fn fill_identity(track: &mut Track) {
    if track.identity_key.is_empty() {
        track.identity_key = identity::identity_key(track);
    }
}
