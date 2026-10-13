//! Delivers session events to the app and persists them.
//!
//! Guarantees: `Started` first, exactly one terminal event, nothing after it;
//! after cancellation only the `Canceled` terminal is delivered; `Level`
//! throttled to 30/s and `Pitch` batched to 10/s; every track gets its identity
//! key; store failures are logged, never fatal.
//!
//! Events from several threads (a live session's DSP thread and its network
//! tasks) are queued under the state lock and delivered by whichever thread
//! finds the queue idle, so delivery is in order per session and no lock is
//! held while the observer runs.

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
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
use crate::util::cancel::CancelToken;

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
    /// False for sessions without a library row (melody index downloads).
    persist: bool,
    /// Once canceled, only the `Canceled` terminal is delivered.
    cancel: Option<CancelToken>,
    /// Kind announced by an implicit `Started` when the session ends before it started.
    kind: Option<SessionKind>,
    state: Mutex<State>,
}

struct State {
    started: bool,
    terminal: bool,
    last_level: Option<Instant>,
    pitch: Vec<PitchFrame>,
    last_pitch: Option<Instant>,
    queue: VecDeque<SessionEvent>,
    delivering: bool,
}

impl Emitter {
    pub fn new(session_id: &str, observer: Arc<dyn SessionObserver>, store: Arc<Store>) -> Self {
        Self::build(session_id, observer, store, true, None, None)
    }

    /// Emitter for a library session: events after `cancel` fires are dropped and
    /// a terminal event turns into `Canceled`; a session that ends before
    /// `started` still announces `Started { kind }` first.
    pub fn for_session(
        session_id: &str,
        kind: SessionKind,
        observer: Arc<dyn SessionObserver>,
        store: Arc<Store>,
        cancel: CancelToken,
    ) -> Self {
        Self::build(session_id, observer, store, true, Some(cancel), Some(kind))
    }

    /// Like [`for_session`](Self::for_session) but nothing is written to the store
    /// (sessions that are not library entries, such as melody index downloads).
    pub fn ephemeral(
        session_id: &str,
        kind: SessionKind,
        observer: Arc<dyn SessionObserver>,
        store: Arc<Store>,
        cancel: CancelToken,
    ) -> Self {
        Self::build(session_id, observer, store, false, Some(cancel), Some(kind))
    }

    fn build(
        session_id: &str,
        observer: Arc<dyn SessionObserver>,
        store: Arc<Store>,
        persist: bool,
        cancel: Option<CancelToken>,
        kind: Option<SessionKind>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                session_id: session_id.to_string(),
                observer,
                store,
                persist,
                cancel,
                kind,
                state: Mutex::new(State {
                    started: false,
                    terminal: false,
                    last_level: None,
                    pitch: Vec::new(),
                    last_pitch: None,
                    queue: VecDeque::new(),
                    delivering: false,
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

    fn canceled_flag(&self) -> bool {
        self.inner.cancel.as_ref().is_some_and(CancelToken::is_canceled)
    }

    /// Whether non-terminal events are currently accepted.
    fn open(&self, state: &State) -> bool {
        state.started && !state.terminal && !self.canceled_flag()
    }

    /// Delivers queued events unless another thread is already doing it. Called
    /// without the state lock held.
    fn drain(&self) {
        {
            let mut state = self.inner.state.lock();
            if state.delivering || state.queue.is_empty() {
                return;
            }
            state.delivering = true;
        }
        loop {
            let event = {
                let mut state = self.inner.state.lock();
                match state.queue.pop_front() {
                    Some(event) => event,
                    None => {
                        state.delivering = false;
                        return;
                    }
                }
            };
            let observer = &self.inner.observer;
            if catch_unwind(AssertUnwindSafe(|| observer.on_event(event))).is_err() {
                tracing::error!(session = %self.inner.session_id, "session observer panicked");
            }
        }
    }

    /// Queue `event` (and run `persist` first) if the session accepts events.
    fn emit_with(&self, event: SessionEvent, persist: impl FnOnce(&Self)) {
        {
            let state = self.inner.state.lock();
            if !self.open(&state) {
                return;
            }
            persist(self);
            let mut state = state;
            state.queue.push_back(event);
        }
        self.drain();
    }

    fn emit(&self, event: SessionEvent) {
        self.emit_with(event, |_| {});
    }

    fn persist(&self, what: &str, write: impl FnOnce(&Store) -> crate::error::Result<()>) {
        if !self.inner.persist {
            return;
        }
        if let Err(e) = write(&self.inner.store) {
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
            state.queue.push_back(SessionEvent::Started { session_id: self.inner.session_id.clone(), kind });
        }
        self.drain();
    }

    pub fn phase(&self, phase: Phase, fraction: Option<f32>) {
        self.emit(SessionEvent::Phase { phase, fraction: fraction.map(|f| f.clamp(0.0, 1.0)) });
    }

    pub fn progress(&self, analyzed_ms: u64, total_ms: u64, eta_ms: Option<u64>) {
        self.emit(SessionEvent::Progress { analyzed_ms: analyzed_ms.min(total_ms), total_ms, eta_ms });
    }

    /// Throttled to ~30 per second; extra calls are dropped.
    pub fn level(&self, rms: f32, peak: f32) {
        {
            let mut state = self.inner.state.lock();
            if !self.open(&state) {
                return;
            }
            let now = Instant::now();
            if state.last_level.is_some_and(|t| now.duration_since(t) < LEVEL_INTERVAL) {
                return;
            }
            state.last_level = Some(now);
            let level = |x: f32| if x.is_finite() { x.clamp(0.0, 1.0) } else { 0.0 };
            state.queue.push_back(SessionEvent::Level { rms: level(rms), peak: level(peak) });
        }
        self.drain();
    }

    /// Batched to ~10 per second.
    pub fn pitch(&self, frames: &[PitchFrame]) {
        {
            let mut state = self.inner.state.lock();
            if !self.open(&state) {
                return;
            }
            state.pitch.extend_from_slice(frames);
            let now = Instant::now();
            let due = state.last_pitch.is_none_or(|t| now.duration_since(t) >= PITCH_INTERVAL);
            if !due || state.pitch.is_empty() {
                return;
            }
            state.last_pitch = Some(now);
            let batch = std::mem::take(&mut state.pitch);
            state.queue.push_back(SessionEvent::Pitch { frames: batch });
        }
        self.drain();
    }

    /// Send any buffered pitch frames now.
    pub fn flush_pitch(&self) {
        {
            let mut state = self.inner.state.lock();
            if !self.open(&state) || state.pitch.is_empty() {
                return;
            }
            let batch = std::mem::take(&mut state.pitch);
            state.queue.push_back(SessionEvent::Pitch { frames: batch });
        }
        self.drain();
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
        let id = self.inner.session_id.clone();
        self.emit_with(SessionEvent::Source { info }, move |me| {
            me.persist("source", |store| store.update_session(&id, update))
        });
    }

    pub fn item(&self, index: u32, count: u32, title: Option<String>) {
        self.emit(SessionEvent::Item { index, count, title });
    }

    /// Insert or replace a segment (by id).
    pub fn segment(&self, segment: Segment) {
        let mut segment = segment;
        if let Some(track) = segment.track.as_mut() {
            fill_identity(track);
        }
        segment.alternates.iter_mut().for_each(fill_identity);
        let stored = segment.clone();
        self.emit_with(SessionEvent::Segment { segment }, |me| {
            me.persist("segment", |store| store.upsert_segment(&me.inner.session_id, &stored))
        });
    }

    pub fn segment_removed(&self, id: &str) {
        self.emit_with(SessionEvent::SegmentRemoved { id: id.to_string() }, |me| {
            me.persist("remove segment", |store| store.remove_segment(&me.inner.session_id, id))
        });
    }

    pub fn candidates(&self, candidates: Vec<Candidate>, is_final: bool) {
        let mut candidates = candidates;
        candidates.iter_mut().for_each(|c| fill_identity(&mut c.track));
        let stored = candidates.clone();
        self.emit_with(SessionEvent::Candidates { candidates, is_final }, |me| {
            me.persist("candidates", |store| store.set_candidates(&me.inner.session_id, &stored))
        });
    }

    pub fn notice(&self, notice: Notice) {
        self.emit(SessionEvent::Notice { notice });
    }

    /// Terminal: the session finished normally. `update` carries extra fields to
    /// persist (title, primary track, kind, transcript). Becomes `Canceled` when
    /// the session was canceled meanwhile.
    pub fn finished(&self, outcome: Outcome, update: SessionUpdate) {
        if self.canceled_flag() {
            self.canceled();
            return;
        }
        let mut update = update;
        let outcome = match outcome {
            Outcome::Matched { mut track } => {
                fill_identity(&mut track);
                update.artwork_url = update.artwork_url.or_else(|| track.artwork_url.clone());
                update.track = Some(track.clone());
                Outcome::Matched { track }
            }
            other => other,
        };
        update.status = Some(match &outcome {
            Outcome::Matched { .. } => SessionStatus::Matched,
            Outcome::Candidates { .. } => SessionStatus::Candidates,
            Outcome::Completed { .. } => SessionStatus::Completed,
            Outcome::NoMatch { .. } => SessionStatus::NoMatch,
        });
        if let Some(track) = update.track.as_mut() {
            fill_identity(track);
        }
        self.terminal(SessionEvent::Finished { outcome }, update, true);
    }

    /// Terminal: the session failed (`MfError::Canceled` means canceled).
    pub fn failed(&self, error: &MfError) {
        if matches!(error, MfError::Canceled) || self.canceled_flag() {
            self.canceled();
            return;
        }
        let update =
            SessionUpdate { status: Some(SessionStatus::Failed), error: Some(error.to_string()), ..Default::default() };
        self.terminal(SessionEvent::Failed { code: error.code(), message: error.to_string() }, update, true);
    }

    /// Terminal: the app canceled the session.
    pub fn canceled(&self) {
        let update = SessionUpdate { status: Some(SessionStatus::Canceled), ..Default::default() };
        self.terminal(SessionEvent::Canceled, update, false);
    }

    /// Claims the terminal slot, persists `update`, queues the event and delivers.
    fn terminal(&self, event: SessionEvent, update: SessionUpdate, flush_pitch: bool) {
        {
            let mut state = self.inner.state.lock();
            if state.terminal {
                return;
            }
            if !state.started {
                let Some(kind) = self.inner.kind else { return };
                state.started = true;
                state.queue.push_back(SessionEvent::Started { session_id: self.inner.session_id.clone(), kind });
            }
            if flush_pitch && !state.pitch.is_empty() {
                let batch = std::mem::take(&mut state.pitch);
                state.queue.push_back(SessionEvent::Pitch { frames: batch });
            }
            state.terminal = true;
            self.persist("finish", |store| store.update_session(&self.inner.session_id, update));
            state.queue.push_back(event);
        }
        self.drain();
    }
}

fn fill_identity(track: &mut Track) {
    if track.identity_key.is_empty() {
        track.identity_key = identity::identity_key(track);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NoMatchReason;
    use crate::util::now_ms;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<SessionEvent>>);

    impl SessionObserver for Recorder {
        fn on_event(&self, event: SessionEvent) {
            self.0.lock().push(event);
        }
    }

    fn setup(kind: SessionKind) -> (Emitter, Arc<Recorder>, Arc<Store>, CancelToken) {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store.create_session("s", kind, "", None, now_ms()).unwrap();
        let recorder = Arc::new(Recorder::default());
        let cancel = CancelToken::new();
        let emitter = Emitter::for_session("s", kind, recorder.clone(), store.clone(), cancel.clone());
        (emitter, recorder, store, cancel)
    }

    fn status(store: &Store) -> SessionStatus {
        store.session_detail("s").unwrap().unwrap().entry.status
    }

    #[test]
    fn started_first_and_one_terminal() {
        let (emitter, recorder, store, _) = setup(SessionKind::Listen);
        emitter.phase(Phase::Listening, None); // before Started: dropped
        emitter.started(SessionKind::Listen);
        emitter.started(SessionKind::Hum);
        emitter.notice(Notice::MoveCloser);
        emitter.finished(Outcome::NoMatch { reason: NoMatchReason::TooQuiet }, SessionUpdate::default());
        emitter.failed(&MfError::internal("late"));
        emitter.canceled();
        emitter.notice(Notice::KeepGoing);
        let events = recorder.0.lock().clone();
        assert_eq!(events.len(), 3, "{events:?}");
        assert!(matches!(events[0], SessionEvent::Started { kind: SessionKind::Listen, .. }));
        assert!(matches!(events[2], SessionEvent::Finished { .. }));
        assert_eq!(status(&store), SessionStatus::NoMatch);
    }

    #[test]
    fn cancel_drops_events_and_wins_over_finish() {
        let (emitter, recorder, store, cancel) = setup(SessionKind::Listen);
        emitter.started(SessionKind::Listen);
        emitter.level(0.5, 0.5);
        cancel.cancel();
        emitter.level(0.5, 0.5);
        emitter.notice(Notice::MoveCloser);
        emitter.finished(Outcome::Matched { track: Track { title: "x".into(), ..Track::default() } }, Default::default());
        emitter.canceled();
        let events = recorder.0.lock().clone();
        assert_eq!(events.len(), 3, "{events:?}");
        assert_eq!(events[2], SessionEvent::Canceled);
        assert_eq!(status(&store), SessionStatus::Canceled);
        assert!(store.session_detail("s").unwrap().unwrap().entry.track.is_none());
    }

    #[test]
    fn terminal_before_start_announces_start() {
        let (emitter, recorder, _, _) = setup(SessionKind::Hum);
        emitter.canceled();
        let events = recorder.0.lock().clone();
        assert!(matches!(events[0], SessionEvent::Started { kind: SessionKind::Hum, .. }));
        assert_eq!(events[1], SessionEvent::Canceled);
    }

    #[test]
    fn pitch_is_flushed_before_the_terminal_event() {
        let (emitter, recorder, _, _) = setup(SessionKind::Hum);
        emitter.started(SessionKind::Hum);
        let frame = PitchFrame { t_ms: 0, midi: 60.0, confidence: 1.0 };
        emitter.pitch(&[frame]);
        emitter.pitch(&[frame]); // batched
        emitter.finished(Outcome::Candidates { count: 0 }, SessionUpdate::default());
        let events = recorder.0.lock().clone();
        let pitches: usize =
            events.iter().map(|e| if let SessionEvent::Pitch { frames } = e { frames.len() } else { 0 }).sum();
        assert_eq!(pitches, 2);
        assert!(matches!(events.last(), Some(SessionEvent::Finished { .. })));
    }

    /// An observer that emits from inside its callback must not deadlock and its
    /// event must follow the current one.
    #[test]
    fn reentrant_observer() {
        struct Echo(Mutex<Option<Emitter>>, Mutex<Vec<SessionEvent>>);
        impl SessionObserver for Echo {
            fn on_event(&self, event: SessionEvent) {
                let first = matches!(event, SessionEvent::Started { .. });
                self.1.lock().push(event);
                if first && let Some(emitter) = self.0.lock().clone() {
                    emitter.notice(Notice::KeepGoing);
                }
            }
        }
        let store = Arc::new(Store::open_in_memory().unwrap());
        store.create_session("s", SessionKind::Hum, "", None, now_ms()).unwrap();
        let echo = Arc::new(Echo(Mutex::new(None), Mutex::new(Vec::new())));
        let emitter = Emitter::for_session("s", SessionKind::Hum, echo.clone(), store, CancelToken::new());
        *echo.0.lock() = Some(emitter.clone());
        emitter.started(SessionKind::Hum);
        emitter.notice(Notice::MoveCloser);
        let events = echo.1.lock().clone();
        assert_eq!(events.len(), 3);
        assert_eq!(events[1], SessionEvent::Notice { notice: Notice::KeepGoing });
        *echo.0.lock() = None;
    }

    #[test]
    fn ephemeral_sessions_are_not_stored() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let recorder = Arc::new(Recorder::default());
        let emitter = Emitter::ephemeral("x", SessionKind::Hum, recorder.clone(), store.clone(), CancelToken::new());
        emitter.started(SessionKind::Hum);
        emitter.finished(Outcome::Completed { identified: 1, total: 1 }, SessionUpdate::default());
        assert_eq!(recorder.0.lock().len(), 2);
        assert!(store.session_detail("x").unwrap().is_none());
    }
}
