//! Impression analysis (hum, whistle, sing). See docs/PIPELINES.md.
//!
//! Three sources run concurrently on the captured 16 kHz audio: melody
//! matching against the installed index, sung-lyric search (speech-to-text from
//! the app, either pushed while the user sings or asked for afterwards, then
//! `lyrics::search`) and ACRCloud humming. Each answer is fused by identity into
//! ranked candidates; interim lists go out as they arrive, the final list after
//! a short enrichment pass.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::join_all;
use futures_util::stream::FuturesUnordered;
use tokio::sync::watch;

use crate::audio::SAMPLE_RATE;
use crate::audio::resample::f32_to_i16;
use crate::audio::wav::{encode_wav, write_wav};
use crate::error::{MfError, Result};
use crate::identity::{self, normalize_artists, normalize_title};
use crate::lyrics::{self, LyricMatch};
use crate::melody::encoder::{EncoderBudget, EncoderHit, EncoderIndex};
use crate::melody::pitch::FrameDetail;
use crate::melody::{MelodyIndex, MelodyMatch, MelodyQuery, SongMeta, matcher, notes};
use crate::model::{
    Candidate, CandidateSource, CandidateSourceKind, CandidateTier, MelodyAlignment, NoMatchReason, Notice, Outcome,
    Phase, SpeechTranscript, Track,
};
use crate::pipeline::ctx::{EngineCtx, SessionCtx};
use crate::platform::SpeechToText;
use crate::providers::ProviderMatch;
use crate::providers::acrcloud::AcrCloud;
use crate::store::SessionUpdate;
use crate::util::cancel::{CancelToken, TaskHandle};

/// Melody matches requested from the index.
const MELODY_TOP_K: usize = 10;
const TRANSCRIBE_TIMEOUT: Duration = Duration::from_secs(15);
const LYRICS_TIMEOUT: Duration = Duration::from_secs(12);
const ACR_TIMEOUT: Duration = Duration::from_secs(10);
/// Newest audio sent to ACRCloud humming.
const ACR_CLIP_MS: usize = 12_000;
const ACR_RATE: u32 = 8_000;
/// Enrichment budget for the final candidates.
const ENRICH_BUDGET: Duration = Duration::from_secs(4);
/// A transcript needs this many distinct real words (not "la la", "hmm") to be searched.
const MIN_REAL_WORDS: usize = 3;
/// Longest transcript hypothesis searched.
const MAX_TRANSCRIPT_CHARS: usize = 500;

const MAX_CANDIDATES: usize = 5;
const MIN_SCORE: f32 = 0.05;
const STRONG: f32 = 0.75;
const POSSIBLE: f32 = 0.35;
const W_MELODY: f32 = 1.0;
const W_LYRICS: f32 = 0.95;
const W_ACR: f32 = 0.9;
/// Bonus when melody and lyrics agree, and the cap of the combined score.
const AGREEMENT_BONUS: f32 = 0.1;
const MAX_COMBINED: f32 = 0.99;

/// Where the sung words of an impression come from.
pub enum SpeechInput {
    /// Transcribe the capture with the app's `SpeechToText`, if it supplied one.
    Recognizer,
    /// The newest transcript the app pushed (`LiveSession::push_transcript`),
    /// waiting up to `wait` for the recognizer's final one.
    Pushed { transcripts: watch::Receiver<Option<SpeechTranscript>>, wait: Duration },
}

/// Where the melody evidence of an impression comes from.
pub enum MelodyInput {
    /// Track pitch on the captured audio.
    Track,
    /// Pitch frames already tracked over the whole capture (live sessions).
    Frames(Vec<FrameDetail>),
    /// A search that already covered every voiced frame of the capture (the
    /// last progressive search of a live session).
    Searched(MelodySearch),
}

/// One melody search: the query and its best matches.
#[derive(Debug, Clone)]
pub struct MelodySearch {
    pub query: MelodyQuery,
    pub matches: Vec<MelodyMatch>,
}

/// What a melody search runs on: the installed index and, when its learned
/// encoder is installed too, the encoder with this device's budget.
#[derive(Clone)]
pub struct MelodySearcher {
    pub index: Arc<MelodyIndex>,
    pub encoder: Option<(Arc<EncoderIndex>, EncoderBudget)>,
}

impl MelodySearcher {
    /// The engine's installed index and encoder (`None` without an index). The
    /// budget follows the device and low-power mode at the time of the call.
    pub fn installed(engine: &EngineCtx) -> Option<Self> {
        let index = engine.melody_index()?;
        let encoder = engine.melody_encoder(&index).map(|e| (e, EncoderBudget::for_device(engine.is_low_power())));
        Some(Self { index, encoder })
    }

    /// The index alone.
    pub fn index_only(index: Arc<MelodyIndex>) -> Self {
        Self { index, encoder: None }
    }

    /// Build a query from pitch frames and search (CPU-bound: run it off the
    /// async workers). `None` when the frames hold too little melody.
    pub fn search_frames(&self, frames: &[FrameDetail]) -> Option<MelodySearch> {
        let query = notes::query_from_details(frames)?;
        let matches = self.search(&query);
        Some(MelodySearch { query, matches })
    }

    /// The best matches for a query: the encoder's hits join the matcher's own
    /// retrieval, and its evidence the score model.
    pub fn search(&self, query: &MelodyQuery) -> Vec<MelodyMatch> {
        let index = &self.index;
        let Some((encoder, budget)) = &self.encoder else {
            return matcher::search(index, query, MELODY_TOP_K);
        };
        match encoder.search(index, query, budget) {
            Ok(hits) => {
                let hints: Vec<_> = hits.iter().map(EncoderHit::hint).collect();
                let options = matcher::MatchOptions::default();
                matcher::search_with_hints(index, query, MELODY_TOP_K, &options, &hints, &budget.hints)
            }
            Err(e) => {
                tracing::warn!("melody encoder failed ({e}); searching without it");
                matcher::search(index, query, MELODY_TOP_K)
            }
        }
    }
}

/// Candidates from melody matches alone (progressive results while humming).
pub fn melody_candidates(matches: &[MelodyMatch]) -> Vec<Candidate> {
    fuse(matches.iter().map(melody_evidence).collect())
}

/// Analyze captured 16 kHz mono audio as an impression: melody matching, sung
/// lyrics and ACRCloud humming, fused into candidates. Emits `Candidates` events
/// and returns the outcome; the caller emits the terminal event.
pub async fn run_impression(ctx: &SessionCtx, audio: &[f32]) -> Result<Outcome> {
    Ok(run_impression_detailed(ctx, audio, MelodyInput::Track, SpeechInput::Recognizer, false, false).await?.0)
}

/// [`run_impression`] that also returns the final candidates (best first).
/// `live`: a live session, which already sent `Notice::MelodyIndexMissing` when
/// the impression started; `shown`: interim candidates were already emitted, so
/// an empty final list must be emitted to clear them.
pub async fn run_impression_detailed(
    ctx: &SessionCtx,
    audio: &[f32],
    melody: MelodyInput,
    speech: SpeechInput,
    live: bool,
    shown: bool,
) -> Result<(Outcome, Vec<Candidate>)> {
    ctx.cancel.check()?;
    keep_recording(ctx, audio).await;
    ctx.emitter.phase(Phase::MatchingMelody, None);
    let index = ctx.engine.melody_index();
    if index.is_none() && !live {
        ctx.emitter.notice(Notice::MelodyIndexMissing);
    }

    type SourceFuture<'a> = Pin<Box<dyn Future<Output = Answer> + Send + 'a>>;
    let mut sources: FuturesUnordered<SourceFuture<'_>> = FuturesUnordered::new();
    let searcher =
        MelodySearcher::installed(&ctx.engine).filter(|s| index.as_ref().is_some_and(|i| Arc::ptr_eq(i, &s.index)));
    sources.push(Box::pin(melody_source(ctx, audio, melody, searcher)));
    match speech {
        SpeechInput::Pushed { transcripts, wait } => {
            sources.push(Box::pin(pushed_lyrics_source(ctx, transcripts, wait)))
        }
        SpeechInput::Recognizer => {
            if let Some(speech) = ctx.engine.speech.clone() {
                sources.push(Box::pin(lyrics_source(ctx, speech, audio)));
            }
        }
    }
    if ctx.credentials.acr_humming.is_some() {
        sources.push(Box::pin(acr_source(ctx, audio)));
    }

    let mut fusion = Fusion::default();
    let mut interim = shown;
    loop {
        let answer = tokio::select! {
            biased;
            _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
            answer = sources.next() => answer,
        };
        let Some(answer) = answer else { break };
        fusion.add(answer);
        if !sources.is_empty() {
            let candidates = fusion.candidates(index.as_ref()).await;
            if !candidates.is_empty() {
                ctx.emitter.candidates(candidates, false);
                interim = true;
            }
        }
    }
    drop(sources);
    let candidates = fusion.candidates(index.as_ref()).await;
    let candidates = enrich(ctx, candidates).await;
    ctx.cancel.check()?;
    if candidates.is_empty() {
        if interim {
            ctx.emitter.candidates(Vec::new(), true);
        }
        return Ok((Outcome::NoMatch { reason: NoMatchReason::NotFound }, candidates));
    }
    ctx.emitter.candidates(candidates.clone(), true);
    Ok((Outcome::Candidates { count: candidates.len() as u32 }, candidates))
}

/// Typed-lyrics search session (`Engine::search_lyrics`): emits every event up
/// to and including the terminal one.
pub async fn run_lyric_search(ctx: &SessionCtx, text: &str) {
    ctx.emitter.phase(Phase::SearchingLyrics, None);
    let hypotheses = vec![text.to_string()];
    let matches = match lyrics::search(&ctx.engine.http, &hypotheses, ctx.engine.country(), &ctx.cancel).await {
        Ok(matches) => matches,
        Err(e) => {
            ctx.emitter.failed(&e);
            return;
        }
    };
    let evidence = matches
        .into_iter()
        .map(|m| Evidence {
            kind: CandidateSourceKind::Lyrics,
            track: m.track,
            song: None,
            score: m.score,
            detail: m.matched_line,
            alignment: None,
        })
        .collect();
    let candidates = enrich(ctx, fuse(evidence)).await;
    if ctx.cancel.is_canceled() {
        ctx.emitter.canceled();
        return;
    }
    if candidates.is_empty() {
        ctx.emitter.finished(Outcome::NoMatch { reason: NoMatchReason::NotFound }, SessionUpdate::default());
        return;
    }
    let top = &candidates[0].track;
    let update = SessionUpdate {
        subtitle: Some(top.artist.clone()),
        artwork_url: top.artwork_url.clone(),
        ..Default::default()
    };
    let count = candidates.len() as u32;
    ctx.emitter.candidates(candidates, true);
    ctx.emitter.finished(Outcome::Candidates { count }, update);
}

/// One source's answer.
enum Answer {
    Melody { query: Option<MelodyQuery>, matches: Vec<MelodyMatch> },
    Lyrics(Vec<LyricMatch>),
    Acr(Vec<ProviderMatch>),
}

/// Keep the capture as `data_dir/recordings/<session>.wav` (best effort).
async fn keep_recording(ctx: &SessionCtx, audio: &[f32]) {
    let path = ctx.engine.recording_path(&ctx.id);
    let samples = f32_to_i16(audio);
    let result = tokio::task::spawn_blocking(move || {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        write_wav(&path, &samples, SAMPLE_RATE)
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!("could not keep the recording: {e}"),
        Err(e) => tracing::warn!("could not keep the recording: {e}"),
    }
}

/// Run CPU-bound or panicking-prone work off the async workers; a panic becomes `None`.
async fn blocking<T: Send + 'static>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    match tokio::task::spawn_blocking(f).await {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::error!("{what} failed: {e}");
            None
        }
    }
}

async fn melody_source(
    ctx: &SessionCtx,
    audio: &[f32],
    melody: MelodyInput,
    searcher: Option<MelodySearcher>,
) -> Answer {
    let empty = || Answer::Melody { query: None, matches: Vec::new() };
    // Without an index the query is only needed to score lyric candidates, which
    // also need the index: nothing to do.
    let Some(searcher) = searcher else { return empty() };
    let frames = match melody {
        MelodyInput::Searched(search) => return Answer::Melody { query: Some(search.query), matches: search.matches },
        MelodyInput::Frames(frames) => frames,
        MelodyInput::Track => {
            let engine = ctx.engine.clone();
            let audio = audio.to_vec();
            let analyzed = blocking("pitch tracking", move || {
                engine.pitch_tracker().and_then(|tracker| tracker.analyze_detailed(&audio))
            })
            .await;
            match analyzed {
                Some(Ok(analysis)) => analysis.frames,
                Some(Err(e)) => {
                    tracing::warn!("pitch tracking unavailable: {e}");
                    return empty();
                }
                None => return empty(),
            }
        }
    };
    match blocking("melody search", move || searcher.search_frames(&frames)).await {
        Some(Some(search)) => Answer::Melody { query: Some(search.query), matches: search.matches },
        _ => empty(),
    }
}

async fn lyrics_source(ctx: &SessionCtx, speech: Arc<dyn SpeechToText>, audio: &[f32]) -> Answer {
    let hypotheses = transcribe(ctx, speech, audio).await;
    sung_lyrics(ctx, hypotheses, 1.0).await
}

/// Sung lyrics from the transcripts the app pushed: the final one, or the newest
/// when the final one does not arrive within `wait`.
async fn pushed_lyrics_source(
    ctx: &SessionCtx,
    transcripts: watch::Receiver<Option<SpeechTranscript>>,
    wait: Duration,
) -> Answer {
    let Some(transcript) = final_transcript(ctx, transcripts, wait).await else { return Answer::Lyrics(Vec::new()) };
    let weight = transcript_weight(transcript.confidence);
    sung_lyrics(ctx, transcript_hypotheses(&transcript), weight).await
}

/// Wait up to `wait` for a final transcript; the newest one otherwise.
pub(crate) async fn final_transcript(
    ctx: &SessionCtx,
    mut transcripts: watch::Receiver<Option<SpeechTranscript>>,
    wait: Duration,
) -> Option<SpeechTranscript> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        if transcripts.borrow_and_update().as_ref().is_some_and(|t| t.is_final) {
            break;
        }
        let changed = tokio::select! {
            biased;
            _ = ctx.cancel.canceled() => return None,
            changed = tokio::time::timeout_at(deadline, transcripts.changed()) => changed,
        };
        // Timed out, or the session handle is gone.
        if !matches!(changed, Ok(Ok(()))) {
            break;
        }
    }
    transcripts.borrow().clone()
}

/// The hypotheses of a transcript that a session searches as lyrics: those with
/// enough real words, best first (empty: humming, "la la", too few words).
pub fn sung_hypotheses(t: &SpeechTranscript) -> Vec<String> {
    transcript_hypotheses(t).into_iter().filter(|h| real_words(h) >= MIN_REAL_WORDS).collect()
}

/// A pushed transcript's hypotheses, best first: trimmed, at most five, no repeats.
pub(crate) fn transcript_hypotheses(t: &SpeechTranscript) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for text in std::iter::once(&t.text).chain(&t.alternatives) {
        let text: String = text.trim().chars().take(MAX_TRANSCRIPT_CHARS).collect();
        if !text.is_empty() && !out.iter().any(|o| o.eq_ignore_ascii_case(&text)) {
            out.push(text);
        }
    }
    out.truncate(5);
    out
}

/// How much a transcript's lyric matches count: recognizers that report a low
/// confidence often misheard the words, which then match the wrong song.
pub fn transcript_weight(confidence: Option<f32>) -> f32 {
    match confidence {
        Some(c) if c.is_finite() && c > 0.0 => 0.4 + 0.6 * c.min(1.0),
        _ => 1.0,
    }
}

/// Lyric search for transcription hypotheses with enough real words; match
/// scores are scaled by `weight`.
async fn sung_lyrics(ctx: &SessionCtx, hypotheses: Vec<String>, weight: f32) -> Answer {
    let none = || Answer::Lyrics(Vec::new());
    let hypotheses: Vec<String> = hypotheses.into_iter().filter(|h| real_words(h) >= MIN_REAL_WORDS).collect();
    let Some(best) = hypotheses.first() else { return none() };
    let update = SessionUpdate { transcript: Some(best.clone()), ..Default::default() };
    if let Err(e) = ctx.engine.store.update_session(&ctx.id, update) {
        tracing::warn!("could not store the transcript: {e}");
    }
    ctx.emitter.phase(Phase::SearchingLyrics, None);
    #[cfg(test)]
    if let Some(stub) = tests::LYRIC_STUB.with(|s| s.borrow().clone()) {
        let mut matches = stub(&hypotheses);
        matches.iter_mut().for_each(|m| m.score *= weight);
        return Answer::Lyrics(matches);
    }
    let search = lyrics::search(&ctx.engine.http, &hypotheses, ctx.engine.country(), &ctx.cancel);
    match tokio::time::timeout(LYRICS_TIMEOUT, search).await {
        Ok(Ok(mut matches)) => {
            for m in &mut matches {
                m.score *= weight;
            }
            Answer::Lyrics(matches)
        }
        Ok(Err(MfError::Canceled)) => none(),
        Ok(Err(e)) => {
            ctx.emitter.notice(Notice::ProviderUnavailable { provider: "Lyrics".into(), message: e.to_string() });
            none()
        }
        Err(_) => {
            tracing::warn!("lyric search timed out");
            none()
        }
    }
}

/// Hypotheses from the app's speech recognizer (empty on failure or timeout).
async fn transcribe(ctx: &SessionCtx, speech: Arc<dyn SpeechToText>, audio: &[f32]) -> Vec<String> {
    let Ok(dir) = ctx.ensure_work_dir().cloned() else { return Vec::new() };
    let path = dir.join("impression.wav");
    let samples = f32_to_i16(audio);
    let wav_path = path.clone();
    if !matches!(
        blocking("writing the speech clip", move || write_wav(&wav_path, &samples, SAMPLE_RATE)).await,
        Some(Ok(()))
    ) {
        return Vec::new();
    }
    // A child token stops the recognizer on timeout without canceling the session.
    let child = CancelToken::new();
    let task = TaskHandle::new(child.clone());
    let language = ctx.engine.language().to_string();
    let call_path = path.to_string_lossy().into_owned();
    let call = tokio::task::spawn_blocking(move || speech.transcribe(call_path, language, task));
    let result = tokio::select! {
        biased;
        _ = ctx.cancel.canceled() => {
            child.cancel();
            None
        }
        r = tokio::time::timeout(TRANSCRIBE_TIMEOUT, call) => Some(r),
    };
    let _ = std::fs::remove_file(&path);
    match result {
        None => Vec::new(),
        Some(Err(_)) => {
            child.cancel();
            tracing::warn!("speech recognition timed out");
            Vec::new()
        }
        Some(Ok(Err(e))) => {
            tracing::error!("speech recognition panicked: {e}");
            Vec::new()
        }
        Some(Ok(Ok(Ok(hypotheses)))) => hypotheses.into_iter().take(5).collect(),
        Some(Ok(Ok(Err(crate::error::PlatformError::Canceled)))) => Vec::new(),
        Some(Ok(Ok(Err(e)))) => {
            let message = e.to_string();
            ctx.emitter.notice(Notice::ProviderUnavailable { provider: "Speech recognition".into(), message });
            Vec::new()
        }
    }
}

async fn acr_source(ctx: &SessionCtx, audio: &[f32]) -> Answer {
    let Some(creds) = ctx.credentials.acr_humming.clone() else { return Answer::Acr(Vec::new()) };
    let wav = acr_humming_clip(audio);
    let client = AcrCloud::new(ctx.engine.http.clone(), creds);
    match tokio::time::timeout(ACR_TIMEOUT, client.recognize_humming(wav, &ctx.cancel)).await {
        Ok(Ok(matches)) => Answer::Acr(matches),
        Ok(Err(MfError::Canceled)) => Answer::Acr(Vec::new()),
        Ok(Err(e)) => {
            ctx.emitter.notice(Notice::ProviderUnavailable { provider: "ACRCloud".into(), message: e.to_string() });
            Answer::Acr(Vec::new())
        }
        Err(_) => {
            tracing::warn!("ACRCloud humming timed out");
            Answer::Acr(Vec::new())
        }
    }
}

/// 16 kHz → 8 kHz: windowed-sinc low-pass at 3.6 kHz, then every second sample.
fn downsample_half(input: &[f32]) -> Vec<f32> {
    const TAPS: usize = 31;
    const CUTOFF: f32 = 3600.0 / 16_000.0;
    let mid = (TAPS / 2) as f32;
    let mut kernel = [0f32; TAPS];
    for (i, k) in kernel.iter_mut().enumerate() {
        let x = i as f32 - mid;
        let sinc = if x == 0.0 {
            2.0 * CUTOFF
        } else {
            (2.0 * std::f32::consts::PI * CUTOFF * x).sin() / (std::f32::consts::PI * x)
        };
        let window = 0.42 - 0.5 * (2.0 * std::f32::consts::PI * i as f32 / (TAPS - 1) as f32).cos()
            + 0.08 * (4.0 * std::f32::consts::PI * i as f32 / (TAPS - 1) as f32).cos();
        *k = sinc * window;
    }
    let sum: f32 = kernel.iter().sum();
    kernel.iter_mut().for_each(|k| *k /= sum);
    let half = TAPS / 2;
    (0..input.len())
        .step_by(2)
        .map(|n| {
            kernel
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let j = n as isize + i as isize - half as isize;
                    if j < 0 || j as usize >= input.len() { 0.0 } else { k * input[j as usize] }
                })
                .sum()
        })
        .collect()
}

/// Vocables and hesitations a recognizer writes for humming; not lyrics.
const FILLER_SYLLABLES: &[&str] = &[
    "la", "na", "da", "doo", "du", "dum", "dee", "di", "ba", "pa", "ta", "oh", "ooh", "oo", "ah", "aah", "uh", "um",
    "hm", "mm", "mhm", "ha", "ho", "hey", "ya", "yeah", "wo", "whoa", "woo", "eh", "ay", "o",
];

/// Distinct words that are not humming filler ("la", "lalala", "hmmm", "da-da"):
/// a recognizer that hears a whistle or a beep may write one word over and over
/// ("the the the", "beep beep beep"), which is no lyric either.
pub(crate) fn real_words(text: &str) -> usize {
    let mut words: Vec<String> = text
        .split(|c: char| c.is_whitespace() || c == '-' || c == ',' || c == '.')
        .map(|w| w.chars().filter(|c| c.is_alphanumeric()).flat_map(char::to_lowercase).collect::<String>())
        .filter(|w| !w.is_empty() && !is_filler(w))
        .collect();
    words.sort_unstable();
    words.dedup();
    words.len()
}

fn is_filler(word: &str) -> bool {
    if word.chars().all(|c| c == 'm' || c == 'h') {
        return true;
    }
    // Repeated syllables: "lalala", "nanana", "doodoo".
    let mut rest = word;
    'outer: while !rest.is_empty() {
        for syllable in FILLER_SYLLABLES {
            if let Some(tail) = rest.strip_prefix(syllable) {
                rest = tail;
                continue 'outer;
            }
        }
        // Elongations: "ooooh", "laaa".
        let mut chars = rest.chars();
        let first = chars.next().unwrap_or_default();
        if rest.chars().all(|c| c == first) && "aeiouhm".contains(first) {
            return true;
        }
        return false;
    }
    true
}

/// Evidence for one track from one source.
#[derive(Debug, Clone)]
pub(crate) struct Evidence {
    pub kind: CandidateSourceKind,
    pub track: Track,
    /// Melody-index song id, when known (groups melody and lyric evidence).
    pub song: Option<u32>,
    pub score: f32,
    pub detail: Option<String>,
    pub alignment: Option<MelodyAlignment>,
}

/// Collected answers plus caches for melody support of lyric/ACR candidates.
#[derive(Default)]
struct Fusion {
    query: Option<MelodyQuery>,
    melody: Vec<MelodyMatch>,
    lyrics: Vec<LyricMatch>,
    acr: Vec<ProviderMatch>,
    /// identity key → melody song id (None: not in the index).
    songs: HashMap<String, Option<u32>>,
    /// song id → how the query scores against it.
    support: HashMap<u32, Option<MelodyMatch>>,
}

impl Fusion {
    fn add(&mut self, answer: Answer) {
        match answer {
            Answer::Melody { query, matches } => {
                self.query = query;
                self.melody = matches;
            }
            Answer::Lyrics(matches) => self.lyrics = matches,
            Answer::Acr(matches) => self.acr = matches,
        }
    }

    /// Fuse everything collected so far.
    async fn candidates(&mut self, index: Option<&Arc<MelodyIndex>>) -> Vec<Candidate> {
        let others = self.others();
        if let Some(index) = index {
            let missing = self.missing_songs(&others);
            if !missing.is_empty() {
                let keys: Vec<String> = missing.iter().map(|(k, _, _)| k.clone()).collect();
                let index = index.clone();
                match blocking("melody index lookup", move || find_songs(&index, missing)).await {
                    Some(found) => self.songs.extend(found),
                    None => self.songs.extend(keys.into_iter().map(|k| (k, None))),
                }
            }
            if let Some(query) = self.query.clone() {
                let ids = self.support_ids(&others);
                if !ids.is_empty() {
                    let (index, wanted) = (index.clone(), ids.clone());
                    match blocking("melody scoring", move || score_songs(&index, &query, wanted)).await {
                        Some(scored) => self.support.extend(scored),
                        None => self.support.extend(ids.into_iter().map(|id| (id, None))),
                    }
                }
            }
        }
        self.assemble(others)
    }

    /// [`candidates`](Self::candidates) on the calling thread.
    fn candidates_now(&mut self, index: Option<&MelodyIndex>) -> Vec<Candidate> {
        let others = self.others();
        if let Some(index) = index {
            let missing = self.missing_songs(&others);
            self.songs.extend(find_songs(index, missing));
            if let Some(query) = self.query.clone() {
                let ids = self.support_ids(&others);
                self.support.extend(score_songs(index, &query, ids));
            }
        }
        self.assemble(others)
    }

    /// Lyric and ACR answers as (kind, track, score, detail).
    fn others(&self) -> Vec<Other> {
        self.lyrics
            .iter()
            .map(|m| (CandidateSourceKind::Lyrics, m.track.clone(), m.score, m.matched_line.clone()))
            .chain(self.acr.iter().map(|m| (CandidateSourceKind::AcrHumming, m.track.clone(), m.score, None)))
            .collect()
    }

    /// Evidence from every answer, plus melody support for lyric/ACR candidates
    /// the melody search did not return, fused into candidates.
    fn assemble(&self, others: Vec<Other>) -> Vec<Candidate> {
        let mut evidence: Vec<Evidence> = self.melody.iter().map(melody_evidence).collect();
        for (kind, track, score, detail) in others {
            let song = self.songs.get(&identity::identity_key(&track)).copied().flatten();
            evidence.push(Evidence { kind, track, song, score, detail, alignment: None });
        }
        let found: Vec<u32> = self.melody.iter().map(|m| m.song.id).collect();
        let mut supported: Vec<u32> = evidence.iter().filter_map(|e| e.song).filter(|id| !found.contains(id)).collect();
        supported.sort_unstable();
        supported.dedup();
        for id in supported {
            if let Some(Some(m)) = self.support.get(&id) {
                evidence.push(melody_evidence(m));
            }
        }
        fuse(evidence)
    }

    /// Lyric/ACR tracks not yet looked up in the index: (identity key, title, artist).
    fn missing_songs(&self, others: &[Other]) -> Vec<(String, String, String)> {
        others
            .iter()
            .map(|(_, t, _, _)| (identity::identity_key(t), t.title.clone(), t.artist.clone()))
            .filter(|(key, _, _)| !self.songs.contains_key(key))
            .collect()
    }

    /// Index songs of lyric/ACR candidates the query was not yet scored against.
    fn support_ids(&self, others: &[Other]) -> Vec<u32> {
        let found: Vec<u32> = self.melody.iter().map(|m| m.song.id).collect();
        let mut ids: Vec<u32> = others
            .iter()
            .filter_map(|(_, t, _, _)| self.songs.get(&identity::identity_key(t)).copied().flatten())
            .filter(|id| !found.contains(id) && !self.support.contains_key(id))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }
}

/// A lyric or ACR answer: (kind, track, score, detail).
type Other = (CandidateSourceKind, Track, f32, Option<String>);

/// Index song ids of (identity key, title, artist) lookups.
fn find_songs(index: &MelodyIndex, missing: Vec<(String, String, String)>) -> Vec<(String, Option<u32>)> {
    missing.into_iter().map(|(key, title, artist)| (key, index.find_song(&title, &artist).map(|s| s.id))).collect()
}

/// How the query scores against each song.
fn score_songs(index: &MelodyIndex, query: &MelodyQuery, ids: Vec<u32>) -> Vec<(u32, Option<MelodyMatch>)> {
    ids.into_iter().map(|id| (id, matcher::score_song(index, query, id))).collect()
}

/// Melody, sung-lyric and ACRCloud answers fused as an impression session fuses
/// them, on the calling thread (CPU-bound; for evaluation tools).
pub fn fuse_answers(
    index: Option<&MelodyIndex>,
    query: Option<MelodyQuery>,
    melody: Vec<MelodyMatch>,
    lyrics: Vec<LyricMatch>,
    acr: Vec<ProviderMatch>,
) -> Vec<Candidate> {
    let mut fusion = Fusion { query, melody, lyrics, acr, ..Fusion::default() };
    fusion.candidates_now(index)
}

/// The clip an impression sends to ACRCloud humming: the newest ≤ 12 s of the
/// 16 kHz capture, as an 8 kHz 16-bit WAV.
pub fn acr_humming_clip(audio: &[f32]) -> Vec<u8> {
    let newest = &audio[audio.len().saturating_sub(ACR_CLIP_MS * SAMPLE_RATE as usize / 1000)..];
    encode_wav(&f32_to_i16(&downsample_half(newest)), ACR_RATE)
}

fn song_track(song: &SongMeta) -> Track {
    let mut ids = HashMap::new();
    ids.insert("melody".to_string(), song.id.to_string());
    if let Some(deezer) = song.deezer_id {
        ids.insert("deezer".to_string(), deezer.to_string());
    }
    Track {
        title: song.title.clone(),
        artist: song.artist.clone(),
        isrc: song.isrc.clone(),
        release_year: song.year,
        ids,
        ..Track::default()
    }
}

fn melody_evidence(m: &MelodyMatch) -> Evidence {
    Evidence {
        kind: CandidateSourceKind::Melody,
        track: song_track(&m.song),
        song: Some(m.song.id),
        score: m.score,
        detail: Some(melody_detail(m.key_shift, m.tempo)),
        alignment: Some(m.alignment.clone()),
    }
}

/// "transposed −2, tempo ×0.9"
pub(crate) fn melody_detail(key_shift: f32, tempo: f32) -> String {
    let semitones = key_shift.round() as i32;
    let key = match semitones {
        0 => "same key".to_string(),
        s if s > 0 => format!("transposed +{s}"),
        s => format!("transposed \u{2212}{}", -s),
    };
    if tempo.is_finite() && tempo > 0.0 && (tempo - 1.0).abs() >= 0.05 {
        let tempo = format!("{tempo:.2}");
        let tempo = tempo.trim_end_matches('0').trim_end_matches('.');
        format!("{key}, tempo \u{d7}{tempo}")
    } else {
        key
    }
}

fn weight(kind: CandidateSourceKind) -> f32 {
    match kind {
        CandidateSourceKind::Melody => W_MELODY,
        CandidateSourceKind::Lyrics => W_LYRICS,
        CandidateSourceKind::AcrHumming => W_ACR,
        CandidateSourceKind::Fingerprint => 1.0,
    }
}

/// Same song for impression purposes: the same melody-index song, the same
/// recording, or the same normalized title and artists (impressions cannot tell
/// versions apart, so differing ISRCs do not veto here).
fn related(a: &Evidence, b: &Evidence) -> bool {
    if let (Some(x), Some(y)) = (a.song, b.song) {
        return x == y;
    }
    if identity::same_track(&a.track, &b.track) {
        return true;
    }
    let title = normalize_title(&a.track.title);
    !title.is_empty()
        && title == normalize_title(&b.track.title)
        && normalize_artists(&a.track.artist) == normalize_artists(&b.track.artist)
}

/// Fuse evidence into at most five candidates, best first.
pub(crate) fn fuse(evidence: Vec<Evidence>) -> Vec<Candidate> {
    let mut groups: Vec<Vec<Evidence>> = Vec::new();
    for e in evidence {
        if !e.score.is_finite() || e.score <= 0.0 || e.track.title.trim().is_empty() {
            continue;
        }
        match groups.iter_mut().find(|g| g.iter().any(|other| related(other, &e))) {
            Some(group) => group.push(e),
            None => groups.push(vec![e]),
        }
    }
    let mut candidates: Vec<Candidate> = groups.into_iter().map(candidate_of).collect();
    candidates.retain(|c| c.score >= MIN_SCORE);
    candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
    candidates.truncate(MAX_CANDIDATES);
    candidates
}

fn candidate_of(group: Vec<Evidence>) -> Candidate {
    // Display metadata: catalog tracks (lyrics, ACR) over index metadata.
    let display = group
        .iter()
        .filter(|e| e.kind != CandidateSourceKind::Melody)
        .max_by(|a, b| a.score.total_cmp(&b.score))
        .or_else(|| group.iter().max_by(|a, b| a.score.total_cmp(&b.score)))
        .map(|e| e.track.clone())
        .unwrap_or_default();
    let mut track = display;
    for e in &group {
        for (key, value) in &e.track.ids {
            track.ids.entry(key.clone()).or_insert_with(|| value.clone());
        }
        if track.release_year.is_none() {
            track.release_year = e.track.release_year;
        }
        if let Some(song) = e.song {
            track.ids.entry("melody".into()).or_insert_with(|| song.to_string());
        }
    }
    track.identity_key = identity::identity_key(&track);

    let mut sources: Vec<CandidateSource> = Vec::new();
    let mut alignment: Option<(f32, MelodyAlignment)> = None;
    for e in &group {
        match sources.iter_mut().find(|s| s.kind == e.kind) {
            Some(source) if source.score >= e.score => {}
            Some(source) => {
                source.score = e.score;
                source.detail = e.detail.clone();
            }
            None => sources.push(CandidateSource { kind: e.kind, score: e.score, detail: e.detail.clone() }),
        }
        if let Some(a) = &e.alignment
            && alignment.as_ref().is_none_or(|(s, _)| e.score > *s)
        {
            alignment = Some((e.score, a.clone()));
        }
    }
    sources.sort_by(|a, b| b.score.total_cmp(&a.score));
    let miss: f32 = sources.iter().map(|s| 1.0 - (weight(s.kind) * s.score.clamp(0.0, 1.0))).product();
    let mut score = 1.0 - miss;
    let has = |kind| sources.iter().any(|s| s.kind == kind);
    if has(CandidateSourceKind::Melody) && has(CandidateSourceKind::Lyrics) {
        score += AGREEMENT_BONUS;
    }
    let score = score.clamp(0.0, MAX_COMBINED);
    Candidate { track, score, tier: tier(score), sources, alignment: alignment.map(|(_, a)| a) }
}

pub(crate) fn tier(score: f32) -> CandidateTier {
    if score >= STRONG {
        CandidateTier::Strong
    } else if score >= POSSIBLE {
        CandidateTier::Possible
    } else {
        CandidateTier::LongShot
    }
}

/// Enrich candidates concurrently within the shared budget; drop any that turn
/// out to be the same recording as a better-ranked one.
pub(crate) async fn enrich(ctx: &SessionCtx, candidates: Vec<Candidate>) -> Vec<Candidate> {
    if candidates.is_empty() {
        return candidates;
    }
    let deadline = tokio::time::Instant::now() + ENRICH_BUDGET;
    let tracks = join_all(candidates.iter().map(|c| {
        let track = c.track.clone();
        async move {
            let fallback = track.clone();
            tokio::time::timeout_at(deadline, ctx.engine.enrich(track, &ctx.cancel)).await.unwrap_or(fallback)
        }
    }))
    .await;
    let mut out: Vec<Candidate> = Vec::with_capacity(candidates.len());
    for (mut candidate, mut track) in candidates.into_iter().zip(tracks) {
        if track.title.trim().is_empty() {
            track = candidate.track.clone();
        }
        for (key, value) in &candidate.track.ids {
            track.ids.entry(key.clone()).or_insert_with(|| value.clone());
        }
        identity::refresh_identity(&mut track);
        candidate.track = track;
        if !out.iter().any(|c| identity::same_track(&c.track, &candidate.track)) {
            out.push(candidate);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    type LyricStub = Arc<dyn Fn(&[String]) -> Vec<LyricMatch>>;

    thread_local! {
        /// Lyric search answers for tests (no network).
        pub(super) static LYRIC_STUB: std::cell::RefCell<Option<LyricStub>> = const { std::cell::RefCell::new(None) };
    }

    fn transcript(text: &str, is_final: bool) -> SpeechTranscript {
        SpeechTranscript { text: text.into(), is_final, ..SpeechTranscript::default() }
    }

    #[test]
    fn transcript_hypotheses_and_weights() {
        let t = SpeechTranscript {
            text: "  never gonna give you up ".into(),
            alternatives: vec!["Never gonna give you up".into(), "".into(), "never going to give you up".into()],
            ..SpeechTranscript::default()
        };
        assert_eq!(transcript_hypotheses(&t), vec!["never gonna give you up", "never going to give you up"]);
        assert_eq!(transcript_weight(None), 1.0);
        assert_eq!(transcript_weight(Some(0.0)), 1.0, "0 means unknown (SFSpeech partials, Android without scores)");
        assert_eq!(transcript_weight(Some(-1.0)), 1.0);
        assert!((transcript_weight(Some(0.5)) - 0.7).abs() < 1e-6);
        assert_eq!(transcript_weight(Some(3.0)), 1.0);
    }

    #[tokio::test]
    async fn the_final_transcript_is_awaited_briefly() {
        let dir = std::env::temp_dir().join(format!("mf-transcript-{}", crate::util::new_id()));
        let (ctx, _) = crate::pipeline::testutil::session(&dir, crate::model::SessionKind::Hum);
        let (tx, rx) = watch::channel(None);
        // Nothing pushed: nothing to wait for beyond the limit.
        assert_eq!(final_transcript(&ctx, rx.clone(), Duration::from_millis(20)).await, None);
        // A partial result, then the final one shortly after capture.
        tx.send_replace(Some(transcript("never gonna", false)));
        let later = tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            later.send_replace(Some(transcript("never gonna give you up", true)));
        });
        let got = final_transcript(&ctx, rx.clone(), Duration::from_secs(5)).await;
        assert_eq!(got.map(|t| t.text).as_deref(), Some("never gonna give you up"));
        // No final one in time: the newest partial.
        tx.send_replace(Some(transcript("let it", false)));
        let got = final_transcript(&ctx, rx, Duration::from_millis(20)).await;
        assert_eq!(got.map(|t| t.text).as_deref(), Some("let it"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn pushed_words_find_the_song_by_its_lyrics() {
        let dir = std::env::temp_dir().join(format!("mf-sung-{}", crate::util::new_id()));
        let (ctx, events) = crate::pipeline::testutil::session(&dir, crate::model::SessionKind::Hum);
        let searched = Arc::new(parking_lot::Mutex::new(Vec::<Vec<String>>::new()));
        let seen = searched.clone();
        let stub: LyricStub = Arc::new(move |hypotheses: &[String]| {
            seen.lock().push(hypotheses.to_vec());
            vec![LyricMatch {
                track: track("Never Gonna Give You Up", "Rick Astley"),
                score: 0.9,
                matched_line: Some("Never gonna give you up".into()),
            }]
        });
        LYRIC_STUB.with(|s| *s.borrow_mut() = Some(stub));
        let audio = vec![0.0f32; 16_000];

        // Filler only ("la la"): no lyric search.
        let (tx, rx) = watch::channel(Some(transcript("la la la lalala", true)));
        let speech = SpeechInput::Pushed { transcripts: rx, wait: Duration::from_millis(50) };
        let (outcome, candidates) =
            run_impression_detailed(&ctx, &audio, MelodyInput::Track, speech, true, false).await.unwrap();
        assert_eq!(outcome, Outcome::NoMatch { reason: NoMatchReason::NotFound });
        assert!(candidates.is_empty() && searched.lock().is_empty());

        // Real words with a middling recognizer confidence.
        tx.send_replace(Some(SpeechTranscript {
            text: "never gonna give you up".into(),
            alternatives: vec!["never gonna give you a cup".into()],
            confidence: Some(0.5),
            is_final: true,
            ..SpeechTranscript::default()
        }));
        let speech = SpeechInput::Pushed { transcripts: tx.subscribe(), wait: Duration::from_millis(50) };
        let (outcome, candidates) =
            run_impression_detailed(&ctx, &audio, MelodyInput::Track, speech, true, false).await.unwrap();
        assert_eq!(outcome, Outcome::Candidates { count: 1 });
        assert_eq!(candidates[0].track.title, "Never Gonna Give You Up");
        let lyric = &candidates[0].sources[0];
        assert_eq!(lyric.kind, CandidateSourceKind::Lyrics);
        assert!((lyric.score - 0.9 * 0.7).abs() < 1e-5, "{}", lyric.score);
        assert_eq!(
            *searched.lock(),
            vec![vec!["never gonna give you up".to_string(), "never gonna give you a cup".into()]]
        );
        let detail = ctx.engine.store.session_detail("s").unwrap().unwrap();
        assert_eq!(detail.transcript.as_deref(), Some("never gonna give you up"));
        assert!(
            events
                .0
                .lock()
                .iter()
                .any(|e| matches!(e, crate::model::SessionEvent::Phase { phase: Phase::SearchingLyrics, .. }))
        );
        LYRIC_STUB.with(|s| *s.borrow_mut() = None);
        ctx.cleanup();
        let _ = std::fs::remove_dir_all(dir);
    }

    fn track(title: &str, artist: &str) -> Track {
        Track { title: title.into(), artist: artist.into(), ..Track::default() }
    }

    fn ev(kind: CandidateSourceKind, t: Track, song: Option<u32>, score: f32) -> Evidence {
        Evidence { kind, track: t, song, score, detail: None, alignment: None }
    }

    #[test]
    fn filler_is_not_lyrics() {
        assert_eq!(real_words("la la la lalala na na hmm mmm"), 0);
        assert_eq!(real_words("ooooh da-da-da doo doo"), 0);
        assert_eq!(real_words("take on me take me on"), 3);
        assert_eq!(real_words("la la I'm gonna be"), 3);
        assert_eq!(real_words("BEEP BEEP BEEP BEEP BEEP BEEP"), 1);
        assert_eq!(real_words("the the the the the the"), 1);
        assert!(!is_filler("love"));
        assert!(!is_filler("do"));
    }

    #[test]
    fn combined_score_follows_the_formula() {
        let melody = ev(CandidateSourceKind::Melody, track("Take On Me", "a-ha"), Some(7), 0.6);
        let lyrics = ev(CandidateSourceKind::Lyrics, track("Take On Me", "a-ha"), None, 0.5);
        let candidates = fuse(vec![melody, lyrics]);
        assert_eq!(candidates.len(), 1);
        let expected = 1.0 - (1.0 - 0.6) * (1.0 - 0.95 * 0.5) + 0.1;
        assert!((candidates[0].score - expected).abs() < 1e-5, "{}", candidates[0].score);
        assert_eq!(candidates[0].tier, CandidateTier::Strong);
        assert_eq!(candidates[0].sources.len(), 2);
        assert_eq!(candidates[0].track.ids.get("melody").map(String::as_str), Some("7"));
    }

    #[test]
    fn groups_by_index_song_even_with_different_isrcs() {
        let mut a = track("Song", "Artist");
        a.isrc = Some("USAAA0000001".into());
        let mut b = track("Song (Remastered)", "Artist");
        b.isrc = Some("GBBBB0000002".into());
        let candidates = fuse(vec![
            ev(CandidateSourceKind::Melody, a, Some(3), 0.4),
            ev(CandidateSourceKind::AcrHumming, b, Some(3), 0.5),
        ]);
        assert_eq!(candidates.len(), 1);
        // Display metadata comes from the catalog source.
        assert_eq!(candidates[0].track.title, "Song (Remastered)");
    }

    #[test]
    fn keeps_five_best_above_the_floor() {
        let mut evidence: Vec<Evidence> = (0..8)
            .map(|i| ev(CandidateSourceKind::Melody, track(&format!("Song {i}"), "A"), Some(i), 0.1 * i as f32))
            .collect();
        evidence.push(ev(CandidateSourceKind::Lyrics, track("Weak", "B"), None, 0.04));
        let candidates = fuse(evidence);
        assert_eq!(candidates.len(), 5);
        assert!(candidates.windows(2).all(|w| w[0].score >= w[1].score));
        assert_eq!(candidates[0].track.title, "Song 7");
        assert!(candidates.iter().all(|c| c.score >= MIN_SCORE));
        assert_eq!(tier(0.5), CandidateTier::Possible);
        assert_eq!(tier(0.2), CandidateTier::LongShot);
    }

    #[test]
    fn score_is_capped() {
        let melody = ev(CandidateSourceKind::Melody, track("X", "Y"), Some(1), 1.0);
        let lyrics = ev(CandidateSourceKind::Lyrics, track("X", "Y"), None, 1.0);
        assert_eq!(fuse(vec![melody, lyrics])[0].score, MAX_COMBINED);
    }

    #[test]
    fn melody_details() {
        assert_eq!(melody_detail(-2.2, 0.9), "transposed \u{2212}2, tempo \u{d7}0.9");
        assert_eq!(melody_detail(0.3, 1.01), "same key");
        assert_eq!(melody_detail(3.0, 1.25), "transposed +3, tempo \u{d7}1.25");
    }

    #[test]
    fn downsampling_halves_and_filters() {
        let tone = |f: f32| {
            (0..16_000).map(|n| (2.0 * std::f32::consts::PI * f * n as f32 / 16_000.0).sin()).collect::<Vec<_>>()
        };
        let rms =
            |x: &[f32]| (x[100..x.len() - 100].iter().map(|v| v * v).sum::<f32>() / (x.len() - 200) as f32).sqrt();
        let low = downsample_half(&tone(440.0));
        assert_eq!(low.len(), 8000);
        assert!((rms(&low) - 0.707).abs() < 0.05);
        assert!(rms(&downsample_half(&tone(6000.0))) < 0.05, "aliasing band is suppressed");
    }
}
