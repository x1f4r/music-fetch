//! Impression analysis (hum, whistle, sing). See docs/PIPELINES.md.
//!
//! Three sources run concurrently on the captured 16 kHz audio: melody
//! matching against the installed index, sung-lyric search (speech-to-text from
//! the app, then `lyrics::search`) and ACRCloud humming. Each answer is fused by
//! identity into ranked candidates; interim lists go out as they arrive, the
//! final list after a short enrichment pass.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::join_all;
use futures_util::stream::FuturesUnordered;

use crate::audio::SAMPLE_RATE;
use crate::audio::resample::f32_to_i16;
use crate::audio::wav::{encode_wav, write_wav};
use crate::error::{MfError, Result};
use crate::identity::{self, normalize_artists, normalize_title};
use crate::lyrics::{self, LyricMatch};
use crate::melody::{MelodyIndex, MelodyMatch, MelodyQuery, SongMeta, matcher, notes};
use crate::model::{
    Candidate, CandidateSource, CandidateSourceKind, CandidateTier, MelodyAlignment, NoMatchReason, Notice, Outcome,
    Phase, PitchFrame, Track,
};
use crate::pipeline::ctx::SessionCtx;
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
/// A transcript needs this many real words (not "la la", "hmm") to be searched.
const MIN_REAL_WORDS: usize = 3;

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

/// Analyze captured 16 kHz mono audio as an impression: melody matching, sung
/// lyrics and ACRCloud humming, fused into candidates. Emits `Candidates` events
/// and returns the outcome; the caller emits the terminal event. `frames` are the
/// live pitch frames when already computed.
pub async fn run_impression(ctx: &SessionCtx, audio: &[f32], frames: Option<Vec<PitchFrame>>) -> Result<Outcome> {
    Ok(run_impression_detailed(ctx, audio, frames).await?.0)
}

/// [`run_impression`] that also returns the final candidates (best first).
pub async fn run_impression_detailed(
    ctx: &SessionCtx,
    audio: &[f32],
    frames: Option<Vec<PitchFrame>>,
) -> Result<(Outcome, Vec<Candidate>)> {
    ctx.cancel.check()?;
    keep_recording(ctx, audio).await;
    ctx.emitter.phase(Phase::MatchingMelody, None);
    let index = ctx.engine.melody_index();
    if index.is_none() {
        ctx.emitter.notice(Notice::MelodyIndexMissing);
    }

    type SourceFuture<'a> = Pin<Box<dyn Future<Output = Answer> + Send + 'a>>;
    let mut sources: FuturesUnordered<SourceFuture<'_>> = FuturesUnordered::new();
    sources.push(Box::pin(melody_source(ctx, audio, frames, index.clone())));
    if let Some(speech) = ctx.engine.speech.clone() {
        sources.push(Box::pin(lyrics_source(ctx, speech, audio)));
    }
    if ctx.credentials.acr_humming.is_some() {
        sources.push(Box::pin(acr_source(ctx, audio)));
    }

    let mut fusion = Fusion::default();
    let mut interim = false;
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
    frames: Option<Vec<PitchFrame>>,
    index: Option<Arc<MelodyIndex>>,
) -> Answer {
    let empty = || Answer::Melody { query: None, matches: Vec::new() };
    // Without an index the query is only needed to score lyric candidates, which
    // also need the index: nothing to do.
    let Some(index) = index else { return empty() };
    let frames = match frames {
        Some(frames) => frames,
        None => {
            let engine = ctx.engine.clone();
            let audio = audio.to_vec();
            let analyzed =
                blocking("pitch tracking", move || engine.pitch_tracker().map(|tracker| tracker.analyze(&audio))).await;
            match analyzed {
                Some(Ok(frames)) => frames,
                Some(Err(e)) => {
                    tracing::warn!("pitch tracker unavailable: {e}");
                    return empty();
                }
                None => return empty(),
            }
        }
    };
    let Some(Some(query)) = blocking("melody query", move || notes::query_from_frames(&frames)).await else {
        return empty();
    };
    let search_query = query.clone();
    let matches = blocking("melody search", move || matcher::search(&index, &search_query, MELODY_TOP_K))
        .await
        .unwrap_or_default();
    Answer::Melody { query: Some(query), matches }
}

async fn lyrics_source(ctx: &SessionCtx, speech: Arc<dyn SpeechToText>, audio: &[f32]) -> Answer {
    let none = || Answer::Lyrics(Vec::new());
    let hypotheses = transcribe(ctx, speech, audio).await;
    let hypotheses: Vec<String> = hypotheses.into_iter().filter(|h| real_words(h) >= MIN_REAL_WORDS).collect();
    let Some(best) = hypotheses.first() else { return none() };
    let update = SessionUpdate { transcript: Some(best.clone()), ..Default::default() };
    if let Err(e) = ctx.engine.store.update_session(&ctx.id, update) {
        tracing::warn!("could not store the transcript: {e}");
    }
    ctx.emitter.phase(Phase::SearchingLyrics, None);
    let search = lyrics::search(&ctx.engine.http, &hypotheses, ctx.engine.country(), &ctx.cancel);
    match tokio::time::timeout(LYRICS_TIMEOUT, search).await {
        Ok(Ok(matches)) => Answer::Lyrics(matches),
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
    let newest = &audio[audio.len().saturating_sub(ACR_CLIP_MS * SAMPLE_RATE as usize / 1000)..];
    let wav = encode_wav(&f32_to_i16(&downsample_half(newest)), ACR_RATE);
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

/// Words that are not humming filler ("la", "lalala", "hmmm", "da-da").
pub(crate) fn real_words(text: &str) -> usize {
    text.split(|c: char| c.is_whitespace() || c == '-' || c == ',' || c == '.')
        .map(|w| w.chars().filter(|c| c.is_alphanumeric()).flat_map(char::to_lowercase).collect::<String>())
        .filter(|w| !w.is_empty() && !is_filler(w))
        .count()
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
        let others: Vec<(CandidateSourceKind, Track, f32, Option<String>)> = self
            .lyrics
            .iter()
            .map(|m| (CandidateSourceKind::Lyrics, m.track.clone(), m.score, m.matched_line.clone()))
            .chain(self.acr.iter().map(|m| (CandidateSourceKind::AcrHumming, m.track.clone(), m.score, None)))
            .collect();
        if let Some(index) = index {
            self.lookup_songs(index, &others).await;
            self.score_support(index, &others).await;
        }

        let mut evidence: Vec<Evidence> = self.melody.iter().map(melody_evidence).collect();
        for (kind, track, score, detail) in others {
            let song = self.songs.get(&identity::identity_key(&track)).copied().flatten();
            evidence.push(Evidence { kind, track, song, score, detail, alignment: None });
        }
        // Melody evidence for lyric/ACR candidates the melody search did not return.
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

    /// Find lyric/ACR tracks in the melody index (cached per identity).
    async fn lookup_songs(
        &mut self,
        index: &Arc<MelodyIndex>,
        others: &[(CandidateSourceKind, Track, f32, Option<String>)],
    ) {
        let missing: Vec<(String, String, String)> = others
            .iter()
            .map(|(_, t, _, _)| (identity::identity_key(t), t.title.clone(), t.artist.clone()))
            .filter(|(key, _, _)| !self.songs.contains_key(key))
            .collect();
        if missing.is_empty() {
            return;
        }
        let index = index.clone();
        let keys: Vec<String> = missing.iter().map(|(k, _, _)| k.clone()).collect();
        let found = blocking("melody index lookup", move || {
            missing
                .into_iter()
                .map(|(key, title, artist)| (key, index.find_song(&title, &artist).map(|s| s.id)))
                .collect::<Vec<_>>()
        })
        .await;
        match found {
            Some(found) => self.songs.extend(found),
            None => self.songs.extend(keys.into_iter().map(|k| (k, None))),
        }
    }

    /// Score the query against index songs of lyric/ACR candidates (cached per song).
    async fn score_support(
        &mut self,
        index: &Arc<MelodyIndex>,
        others: &[(CandidateSourceKind, Track, f32, Option<String>)],
    ) {
        let Some(query) = self.query.clone() else { return };
        let found: Vec<u32> = self.melody.iter().map(|m| m.song.id).collect();
        let mut ids: Vec<u32> = others
            .iter()
            .filter_map(|(_, t, _, _)| self.songs.get(&identity::identity_key(t)).copied().flatten())
            .filter(|id| !found.contains(id) && !self.support.contains_key(id))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return;
        }
        let index = index.clone();
        let wanted = ids.clone();
        let scored = blocking("melody scoring", move || {
            wanted.into_iter().map(|id| (id, matcher::score_song(&index, &query, id))).collect::<Vec<_>>()
        })
        .await;
        match scored {
            Some(scored) => self.support.extend(scored),
            None => self.support.extend(ids.into_iter().map(|id| (id, None))),
        }
    }
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
        assert_eq!(real_words("take on me take me on"), 6);
        assert_eq!(real_words("la la I'm gonna be"), 3);
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
