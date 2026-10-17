//! Survey: one song or many? See docs/PIPELINES.md ("Survey").
//!
//! Never decided by duration alone, and a single song must never be split:
//! probes that all name the same song (in any version, at any offset) make a
//! single, as does a strong prior that a probe confirms. Only clear evidence of
//! two or more songs makes a mix.

use crate::audio::PcmFile;
use crate::error::{MfError, Result};
use crate::fingerprint;
use crate::model::{Evidence, EvidenceSource, Track};
use crate::pipeline::analyze::services::Services;
use crate::pipeline::hints;
use crate::pipeline::mix::{PROBE_MS, Probe};
use crate::pipeline::recognize::RecognitionHit;
use crate::pipeline::stitch::{SAME_START_MS, VERIFIED_HINT};
use crate::shazam::Priority;

/// Windows quieter than this (dBFS RMS) are moved to find audible audio.
const QUIET_DB: f32 = -45.0;

/// What the page says the media is.
#[derive(Debug, Clone, Default)]
pub struct Prior {
    pub track: Option<Track>,
    /// Extractor metadata, or a catalog match whose duration fits the media.
    pub strong: bool,
}

/// The survey's verdict.
#[derive(Debug, Clone)]
pub enum Verdict {
    /// One recording for the whole media.
    Single { track: Track, evidence: Vec<Evidence>, alternates: Vec<Track> },
    /// Several songs; the probes made so far seed the mix pipeline.
    Mix { probes: Vec<Probe> },
    /// Nothing recognized. `hint` is the page's best guess, if any.
    Nothing { hint: Option<Track> },
}

/// Survey result and the recognizer probes it made.
#[derive(Debug, Clone)]
pub struct Survey {
    pub verdict: Verdict,
    pub probes: u32,
    pub paid: u32,
    pub log: Vec<String>,
}

/// Probe positions as fractions of the duration: the middle first so a
/// confirmed prior costs one call.
const FIRST: [f64; 3] = [0.5, 0.2, 0.8];
const EXTRA: [f64; 2] = [0.35, 0.65];

/// Decide whether `pcm` holds one song or several.
pub async fn run<S: Services>(services: &S, pcm: &PcmFile, prior: &Prior, priority: Priority) -> Result<Survey> {
    let mut state = State { services, pcm, priority, probes: Vec::new(), calls: 0, paid: 0, log: Vec::new() };
    for (k, fraction) in FIRST.iter().enumerate() {
        state.probe(*fraction).await?;
        // A strong prior confirmed by the first probe settles it.
        if k == 0
            && prior.strong
            && let (Some(p), Some(hit)) = (&prior.track, state.probes.last().and_then(|x| x.hit.as_ref()))
            && hints::same_song(&hit.track, p)
        {
            state.log.push("first probe confirms the page metadata".into());
            let verdict = single(&state.probes, Some(prior));
            return Ok(state.finish(verdict));
        }
    }
    if state.matched().is_empty() {
        for fraction in EXTRA {
            state.probe(fraction).await?;
        }
    }
    if state.matched().is_empty() && services.has_paid() {
        let at = window_start(pcm, 0.5);
        let samples = pcm.read_ms(at, PROBE_MS)?;
        state.paid += 1;
        if let Some(hit) = services.paid(&samples).await? {
            state.probes.push(Probe { at_ms: at, len_ms: PROBE_MS, hit: Some(hit) });
        }
    }
    if state.matched().is_empty() {
        state.log.push("no probe matched".into());
        return Ok(state.finish(Verdict::Nothing { hint: prior.track.clone() }));
    }
    let group_count = groups(&state.matched()).len();
    if group_count == 1 {
        let verdict = single(&state.probes, Some(prior));
        return Ok(state.finish(verdict));
    }
    // Disagreement: look closer before calling it a mix.
    if state.probes.len() < FIRST.len() + EXTRA.len() {
        for fraction in EXTRA {
            state.probe(fraction).await?;
        }
    }
    let verdict = decide(&state.probes, prior);
    if matches!(verdict, Verdict::Mix { .. }) {
        state.log.push(format!("{group_count}+ different songs: mix"));
    } else {
        state.log.push("disagreeing probes outvoted: single".into());
    }
    Ok(state.finish(verdict))
}

/// The page's song when the recognizer named something else: its catalog
/// preview found in the audio settles it (re-uploads, sped-up copies and
/// "type beats" of a song are often in the recognizer's catalog too).
pub async fn page_song_heard<S: Services>(services: &S, pcm: &PcmFile, prior: &Prior) -> Result<Option<Track>> {
    let Some(track) = prior.track.as_ref().filter(|t| t.ids.contains_key("deezer") || t.ids.contains_key("apple"))
    else {
        return Ok(None);
    };
    let Some(preview) = services.preview(track).await else { return Ok(None) };
    let media = pcm.clone();
    let cancel = services.cancel().clone();
    let found = tokio::task::spawn_blocking(move || -> Result<bool> {
        let peaks = fingerprint::peaks_in(&media, 0, media.duration_ms(), &cancel)?;
        let mut index = fingerprint::Index::new();
        index.add(&fingerprint::peaks(&preview));
        let best = index.search(&peaks, &fingerprint::warps(0.03, 0.01)).into_iter().next();
        Ok(best.is_some_and(|m| m.is_confident(&fingerprint::MatchOptions::default())))
    })
    .await
    .map_err(|e| MfError::internal(format!("local match: {e}")))??;
    Ok(found.then(|| track.clone()))
}

/// Overrule a verdict that missed the page's song when the song's preview is
/// found in the audio. Mixes stay mixes (the song is then one of several).
pub fn prefer_page_song(verdict: Verdict, page: Track) -> Verdict {
    let page_evidence = Evidence {
        source: EvidenceSource::Metadata,
        at_ms: 0,
        score: VERIFIED_HINT,
        detail: Some("the page names this song and its preview matches the audio".into()),
    };
    match verdict {
        Verdict::Single { track, evidence: _, mut alternates } if !hints::same_song(&track, &page) => {
            alternates.insert(0, track);
            Verdict::Single { track: page, evidence: vec![page_evidence], alternates }
        }
        Verdict::Nothing { .. } => {
            Verdict::Single { track: page, evidence: vec![page_evidence], alternates: Vec::new() }
        }
        other => other,
    }
}

struct State<'a, S: Services> {
    services: &'a S,
    pcm: &'a PcmFile,
    priority: Priority,
    probes: Vec<Probe>,
    calls: u32,
    paid: u32,
    log: Vec<String>,
}

impl<S: Services> State<'_, S> {
    async fn probe(&mut self, fraction: f64) -> Result<()> {
        let at = window_start(self.pcm, fraction);
        if self.probes.iter().any(|p| p.at_ms.abs_diff(at) < PROBE_MS) {
            return Ok(());
        }
        let len = PROBE_MS.min(self.pcm.duration_ms().saturating_sub(at));
        if len < 3_000 {
            return Ok(());
        }
        let samples = self.pcm.read_ms(at, len)?;
        self.calls += 1;
        let hit = match self.services.shazam(&samples, self.priority).await {
            Ok(hit) => hit,
            // Throttled beyond the wait limit: decide with what we have.
            Err(MfError::RateLimited { .. }) => None,
            Err(e) => return Err(e),
        };
        self.log.push(match &hit {
            Some(h) => format!(
                "survey {:.0} % → {} – {} (starts {:+.0} s, speed {:+.1} %, pitch {:+.1} %)",
                fraction * 100.0,
                h.track.artist,
                h.track.title,
                h.offset_ms.map_or(f64::NAN, |o| (at as f64 - o as f64) / 1000.0),
                h.timeskew * 100.0,
                h.frequency_skew * 100.0
            ),
            None => format!("survey {:.0} % → no match", fraction * 100.0),
        });
        self.probes.push(Probe { at_ms: at, len_ms: len, hit });
        Ok(())
    }

    fn matched(&self) -> Vec<&Probe> {
        self.probes.iter().filter(|p| p.hit.is_some()).collect()
    }

    fn finish(self, verdict: Verdict) -> Survey {
        let mut probes = self.probes;
        probes.sort_by_key(|p| p.at_ms);
        let verdict = match verdict {
            Verdict::Mix { .. } => Verdict::Mix { probes },
            other => other,
        };
        Survey { verdict, probes: self.calls, paid: self.paid, log: self.log }
    }
}

/// Start of a 12 s window around `fraction` of the media, moved forward (then
/// backward) up to 30 s when the spot is quiet.
fn window_start(pcm: &PcmFile, fraction: f64) -> u64 {
    let duration = pcm.duration_ms();
    let last = duration.saturating_sub(PROBE_MS);
    let centre = (duration as f64 * fraction) as u64;
    let base = centre.saturating_sub(PROBE_MS / 2).min(last);
    for shift in [0i64, 10_000, 20_000, 30_000, -10_000, -20_000] {
        let at = (base as i64 + shift).clamp(0, last as i64) as u64;
        if loudness_db(pcm, at) > QUIET_DB {
            return at;
        }
    }
    base
}

fn loudness_db(pcm: &PcmFile, at_ms: u64) -> f32 {
    let Ok(samples) = pcm.read_ms(at_ms, PROBE_MS) else { return -120.0 };
    if samples.is_empty() {
        return -120.0;
    }
    let sum: f64 = samples.iter().map(|s| (*s as f64 / 32768.0).powi(2)).sum();
    (10.0 * (sum / samples.len() as f64).max(1e-12).log10()) as f32
}

/// Matched probes grouped by song (versions together), largest group first.
fn groups<'p>(matched: &[&'p Probe]) -> Vec<Vec<&'p Probe>> {
    let mut out: Vec<Vec<&Probe>> = Vec::new();
    for p in matched {
        let Some(hit) = &p.hit else { continue };
        match out.iter_mut().find(|g| g[0].hit.as_ref().is_some_and(|h| hints::same_song(&h.track, &hit.track))) {
            Some(g) => g.push(p),
            None => out.push(vec![p]),
        }
    }
    out.sort_by_key(|g| std::cmp::Reverse(g.len()));
    out
}

/// Single or mix after all probes disagreed at first.
fn decide(probes: &[Probe], prior: &Prior) -> Verdict {
    let matched: Vec<&Probe> = probes.iter().filter(|p| p.hit.is_some()).collect();
    let groups = groups(&matched);
    if groups.len() <= 1 {
        return single(probes, Some(prior));
    }
    let big = groups.iter().filter(|g| g.len() >= 2).count();
    if big >= 2 {
        if !interleaved(&groups) {
            return Verdict::Mix { probes: probes.to_vec() };
        }
        // Answers that alternate (A B A A B) are one recording known under two
        // names: a DJ set or compilation containing the song, a record sampling
        // it. Consecutive songs never alternate.
        return single_of(preferred_group(&groups, prior), probes, prior);
    }
    // One clear majority with stray answers (samples, interpolations).
    if groups[0].len() >= 3 && groups[1..].iter().all(|g| g.len() == 1) {
        return single_of(&groups[0], probes, prior);
    }
    // The page names one of the songs and it was heard: a single with a
    // misleading probe, unless the answers line up as consecutive songs.
    let consecutive = consecutive_songs(&groups);
    if !consecutive
        && let Some(p) = &prior.track
        && let Some(g) =
            groups.iter().find(|g| g.iter().any(|x| x.hit.as_ref().is_some_and(|h| hints::same_song(&h.track, p))))
    {
        return single_of(g, probes, prior);
    }
    if consecutive {
        return Verdict::Mix { probes: probes.to_vec() };
    }
    // Ambiguous: never split a short file on weak evidence.
    single_of(&groups[0], probes, prior)
}

/// Some group's answers have another group's answer between them in time.
fn interleaved(groups: &[Vec<&Probe>]) -> bool {
    let mut order: Vec<(u64, usize)> =
        groups.iter().enumerate().flat_map(|(g, probes)| probes.iter().map(move |p| (p.at_ms, g))).collect();
    order.sort_unstable();
    let mut finished: Vec<usize> = Vec::new();
    let mut current = None;
    for (_, g) in order {
        if current == Some(g) {
            continue;
        }
        if finished.contains(&g) {
            return true;
        }
        if let Some(c) = current {
            finished.push(c);
        }
        current = Some(g);
    }
    false
}

/// The group a single verdict should come from: the page's song, else the
/// recording that begins with the media (its answers place its start near
/// 0), else the biggest group.
fn preferred_group<'g, 'p>(groups: &'g [Vec<&'p Probe>], prior: &Prior) -> &'g [&'p Probe] {
    if let Some(page) = &prior.track
        && let Some(g) =
            groups.iter().find(|g| g.iter().any(|p| p.hit.as_ref().is_some_and(|h| hints::same_song(&h.track, page))))
    {
        return g;
    }
    let start_offset = |g: &Vec<&Probe>| -> i64 {
        let mut starts: Vec<i64> = g
            .iter()
            .filter_map(|p| {
                let hit = p.hit.as_ref()?;
                Some((p.at_ms as i64 - hit.offset_ms?).abs())
            })
            .collect();
        starts.sort_unstable();
        starts.get(starts.len() / 2).copied().unwrap_or(i64::MAX)
    };
    groups.iter().min_by_key(|g| (start_offset(g) > 30_000, std::cmp::Reverse(g.len()))).map_or(&[], |g| g.as_slice())
}

/// The songs' start estimates put each one after the previous one was heard.
fn consecutive_songs(groups: &[Vec<&Probe>]) -> bool {
    let mut spans: Vec<(u64, i64)> = groups
        .iter()
        .filter_map(|g| {
            let first = g.iter().min_by_key(|p| p.at_ms)?;
            let hit = first.hit.as_ref()?;
            let start = first.at_ms as i64 - hit.offset_ms?;
            Some((first.at_ms, start))
        })
        .collect();
    if spans.len() < 2 {
        return false;
    }
    spans.sort_by_key(|s| s.0);
    spans.windows(2).all(|w| {
        let (prev_at, _) = w[0];
        let (_, next_start) = w[1];
        // The later song started after the earlier one was heard.
        next_start >= prev_at as i64
    })
}

/// The single verdict for one group of agreeing probes; the other answers
/// become alternates.
fn single_of(group: &[&Probe], all: &[Probe], prior: &Prior) -> Verdict {
    let probes: Vec<Probe> = group.iter().map(|p| (*p).clone()).collect();
    match single(&probes, Some(prior)) {
        Verdict::Single { track, evidence, mut alternates } => {
            for hit in all.iter().filter_map(|p| p.hit.as_ref()) {
                let known = hints::same_recording(&hit.track, &track)
                    || alternates.iter().any(|a| hints::same_recording(a, &hit.track));
                if !known {
                    alternates.push(hit.track.clone());
                }
            }
            Verdict::Single { track, evidence, alternates }
        }
        other => other,
    }
}

/// The single verdict from the matched probes: the recording most probes
/// name, preferring the prior's version, then the tightest skew.
fn single(probes: &[Probe], prior: Option<&Prior>) -> Verdict {
    let hits: Vec<(&Probe, &RecognitionHit)> = probes.iter().filter_map(|p| p.hit.as_ref().map(|h| (p, h))).collect();
    let prior_track = prior.and_then(|p| p.track.as_ref());
    let best = hits
        .iter()
        .max_by(|(_, a), (_, b)| {
            let votes =
                |h: &RecognitionHit| hits.iter().filter(|(_, o)| hints::same_recording(&o.track, &h.track)).count();
            let prior_version = |h: &RecognitionHit| prior_track.is_some_and(|p| hints::same_version(&h.track, p));
            (prior_version(a), votes(a), -(a.skew() * 1e4) as i64).cmp(&(
                prior_version(b),
                votes(b),
                -(b.skew() * 1e4) as i64,
            ))
        })
        .map(|(_, h)| (*h).clone());
    let Some(best) = best else {
        return Verdict::Nothing { hint: prior_track.cloned() };
    };
    let mut evidence: Vec<Evidence> = hits
        .iter()
        .filter(|(_, h)| hints::same_song(&h.track, &best.track))
        .map(|(p, h)| Evidence {
            source: h.source,
            at_ms: p.at_ms,
            score: h.score,
            detail: h.offset_ms.map(|o| format!("offset {} s, skew {:.1} %", o / 1000, h.skew() * 100.0)),
        })
        .collect();
    if let Some(p) = prior.filter(|p| p.strong)
        && let Some(t) = &p.track
        && hints::same_song(t, &best.track)
    {
        evidence.push(Evidence {
            source: EvidenceSource::Metadata,
            at_ms: 0,
            score: VERIFIED_HINT,
            detail: Some("the page names this song".into()),
        });
    }
    let mut alternates: Vec<Track> = hits
        .iter()
        .filter(|(_, h)| !hints::same_recording(&h.track, &best.track))
        .map(|(_, h)| h.track.clone())
        .fold(Vec::new(), |mut acc: Vec<Track>, t| {
            if !acc.iter().any(|a| hints::same_recording(a, &t)) {
                acc.push(t);
            }
            acc
        });
    // The page names a version (a live performance, a remix) of the song the
    // recognizer only knows in its original, or whose versions the answers
    // disagree on (none named twice): show the page's version, keep the
    // recognizer's as the first alternate.
    // Answers count for the recording itself only when they place its start
    // alike: a remix built on another heard twice at unrelated offsets is not
    // that remix.
    let best_votes = consistent_votes(&hits, &best.track);
    if let Some(page) = prior_track
        && hints::same_song(page, &best.track)
        && !hints::version_key(&page.title).is_empty()
        && !hints::same_version(page, &best.track)
        && (hints::version_key(&best.track.title).is_empty() || best_votes < 2)
    {
        alternates.insert(0, best.track);
        return Verdict::Single { track: page.clone(), evidence, alternates };
    }
    Verdict::Single { track: best.track, evidence, alternates }
}

/// The most answers of `track`'s recording whose start estimates agree
/// within a few seconds.
fn consistent_votes(hits: &[(&Probe, &RecognitionHit)], track: &Track) -> usize {
    let starts: Vec<Option<i64>> = hits
        .iter()
        .filter(|(_, h)| hints::same_recording(&h.track, track))
        .map(|(p, h)| h.offset_ms.map(|o| p.at_ms as i64 - (o as f64 / (1.0 + h.timeskew as f64)) as i64))
        .collect();
    starts
        .iter()
        .map(|s| match s {
            Some(s) => starts.iter().flatten().filter(|t| (**t - s).abs() <= SAME_START_MS).count(),
            None => 1,
        })
        .max()
        .unwrap_or(0)
}

/// Weigh the page's song against a verdict that names another song. One
/// recognizer answer (the other probes silent or disagreeing) does not
/// outweigh the page; neither does a cover of the page's song by another
/// artist when the artist's own channel uploaded the media (`official`).
/// The overruled answer stays as the first alternate.
pub fn weigh_page(verdict: Verdict, prior: &Prior, official: bool) -> Verdict {
    let Some(page) = prior.track.as_ref() else { return verdict };
    let Verdict::Single { track, evidence, mut alternates } = verdict else { return verdict };
    if hints::same_song(&track, page) {
        return Verdict::Single { track, evidence, alternates };
    }
    let votes = evidence
        .iter()
        .filter(|e| matches!(e.source, EvidenceSource::Shazam | EvidenceSource::AudD | EvidenceSource::AcrCloud))
        .count();
    let cover = official && hints::same_song(&Track { artist: String::new(), ..track.clone() }, page);
    if votes >= 2 && !cover {
        return Verdict::Single { track, evidence, alternates };
    }
    alternates.insert(0, track);
    let page_evidence = if official || prior.strong {
        Evidence {
            source: EvidenceSource::Metadata,
            at_ms: 0,
            score: OFFICIAL_PAGE,
            detail: Some("the artist's own upload names this song".into()),
        }
    } else {
        Evidence {
            source: EvidenceSource::PageHint,
            at_ms: 0,
            score: crate::pipeline::stitch::UNVERIFIED_HINT,
            detail: Some("from the title; the recognizer's answer was not confirmed".into()),
        }
    };
    Verdict::Single { track: page.clone(), evidence: vec![page_evidence], alternates }
}

/// Confidence of a song named by the artist's own upload that the recognizer
/// did not contradict convincingly.
const OFFICIAL_PAGE: f32 = 0.8;

#[cfg(test)]
mod tests;
