//! Find songs from sung (transcribed) or typed lyrics.
//!
//! 1. Plan: normalize the hypotheses and cut them into 6–10-word windows within a
//!    request budget that favours the best hypothesis ([`plan`]).
//! 2. Search iTunes, Genius lyric search, YouTube Music and Deezer in parallel;
//!    each backend is rate-limited and a failing backend only drops out.
//! 3. Merge hits by normalized title and overlapping artists.
//! 4. Fetch lyrics for the leading candidates from LRCLIB and align the
//!    transcripts against them with fuzzy, sound-aware local alignment.
//! 5. Fuse backend agreement, ranks and alignment into a calibrated score.

mod align;
pub mod eval;
mod fetch;
mod lrclib;
mod merge;
mod phonetic;
mod plan;
mod score;
mod sources;
mod text;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::join_all;

use crate::error::{MfError, Result};
use crate::model::Track;
use crate::util::cancel::CancelToken;

use self::fetch::{FetchError, Fetcher, Remote, Stats};
use self::merge::{Candidate, Sighting};
use self::score::{Features, LyricEvidence};
use self::sources::{Hit, ParseError};

/// Results returned, at most.
const MAX_RESULTS: usize = 5;
/// Results below this score are dropped.
const MIN_SCORE: f32 = 0.05;
/// Candidates listed in [`SearchReport::ranked`].
const DIAGNOSTIC_RESULTS: usize = 10;
/// Score factor per better-ranked version of the same title.
const VERSION_DISCOUNT: f32 = 0.85;
/// Candidates per title (covers) among those whose lyrics are checked.
const MAX_VERIFIED_PER_TITLE: usize = 3;
/// Lyric lookups in flight.
const VERIFY_CONCURRENCY: usize = 4;
/// Time budget for the lyric check; candidates still pending are scored without it.
const VERIFY_DEADLINE: Duration = Duration::from_secs(12);

#[derive(Debug, Clone)]
pub struct LyricMatch {
    pub track: Track,
    /// Calibrated 0..=1.
    pub score: f32,
    /// The lyric line that matched best.
    pub matched_line: Option<String>,
}

/// A search backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    ITunes,
    Genius,
    YouTubeMusic,
    Deezer,
}

impl Backend {
    pub const ALL: [Backend; 4] = [Backend::ITunes, Backend::Genius, Backend::YouTubeMusic, Backend::Deezer];

    /// Short lowercase name, e.g. "itunes".
    pub fn name(self) -> &'static str {
        self.remote().name()
    }

    fn remote(self) -> Remote {
        match self {
            Backend::ITunes => Remote::ITunes,
            Backend::Genius => Remote::Genius,
            Backend::YouTubeMusic => Remote::YouTubeMusic,
            Backend::Deezer => Remote::Deezer,
        }
    }
}

/// Tuning knobs for [`search_with`]. The defaults are what [`search`] uses.
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// Backends to query.
    pub backends: Vec<Backend>,
    /// Backend requests per search, at most.
    pub max_requests: usize,
    /// Leading candidates whose lyrics are fetched and aligned (0 disables the check).
    pub verify_top: usize,
    /// Longest wait for a rate-limit slot before a request is skipped.
    pub max_rate_wait: Duration,
    /// Cache raw responses in this directory (development and evaluation only).
    pub cache_dir: Option<PathBuf>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            backends: Backend::ALL.to_vec(),
            max_requests: 12,
            verify_top: 8,
            max_rate_wait: Duration::from_secs(3),
            cache_dir: None,
        }
    }
}

/// Matches plus diagnostics.
#[derive(Debug, Clone, Default)]
pub struct SearchReport {
    pub matches: Vec<LyricMatch>,
    /// Requests sent (or answered from the cache), in plan order.
    pub queries: Vec<(Backend, String)>,
    /// Requests that failed, with a short reason.
    pub failures: Vec<(Backend, String)>,
    /// Network requests sent, including lyric lookups.
    pub requests: u32,
    /// Responses answered from the disk cache.
    pub cache_hits: u32,
    /// Distinct songs suggested by the backends.
    pub candidates: usize,
    /// Candidates whose full lyrics were aligned.
    pub verified: usize,
    /// The leading candidates with the evidence behind their scores.
    pub ranked: Vec<Ranked>,
}

/// A scored candidate with its evidence, for diagnostics.
#[derive(Debug, Clone)]
pub struct Ranked {
    pub title: String,
    pub artist: String,
    pub score: f32,
    /// Ranking before the lyric check.
    pub prior: f32,
    pub backends: Vec<Backend>,
    /// "lyrics", "excerpt", "instrumental" or "none".
    pub evidence: &'static str,
    pub coverage: f32,
    pub longest_run: usize,
    pub popularity: f32,
}

/// Search with one or more transcription hypotheses (best first). Typed lyrics are
/// a single hypothesis.
///
/// Returns at most five matches, best first. Hypotheses without searchable words
/// give an empty list. Fails with `RateLimited` or `Network` only when no backend
/// could be reached, and with `Canceled` once `cancel` fires.
pub async fn search(
    http: &reqwest::Client,
    hypotheses: &[String],
    country: &str,
    cancel: &CancelToken,
) -> Result<Vec<LyricMatch>> {
    Ok(search_with(http, hypotheses, country, cancel, &SearchOptions::default()).await?.matches)
}

/// [`search`] with explicit options, returning diagnostics as well.
pub async fn search_with(
    http: &reqwest::Client,
    hypotheses: &[String],
    country: &str,
    cancel: &CancelToken,
    options: &SearchOptions,
) -> Result<SearchReport> {
    cancel.check()?;
    tokio::select! {
        biased;
        _ = cancel.canceled() => Err(MfError::Canceled),
        report = run(http, hypotheses, country, cancel, options) => report,
    }
}

async fn run(
    http: &reqwest::Client,
    hypotheses: &[String],
    country: &str,
    cancel: &CancelToken,
    options: &SearchOptions,
) -> Result<SearchReport> {
    let country = storefront(country);
    let plan = plan::plan(hypotheses, &options.backends, options.max_requests);
    let mut report = SearchReport {
        queries: plan.requests.iter().map(|r| (r.backend, r.query.clone())).collect(),
        ..SearchReport::default()
    };
    if plan.requests.is_empty() {
        return Ok(report);
    }
    let stats = Stats::default();
    let fetcher = Fetcher {
        http,
        cancel,
        cache_dir: options.cache_dir.as_deref(),
        max_wait: options.max_rate_wait,
        stats: &stats,
    };

    let responses =
        join_all(plan.requests.iter().map(|r| query_backend(&fetcher, r.backend, &r.query, &country))).await;
    let mut sightings = Vec::new();
    let mut answered = false;
    let mut throttled: Option<Duration> = None;
    let mut throttled_count = 0;
    for (index, (request, response)) in plan.requests.iter().zip(responses).enumerate() {
        match response {
            Ok(hits) => {
                answered = true;
                let query_words = request.query.split_whitespace().count();
                sightings.extend(hits.into_iter().enumerate().map(|(rank, hit)| Sighting {
                    backend: request.backend,
                    request: index,
                    query_words,
                    rank,
                    hit,
                }));
            }
            Err(FetchError::Canceled) => return Err(MfError::Canceled),
            Err(error) => {
                if let FetchError::Throttled { retry_in } = &error {
                    throttled = Some(throttled.map_or(*retry_in, |t| t.min(*retry_in)));
                    throttled_count += 1;
                }
                tracing::debug!(backend = request.backend.name(), ?error, "lyric search request failed");
                report.failures.push((request.backend, describe(&error)));
            }
        }
    }
    if !answered {
        return Err(match throttled {
            Some(wait) if throttled_count == plan.requests.len() => {
                MfError::RateLimited { retry_in_ms: wait.as_millis() as u64 }
            }
            _ => MfError::network("lyric search services are unreachable"),
        });
    }

    let mut candidates = merge::merge(sightings);
    candidates.sort_by(|a, b| prior(b).total_cmp(&prior(a)));
    report.candidates = candidates.len();

    let queries: Vec<align::Query> = plan.transcripts.iter().map(|t| align::Query::new(t)).collect();
    let selected = select_for_check(&candidates, options.verify_top);
    let mut evidence = check_lyrics(&fetcher, &candidates, &selected, &queries).await?;
    report.verified = evidence.values().filter(|e| matches!(e, LyricEvidence::Full(_))).count();

    let mut scored: Vec<(LyricMatch, Ranked, &str)> = candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            let full = evidence.remove(&index).unwrap_or(LyricEvidence::Unknown);
            let (score, lyrics) = best_evidence(candidate, full, &queries);
            let alignment = lyrics.alignment();
            let track = candidate.track();
            let mut backends: Vec<Backend> = Backend::ALL.into_iter().filter(|b| candidate.seen_on(*b)).collect();
            backends.dedup();
            let ranked = Ranked {
                title: track.title.clone(),
                artist: track.artist.clone(),
                score,
                prior: prior(candidate),
                backends,
                evidence: match lyrics {
                    LyricEvidence::Full(_) => "lyrics",
                    LyricEvidence::Excerpt(_) => "excerpt",
                    LyricEvidence::Instrumental => "instrumental",
                    LyricEvidence::Unknown => "none",
                },
                coverage: alignment.map_or(0.0, |a| a.coverage),
                longest_run: alignment.map_or(0, |a| a.longest_run),
                popularity: candidate.popularity(),
            };
            let matched_line = alignment.and_then(|a| a.best_line.clone());
            (LyricMatch { track, score, matched_line }, ranked, candidate.title_key.as_str())
        })
        .filter(|(m, _, _)| !m.track.title.is_empty())
        .collect();
    let by_score = |a: &(LyricMatch, Ranked, &str), b: &(LyricMatch, Ranked, &str)| {
        b.1.score.total_cmp(&a.1.score).then(b.1.prior.total_cmp(&a.1.prior))
    };
    scored.sort_by(by_score);
    // Lyrics cannot tell a song's versions apart: each further version of a
    // title is less likely to be the one meant than the best-ranked one.
    let mut versions: HashMap<&str, i32> = HashMap::new();
    for (m, ranked, title) in scored.iter_mut() {
        let earlier = versions.entry(title).or_default();
        ranked.score *= VERSION_DISCOUNT.powi(*earlier);
        m.score = ranked.score;
        *earlier += 1;
    }
    scored.sort_by(by_score);
    report.ranked = scored.iter().take(DIAGNOSTIC_RESULTS).map(|(_, r, _)| r.clone()).collect();
    report.matches =
        scored.into_iter().filter(|(m, _, _)| m.score >= MIN_SCORE).take(MAX_RESULTS).map(|(m, _, _)| m).collect();
    report.requests = stats.requests.load(std::sync::atomic::Ordering::Relaxed);
    report.cache_hits = stats.cache_hits.load(std::sync::atomic::Ordering::Relaxed);
    Ok(report)
}

/// Query one backend and parse its answer.
async fn query_backend(
    fetcher: &Fetcher<'_>,
    backend: Backend,
    query: &str,
    country: &str,
) -> std::result::Result<Vec<Hit>, FetchError> {
    let remote = backend.remote();
    let key = sources::cache_key(backend, query, country);
    let fetched = fetcher.fetch(remote, &key, || sources::request(backend, fetcher.http, query, country)).await?;
    match sources::parse(backend, &fetched.body) {
        Ok(hits) => {
            fetcher.remember(remote, &key, &fetched).await;
            Ok(hits)
        }
        Err(ParseError::Throttled) => Err(fetcher.throttled(remote).await),
        Err(ParseError::Malformed(why)) => {
            tracing::debug!(backend = backend.name(), why, "unexpected lyric search response");
            Err(FetchError::Malformed)
        }
    }
}

/// Ranking before the lyric check: backend agreement, ranks and popularity.
fn prior(candidate: &Candidate) -> f32 {
    let noise = if candidate.is_noise() { 1.0 } else { 0.0 };
    let catalog = if candidate.in_catalog() { 0.3 } else { 0.0 };
    candidate.rrf()
        + 0.3 * candidate.backends().saturating_sub(1) as f32
        + 0.5 * candidate.genius_exactness()
        + 0.6 * candidate.popularity()
        + catalog
        - noise
}

/// Indices of the candidates whose lyrics are checked: first each backend's own
/// best answer (so one backend's many similar hits cannot crowd out another's),
/// then the rest by prior. `candidates` is sorted by prior. At most a few
/// versions of one title are checked.
fn select_for_check(candidates: &[Candidate], limit: usize) -> Vec<usize> {
    let mut order: Vec<usize> = Vec::with_capacity(candidates.len() + Backend::ALL.len());
    for backend in Backend::ALL {
        let best = candidates
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.is_noise())
            .filter_map(|(i, c)| c.best_rank_on(backend).map(|rank| (rank, i)))
            .min();
        order.extend(best.map(|(_, i)| i));
    }
    order.extend(0..candidates.len());
    let mut per_title: HashMap<&str, usize> = HashMap::new();
    let mut selected = Vec::new();
    for index in order {
        if selected.len() >= limit {
            break;
        }
        let Some(candidate) = candidates.get(index) else { continue };
        if selected.contains(&index) || candidate.is_noise() {
            continue;
        }
        let count = per_title.entry(candidate.title_key.as_str()).or_default();
        if *count < MAX_VERIFIED_PER_TITLE {
            *count += 1;
            selected.push(index);
        }
    }
    selected
}

/// Fetch and align lyrics for the selected candidates, within [`VERIFY_DEADLINE`].
async fn check_lyrics(
    fetcher: &Fetcher<'_>,
    candidates: &[Candidate],
    selected: &[usize],
    queries: &[align::Query],
) -> Result<HashMap<usize, LyricEvidence>> {
    let mut evidence = HashMap::new();
    if queries.is_empty() {
        return Ok(evidence);
    }
    let deadline = tokio::time::Instant::now() + VERIFY_DEADLINE;
    let mut lookups = futures_util::stream::iter(selected.iter().filter_map(|&i| candidates.get(i).map(|c| (i, c))))
        .map(|(index, candidate)| async move { (index, check_one(fetcher, candidate, queries).await) })
        .buffer_unordered(VERIFY_CONCURRENCY);
    loop {
        match tokio::time::timeout_at(deadline, lookups.next()).await {
            Ok(Some((_, Err(FetchError::Canceled)))) => return Err(MfError::Canceled),
            Ok(Some((index, Ok(found)))) => {
                evidence.insert(index, found);
            }
            Ok(Some((index, Err(error)))) => {
                tracing::debug!(?error, index, "lyrics lookup failed");
            }
            Ok(None) => break,
            Err(_) => {
                tracing::debug!("lyrics check ran out of time");
                break;
            }
        }
    }
    Ok(evidence)
}

async fn check_one(
    fetcher: &Fetcher<'_>,
    candidate: &Candidate,
    queries: &[align::Query],
) -> std::result::Result<LyricEvidence, FetchError> {
    let Some(hit) = candidate.primary() else { return Ok(LyricEvidence::Unknown) };
    let duration = candidate.track().duration_ms;
    Ok(match lrclib::lyrics(fetcher, &hit.title, &hit.artist, duration).await? {
        Some(lrclib::Found::Lyrics(raw)) => {
            let lyrics = align::Lyrics::new(&raw);
            best_alignment(queries, &lyrics).map_or(LyricEvidence::Unknown, LyricEvidence::Full)
        }
        Some(lrclib::Found::Instrumental) => LyricEvidence::Instrumental,
        None => LyricEvidence::Unknown,
    })
}

/// The best-covered alignment over all hypotheses.
fn best_alignment(queries: &[align::Query], lyrics: &align::Lyrics) -> Option<align::Alignment> {
    if lyrics.is_empty() {
        return None;
    }
    queries
        .iter()
        .map(|q| align::align(q, lyrics))
        .max_by(|a, b| a.coverage.total_cmp(&b.coverage).then(a.score.total_cmp(&b.score)))
}

/// Score with the full-lyrics evidence or the backend excerpts, whichever is stronger.
fn best_evidence(candidate: &Candidate, full: LyricEvidence, queries: &[align::Query]) -> (f32, LyricEvidence) {
    let features = |lyrics: LyricEvidence| Features {
        backends: candidate.backends(),
        rrf: candidate.rrf(),
        genius_exact: candidate.genius_exactness(),
        popularity: candidate.popularity(),
        in_catalog: candidate.in_catalog(),
        noise: candidate.is_noise(),
        lyrics,
    };
    let full_features = features(full);
    let mut best = (score::fuse(&full_features), full_features.lyrics);
    if matches!(best.1, LyricEvidence::Instrumental) {
        return best;
    }
    let excerpt = candidate
        .snippets()
        .into_iter()
        .filter_map(|s| best_alignment(queries, &align::Lyrics::new(s)))
        .max_by(|a, b| a.coverage.total_cmp(&b.coverage));
    if let Some(alignment) = excerpt {
        let excerpt_features = features(LyricEvidence::Excerpt(alignment));
        let excerpt_score = score::fuse(&excerpt_features);
        if excerpt_score > best.0 {
            best = (excerpt_score, excerpt_features.lyrics);
        }
    }
    best
}

/// Two-letter storefront code, "US" when `country` is not one.
fn storefront(country: &str) -> String {
    let code = country.trim();
    if code.len() == 2 && code.bytes().all(|b| b.is_ascii_alphabetic()) {
        code.to_ascii_uppercase()
    } else {
        "US".into()
    }
}

fn describe(error: &FetchError) -> String {
    match error {
        FetchError::Canceled => "canceled".into(),
        FetchError::Throttled { .. } => "rate limited".into(),
        FetchError::NotFound => "not found".into(),
        FetchError::Status(code) => format!("HTTP {code}"),
        FetchError::Network(why) => why.clone(),
        FetchError::Malformed => "unexpected response".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storefront_codes() {
        assert_eq!(storefront("de"), "DE");
        assert_eq!(storefront(" US "), "US");
        assert_eq!(storefront("USA"), "US");
        assert_eq!(storefront(""), "US");
        assert_eq!(storefront("ü1"), "US");
    }

    pub(crate) fn test_client() -> reqwest::Client {
        let _ = rustls::crypto::ring::default_provider().install_default();
        reqwest::Client::new()
    }

    #[tokio::test]
    async fn empty_hypotheses_return_nothing_without_network() {
        let http = test_client();
        let cancel = CancelToken::new();
        let result = search(&http, &["  [Music] ".to_string(), String::new()], "US", &cancel).await;
        assert!(matches!(result, Ok(ref m) if m.is_empty()), "{result:?}");
    }

    /// A client whose every request fails at once: the services' hosts resolve to
    /// a closed local port.
    fn offline_client() -> reqwest::Client {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let closed = std::net::SocketAddr::from(([127, 0, 0, 1], 9));
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(2));
        for host in ["itunes.apple.com", "genius.com", "music.youtube.com", "api.deezer.com", "lrclib.net"] {
            builder = builder.resolve(host, closed);
        }
        builder.build().unwrap_or_default()
    }

    const ITUNES: &str = r#"{"resultCount":2,"results":[
        {"wrapperType":"track","kind":"song","trackId":11,"artistName":"Kitty Kat Dirt Nap","trackName":"(Hold Me Closer) Tony Danza",
         "collectionName":"Parodies","trackTimeMillis":180000},
        {"wrapperType":"track","kind":"song","trackId":12,"artistName":"Elton John","trackName":"Tiny Dancer",
         "collectionName":"Madman Across the Water","trackTimeMillis":377000,
         "trackViewUrl":"https://music.apple.com/us/album/tiny-dancer/1?i=12&uo=4",
         "artworkUrl100":"https://is1-ssl.mzstatic.com/image/thumb/a/b.jpg/100x100bb.jpg",
         "previewUrl":"https://audio-ssl.itunes.apple.com/p.m4a"}]}"#;

    const GENIUS: &str = r#"{"response":{"sections":[{"type":"lyric","hits":[
        {"highlights":[{"value":"Hold me closer Tony Danza\nRun the playground"}],"matched_words":5,"nb_exact_words":5,"type":"song",
         "result":{"id":3,"title":"Life's a Joke","artist_names":"Surfbort","url":"https://genius.com/x","primary_artist":{"name":"Surfbort"}}}]}]}}"#;

    const YOUTUBE_MUSIC: &str = r#"{"contents":{"tabbedSearchResultsRenderer":{"tabs":[{"tabRenderer":{"content":{"sectionListRenderer":{"contents":[
        {"musicShelfRenderer":{"contents":[{"musicResponsiveListItemRenderer":{
            "flexColumns":[
                {"musicResponsiveListItemFlexColumnRenderer":{"text":{"runs":[{"text":"Tiny Dancer"}]}}},
                {"musicResponsiveListItemFlexColumnRenderer":{"text":{"runs":[{"text":"Elton John"},{"text":" • "},{"text":"6:17"}]}}},
                {"musicResponsiveListItemFlexColumnRenderer":{"text":{"runs":[{"text":"355M plays"}]}}}],
            "playlistItemData":{"videoId":"aqlGlaNlcWE"}}}]}}]}}}}]}}}"#;

    const TINY_DANCER: &str = r#"{"trackName":"Tiny Dancer","artistName":"Elton John","duration":377.0,"instrumental":false,
        "plainLyrics":"Blue jean baby, L.A. lady, seamstress for the band\nPretty eyed, pirate smile, you'll marry a music man\nBallerina, you must have seen her dancing in the sand\nHold me closer, tiny dancer\nCount the headlights on the highway"}"#;

    #[tokio::test]
    async fn pipeline_ranks_the_verified_song_first() {
        let dir = std::env::temp_dir().join(format!("mf-lyrics-e2e-{}", crate::util::new_id()));
        let http = offline_client();
        let cancel = CancelToken::new();
        let stats = Stats::default();
        let fetcher =
            Fetcher { http: &http, cancel: &cancel, cache_dir: Some(&dir), max_wait: Duration::ZERO, stats: &stats };
        let query = "hold me closer tony danza";
        let fixtures = [
            (Remote::ITunes, sources::cache_key(Backend::ITunes, query, "US"), ITUNES),
            (Remote::Genius, sources::cache_key(Backend::Genius, query, "US"), GENIUS),
            (Remote::YouTubeMusic, sources::cache_key(Backend::YouTubeMusic, query, "US"), YOUTUBE_MUSIC),
            (Remote::Deezer, sources::cache_key(Backend::Deezer, query, "US"), r#"{"data":[]}"#),
            (Remote::Lrclib, lrclib::get_key("Tiny Dancer", "Elton John", Some(377)), TINY_DANCER),
        ];
        for (remote, key, body) in fixtures {
            fetcher.remember(remote, &key, &fetch::Fetched { body: body.to_string(), from_cache: false }).await;
        }

        let options =
            SearchOptions { cache_dir: Some(dir.clone()), max_rate_wait: Duration::ZERO, ..SearchOptions::default() };
        let report = search_with(&http, &["Hold me closer, Tony Danza!".to_string()], "us", &cancel, &options).await;
        let _ = std::fs::remove_dir_all(&dir);
        let report = report.expect("search succeeds from the cache");

        assert!(report.failures.is_empty(), "{:?}", report.failures);
        assert_eq!(report.cache_hits, 5);
        let best = &report.matches[0];
        assert_eq!((best.track.title.as_str(), best.track.artist.as_str()), ("Tiny Dancer", "Elton John"));
        assert_eq!(best.matched_line.as_deref(), Some("Hold me closer, tiny dancer"));
        assert!(best.score > 0.8, "{}", best.score);
        assert!(best.track.identity_key.is_empty());
        assert_eq!(best.track.ids.get("apple").map(String::as_str), Some("12"));
        assert_eq!(best.track.ids.get("youtube").map(String::as_str), Some("aqlGlaNlcWE"));
        assert_eq!(best.track.duration_ms, Some(377_000));
        assert_eq!(best.track.preview_url.as_deref(), Some("https://audio-ssl.itunes.apple.com/p.m4a"));
        assert!(best.track.artwork_url.as_deref().unwrap_or_default().ends_with("/1000x1000bb.jpg"));
        let urls: Vec<&str> = best.track.links.iter().map(|l| l.url.as_str()).collect();
        assert_eq!(
            urls,
            ["https://music.apple.com/us/album/tiny-dancer/1?i=12", "https://music.youtube.com/watch?v=aqlGlaNlcWE"]
        );
        // The Genius-only parody and the unverified catalog parody follow.
        assert!(report.matches[1..].iter().all(|m| m.score < best.score));
        assert!(report.matches.len() <= MAX_RESULTS);
    }

    #[tokio::test]
    async fn unreachable_services_fail_with_a_network_error() {
        let http = offline_client();
        let cancel = CancelToken::new();
        let options = SearchOptions { max_rate_wait: Duration::ZERO, ..SearchOptions::default() };
        let result = search_with(&http, &["a song nobody cached".to_string()], "US", &cancel, &options).await;
        assert!(matches!(result, Err(MfError::Network { .. })), "{result:?}");
    }

    #[test]
    fn each_backend_gets_its_best_answer_checked() {
        let sighting = |backend, request, rank, title: &str, artist: &str| Sighting {
            backend,
            request,
            query_words: 7,
            rank,
            hit: Hit { title: title.into(), artist: artist.into(), exact_words: Some(7), ..Hit::default() },
        };
        let mut sightings = Vec::new();
        for rank in 0..6 {
            let title = format!("Parody {rank}");
            sightings.push(sighting(Backend::Genius, 0, rank, &title, "Someone"));
            sightings.push(sighting(Backend::Deezer, 1, rank, &title, "Someone"));
        }
        sightings.push(sighting(Backend::ITunes, 2, 0, "Purple Haze", "The Jimi Hendrix Experience"));
        let mut candidates = merge::merge(sightings);
        candidates.sort_by(|a, b| prior(b).total_cmp(&prior(a)));
        let selected = select_for_check(&candidates, 3);
        assert_eq!(selected.len(), 3);
        assert!(selected.iter().any(|&i| candidates[i].title_key == "purplehaze"));
    }

    #[tokio::test]
    async fn canceled_search_fails_fast() {
        let http = test_client();
        let cancel = CancelToken::new();
        cancel.cancel();
        let result = search(&http, &["is this the real life".to_string()], "US", &cancel).await;
        assert!(matches!(result, Err(MfError::Canceled)));
    }
}
