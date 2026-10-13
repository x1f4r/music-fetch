//! Lyric-search evaluation: the fourteen queries of the lyric-search research
//! (exact, altered, misheard, German and speech-recognizer output) and two real
//! transcripts of sung clips, scored against each backend on its own.

use std::time::{Duration, Instant};

use super::fetch::{Fetcher, Stats};
use super::{Backend, SearchOptions, query_backend, search_with, text};
use crate::error::Result;
use crate::util::cancel::CancelToken;

/// What kind of input a case represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseKind {
    Exact,
    Altered,
    Misheard,
    German,
    /// Speech-recognizer output with errors.
    Recognized,
    /// A real transcript of a sung recording.
    SungClip,
}

/// One query with the song it should find.
#[derive(Debug, Clone)]
pub struct EvalCase {
    pub kind: CaseKind,
    /// Hypotheses, best first.
    pub hypotheses: &'static [&'static str],
    pub title: &'static str,
    /// Any of these artists counts; empty means any artist (traditional songs).
    pub artists: &'static [&'static str],
}

const fn case(
    kind: CaseKind,
    text: &'static [&'static str],
    title: &'static str,
    artists: &'static [&'static str],
) -> EvalCase {
    EvalCase { kind, hypotheses: text, title, artists }
}

/// The evaluation set. Sung-clip transcripts are whisper-tiny output for
/// `datasets/sung/twinkle45.wav` and `amazing45.wav`.
pub const CASES: &[EvalCase] = &[
    case(CaseKind::Exact, &["is this the real life is this just fantasy"], "Bohemian Rhapsody", &["Queen"]),
    case(CaseKind::Exact, &["caught in a landslide no escape from reality"], "Bohemian Rhapsody", &["Queen"]),
    case(CaseKind::Exact, &["a full commitments what im thinking of"], "Never Gonna Give You Up", &["Rick Astley"]),
    case(
        CaseKind::Exact,
        &["ive been reading books of old the legends and the myths"],
        "Something Just Like This",
        &["The Chainsmokers", "Coldplay"],
    ),
    case(
        CaseKind::Altered,
        &["ive been reading book of old the legend and the myth"],
        "Something Just Like This",
        &["The Chainsmokers", "Coldplay"],
    ),
    case(CaseKind::Altered, &["somebody once told me the world is going to roll me"], "All Star", &["Smash Mouth"]),
    case(CaseKind::Altered, &["is this the real life or is it fantasy"], "Bohemian Rhapsody", &["Queen"]),
    case(CaseKind::Exact, &["wake me up when its all over when im wiser and im older"], "Wake Me Up", &["Avicii"]),
    case(CaseKind::Misheard, &["hold me closer tony danza"], "Tiny Dancer", &["Elton John"]),
    case(CaseKind::Misheard, &["excuse me while i kiss this guy"], "Purple Haze", &["Jimi Hendrix"]),
    case(
        CaseKind::German,
        &["an tagen wie diesen wünscht man sich unendlichkeit"],
        "Tage wie diese",
        &["Die Toten Hosen"],
    ),
    case(
        CaseKind::Recognized,
        &["Amazing grace how sweet this sound Let's save our edge like me"],
        "Amazing Grace",
        &[],
    ),
    case(
        CaseKind::Recognized,
        &["then the traveler in the dark thanks you for your tiny spark"],
        "Twinkle Twinkle Little Star",
        &[],
    ),
    case(CaseKind::Recognized, &["I once was lost but now and found was blind but now I see"], "Amazing Grace", &[]),
    case(
        CaseKind::SungClip,
        &["Twinkle, twinkle little star, how I wonder what you are. Hop, hop, hop, the world so high. Like a diamond \
           in the sky. When the blazing sun is gone, When he nothing shines upon. Then you show your little light, \
           Twinkle, twinkle, all the night. Then the traveler in the dark, Thanks you for your tiny spark."],
        "Twinkle Twinkle Little Star",
        &[],
    ),
    case(
        CaseKind::SungClip,
        &["Amazing grace, how sweet is now? Let's save our edge like me. By you I was lost, but now in found was \
           blind, but now I see."],
        "Amazing Grace",
        &[],
    ),
];

/// True when `title`/`artist` is the song `case` expects.
pub fn is_expected(case: &EvalCase, title: &str, artist: &str) -> bool {
    let want = text::title_key(case.title);
    let got = text::title_key(title);
    let title_ok = got == want || (case.artists.is_empty() && got.starts_with(&want));
    if !title_ok {
        return false;
    }
    if case.artists.is_empty() {
        return true;
    }
    let got: Vec<String> = text::artist_keys(artist);
    case.artists.iter().flat_map(|a| text::artist_keys(a)).any(|want| got.iter().any(|g| g.contains(&want)))
}

/// Outcome of one case.
#[derive(Debug, Clone)]
pub struct CaseResult {
    pub case: &'static EvalCase,
    /// 1-based rank of the expected song in the fused results.
    pub rank: Option<usize>,
    /// Fused results: title, artist, score, matched line.
    pub top: Vec<(String, String, f32, Option<String>)>,
    /// 1-based rank of the expected song in each backend's own results for the
    /// whole first hypothesis.
    pub baselines: Vec<(Backend, Option<usize>)>,
    pub elapsed: Duration,
    /// Network requests (search, lyrics and baselines) and cache hits.
    pub requests: u32,
    pub cache_hits: u32,
    /// Failed backend requests, and the search error if it failed outright.
    pub failures: Vec<(Backend, String)>,
    pub error: Option<String>,
}

/// Top-1 / top-3 counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    pub cases: usize,
    pub top1: usize,
    pub top3: usize,
}

impl Tally {
    fn add(&mut self, rank: Option<usize>) {
        self.cases += 1;
        self.top1 += usize::from(rank == Some(1));
        self.top3 += usize::from(rank.is_some_and(|r| r <= 3));
    }
}

/// All case results.
#[derive(Debug, Clone, Default)]
pub struct EvalReport {
    pub results: Vec<CaseResult>,
}

impl EvalReport {
    /// Fused results over the research queries (`sung = false`) or the sung clips.
    pub fn fused(&self, sung: bool) -> Tally {
        let mut tally = Tally::default();
        self.subset(sung).for_each(|r| tally.add(r.rank));
        tally
    }

    /// One backend alone over the same subset.
    pub fn baseline(&self, backend: Backend, sung: bool) -> Tally {
        let mut tally = Tally::default();
        for result in self.subset(sung) {
            tally.add(result.baselines.iter().find(|(b, _)| *b == backend).and_then(|(_, rank)| *rank));
        }
        tally
    }

    fn subset(&self, sung: bool) -> impl Iterator<Item = &CaseResult> {
        self.results.iter().filter(move |r| (r.case.kind == CaseKind::SungClip) == sung)
    }
}

/// Run every case: the fused search with `options`, then each backend alone on
/// the whole first hypothesis. `on_result` sees each case as it finishes.
pub async fn run(
    http: &reqwest::Client,
    country: &str,
    options: &SearchOptions,
    mut on_result: impl FnMut(&CaseResult),
) -> EvalReport {
    let mut report = EvalReport::default();
    for case in CASES {
        let cancel = CancelToken::new();
        let hypotheses: Vec<String> = case.hypotheses.iter().map(|h| h.to_string()).collect();
        let started = Instant::now();
        let outcome = search_with(http, &hypotheses, country, &cancel, options).await;
        let elapsed = started.elapsed();
        let mut result = CaseResult {
            case,
            rank: None,
            top: Vec::new(),
            baselines: Vec::new(),
            elapsed,
            requests: 0,
            cache_hits: 0,
            failures: Vec::new(),
            error: None,
        };
        match outcome {
            Ok(search) => {
                result.rank = search
                    .matches
                    .iter()
                    .position(|m| is_expected(case, &m.track.title, &m.track.artist))
                    .map(|p| p + 1);
                result.top = search
                    .matches
                    .iter()
                    .map(|m| (m.track.title.clone(), m.track.artist.clone(), m.score, m.matched_line.clone()))
                    .collect();
                result.requests = search.requests;
                result.cache_hits = search.cache_hits;
                result.failures = search.failures;
            }
            Err(e) => result.error = Some(e.to_string()),
        }

        let stats = Stats::default();
        let fetcher = Fetcher {
            http,
            cancel: &cancel,
            cache_dir: options.cache_dir.as_deref(),
            max_wait: options.max_rate_wait,
            stats: &stats,
        };
        let query = text::query_words(case.hypotheses.first().copied().unwrap_or_default()).join(" ");
        let country = super::storefront(country);
        for &backend in &options.backends {
            let rank = match query_backend(&fetcher, backend, &query, &country).await {
                Ok(hits) => hits.iter().position(|h| is_expected(case, &h.title, &h.artist)).map(|p| p + 1),
                Err(e) => {
                    result.failures.push((backend, format!("baseline: {}", super::describe(&e))));
                    None
                }
            };
            result.baselines.push((backend, rank));
        }
        result.requests += stats.requests.load(std::sync::atomic::Ordering::Relaxed);
        result.cache_hits += stats.cache_hits.load(std::sync::atomic::Ordering::Relaxed);
        on_result(&result);
        report.results.push(result);
    }
    report
}

/// HTTP client for the CLI and the live evaluation: installs the ring TLS
/// provider when the process has none, then builds the engine's client.
pub fn http_client() -> Result<reqwest::Client> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    crate::util::http::build_client(env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_set_matches_the_research_table() {
        assert_eq!(CASES.iter().filter(|c| c.kind != CaseKind::SungClip).count(), 14);
        assert_eq!(CASES.iter().filter(|c| c.kind == CaseKind::SungClip).count(), 2);
        assert!(CASES.iter().all(|c| !c.hypotheses.is_empty()));
    }

    #[test]
    fn expected_song_matching() {
        let wake = &CASES[7];
        assert!(is_expected(wake, "Wake Me Up", "Avicii"));
        assert!(is_expected(wake, "Wake Me Up (Radio Edit)", "Avicii"));
        assert!(!is_expected(wake, "Wake Me Up When September Ends", "Green Day"));
        assert!(!is_expected(wake, "Wake Me Up", "Ed Sheeran"));
        let haze = &CASES[9];
        assert!(is_expected(haze, "Purple Haze", "The Jimi Hendrix Experience"));
        let sjlt = &CASES[3];
        assert!(is_expected(sjlt, "Something Just Like This", "The Chainsmokers & Coldplay"));
        assert!(is_expected(sjlt, "Something Just Like This", "Coldplay & The Chainsmokers"));
        let twinkle = &CASES[12];
        assert!(is_expected(twinkle, "Twinkle, Twinkle, Little Star", "Super Simple Songs"));
        assert!(is_expected(twinkle, "Twinkle Twinkle Little Star (Lullaby)", "Anyone"));
        assert!(!is_expected(twinkle, "Little Star", "Anyone"));
        let german = &CASES[10];
        assert!(is_expected(german, "Tage wie diese", "Die Toten Hosen"));
    }

    /// Live evaluation against the real services. Set `MF_LYRICS_CACHE` to a
    /// directory to cache responses between runs.
    #[tokio::test]
    #[ignore = "queries live services"]
    async fn live_eval_meets_targets() {
        let http = http_client().expect("http client");
        let options = SearchOptions {
            cache_dir: std::env::var_os("MF_LYRICS_CACHE").map(Into::into),
            max_rate_wait: Duration::from_secs(90),
            ..SearchOptions::default()
        };
        let report = run(&http, "US", &options, |r| {
            eprintln!(
                "{:?} rank {:?} baselines {:?} top {:?}",
                r.case.hypotheses[0],
                r.rank,
                r.baselines,
                r.top.first()
            );
        })
        .await;
        let fused = report.fused(false);
        eprintln!("fused {fused:?}");
        assert!(fused.top3 >= 11, "{fused:?}");
        assert!(fused.top1 >= 9, "{fused:?}");
        for backend in Backend::ALL {
            let single = report.baseline(backend, false);
            eprintln!("{} {single:?}", backend.name());
            assert!(fused.top3 > single.top3 || (fused.top3 == single.top3 && fused.top1 > single.top1), "{backend:?}");
        }
        let sung = report.fused(true);
        assert_eq!(sung.top3, sung.cases, "{sung:?}");
    }
}
