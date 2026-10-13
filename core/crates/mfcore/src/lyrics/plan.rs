//! Query planning: which word windows of which hypotheses go to which backend,
//! within a fixed request budget that favours the best hypothesis.

use std::collections::HashSet;

use super::Backend;
use super::text;

/// Transcripts up to this many words are also searched whole on every backend.
const FULL_TEXT_MAX_WORDS: usize = 16;
/// iTunes and YouTube Music rank well even on long, noisy text: they also get the
/// whole transcript up to this length.
const TOLERANT_FULL_TEXT_MAX_WORDS: usize = 64;
/// Shorter transcripts are only searched whole.
const MIN_WORDS_FOR_WINDOWS: usize = 8;
/// Window length for long transcripts.
const LONG_WINDOW: usize = 8;
/// Windows taken from one long transcript, at most.
const MAX_WINDOWS: usize = 4;
/// Deezer only matches when every word matches: query it with short windows.
const DEEZER_WINDOW: usize = 6;
/// Queries that share this much of their words with an earlier one are skipped.
const NEAR_DUPLICATE: f32 = 0.9;
/// Hypotheses considered, at most.
const MAX_HYPOTHESES: usize = 5;

/// One backend request.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Request {
    pub backend: Backend,
    pub query: String,
}

/// What to send, and what to align against lyrics afterwards.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Plan {
    pub requests: Vec<Request>,
    /// Alignment tokens per distinct hypothesis, best first.
    pub transcripts: Vec<Vec<String>>,
    /// Query words of the best hypothesis.
    pub best_words: usize,
}

/// Requests per backend, at most. The defaults add up to 12.
pub(crate) fn backend_cap(backend: Backend) -> usize {
    match backend {
        Backend::ITunes => 4,
        Backend::YouTubeMusic | Backend::Genius => 3,
        Backend::Deezer => 2,
    }
}

/// Plan requests for `hypotheses` (best first) on `backends`, at most `max_requests` in total.
pub(crate) fn plan(hypotheses: &[String], backends: &[Backend], max_requests: usize) -> Plan {
    let mut seen_texts = HashSet::new();
    let hypotheses: Vec<Vec<String>> = hypotheses
        .iter()
        .map(|h| text::query_words(h))
        .filter(|words| !words.is_empty() && seen_texts.insert(words.join(" ")))
        .take(MAX_HYPOTHESES)
        .collect();
    let Some(best) = hypotheses.first() else { return Plan::default() };

    // Global priority order: position within a hypothesis, then hypothesis rank.
    let mut ranked: Vec<(f32, Vec<String>)> = Vec::new();
    for (h, words) in hypotheses.iter().enumerate() {
        for (k, query) in hypothesis_queries(words).into_iter().enumerate() {
            ranked.push((k as f32 + 1.5 * h as f32, query));
        }
    }
    ranked.sort_by(|a, b| a.0.total_cmp(&b.0));
    let general = dedupe(ranked.into_iter().map(|(_, q)| q));

    let mut deezer: Vec<Vec<String>> = Vec::new();
    for words in &hypotheses {
        if words.len() <= MIN_WORDS_FOR_WINDOWS {
            deezer.push(words.clone());
        } else {
            deezer.push(words[..DEEZER_WINDOW].to_vec());
            deezer.push(words[words.len() - DEEZER_WINDOW..].to_vec());
        }
    }
    let deezer = dedupe(deezer.into_iter());

    // Interleave backends so a smaller budget still reaches every backend.
    let mut lists: Vec<(Backend, std::vec::IntoIter<Vec<String>>)> = backends
        .iter()
        .map(|&b| {
            let source = if b == Backend::Deezer { &deezer } else { &general };
            let max_words = match b {
                Backend::ITunes | Backend::YouTubeMusic => TOLERANT_FULL_TEXT_MAX_WORDS,
                Backend::Genius | Backend::Deezer => FULL_TEXT_MAX_WORDS,
            };
            let queue: Vec<Vec<String>> =
                source.iter().filter(|q| q.len() <= max_words).take(backend_cap(b)).cloned().collect();
            (b, queue.into_iter())
        })
        .collect();
    let mut requests = Vec::new();
    let mut progressed = true;
    while progressed && requests.len() < max_requests {
        progressed = false;
        for (backend, queue) in lists.iter_mut() {
            if requests.len() >= max_requests {
                break;
            }
            if let Some(words) = queue.next() {
                requests.push(Request { backend: *backend, query: words.join(" ") });
                progressed = true;
            }
        }
    }

    let mut seen_tokens = HashSet::new();
    let transcripts = hypotheses
        .iter()
        .map(|words| text::transcript_tokens(&words.join(" ")))
        .filter(|tokens| !tokens.is_empty() && seen_tokens.insert(tokens.clone()))
        .collect();
    Plan { requests, transcripts, best_words: best.len() }
}

/// Queries for one hypothesis, most useful first: the whole text, then 6–10-word
/// windows.
fn hypothesis_queries(words: &[String]) -> Vec<Vec<String>> {
    let n = words.len();
    let mut out = Vec::new();
    if n <= TOLERANT_FULL_TEXT_MAX_WORDS {
        out.push(words.to_vec());
    }
    if n < MIN_WORDS_FOR_WINDOWS {
        return out;
    }
    if n <= FULL_TEXT_MAX_WORDS {
        // A prefix and a suffix window survive a wrong word at either end.
        let w = (2 * n).div_ceil(3).clamp(6, 10);
        out.push(words[..w].to_vec());
        out.push(words[n - w..].to_vec());
        return out;
    }
    // Long transcripts: the most informative, mostly disjoint windows.
    let w = LONG_WINDOW;
    let mut candidates: Vec<(f32, usize)> =
        (0..=n - w).step_by(2).map(|start| (informativeness(&words[start..start + w]), start)).collect();
    candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut chosen: Vec<usize> = Vec::new();
    for (_, start) in candidates {
        if chosen.len() >= MAX_WINDOWS {
            break;
        }
        if chosen.iter().all(|&c| c.abs_diff(start) >= w - 2) {
            chosen.push(start);
        }
    }
    out.extend(chosen.into_iter().map(|start| words[start..start + w].to_vec()));
    out
}

/// Content words weigh more than function words; long words more than short ones.
fn informativeness(words: &[String]) -> f32 {
    words.iter().map(|w| if text::is_stopword(w) { 0.3 } else { (w.chars().count().min(8) as f32) / 4.0 }).sum()
}

/// Drop exact and near duplicates, keeping order.
fn dedupe(queries: impl Iterator<Item = Vec<String>>) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    for query in queries {
        if query.is_empty() {
            continue;
        }
        let set: HashSet<&String> = query.iter().collect();
        let duplicate = out.iter().any(|kept| {
            let other: HashSet<&String> = kept.iter().collect();
            let inter = set.intersection(&other).count() as f32;
            let union = set.union(&other).count() as f32;
            kept == &query || (union > 0.0 && inter / union >= NEAR_DUPLICATE)
        });
        if !duplicate {
            out.push(query);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queries(plan: &Plan, backend: Backend) -> Vec<&str> {
        plan.requests.iter().filter(|r| r.backend == backend).map(|r| r.query.as_str()).collect()
    }

    #[test]
    fn short_text_is_searched_whole_everywhere() {
        let plan = plan(&["Hold me closer, Tony Danza".into()], &Backend::ALL, 12);
        assert_eq!(plan.requests.len(), 4);
        assert!(plan.requests.iter().all(|r| r.query == "hold me closer tony danza"));
        assert_eq!(plan.transcripts, [["hold", "me", "closer", "tony", "danza"]]);
    }

    #[test]
    fn medium_text_adds_prefix_and_suffix_windows() {
        let plan = plan(&["is this the real life or is it fantasy".into()], &Backend::ALL, 12);
        assert_eq!(
            queries(&plan, Backend::ITunes),
            ["is this the real life or is it fantasy", "is this the real life or", "real life or is it fantasy"]
        );
        assert_eq!(queries(&plan, Backend::Deezer), ["is this the real life or", "real life or is it fantasy"]);
        assert!(plan.requests.len() <= 12);
    }

    #[test]
    fn long_transcripts_use_disjoint_windows_within_budget() {
        let text = "twinkle twinkle little star how i wonder what you are up above the world so high like a diamond \
                    in the sky when the blazing sun is gone when he nothing shines upon then you show your little light";
        let plan = plan(&[text.into()], &Backend::ALL, 12);
        assert_eq!(plan.requests.len(), 12);
        let itunes = queries(&plan, Backend::ITunes);
        assert_eq!(itunes.len(), 4);
        assert_eq!(itunes[0].split(' ').count(), 39);
        assert!(itunes[1..].iter().all(|q| q.split(' ').count() == LONG_WINDOW));
        let genius = queries(&plan, Backend::Genius);
        assert_eq!(genius.len(), 3);
        assert!(genius.iter().all(|q| q.split(' ').count() == LONG_WINDOW));
        assert_eq!(queries(&plan, Backend::Deezer).len(), 2);
    }

    #[test]
    fn best_hypothesis_comes_first_and_duplicates_are_dropped() {
        let hypotheses = vec![
            "then the traveler in the dark thanks you for your tiny spy".to_string(),
            "Then the traveler in the dark, thanks you for your tiny spy!".to_string(),
            "then the traveler in the dark thanks you for your tiny spark".to_string(),
        ];
        let plan = plan(&hypotheses, &[Backend::ITunes], 12);
        let itunes = queries(&plan, Backend::ITunes);
        assert_eq!(itunes[0], "then the traveler in the dark thanks you for your tiny spy");
        assert!(itunes.contains(&"then the traveler in the dark thanks you for your tiny spark"));
        assert_eq!(plan.transcripts.len(), 2);
    }

    #[test]
    fn budget_is_shared_across_backends() {
        let plan = plan(&["is this the real life or is it fantasy".into()], &Backend::ALL, 4);
        let backends: HashSet<Backend> = plan.requests.iter().map(|r| r.backend).collect();
        assert_eq!(plan.requests.len(), 4);
        assert_eq!(backends.len(), 4);
    }

    #[test]
    fn empty_input_plans_nothing() {
        assert_eq!(plan(&["[Music]".into(), "  ".into()], &Backend::ALL, 12), Plan::default());
        assert_eq!(plan(&[], &Backend::ALL, 12), Plan::default());
    }
}
