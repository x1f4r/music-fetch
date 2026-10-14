//! Scoring catalog candidates against a title/artist/duration query.

use crate::identity::{normalize_artists, normalize_title};
use crate::model::Track;

/// Largest duration difference accepted when both durations are known.
pub(crate) const DURATION_TOLERANCE_MS: u64 = 3_000;

/// Words that mark a different rendition when only the candidate has them.
const VERSION_WORDS: &[&str] =
    &["remix", "live", "acoustic", "edit", "version", "mix", "demo", "mono", "extended", "instrumental"];
/// Renditions that are almost never what the user means.
const UNWANTED_WORDS: &[&str] =
    &["karaoke", "cover", "tribute", "sped up", "slowed", "nightcore", "8d audio", "made famous by"];

/// What we are looking for.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Query<'a> {
    pub title: &'a str,
    pub artist: &'a str,
    pub duration_ms: Option<u64>,
}

impl<'a> Query<'a> {
    pub(crate) fn of(track: &'a Track) -> Self {
        Self { title: &track.title, artist: &track.artist, duration_ms: track.duration_ms }
    }
}

fn similarity(a: &str, b: &str) -> f64 {
    if a == b { 1.0 } else { strsim::normalized_levenshtein(a, b) }
}

fn artist_similarity(query: &str, candidate: &str) -> Option<f64> {
    let q = normalize_artists(query);
    if q.is_empty() {
        return None;
    }
    let c = normalize_artists(candidate);
    let q_tokens: Vec<&str> = q.split(';').collect();
    let c_tokens: Vec<&str> = c.split(';').filter(|t| !t.is_empty()).collect();
    if c_tokens.is_empty() {
        return Some(0.0);
    }
    if q_tokens.iter().any(|t| c_tokens.contains(t)) {
        return Some(1.0);
    }
    let best_pair = q_tokens.iter().flat_map(|a| c_tokens.iter().map(move |b| similarity(a, b))).fold(0.0, f64::max);
    Some(best_pair.max(similarity(&q.replace(';', ""), &c.replace(';', ""))))
}

fn words_in(text: &str, words: &[&str]) -> Vec<String> {
    let lower = text.to_lowercase();
    let padded = format!(" {} ", lower.replace(|c: char| !c.is_alphanumeric(), " "));
    words.iter().filter(|w| padded.contains(&format!(" {w} "))).map(|w| (*w).to_string()).collect()
}

/// Score in 0..=1 when `candidate` is an acceptable match for `query`, else None.
pub(crate) fn score(query: &Query<'_>, candidate: &Track) -> Option<f64> {
    let q_title = normalize_title(query.title);
    if q_title.is_empty() {
        return None;
    }
    let title = similarity(&q_title, &normalize_title(&candidate.title));
    if title < 0.85 {
        return None;
    }
    let artist = artist_similarity(query.artist, &candidate.artist);
    match artist {
        Some(a) if a < 0.75 => return None,
        // Without an artist only an exact title is good enough.
        None if title < 1.0 => return None,
        _ => {}
    }
    if let (Some(q), Some(c)) = (query.duration_ms, candidate.duration_ms)
        && q.abs_diff(c) > DURATION_TOLERANCE_MS
    {
        return None;
    }
    let raw_query = format!("{} {}", query.title, query.artist);
    let raw_candidate = format!("{} {}", candidate.title, candidate.album.as_deref().unwrap_or_default());
    let unwanted =
        words_in(&raw_candidate, UNWANTED_WORDS).iter().any(|w| !words_in(&raw_query, UNWANTED_WORDS).contains(w));
    if unwanted {
        return None;
    }
    let query_versions = words_in(query.title, VERSION_WORDS);
    let extra_versions =
        words_in(&candidate.title, VERSION_WORDS).into_iter().filter(|w| !query_versions.contains(w)).count();
    let penalty = 0.12 * extra_versions as f64;
    let duration_bonus = match (query.duration_ms, candidate.duration_ms) {
        (Some(q), Some(c)) => 0.05 * (1.0 - q.abs_diff(c) as f64 / DURATION_TOLERANCE_MS as f64),
        _ => 0.0,
    };
    let total = 0.6 * title + 0.4 * artist.unwrap_or(0.8) - penalty + duration_bonus;
    (total >= 0.8).then_some(total.min(1.0))
}

/// The best acceptable candidate; earlier candidates win ties (services return
/// results by relevance).
pub(crate) fn best<'a>(query: &Query<'_>, candidates: &'a [Track]) -> Option<&'a Track> {
    let mut best: Option<(f64, &Track)> = None;
    for candidate in candidates {
        if let Some(s) = score(query, candidate)
            && best.is_none_or(|(b, _)| s > b + 1e-9)
        {
            best = Some((s, candidate));
        }
    }
    best.map(|(_, t)| t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(title: &str, artist: &str, duration_s: u64) -> Track {
        Track { title: title.into(), artist: artist.into(), duration_ms: Some(duration_s * 1000), ..Track::default() }
    }

    fn query<'a>(title: &'a str, artist: &'a str, duration_s: Option<u64>) -> Query<'a> {
        Query { title, artist, duration_ms: duration_s.map(|s| s * 1000) }
    }

    #[test]
    fn picks_the_original_over_versions_and_covers() {
        let candidates = vec![
            track("Harder Better Faster Stronger (live bonus track)", "WHAT ABOUT YOUR MOM", 221),
            track("Harder, Better, Faster, Stronger (Alive Radio Edit 2007)", "Daft Punk", 204),
            track("Harder, Better, Faster, Stronger", "Daft Punk", 226),
            track("Harder, Better, Faster, Stronger", "Scary Pockets", 161),
        ];
        let q = query("Harder, Better, Faster, Stronger", "Daft Punk", Some(225));
        assert_eq!(best(&q, &candidates).map(|t| t.duration_ms), Some(Some(226_000)));
        let q = query("Harder Better Faster Stronger", "Daft Punk", None);
        assert_eq!(best(&q, &candidates).map(|t| t.duration_ms), Some(Some(226_000)));
    }

    #[test]
    fn duration_must_be_within_three_seconds() {
        let candidates = vec![track("Song", "Artist", 200)];
        assert!(best(&query("Song", "Artist", Some(203)), &candidates).is_some());
        assert!(best(&query("Song", "Artist", Some(204)), &candidates).is_none());
    }

    #[test]
    fn rejects_other_artists_and_titles() {
        let candidates = vec![track("Halo", "Beyoncé", 261)];
        assert!(best(&query("Halo", "Ed Sheeran", None), &candidates).is_none());
        assert!(best(&query("Hello", "Beyonce", None), &candidates).is_none());
        assert!(best(&query("Halo", "Beyonce", None), &candidates).is_some());
        assert!(best(&query("", "Beyonce", None), &candidates).is_none());
    }

    #[test]
    fn featured_artists_and_order_do_not_matter() {
        let candidates = vec![track("Global Warming (feat. Sensato)", "Pitbull & Sensato", 85)];
        assert!(best(&query("Global Warming", "Pitbull", None), &candidates).is_some());
    }

    #[test]
    fn artistless_queries_need_exact_titles() {
        let candidates = vec![track("Never Gonna Give You Up", "Rick Astley", 213)];
        assert!(best(&query("Never Gonna Give You Up", "", Some(213)), &candidates).is_some());
        assert!(best(&query("Never Gonna Give You", "", Some(213)), &candidates).is_none());
    }

    #[test]
    fn unwanted_renditions_are_skipped_unless_asked_for() {
        let candidates = vec![track("Song (Karaoke Version)", "Artist", 200)];
        assert!(best(&query("Song", "Artist", None), &candidates).is_none());
        let candidates = vec![track("Song (Karaoke Version)", "Artist", 200)];
        assert!(best(&query("Song (Karaoke Version)", "Artist", None), &candidates).is_some());
    }
}
