//! Score fusion: backend agreement and ranks plus lyric alignment, mapped to a
//! calibrated 0..=1 score with a logistic link.

use super::align::Alignment;

/// What the lyric check found for a candidate.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LyricEvidence {
    /// Aligned against the full lyrics.
    Full(Alignment),
    /// Aligned against a backend's lyric excerpt only.
    Excerpt(Alignment),
    /// The track has no vocals.
    Instrumental,
    /// No lyrics available (not checked, not found, or the lookup failed).
    Unknown,
}

impl LyricEvidence {
    pub fn alignment(&self) -> Option<&Alignment> {
        match self {
            LyricEvidence::Full(a) | LyricEvidence::Excerpt(a) => Some(a),
            _ => None,
        }
    }
}

/// Inputs of the fused score.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Features {
    /// Distinct backends that returned the song (1..=4).
    pub backends: usize,
    /// Reciprocal-rank fusion over all result lists.
    pub rrf: f32,
    /// Share of query words Genius found verbatim, 0..=1.
    pub genius_exact: f32,
    /// Popularity hint, 0..=1.
    pub popularity: f32,
    /// Seen on a music catalog, not only on Genius.
    pub in_catalog: bool,
    /// Every content word of the title was heard in the transcript.
    pub title_heard: bool,
    /// Karaoke, instrumental or tribute release.
    pub noise: bool,
    pub lyrics: LyricEvidence,
}

/// Runs this long (in transcript tokens) count as fully convincing.
const RUN_SATURATION: f32 = 6.0;

/// Calibrated probability-like score in 0..=1.
pub(crate) fn fuse(f: &Features) -> f32 {
    let rrf = f.rrf / (f.rrf + 1.0);
    let agreement = (f.backends.saturating_sub(1) as f32 / 3.0).min(1.0);
    let title = if f.title_heard { 0.5 } else { 0.0 };
    let retrieval = 1.5 * rrf + 0.5 * agreement + 0.8 * f.popularity + title;
    // Genius's word match only matters without lyrics: an alignment measures the
    // same thing, and Genius indexes one version of a song more or less at random.
    let exact = 0.6 * f.genius_exact;
    let penalty = if f.noise { 1.5 } else { 0.0 } + if f.in_catalog { 0.0 } else { 1.0 };
    let z = match &f.lyrics {
        LyricEvidence::Full(a) => -4.3 + verified(a) + retrieval - penalty,
        // Excerpts are the passage the backend matched, so they always align well.
        LyricEvidence::Excerpt(a) => -4.6 + 0.75 * verified(a) + retrieval - penalty,
        LyricEvidence::Unknown => -2.8 + 1.6 * (retrieval + exact) - penalty,
        LyricEvidence::Instrumental => -5.0 + retrieval,
    };
    sigmoid(z)
}

fn verified(a: &Alignment) -> f32 {
    5.0 * a.coverage + 1.2 * (a.longest_run as f32 / RUN_SATURATION).min(1.0)
}

fn sigmoid(z: f32) -> f32 {
    1.0 / (1.0 + (-z).exp())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn features(lyrics: LyricEvidence, rrf: f32, backends: usize) -> Features {
        Features {
            backends,
            rrf,
            genius_exact: 0.0,
            popularity: 0.5,
            in_catalog: true,
            title_heard: false,
            noise: false,
            lyrics,
        }
    }

    fn aligned(coverage: f32, longest_run: usize) -> Alignment {
        Alignment { coverage, longest_run, ..Alignment::default() }
    }

    #[test]
    fn verified_lyrics_dominate() {
        let strong = fuse(&features(LyricEvidence::Full(aligned(0.95, 9)), 2.0, 3));
        let weak = fuse(&features(LyricEvidence::Full(aligned(0.25, 2)), 3.0, 4));
        let unknown = fuse(&features(LyricEvidence::Unknown, 3.0, 4));
        assert!(strong > 0.95, "{strong}");
        assert!(weak < 0.5, "{weak}");
        assert!(unknown < strong);
        assert!(unknown > weak);
    }

    #[test]
    fn retrieval_breaks_ties() {
        let original = fuse(&features(LyricEvidence::Full(aligned(0.8, 6)), 3.0, 4));
        let cover = fuse(&features(LyricEvidence::Full(aligned(0.8, 6)), 0.3, 1));
        assert!(original > cover);
    }

    #[test]
    fn genius_only_songs_rank_below_catalog_songs() {
        let mut f = features(LyricEvidence::Excerpt(aligned(1.0, 5)), 0.5, 1);
        let catalog = fuse(&f);
        f.in_catalog = false;
        assert!(fuse(&f) < catalog);
    }

    #[test]
    fn noise_and_instrumentals_sink() {
        let mut f = features(LyricEvidence::Full(aligned(0.9, 8)), 2.0, 2);
        let clean = fuse(&f);
        f.noise = true;
        assert!(fuse(&f) < clean);
        assert!(fuse(&features(LyricEvidence::Instrumental, 3.0, 4)) < 0.2);
    }

    #[test]
    fn scores_are_probabilities() {
        for lyrics in [LyricEvidence::Full(aligned(1.0, 50)), LyricEvidence::Unknown, LyricEvidence::Instrumental] {
            let s = fuse(&features(lyrics, 100.0, 4));
            assert!((0.0..=1.0).contains(&s));
        }
    }
}
