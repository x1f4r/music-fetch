//! Local alignment (Smith–Waterman) of transcript words against song lyrics.
//!
//! Substitutions are scored with fuzzy token similarity, and two-token merges
//! ("this guy" ~ "the sky") are allowed, so misheard and mis-transcribed words
//! still extend an alignment instead of breaking it.

use std::collections::HashMap;

use super::phonetic::{Token, similarity};
use super::text;

/// Longest transcript considered, in tokens.
const MAX_QUERY_TOKENS: usize = 80;
/// Longest lyrics considered, in tokens.
const MAX_LYRIC_TOKENS: usize = 4000;
/// Cost of skipping one token on either side, scaled by the token's weight.
const GAP: f32 = 0.6;
/// Token similarity from which a substitution counts as a match.
const MATCH_SIM: f32 = 0.6;
/// Token similarity from which a match extends a run.
const RUN_SIM: f32 = 0.7;
/// Two-token merges must be at least this similar.
const MERGE_SIM: f32 = 0.75;
/// Minimum score of an extra alignment piece (outside the best one).
const MIN_PIECE_SCORE: f32 = 2.0;
/// Alignment pieces per transcript (best piece plus the parts before and after it).
const MAX_PIECES: usize = 3;

/// A transcript prepared for alignment.
#[derive(Debug, Clone)]
pub(crate) struct Query {
    tokens: Vec<Token>,
    weights: Vec<f32>,
    total_weight: f32,
}

impl Query {
    pub fn new(tokens: &[String]) -> Self {
        let tokens: Vec<Token> = tokens.iter().take(MAX_QUERY_TOKENS).map(|t| Token::new(t)).collect();
        let weights: Vec<f32> = tokens.iter().map(|t| token_weight(&t.text)).collect();
        let total_weight = weights.iter().sum();
        Self { tokens, weights, total_weight }
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

/// Song lyrics prepared for alignment.
#[derive(Debug, Clone)]
pub(crate) struct Lyrics {
    tokens: Vec<Token>,
    weights: Vec<f32>,
    line_of: Vec<usize>,
    lines: Vec<String>,
}

impl Lyrics {
    /// Plain or LRC lyrics; timestamps, section headers and blank lines are skipped.
    pub fn new(raw: &str) -> Self {
        let mut tokens = Vec::new();
        let mut line_of = Vec::new();
        let mut lines = Vec::new();
        for line in raw.lines() {
            let stripped = strip_timestamps(line);
            let line = stripped.trim();
            let words = text::lyric_tokens(line);
            if words.is_empty() {
                continue;
            }
            for word in words {
                if tokens.len() >= MAX_LYRIC_TOKENS {
                    break;
                }
                tokens.push(Token::new(&word));
                line_of.push(lines.len());
            }
            lines.push(line.to_string());
        }
        let weights = tokens.iter().map(|t| token_weight(&t.text)).collect();
        Self { tokens, weights, line_of, lines }
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

/// Result of aligning one transcript with one set of lyrics.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Alignment {
    /// Raw alignment score summed over pieces.
    pub score: f32,
    /// Weighted share of transcript tokens that found a lyric counterpart, 0..=1.
    pub coverage: f32,
    /// Longest run of consecutive matched transcript tokens.
    pub longest_run: usize,
    /// Transcript tokens matched.
    pub matched: usize,
    /// The lyric line with the most matched tokens.
    pub best_line: Option<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum Move {
    Stop,
    /// `q` query tokens against `l` lyric tokens, with their similarity.
    Pair {
        q: u8,
        l: u8,
        sim: f32,
    },
    SkipQuery,
    SkipLyric,
}

struct Piece {
    score: f32,
    q_start: usize,
    q_end: usize,
    credit: f32,
    matched: usize,
    longest_run: usize,
    line_credit: HashMap<usize, f32>,
}

/// Align a transcript against lyrics.
pub(crate) fn align(query: &Query, lyrics: &Lyrics) -> Alignment {
    if query.is_empty() || lyrics.is_empty() || query.total_weight <= 0.0 {
        return Alignment::default();
    }
    let mut sims = SimCache::new(query, lyrics);
    let mut pieces: Vec<Piece> = Vec::new();
    let mut pending = vec![(0usize, query.tokens.len())];
    while let Some((start, end)) = pending.pop() {
        if pieces.len() >= MAX_PIECES || end <= start {
            continue;
        }
        let Some(piece) = best_piece(query, lyrics, &mut sims, start, end) else { continue };
        if !pieces.is_empty() && piece.score < MIN_PIECE_SCORE {
            continue;
        }
        pending.push((start, piece.q_start));
        pending.push((piece.q_end, end));
        pieces.push(piece);
    }
    let Some(best) = pieces.first() else { return Alignment::default() };
    let best_line = best
        .line_credit
        .iter()
        .max_by(|a, b| a.1.total_cmp(b.1).then(b.0.cmp(a.0)))
        .and_then(|(&line, _)| lyrics.lines.get(line).cloned());
    Alignment {
        score: pieces.iter().map(|p| p.score).sum(),
        coverage: (pieces.iter().map(|p| p.credit).sum::<f32>() / query.total_weight).clamp(0.0, 1.0),
        longest_run: pieces.iter().map(|p| p.longest_run).max().unwrap_or(0),
        matched: pieces.iter().map(|p| p.matched).sum(),
        best_line,
    }
}

/// Smith–Waterman over `query[start..end]` against all lyrics.
fn best_piece(query: &Query, lyrics: &Lyrics, sims: &mut SimCache, start: usize, end: usize) -> Option<Piece> {
    let m = end - start;
    let n = lyrics.tokens.len();
    let width = n + 1;
    let mut h = vec![0f32; (m + 1) * width];
    let mut moves = vec![Move::Stop; (m + 1) * width];
    let mut best = (0f32, 0usize, 0usize);
    for i in 1..=m {
        let qi = start + i - 1;
        let w1 = query.weights[qi];
        for j in 1..=n {
            let lj = j - 1;
            let mut score = 0f32;
            let mut mv = Move::Stop;
            let mut consider = |value: f32, candidate: Move| {
                if value > score {
                    score = value;
                    mv = candidate;
                }
            };
            let sim = sims.single(qi, lj);
            consider(h[(i - 1) * width + j - 1] + gain(sim, w1), Move::Pair { q: 1, l: 1, sim });
            consider(h[(i - 1) * width + j] - GAP * w1, Move::SkipQuery);
            consider(h[i * width + j - 1] - GAP * lyrics.weights[lj], Move::SkipLyric);
            if sim < RUN_SIM {
                if i >= 2 {
                    let sim = sims.joined(&query.tokens[qi - 1..=qi], &lyrics.tokens[lj..=lj]);
                    if sim >= MERGE_SIM {
                        let w = w1 + query.weights[qi - 1];
                        consider(h[(i - 2) * width + j - 1] + gain(sim, w) - 0.1, Move::Pair { q: 2, l: 1, sim });
                    }
                }
                if j >= 2 {
                    let sim = sims.joined(&query.tokens[qi..=qi], &lyrics.tokens[lj - 1..=lj]);
                    if sim >= MERGE_SIM {
                        consider(h[(i - 1) * width + j - 2] + gain(sim, w1) - 0.1, Move::Pair { q: 1, l: 2, sim });
                    }
                }
                if i >= 2 && j >= 2 {
                    let sim = sims.joined(&query.tokens[qi - 1..=qi], &lyrics.tokens[lj - 1..=lj]);
                    if sim >= MERGE_SIM {
                        let w = w1 + query.weights[qi - 1];
                        consider(h[(i - 2) * width + j - 2] + gain(sim, w) - 0.1, Move::Pair { q: 2, l: 2, sim });
                    }
                }
            }
            h[i * width + j] = score;
            moves[i * width + j] = mv;
            if score > best.0 {
                best = (score, i, j);
            }
        }
    }
    let (score, mut i, mut j) = best;
    if score <= 0.0 {
        return None;
    }
    let q_end = start + i;
    let mut credit = 0f32;
    let mut matched = 0usize;
    let mut run = 0usize;
    let mut longest_run = 0usize;
    let mut line_credit: HashMap<usize, f32> = HashMap::new();
    while i > 0 && j > 0 {
        match moves[i * width + j] {
            Move::Stop => break,
            Move::Pair { q, l, sim } => {
                let (q, l) = (q as usize, l as usize);
                if sim >= MATCH_SIM {
                    let weight: f32 = (0..q).map(|k| query.weights[start + i - 1 - k]).sum();
                    credit += weight * sim;
                    matched += q;
                    for k in 0..l {
                        *line_credit.entry(lyrics.line_of[j - 1 - k]).or_default() += weight * sim / l as f32;
                    }
                }
                if sim >= RUN_SIM {
                    run += q;
                    longest_run = longest_run.max(run);
                } else {
                    run = 0;
                }
                i -= q;
                j -= l;
            }
            Move::SkipQuery => {
                run = 0;
                i -= 1;
            }
            Move::SkipLyric => {
                run = 0;
                j -= 1;
            }
        }
    }
    Some(Piece { score, q_start: start + i, q_end, credit, matched, longest_run, line_credit })
}

/// Substitution score: +weight for identical tokens, 0 at the match threshold,
/// down to −weight for unrelated tokens.
fn gain(sim: f32, weight: f32) -> f32 {
    weight * (2.5 * sim - 1.5).clamp(-1.0, 1.0)
}

fn token_weight(token: &str) -> f32 {
    if text::is_stopword(token) || token.chars().count() == 1 { 0.5 } else { 1.0 }
}

/// Memoized similarities between unique query and lyric tokens.
struct SimCache {
    q_ids: Vec<usize>,
    l_ids: Vec<usize>,
    l_unique: usize,
    single: Vec<f32>,
    joined: HashMap<(String, String), f32>,
}

impl SimCache {
    fn new(query: &Query, lyrics: &Lyrics) -> Self {
        let mut q_index: HashMap<&str, usize> = HashMap::new();
        let mut q_tokens: Vec<&Token> = Vec::new();
        let q_ids = query
            .tokens
            .iter()
            .map(|t| {
                *q_index.entry(&t.text).or_insert_with(|| {
                    q_tokens.push(t);
                    q_tokens.len() - 1
                })
            })
            .collect();
        let mut l_index: HashMap<&str, usize> = HashMap::new();
        let mut l_tokens: Vec<&Token> = Vec::new();
        let l_ids = lyrics
            .tokens
            .iter()
            .map(|t| {
                *l_index.entry(&t.text).or_insert_with(|| {
                    l_tokens.push(t);
                    l_tokens.len() - 1
                })
            })
            .collect();
        let mut single = Vec::with_capacity(q_tokens.len() * l_tokens.len());
        for q in &q_tokens {
            for l in &l_tokens {
                single.push(similarity(q, l));
            }
        }
        Self { q_ids, l_ids, l_unique: l_tokens.len(), single, joined: HashMap::new() }
    }

    fn single(&self, qi: usize, lj: usize) -> f32 {
        self.single[self.q_ids[qi] * self.l_unique + self.l_ids[lj]]
    }

    fn joined(&mut self, q: &[Token], l: &[Token]) -> f32 {
        let qa = Self::concat(q);
        let la = Self::concat(l);
        // Lengths far apart can never reach the merge threshold.
        let (ql, ll) = (qa.chars().count() as f32, la.chars().count() as f32);
        if ql.min(ll) < 0.6 * ql.max(ll) {
            return 0.0;
        }
        if let Some(&sim) = self.joined.get(&(qa.clone(), la.clone())) {
            return sim;
        }
        let sim = similarity(&Token::new(&qa), &Token::new(&la));
        self.joined.insert((qa, la), sim);
        sim
    }

    fn concat(tokens: &[Token]) -> String {
        tokens.iter().map(|t| t.text.as_str()).collect()
    }
}

/// Drop LRC timestamps such as "[01:23.45]" and "<01:23.45>" word stamps.
fn strip_timestamps(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '[' || c == '<' {
            let close = if c == '[' { ']' } else { '>' };
            let rest: String = chars.clone().take_while(|&x| x != close).collect();
            let is_stamp = !rest.is_empty() && rest.chars().all(|x| x.is_ascii_digit() || x == ':' || x == '.');
            if is_stamp {
                for _ in 0..=rest.chars().count() {
                    chars.next();
                }
                continue;
            }
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOHEMIAN: &str = "[00:00.15] Is this the real life? Is this just fantasy?\n\
        [00:07.13] Caught in a landslide, no escape from reality\n\
        [00:14.77] Open your eyes, look up to the skies and see\n\
        [00:25.37] I'm just a poor boy, I need no sympathy";

    const PURPLE_HAZE: &str = "Purple haze all in my brain\n\
        Lately things they don't seem the same\n\
        Actin' funny, but I don't know why\n\
        'Scuse me while I kiss the sky";

    const TINY_DANCER: &str = "Blue jean baby, L.A. lady, seamstress for the band\n\
        Hold me closer, tiny dancer\n\
        Count the headlights on the highway";

    fn run(transcript: &str, lyrics: &str) -> Alignment {
        align(&Query::new(&text::transcript_tokens(transcript)), &Lyrics::new(lyrics))
    }

    #[test]
    fn exact_lyrics_align_fully() {
        let a = run("is this the real life is this just fantasy", BOHEMIAN);
        assert!(a.coverage > 0.99, "{a:?}");
        assert_eq!(a.longest_run, 9);
        assert_eq!(a.best_line.as_deref(), Some("Is this the real life? Is this just fantasy?"));
    }

    #[test]
    fn altered_lyrics_still_align() {
        let a = run("is this the real life or is it fantasy", BOHEMIAN);
        assert!(a.coverage > 0.6, "{a:?}");
        assert!(a.longest_run >= 5);
    }

    #[test]
    fn misheard_lyrics_align_through_sound() {
        let a = run("hold me closer tony danza", TINY_DANCER);
        assert!(a.coverage > 0.8, "{a:?}");
        assert_eq!(a.best_line.as_deref(), Some("Hold me closer, tiny dancer"));
        let b = run("excuse me while i kiss this guy", PURPLE_HAZE);
        assert!(b.coverage > 0.6, "{b:?}");
        assert_eq!(b.best_line.as_deref(), Some("'Scuse me while I kiss the sky"));
    }

    #[test]
    fn unrelated_lyrics_score_low() {
        let a = run("is this the real life is this just fantasy", PURPLE_HAZE);
        assert!(a.coverage < 0.35, "{a:?}");
        let b = run("hold me closer tony danza", BOHEMIAN);
        assert!(b.coverage < 0.35, "{b:?}");
    }

    #[test]
    fn out_of_order_parts_are_both_counted() {
        let a =
            run("open your eyes look up to the skies and see caught in a landslide no escape from reality", BOHEMIAN);
        assert!(a.coverage > 0.9, "{a:?}");
    }

    #[test]
    fn timestamps_and_headers_are_ignored() {
        assert_eq!(strip_timestamps("[01:02.03] Hello <01:02.50> world"), " Hello  world");
        assert_eq!(strip_timestamps("[Chorus]"), "[Chorus]");
        let lyrics = Lyrics::new("[Chorus]\n[00:01.00] Hello world\n\n");
        assert_eq!(lyrics.lines, ["Hello world"]);
    }

    #[test]
    fn empty_inputs() {
        assert_eq!(run("", BOHEMIAN), Alignment::default());
        assert_eq!(run("hello", ""), Alignment::default());
    }
}
