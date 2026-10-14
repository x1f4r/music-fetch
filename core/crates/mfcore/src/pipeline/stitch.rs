//! Timeline stitching (rules ported from 0.4). See docs/PIPELINES.md ("Mix strategy", step 5).
//!
//! 1. Bridge: Track(X) → short non-track piece → Track(X) becomes one piece when
//!    both gaps and the bridged piece fit the budget of the longer neighbour
//!    (< 10 s → 2 s, < 60 s → 5 s, < 600 s → 8 s, else 19 s). Long speech is
//!    never bridged: a host talking between two plays of a song keeps them apart.
//! 2. Merge: adjacent pieces of the same recording merge when their start
//!    estimates agree within 5 s or the gap fits the allowance (< 10 s → 2 s,
//!    < 60 s → 5 s, < 600 s → 12 s, else 30 s), unless the second is a replay
//!    (its own start estimate begins where the first ends). Adjacent non-track
//!    pieces of the same kind merge with the bridge budget.
//! 3. Overlaps are clamped (tracks win over gaps) and empty pieces dropped.
//!
//! Different ISRCs and different versions (remix vs original) never merge.

use crate::model::{Evidence, EvidenceSource, Segment, SegmentKind, Tier, Track};
use crate::pipeline::hints;

/// Start estimates closer than this describe the same play of a recording.
pub const SAME_START_MS: i64 = 5_000;
/// Most alternates kept on a piece.
const MAX_ALTERNATES: usize = 5;

/// A stretch of the timeline before it becomes a [`Segment`].
#[derive(Debug, Clone, PartialEq)]
pub struct Piece {
    pub id: String,
    pub item_index: u32,
    pub start_ms: u64,
    pub end_ms: u64,
    pub kind: SegmentKind,
    pub track: Option<Track>,
    pub evidence: Vec<Evidence>,
    pub alternates: Vec<Track>,
    /// Where the recording's own beginning sits in the media (probe time minus
    /// the recognizer's offset); may be negative when the media starts mid-track.
    pub track_start_ms: Option<i64>,
}

impl Piece {
    /// A piece without a track.
    pub fn gap(id: impl Into<String>, start_ms: u64, end_ms: u64, kind: SegmentKind) -> Self {
        Piece {
            id: id.into(),
            item_index: 0,
            start_ms,
            end_ms,
            kind,
            track: None,
            evidence: Vec::new(),
            alternates: Vec::new(),
            track_start_ms: None,
        }
    }

    pub fn len_ms(&self) -> u64 {
        self.end_ms.saturating_sub(self.start_ms)
    }

    fn is_track(&self) -> bool {
        self.kind == SegmentKind::Track && self.track.is_some()
    }

    /// The segment for this piece, with tier and confidence from its evidence.
    pub fn to_segment(&self, source_url: Option<String>) -> Segment {
        let (tier, confidence) = if self.is_track() { grade(&self.evidence) } else { (Tier::Unknown, 0.0) };
        Segment {
            id: self.id.clone(),
            item_index: self.item_index,
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            kind: self.kind,
            track: self.track.clone(),
            confidence,
            tier,
            evidence: self.evidence.clone(),
            alternates: self.alternates.clone(),
            source_url,
        }
    }
}

/// Stitch result: the new timeline and the ids that were merged away.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stitched {
    pub pieces: Vec<Piece>,
    pub removed: Vec<String>,
}

/// Bridge budget for the longer neighbour.
pub fn bridge_budget_ms(longer_ms: u64) -> u64 {
    match longer_ms {
        0..10_000 => 2_000,
        10_000..60_000 => 5_000,
        60_000..600_000 => 8_000,
        _ => 19_000,
    }
}

/// Same-recording merge allowance for the longer neighbour.
pub fn merge_allowance_ms(longer_ms: u64) -> u64 {
    match longer_ms {
        0..10_000 => 2_000,
        10_000..60_000 => 5_000,
        60_000..600_000 => 12_000,
        _ => 30_000,
    }
}

fn same_recording(a: &Piece, b: &Piece) -> bool {
    match (&a.track, &b.track) {
        (Some(x), Some(y)) => a.is_track() && b.is_track() && hints::same_recording(x, y),
        _ => false,
    }
}

/// Stitch a timeline. Pieces may arrive in any order.
pub fn stitch(pieces: Vec<Piece>) -> Stitched {
    let mut removed = Vec::new();
    let mut ordered = pieces;
    ordered.retain(|p| {
        let keep = p.end_ms > p.start_ms;
        if !keep {
            removed.push(p.id.clone());
        }
        keep
    });
    ordered.sort_by_key(|p| (p.start_ms, p.end_ms));

    // Pass 1: bridges.
    let mut bridged: Vec<Piece> = Vec::with_capacity(ordered.len());
    let mut slots: Vec<Option<Piece>> = ordered.into_iter().map(Some).collect();
    let mut i = 0;
    while i < slots.len() {
        let Some(mut current) = slots[i].take() else {
            i += 1;
            continue;
        };
        let mut next = i + 1;
        while current.is_track() && next + 1 < slots.len() {
            let (Some(bridge), Some(successor)) = (&slots[next], &slots[next + 1]) else { break };
            if bridge.is_track() || !same_recording(&current, successor) {
                break;
            }
            let budget = bridge_budget_ms(current.len_ms().max(successor.len_ms()));
            let gap_before = bridge.start_ms.saturating_sub(current.end_ms);
            let gap_after = successor.start_ms.saturating_sub(bridge.end_ms);
            if gap_before > budget || gap_after > budget || bridge.len_ms() > budget || is_replay(&current, successor) {
                break;
            }
            if let Some(bridge) = slots[next].take() {
                removed.push(bridge.id);
            }
            if let Some(successor) = slots[next + 1].take() {
                removed.push(successor.id.clone());
                absorb(&mut current, successor);
            }
            next += 2;
        }
        bridged.push(current);
        i = next;
    }

    // Pass 2: merge neighbours.
    let mut merged: Vec<Piece> = Vec::with_capacity(bridged.len());
    for piece in bridged {
        if let Some(last) = merged.last_mut()
            && can_merge(last, &piece)
        {
            removed.push(piece.id.clone());
            absorb(last, piece);
            continue;
        }
        merged.push(piece);
    }

    // Pass 3: clamp overlaps (tracks keep their extent over gaps).
    let mut out: Vec<Piece> = Vec::with_capacity(merged.len());
    for mut piece in merged {
        if let Some(last) = out.last_mut()
            && last.end_ms > piece.start_ms
        {
            if last.is_track() && !piece.is_track() {
                piece.start_ms = last.end_ms.min(piece.end_ms);
            } else {
                last.end_ms = piece.start_ms.max(last.start_ms);
            }
        }
        if piece.end_ms <= piece.start_ms {
            removed.push(piece.id);
            continue;
        }
        if out.last().is_some_and(|l| l.end_ms <= l.start_ms)
            && let Some(empty) = out.pop()
        {
            removed.push(empty.id);
        }
        out.push(piece);
    }
    removed.sort();
    removed.dedup();
    removed.retain(|id| !out.iter().any(|p| &p.id == id));
    Stitched { pieces: out, removed }
}

/// The right piece is a fresh play of the same recording: its start estimate
/// lies later than the left one's and at or after the left piece's end.
fn is_replay(left: &Piece, right: &Piece) -> bool {
    match (left.track_start_ms, right.track_start_ms) {
        (Some(a), Some(b)) => b - a > SAME_START_MS && b >= left.end_ms as i64 - SAME_START_MS,
        _ => false,
    }
}

fn can_merge(left: &Piece, right: &Piece) -> bool {
    let gap = right.start_ms.saturating_sub(left.end_ms);
    let longer = left.len_ms().max(right.len_ms());
    if left.is_track() || right.is_track() {
        if !same_recording(left, right) || is_replay(left, right) {
            return false;
        }
        let starts_agree =
            matches!((left.track_start_ms, right.track_start_ms), (Some(a), Some(b)) if (a - b).abs() <= SAME_START_MS);
        return starts_agree || gap <= merge_allowance_ms(longer);
    }
    left.kind == right.kind && gap <= bridge_budget_ms(longer)
}

/// Extend `target` over `other` and combine their evidence.
fn absorb(target: &mut Piece, other: Piece) {
    target.start_ms = target.start_ms.min(other.start_ms);
    target.end_ms = target.end_ms.max(other.end_ms);
    if target.track_start_ms.is_none() {
        target.track_start_ms = other.track_start_ms;
    }
    // Prefer the richer track description (enriched, with ids).
    if let (Some(mine), Some(theirs)) = (&target.track, &other.track)
        && richness(theirs) > richness(mine)
    {
        target.track = other.track.clone();
    }
    for evidence in other.evidence {
        if !target.evidence.iter().any(|e| e.source == evidence.source && e.at_ms == evidence.at_ms) {
            target.evidence.push(evidence);
        }
    }
    target.evidence.sort_by_key(|e| e.at_ms);
    for alternate in other.alternates {
        let known = target.track.as_ref().is_some_and(|t| hints::same_recording(t, &alternate))
            || target.alternates.iter().any(|a| hints::same_recording(a, &alternate));
        if !known && target.alternates.len() < MAX_ALTERNATES {
            target.alternates.push(alternate);
        }
    }
}

fn richness(track: &Track) -> usize {
    track.isrc.is_some() as usize * 4
        + track.ids.len()
        + track.duration_ms.is_some() as usize
        + track.artwork_url.is_some() as usize
        + track.links.iter().filter(|l| !l.is_search).count()
}

/// Tier and confidence of a track piece from its evidence:
/// two agreeing recognizer answers, or a tracklist hint verified locally or by a
/// recognizer → Confirmed 0.95; one recognizer answer → its own score (0.88
/// tight skew, 0.72 loose); a hint alone → Possible 0.55.
pub fn grade(evidence: &[Evidence]) -> (Tier, f32) {
    let recognizers: Vec<&Evidence> = evidence
        .iter()
        .filter(|e| matches!(e.source, EvidenceSource::Shazam | EvidenceSource::AudD | EvidenceSource::AcrCloud))
        .collect();
    let hint = evidence.iter().filter(|e| e.source == EvidenceSource::PageHint).map(|e| e.score).fold(0.0, f32::max);
    let metadata = evidence.iter().any(|e| e.source == EvidenceSource::Metadata);
    let user = evidence.iter().any(|e| e.source == EvidenceSource::User);
    if user {
        return (Tier::Confirmed, 1.0);
    }
    if metadata {
        let score =
            evidence.iter().filter(|e| e.source == EvidenceSource::Metadata).map(|e| e.score).fold(0.0, f32::max);
        return if score >= 0.95 { (Tier::Confirmed, score) } else { (Tier::Likely, score.max(0.6)) };
    }
    if recognizers.len() >= 2 || hint >= VERIFIED_HINT {
        return (Tier::Confirmed, 0.95);
    }
    if let Some(best) = recognizers.iter().map(|e| e.score).reduce(f32::max) {
        let tier = if best >= 0.7 { Tier::Likely } else { Tier::Possible };
        return (tier, best);
    }
    if hint > 0.0 {
        return (Tier::Possible, hint.min(0.55));
    }
    (Tier::Unknown, 0.0)
}

/// Score given to a tracklist hint confirmed by local matching or a recognizer.
pub const VERIFIED_HINT: f32 = 0.95;
/// Score given to an unverified tracklist hint.
pub const UNVERIFIED_HINT: f32 = 0.55;

#[cfg(test)]
mod tests;
