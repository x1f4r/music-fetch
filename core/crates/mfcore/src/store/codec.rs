//! Column encodings: stable enum names, integer conversions and JSON blobs.
//!
//! Enum columns use snake_case names that never change once released; the JSON
//! blobs are the serde form of the model records.

use std::borrow::Cow;

use serde::Deserialize;

use crate::model::{
    Candidate, CandidateTier, MelodyAlignment, Segment, SegmentKind, SessionKind, SessionStatus, SourceInfo, Tier,
    Track,
};

/// An enum stored as a fixed text value.
pub(crate) trait DbText: Sized + Copy {
    fn as_text(self) -> &'static str;
    fn from_text(text: &str) -> Option<Self>;
}

macro_rules! db_text {
    ($ty:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        impl DbText for $ty {
            fn as_text(self) -> &'static str {
                match self {
                    $($ty::$variant => $text,)+
                }
            }

            fn from_text(text: &str) -> Option<Self> {
                match text {
                    $($text => Some($ty::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

db_text!(SessionKind {
    Listen => "listen",
    SystemAudio => "system_audio",
    Hum => "hum",
    Link => "link",
    File => "file",
    Lyrics => "lyrics",
});

db_text!(SessionStatus {
    Running => "running",
    Matched => "matched",
    Candidates => "candidates",
    Completed => "completed",
    NoMatch => "no_match",
    Failed => "failed",
    Canceled => "canceled",
    Waiting => "waiting",
});

db_text!(SegmentKind {
    Track => "track",
    Unidentified => "unidentified",
    Speech => "speech",
    Silence => "silence",
});

db_text!(Tier {
    Confirmed => "confirmed",
    Likely => "likely",
    Possible => "possible",
    Unknown => "unknown",
});

db_text!(CandidateTier {
    Strong => "strong",
    Possible => "possible",
    LongShot => "long_shot",
});

/// SQLite integers are signed; durations beyond `i64::MAX` saturate.
pub(crate) fn to_db_int(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Negative values (never written by the store) read back as zero.
pub(crate) fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

pub(crate) fn to_u32(value: i64) -> u32 {
    u32::try_from(value.max(0)).unwrap_or(u32::MAX)
}

/// serde_json writes non-finite floats as `null`, which does not read back into
/// `f32`; scores are clamped to finite values before they are stored.
fn finite(value: f32) -> f32 {
    if value.is_finite() { value } else { 0.0 }
}

fn segment_is_finite(segment: &Segment) -> bool {
    segment.confidence.is_finite() && segment.evidence.iter().all(|e| e.score.is_finite())
}

fn candidate_is_finite(candidate: &Candidate) -> bool {
    candidate.score.is_finite() && candidate.sources.iter().all(|s| s.score.is_finite())
}

/// The segment with non-finite scores replaced by 0.
pub(crate) fn sanitize_segment(segment: &Segment) -> Cow<'_, Segment> {
    if segment_is_finite(segment) {
        return Cow::Borrowed(segment);
    }
    let mut owned = segment.clone();
    owned.confidence = finite(owned.confidence);
    for evidence in &mut owned.evidence {
        evidence.score = finite(evidence.score);
    }
    Cow::Owned(owned)
}

/// The candidate with non-finite scores replaced by 0. Alignment contours keep
/// their NaN frames; see [`decode_candidate`].
pub(crate) fn sanitize_candidate(candidate: &Candidate) -> Cow<'_, Candidate> {
    if candidate_is_finite(candidate) {
        return Cow::Borrowed(candidate);
    }
    let mut owned = candidate.clone();
    owned.score = finite(owned.score);
    for source in &mut owned.sources {
        source.score = finite(source.score);
    }
    Cow::Owned(owned)
}

pub(crate) fn encode<T: serde::Serialize>(value: &T) -> serde_json::Result<String> {
    serde_json::to_string(value)
}

pub(crate) fn decode_track(json: &str) -> serde_json::Result<Track> {
    serde_json::from_str(json)
}

pub(crate) fn decode_source(json: &str) -> serde_json::Result<SourceInfo> {
    serde_json::from_str(json)
}

pub(crate) fn decode_segment(json: &str) -> serde_json::Result<Segment> {
    serde_json::from_str(json)
}

/// Alignment contours mark unvoiced frames with NaN, stored as JSON `null`.
#[derive(Deserialize)]
struct StoredAlignment {
    frame_ms: u32,
    query: Vec<Option<f32>>,
    reference: Vec<Option<f32>>,
}

impl From<StoredAlignment> for MelodyAlignment {
    fn from(stored: StoredAlignment) -> Self {
        let restore = |frames: Vec<Option<f32>>| frames.into_iter().map(|f| f.unwrap_or(f32::NAN)).collect();
        MelodyAlignment {
            frame_ms: stored.frame_ms,
            query: restore(stored.query),
            reference: restore(stored.reference),
        }
    }
}

pub(crate) fn decode_candidate(json: &str) -> serde_json::Result<Candidate> {
    let mut value: serde_json::Value = serde_json::from_str(json)?;
    let alignment = value.get_mut("alignment").map(serde_json::Value::take).filter(|a| !a.is_null());
    let mut candidate: Candidate = serde_json::from_value(value)?;
    if let Some(alignment) = alignment {
        candidate.alignment = Some(serde_json::from_value::<StoredAlignment>(alignment)?.into());
    }
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CandidateSource, CandidateSourceKind, Evidence, EvidenceSource};

    fn round_trip<T: DbText + PartialEq + std::fmt::Debug>(all: &[T]) {
        let mut seen = std::collections::HashSet::new();
        for &value in all {
            let text = value.as_text();
            assert!(seen.insert(text), "duplicate text {text}");
            assert_eq!(T::from_text(text), Some(value));
        }
        assert_eq!(T::from_text("Nope"), None);
    }

    #[test]
    fn enum_text_round_trips() {
        use SessionKind as K;
        round_trip(&[K::Listen, K::SystemAudio, K::Hum, K::Link, K::File, K::Lyrics]);
        use SessionStatus as S;
        round_trip(&[S::Running, S::Matched, S::Candidates, S::Completed, S::NoMatch, S::Failed, S::Canceled]);
        round_trip(&[SegmentKind::Track, SegmentKind::Unidentified, SegmentKind::Speech, SegmentKind::Silence]);
        round_trip(&[Tier::Confirmed, Tier::Likely, Tier::Possible, Tier::Unknown]);
        round_trip(&[CandidateTier::Strong, CandidateTier::Possible, CandidateTier::LongShot]);
    }

    #[test]
    fn enum_text_is_stable() {
        assert_eq!(SessionKind::SystemAudio.as_text(), "system_audio");
        assert_eq!(SessionStatus::NoMatch.as_text(), "no_match");
        assert_eq!(CandidateTier::LongShot.as_text(), "long_shot");
    }

    #[test]
    fn integer_conversions_saturate() {
        assert_eq!(to_db_int(u64::MAX), i64::MAX);
        assert_eq!(to_db_int(42), 42);
        assert_eq!(to_u64(-5), 0);
        assert_eq!(to_u32(-1), 0);
        assert_eq!(to_u32(i64::MAX), u32::MAX);
    }

    fn candidate() -> Candidate {
        Candidate {
            track: Track { title: "Song".into(), artist: "Artist".into(), ..Default::default() },
            score: 0.7,
            tier: CandidateTier::Possible,
            sources: vec![CandidateSource { kind: CandidateSourceKind::Melody, score: 0.7, detail: None }],
            alignment: Some(MelodyAlignment {
                frame_ms: 50,
                query: vec![0.0, f32::NAN, 1.5],
                reference: vec![f32::NAN, 0.5, 1.0],
            }),
        }
    }

    #[test]
    fn candidate_alignment_keeps_unvoiced_frames() {
        let json = encode(&candidate()).unwrap();
        let back = decode_candidate(&json).unwrap();
        let alignment = back.alignment.unwrap();
        assert_eq!(alignment.frame_ms, 50);
        assert_eq!(alignment.query[0], 0.0);
        assert!(alignment.query[1].is_nan());
        assert!(alignment.reference[0].is_nan());
        assert_eq!(alignment.reference[2], 1.0);
    }

    #[test]
    fn candidate_without_alignment_decodes() {
        let mut c = candidate();
        c.alignment = None;
        let back = decode_candidate(&encode(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn non_finite_scores_are_sanitized() {
        let mut c = candidate();
        c.score = f32::NAN;
        c.sources[0].score = f32::INFINITY;
        let clean = sanitize_candidate(&c);
        assert!(matches!(clean, Cow::Owned(_)));
        let back = decode_candidate(&encode(clean.as_ref()).unwrap()).unwrap();
        assert_eq!(back.score, 0.0);
        assert_eq!(back.sources[0].score, 0.0);

        let segment = Segment {
            id: "s".into(),
            item_index: 0,
            start_ms: 0,
            end_ms: 1,
            kind: SegmentKind::Track,
            track: None,
            confidence: f32::NAN,
            tier: Tier::Unknown,
            evidence: vec![Evidence { source: EvidenceSource::Shazam, at_ms: 0, score: f32::NAN, detail: None }],
            alternates: vec![],
            source_url: None,
        };
        let clean = sanitize_segment(&segment);
        let back = decode_segment(&encode(clean.as_ref()).unwrap()).unwrap();
        assert_eq!(back.confidence, 0.0);
        assert_eq!(back.evidence[0].score, 0.0);
    }

    #[test]
    fn finite_values_are_borrowed() {
        assert!(matches!(sanitize_candidate(&candidate()), Cow::Borrowed(_)));
    }
}
