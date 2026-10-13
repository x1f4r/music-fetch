//! Track identity: fuzzy keys, tiered identity and the ISRC veto (ported from 0.4).
//!
//! Providers describe the same recording differently ("CADMIUM, Chris Linton" vs
//! "Chris Linton & CADMIUM", "Purple Rain (Remastered 2015)" vs "Purple Rain").
//! `fuzzy_key` collapses that drift; `identity_key` prefers stronger evidence
//! (ISRC, then provider ids) and `same_track` is the merge predicate used for
//! deduplication and timeline stitching.
//!
//! Deliberate differences from 0.4:
//! - Two tracks whose strongest ids come from different tiers (one has a provider
//!   id, the other only text, or ids from different providers) are compared by
//!   fuzzy key instead of never merging.
//! - Non-Latin letters survive normalization, so titles in Cyrillic, Greek or CJK
//!   no longer collapse to an empty key.
//! - A bare "x" separates artists only between spaces ("A x B"), so the band "X"
//!   keeps its name.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;

use crate::model::Track;

/// Provider ids that identify a recording, most trustworthy first, with the
/// prefix used in identity keys. Keys match `Track.ids`.
pub const PROVIDER_ID_PREFIXES: [(&str, &str); 6] = [
    ("acrcloud", "acr"),
    ("audd", "audd"),
    ("shazam", "shz"),
    ("deezer", "dz"),
    ("apple", "am"),
    ("spotify", "sp"),
];

/// Strength of the evidence behind an identity key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdentityTier {
    Isrc,
    ProviderId,
    Fuzzy,
    Unknown,
}

// Bracketed version suffixes: "(Remastered 2015)", "[Live]", "(Radio Edit)".
static PARENS_SUFFIX: LazyLock<Regex> = LazyLock::new(|| {
    regex(
        r"(?i)\s*[\(\[][^)\]]*\b(remaster(?:ed)?|remix|edit|radio\s+edit|extended|bonus\s+track|live|explicit|clean|version|acoustic|mono|stereo|deluxe|re[\- ]?record(?:ed)?)\b[^)\]]*[\)\]]",
    )
});

// Dash clauses: "Yesterday - Remastered 2009", "Song – Radio Edit".
static TRAILING_DASH: LazyLock<Regex> = LazyLock::new(|| {
    regex(
        r"(?i)\s[-–—]\s.*\b(remaster(?:ed)?|edit|version|mix|acoustic|re[\- ]?record(?:ed)?)\b.*$",
    )
});

// "feat. X" / "ft. X" / "(featuring X)" in either field.
static FEAT: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?i)\s*(?:[\(\[])?\s*\b(?:feat(?:\.|uring)?|ft\.?)\b.*?(?:[\)\]]|$)"));

// Separators between credited artists. Deliberately broad: providers use ",", "&",
// "and", "/", "+", ";", "×" and " x ". Unlike 0.4, a bare "x" only separates when
// surrounded by spaces, so "X" and "Malcolm X" stay intact.
static ARTIST_SEPARATORS: LazyLock<Regex> =
    LazyLock::new(|| regex(r"(?i)\s*(?:,|&|\band\b|/|\+|;|×)\s*|\s+x\s+"));

static LEADING_THE: LazyLock<Regex> = LazyLock::new(|| regex(r"(?i)^the\s+"));

fn regex(pattern: &str) -> Regex {
    // Patterns are compile-time constants covered by the tests below.
    Regex::new(pattern).expect("identity regex is valid")
}

/// `fuzzy::<artists>::<title>` with feat./remaster/edit noise removed.
///
/// Artists are split, cleaned, sorted and deduplicated, so credit order and
/// separators do not matter.
pub fn fuzzy_key(title: &str, artist: &str) -> String {
    format!(
        "fuzzy::{}::{}",
        normalize_artists(artist),
        normalize_title(title)
    )
}

/// The title part of the fuzzy key: no featured artists, no version suffixes,
/// no leading "The", lowercase letters and digits only.
pub fn normalize_title(title: &str) -> String {
    let mut working = FEAT.replace_all(title.trim(), "").into_owned();
    loop {
        let next = PARENS_SUFFIX.replace_all(&working, "").into_owned();
        if next == working {
            break;
        }
        working = next;
    }
    let working = TRAILING_DASH.replace(&working, "");
    let working = LEADING_THE.replace(&working, "");
    clean_token(&working)
}

/// The artist part of the fuzzy key: cleaned individual artists, sorted,
/// deduplicated and joined with `;`.
pub fn normalize_artists(artist: &str) -> String {
    let mut tokens = artist_tokens(artist);
    tokens.sort();
    tokens.dedup();
    tokens.join(";")
}

/// Cleaned individual artist names (unsorted, may contain duplicates).
fn artist_tokens(artist: &str) -> Vec<String> {
    let working = FEAT.replace_all(artist.trim(), "");
    ARTIST_SEPARATORS
        .split(&working)
        .map(|part| clean_token(&LEADING_THE.replace(part.trim(), "")))
        .filter(|token| !token.is_empty())
        .collect()
}

/// NFKD, diacritics stripped, lowercase, letters and digits only.
fn clean_token(text: &str) -> String {
    text.nfkd()
        .filter(|c| canonical_combining_class(*c) == 0)
        .flat_map(char::to_lowercase)
        .filter(|c| c.is_alphanumeric())
        .collect()
}

/// ISRC in canonical form (uppercase, no separators), if non-empty.
pub fn normalize_isrc(isrc: &str) -> Option<String> {
    let cleaned: String = isrc
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .flat_map(char::to_uppercase)
        .collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

fn provider_id<'a>(ids: &'a HashMap<String, String>, provider: &str) -> Option<&'a str> {
    ids.get(provider)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
}

/// Tiered identity from loose fields: `isrc::…`, else the first provider id in
/// `PROVIDER_ID_PREFIXES` order (`acr::…`, `shz::…`, …), else the fuzzy key.
pub fn tiered_identity(
    isrc: Option<&str>,
    ids: &HashMap<String, String>,
    title: &str,
    artist: &str,
) -> String {
    if let Some(isrc) = isrc.and_then(normalize_isrc) {
        return format!("isrc::{isrc}");
    }
    for (provider, prefix) in PROVIDER_ID_PREFIXES {
        if let Some(id) = provider_id(ids, provider) {
            return format!("{prefix}::{}", id.to_lowercase());
        }
    }
    fuzzy_key(title, artist)
}

/// `isrc::…`, else a provider id key, else the fuzzy key.
pub fn identity_key(track: &Track) -> String {
    tiered_identity(
        track.isrc.as_deref(),
        &track.ids,
        &track.title,
        &track.artist,
    )
}

/// The tier an identity key was built from.
pub fn identity_tier(key: &str) -> IdentityTier {
    if key.starts_with("isrc::") {
        return IdentityTier::Isrc;
    }
    if PROVIDER_ID_PREFIXES.iter().any(|(_, prefix)| {
        key.strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with("::"))
    }) {
        return IdentityTier::ProviderId;
    }
    if key.starts_with("fuzzy::") {
        return IdentityTier::Fuzzy;
    }
    IdentityTier::Unknown
}

/// Same recording? Different ISRCs veto; otherwise ids or fuzzy keys decide.
///
/// 1. Both have an ISRC: equal ISRCs merge, different ISRCs never do.
/// 2. Any provider both sides know with the same id merges; if they share
///    providers but every shared id differs, they are distinct catalog entries.
/// 3. Otherwise (no shared evidence) the fuzzy keys decide; an empty title never
///    matches.
pub fn same_track(a: &Track, b: &Track) -> bool {
    if let (Some(x), Some(y)) = (
        a.isrc.as_deref().and_then(normalize_isrc),
        b.isrc.as_deref().and_then(normalize_isrc),
    ) {
        return x == y;
    }
    let mut shared_provider = false;
    for (provider, _) in PROVIDER_ID_PREFIXES {
        if let (Some(x), Some(y)) = (provider_id(&a.ids, provider), provider_id(&b.ids, provider)) {
            if x.eq_ignore_ascii_case(y) {
                return true;
            }
            shared_provider = true;
        }
    }
    if shared_provider {
        return false;
    }
    let title = normalize_title(&a.title);
    !title.is_empty()
        && title == normalize_title(&b.title)
        && normalize_artists(&a.artist) == normalize_artists(&b.artist)
}

/// Sets `track.identity_key` from its current fields.
pub fn refresh_identity(track: &mut Track) {
    track.identity_key = identity_key(track);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(title: &str, artist: &str) -> Track {
        Track {
            title: title.into(),
            artist: artist.into(),
            ..Track::default()
        }
    }

    fn with_isrc(mut t: Track, isrc: &str) -> Track {
        t.isrc = Some(isrc.into());
        t
    }

    fn with_id(mut t: Track, provider: &str, id: &str) -> Track {
        t.ids.insert(provider.into(), id.into());
        t
    }

    /// Two (artist, title) pairs.
    type Pair = ((&'static str, &'static str), (&'static str, &'static str));

    // Golden table ported from 0.4 tests/test_identity.py (artist, title).
    const SAME_FUZZY_PAIRS: &[Pair] = &[
        (
            ("Prince", "Purple Rain"),
            ("Prince", "Purple Rain (Remastered 2015)"),
        ),
        (
            ("The Beatles", "Yesterday"),
            ("The Beatles", "Yesterday - Remastered 2015"),
        ),
        (
            ("CADMIUM, Chris Linton", "Slow Down"),
            ("Chris Linton & CADMIUM", "Slow Down"),
        ),
        (("A & B", "Anthem"), ("B and A", "Anthem")),
        (("Beyoncé", "Halo"), ("Beyonce", "Halo")),
        (("The Beatles", "Hey Jude"), ("Beatles", "Hey Jude")),
        (
            ("Drake ft. Rihanna", "What's My Name"),
            ("Drake", "What's My Name"),
        ),
        (
            ("Drake", "What's My Name (feat. Rihanna)"),
            ("Drake", "What's My Name"),
        ),
        (
            ("  Drake  ", "  whats my name  "),
            ("drake", "Whats My Name"),
        ),
    ];

    const DIFFERENT_FUZZY_PAIRS: &[Pair] = &[
        (("The Beatles", "Yesterday"), ("Boyz II Men", "Yesterday")),
        (("Prince", "Purple Rain"), ("Prince", "Raspberry Beret")),
        (("Ed Sheeran", "Halo"), ("Beyoncé", "Halo")),
        (
            ("The The", "This Is the Day"),
            ("Unknown", "This Is the Day"),
        ),
    ];

    #[test]
    fn fuzzy_key_collapses_equivalents() {
        for ((la, lt), (ra, rt)) in SAME_FUZZY_PAIRS {
            assert_eq!(
                fuzzy_key(lt, la),
                fuzzy_key(rt, ra),
                "should merge: {la}/{lt} vs {ra}/{rt}"
            );
        }
    }

    #[test]
    fn fuzzy_key_separates_distinct_tracks() {
        for ((la, lt), (ra, rt)) in DIFFERENT_FUZZY_PAIRS {
            assert_ne!(
                fuzzy_key(lt, la),
                fuzzy_key(rt, ra),
                "should stay separate: {la}/{lt} vs {ra}/{rt}"
            );
        }
    }

    #[test]
    fn fuzzy_key_shape() {
        assert_eq!(
            fuzzy_key("Purple Rain (Remastered 2015)", "Prince"),
            "fuzzy::prince::purplerain"
        );
        assert_eq!(
            fuzzy_key("Slow Down", "Chris Linton & CADMIUM"),
            "fuzzy::cadmium;chrislinton::slowdown"
        );
        assert_eq!(fuzzy_key("Song", "A & A"), "fuzzy::a::song");
        assert_eq!(fuzzy_key("Song - Radio Edit", "X"), "fuzzy::x::song");
        assert_eq!(
            fuzzy_key("Song [Live at Wembley] (Remastered)", "X"),
            "fuzzy::x::song"
        );
        assert_eq!(
            fuzzy_key("Song", "Fred again.. x Skrillex"),
            fuzzy_key("Song", "Skrillex & Fred again..")
        );
        assert_eq!(normalize_artists("Malcolm X"), "malcolmx");
        assert_eq!(normalize_artists("YOASOBI×Ado"), "ado;yoasobi");
    }

    #[test]
    fn non_latin_titles_keep_their_letters() {
        assert_eq!(normalize_title("Кино"), "кино");
        assert_ne!(
            fuzzy_key("夜に駆ける", "YOASOBI"),
            fuzzy_key("群青", "YOASOBI")
        );
        assert_eq!(normalize_title("ＦＵＬＬ ＷＩＤＴＨ"), "fullwidth");
    }

    #[test]
    fn tiered_identity_prefers_isrc() {
        let t = with_id(
            with_isrc(track("Some Title", "Some Artist"), "USRC17607839"),
            "shazam",
            "abc",
        );
        assert_eq!(identity_key(&t), "isrc::USRC17607839");
        assert_eq!(identity_tier(&identity_key(&t)), IdentityTier::Isrc);
    }

    #[test]
    fn tiered_identity_falls_back_to_provider_id() {
        let t = with_id(track("Some Title", "Some Artist"), "acrcloud", "ACR-42");
        assert_eq!(identity_key(&t), "acr::acr-42");
        assert_eq!(identity_tier(&identity_key(&t)), IdentityTier::ProviderId);
        let t = with_id(with_id(track("X", "Y"), "shazam", "s"), "acrcloud", "a");
        assert!(identity_key(&t).starts_with("acr::"));
        assert_eq!(
            identity_key(&with_id(track("X", "Y"), "deezer", "3135556")),
            "dz::3135556"
        );
    }

    #[test]
    fn tiered_identity_falls_back_to_fuzzy() {
        let a = track("Purple Rain (Remastered 2015)", "Prince");
        let b = track("Purple Rain", "Prince");
        assert!(identity_key(&a).starts_with("fuzzy::"));
        assert_eq!(identity_key(&a), identity_key(&b));
        assert_eq!(identity_tier(&identity_key(&a)), IdentityTier::Fuzzy);
        assert_eq!(identity_tier("something"), IdentityTier::Unknown);
    }

    #[test]
    fn blank_ids_and_isrcs_are_ignored() {
        let t = with_id(with_isrc(track("X", "Y"), "  "), "shazam", " ");
        assert!(identity_key(&t).starts_with("fuzzy::"));
    }

    #[test]
    fn isrc_normalization() {
        assert_eq!(
            normalize_isrc(" us-rc1-76-07839 ").as_deref(),
            Some("USRC17607839")
        );
        assert_eq!(normalize_isrc(""), None);
    }

    #[test]
    fn merges_remaster_and_live_variants() {
        assert!(same_track(
            &track("Purple Rain", "Prince"),
            &track("Purple Rain (Remastered 2015)", "Prince")
        ));
        assert!(same_track(
            &track("Purple Rain", "Prince"),
            &track("Purple Rain (Live)", "Prince")
        ));
    }

    #[test]
    fn distinct_isrcs_veto_even_when_fuzzy_key_matches() {
        let a = with_isrc(track("Purple Rain", "Prince"), "USRC17607839");
        let b = with_isrc(track("Purple Rain", "Prince"), "USRC12345678");
        assert!(!same_track(&a, &b));
    }

    #[test]
    fn shared_isrc_merges_case_insensitively() {
        let a = with_isrc(track("Purple Rain", "Prince"), "USRC17607839");
        let b = with_isrc(track("Purple Rain (Remastered)", "Prince"), "usrc17607839");
        assert!(same_track(&a, &b));
    }

    #[test]
    fn shared_provider_id_merges_despite_different_titles() {
        let a = with_id(track("Track", "Artist"), "acrcloud", "acr-1");
        let b = with_id(track("different title", "Artist"), "acrcloud", "acr-1");
        assert!(same_track(&a, &b));
    }

    #[test]
    fn any_shared_provider_id_merges() {
        let a = with_id(with_id(track("A", "B"), "acrcloud", "1"), "shazam", "5");
        let b = with_id(with_id(track("A", "B"), "acrcloud", "2"), "shazam", "5");
        assert!(same_track(&a, &b));
    }

    #[test]
    fn different_ids_from_the_same_provider_do_not_merge() {
        let a = with_id(track("Purple Rain", "Prince"), "shazam", "1");
        let b = with_id(track("Purple Rain", "Prince"), "shazam", "2");
        assert!(!same_track(&a, &b));
    }

    #[test]
    fn cross_tier_falls_back_to_fuzzy_key() {
        // Provider id on one side only: 0.4 never merged these.
        let a = with_id(track("Slow Down", "CADMIUM, Chris Linton"), "shazam", "123");
        let b = track("Slow Down", "Chris Linton & CADMIUM");
        assert!(same_track(&a, &b));
        // Ids from different providers.
        let c = with_id(
            track("Slow Down", "Chris Linton & CADMIUM"),
            "acrcloud",
            "x",
        );
        assert!(same_track(&a, &c));
        // ISRC on one side only.
        let d = with_isrc(track("Slow Down", "CADMIUM"), "GBX1");
        assert!(!same_track(&d, &track("Other", "CADMIUM")));
        assert!(same_track(
            &with_isrc(track("Slow Down", "CADMIUM"), "GBX1"),
            &track("Slow Down", "CADMIUM")
        ));
    }

    #[test]
    fn separates_different_songs() {
        assert!(!same_track(
            &track("Purple Rain", "Prince"),
            &track("Raspberry Beret", "Prince")
        ));
        assert!(!same_track(&track("", "Prince"), &track("", "Prince")));
    }

    #[test]
    fn refresh_sets_key() {
        let mut t = with_isrc(track("X", "Y"), "abc");
        refresh_identity(&mut t);
        assert_eq!(t.identity_key, "isrc::ABC");
    }
}
