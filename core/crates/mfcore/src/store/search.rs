//! Library search text.
//!
//! Each session keeps two denormalized, folded columns: `search_text` (its own
//! title, subtitle, track and source) and `search_tracks` (identified segment
//! tracks and the top candidate). A query matches when every term is a
//! substring of either column. Folding is done here rather than in SQL because
//! SQLite's `LIKE`/`lower()` only fold ASCII.

use crate::model::{SourceInfo, Track};

/// Queries are split into at most this many terms.
const MAX_TERMS: usize = 8;

/// Lowercase, strip Latin diacritics, drop apostrophes and turn all other
/// punctuation into single spaces, so "Beyoncé – Don't" folds to "beyonce dont".
pub(crate) fn fold(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for c in text.chars().flat_map(char::to_lowercase) {
        if matches!(c, '\'' | '\u{2019}' | '\u{02BC}') || is_combining_mark(c) {
            continue;
        }
        if !c.is_alphanumeric() {
            pending_space = !out.is_empty();
            continue;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
        }
        match base_letter(c) {
            Some(base) => out.push_str(base),
            None => out.push(c),
        }
    }
    out
}

/// Folded query terms, deduplicated, at most [`MAX_TERMS`].
pub(crate) fn terms(query: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for term in fold(query).split(' ').filter(|t| !t.is_empty()) {
        if out.len() == MAX_TERMS {
            break;
        }
        if !out.iter().any(|t| t == term) {
            out.push(term.to_string());
        }
    }
    out
}

/// Folds each part and joins the non-empty ones with spaces.
pub(crate) fn join_folded<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    let mut out = String::new();
    for part in parts {
        let folded = fold(part);
        if folded.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(&folded);
    }
    out
}

/// Searchable text of a track: title, artist and album.
pub(crate) fn track_text(track: &Track) -> String {
    join_folded([track.title.as_str(), track.artist.as_str(), track.album.as_deref().unwrap_or("")])
}

/// Searchable text of a session's own fields.
pub(crate) fn session_text(
    title: &str,
    subtitle: Option<&str>,
    track: Option<&Track>,
    source: Option<&SourceInfo>,
) -> String {
    let track = track.map(track_text).unwrap_or_default();
    let (source_title, uploader, site) =
        source.map(|s| (s.title.as_deref(), s.uploader.as_deref(), s.site.as_deref())).unwrap_or_default();
    join_folded([
        title,
        subtitle.unwrap_or(""),
        &track,
        source_title.unwrap_or(""),
        uploader.unwrap_or(""),
        site.unwrap_or(""),
    ])
}

/// Key used to count distinct identified tracks. Falls back to the folded
/// artist and title when the pipeline did not set `identity_key`.
pub(crate) fn identity_key(track: Option<&Track>, segment_id: &str) -> String {
    match track {
        Some(t) if !t.identity_key.is_empty() => t.identity_key.clone(),
        Some(t) if !t.title.trim().is_empty() => {
            format!("fold::{}::{}", fold(&t.artist), fold(&t.title))
        }
        _ => format!("segment::{segment_id}"),
    }
}

fn is_combining_mark(c: char) -> bool {
    matches!(c, '\u{0300}'..='\u{036F}' | '\u{1AB0}'..='\u{1AFF}' | '\u{1DC0}'..='\u{1DFF}' | '\u{20D0}'..='\u{20FF}')
}

/// ASCII base for lowercase Latin-1 Supplement and Latin Extended-A letters.
fn base_letter(c: char) -> Option<&'static str> {
    Some(match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => "a",
        'æ' => "ae",
        'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => "c",
        'ď' | 'đ' | 'ð' => "d",
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => "e",
        'ĝ' | 'ğ' | 'ġ' | 'ģ' => "g",
        'ĥ' | 'ħ' => "h",
        'ì' | 'í' | 'î' | 'ï' | 'ĩ' | 'ī' | 'ĭ' | 'į' | 'ı' => "i",
        'ĳ' => "ij",
        'ĵ' => "j",
        'ķ' => "k",
        'ĺ' | 'ļ' | 'ľ' | 'ŀ' | 'ł' => "l",
        'ñ' | 'ń' | 'ņ' | 'ň' | 'ŉ' => "n",
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ŏ' | 'ő' => "o",
        'œ' => "oe",
        'ŕ' | 'ŗ' | 'ř' => "r",
        'ś' | 'ŝ' | 'ş' | 'š' | 'ș' => "s",
        'ß' => "ss",
        'ţ' | 'ť' | 'ŧ' | 'ț' => "t",
        'ù' | 'ú' | 'û' | 'ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => "u",
        'ŵ' => "w",
        'ý' | 'ÿ' | 'ŷ' => "y",
        'ź' | 'ż' | 'ž' => "z",
        'þ' => "th",
        'ς' => "σ",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_lowercases_and_strips_diacritics() {
        assert_eq!(fold("Beyoncé"), "beyonce");
        assert_eq!(fold("Mötley Crüe"), "motley crue");
        assert_eq!(fold("Sigur Rós"), "sigur ros");
        assert_eq!(fold("STRAẞE"), "strasse");
        assert_eq!(fold("Ørjan Nilsen"), "orjan nilsen");
    }

    #[test]
    fn fold_handles_decomposed_input() {
        assert_eq!(fold("Beyonce\u{0301}"), "beyonce");
    }

    #[test]
    fn fold_normalizes_punctuation() {
        assert_eq!(fold("  Daft Punk — One More Time!! "), "daft punk one more time");
        assert_eq!(fold("Don't Stop Me Now"), "dont stop me now");
        assert_eq!(fold("Don’t"), "dont");
        assert_eq!(fold("AC/DC"), "ac dc");
        assert_eq!(fold("..."), "");
    }

    #[test]
    fn fold_keeps_non_latin_scripts() {
        assert_eq!(fold("Пётр"), "пётр");
        assert_eq!(fold("坂本龍一"), "坂本龍一");
        assert_eq!(fold("ΜΟΥΣΙΚΗ"), "μουσικη");
        assert_eq!(fold("ΟΔΟΣ"), fold("οδος"));
    }

    #[test]
    fn terms_are_deduplicated_and_capped() {
        assert_eq!(terms("Daft  daft PUNK"), vec!["daft", "punk"]);
        assert!(terms("  - ").is_empty());
        assert_eq!(terms("a b c d e f g h i j").len(), MAX_TERMS);
    }

    #[test]
    fn session_text_includes_track_and_source() {
        let track = Track { title: "Halo".into(), artist: "Beyoncé".into(), ..Default::default() };
        let source =
            SourceInfo { title: Some("Live set".into()), uploader: Some("Boiler Room".into()), ..Default::default() };
        assert_eq!(
            session_text("My mix", Some("2 tracks"), Some(&track), Some(&source)),
            "my mix 2 tracks halo beyonce live set boiler room"
        );
    }

    #[test]
    fn identity_key_falls_back() {
        let mut track = Track { title: "Halo".into(), artist: "Beyoncé".into(), ..Default::default() };
        assert_eq!(identity_key(Some(&track), "s1"), "fold::beyonce::halo");
        track.identity_key = "isrc::USSM10804556".into();
        assert_eq!(identity_key(Some(&track), "s1"), "isrc::USSM10804556");
        assert_eq!(identity_key(None, "s1"), "segment::s1");
    }
}
