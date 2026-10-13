//! Text normalization for query planning, alignment and candidate merging.
//!
//! Two forms of a transcript are used: *query words* keep the original letters
//! (remote search engines handle "wünscht" better than "wunscht"), *match tokens*
//! are folded to plain lowercase letters for alignment against lyrics.

use std::collections::HashSet;
use std::sync::LazyLock;

use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::is_combining_mark;

/// Vocables that carry no lyric information when they come in runs ("la la la").
const FILLER: &[&str] = &[
    "aah", "ah", "ay", "ayy", "ba", "da", "doo", "dum", "eh", "ha", "hey", "la", "na", "o", "oh", "oo", "ooh", "wo",
    "woah", "whoa", "woo", "ya", "yeah", "yeh",
];

/// Hesitations and humming that speech recognizers write out; dropped even alone.
const HESITATION: &[&str] = &["erm", "hm", "hmm", "mhm", "mm", "mmm", "uh", "uhm", "um"];

/// Frequent function words (English and German) that weigh less in alignment and
/// query selection.
static STOPWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "a", "all", "am", "an", "and", "are", "as", "at", "be", "but", "by", "can", "do", "for", "from", "got", "he",
        "her", "him", "his", "i", "if", "im", "in", "is", "it", "its", "just", "me", "my", "no", "not", "of", "oh",
        "on", "or", "so", "that", "the", "then", "there", "they", "this", "to", "up", "was", "we", "what", "when",
        "where", "will", "with", "you", "your", "an", "auf", "das", "dem", "den", "der", "die", "du", "ein", "eine",
        "er", "es", "fur", "ich", "ist", "mit", "nicht", "sie", "und", "wir", "zu",
    ]
    .into_iter()
    .collect()
});

/// Title words that mark a release nobody means when singing the song.
const NOISE_MARKERS: &[&str] = &[
    "karaoke",
    "instrumental",
    "in the style of",
    "originally performed",
    "made famous",
    "backing track",
    "tribute to",
    "cover of",
    "8 bit",
    "music box",
];

/// Words that turn a " - …" title suffix into version information.
const VERSION_WORDS: &[&str] = &[
    "remaster",
    "remix",
    "live",
    "edit",
    "version",
    "mix",
    "mono",
    "stereo",
    "acoustic",
    "demo",
    "recorded",
    "from",
    "single",
    "radio",
    "explicit",
    "clean",
    "instrumental",
    "karaoke",
    "cover",
    "bonus",
    "take",
    "session",
];

/// True for function words such as "the", "you", "und".
pub(crate) fn is_stopword(token: &str) -> bool {
    STOPWORDS.contains(token)
}

/// Lowercase, strip diacritics and expand ligatures ("Wünscht" → "wunscht", "ß" → "ss").
pub(crate) fn fold(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.nfkd() {
        if is_combining_mark(c) {
            continue;
        }
        match c {
            'ß' => out.push_str("ss"),
            'æ' | 'Æ' => out.push_str("ae"),
            'œ' | 'Œ' => out.push_str("oe"),
            'ø' | 'Ø' => out.push('o'),
            'đ' | 'Đ' => out.push('d'),
            'ł' | 'Ł' => out.push('l'),
            'þ' | 'Þ' => out.push_str("th"),
            'ı' => out.push('i'),
            _ => out.extend(c.to_lowercase()),
        }
    }
    out
}

/// Words for remote queries: lowercase, punctuation and bracketed annotations
/// ("[Music]", "(ooh)") removed, apostrophes joined ("I'm" → "im"), elongations
/// shortened, hesitations and filler runs dropped, loops collapsed.
pub(crate) fn query_words(text: &str) -> Vec<String> {
    let mut words = clean_words(text);
    collapse_repeats(&mut words);
    words
}

/// Alignment tokens of a transcript: [`query_words`], folded, CJK split per character.
pub(crate) fn transcript_tokens(text: &str) -> Vec<String> {
    to_tokens(query_words(text))
}

/// Alignment tokens of one lyric line. Repeats are kept: they are part of the song.
pub(crate) fn lyric_tokens(line: &str) -> Vec<String> {
    to_tokens(clean_words(line))
}

fn to_tokens(words: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(words.len());
    for word in words {
        let folded = fold(&word);
        if folded.chars().any(is_cjk) {
            // Chinese and Japanese are not space-separated: align per character.
            out.extend(folded.chars().filter(|c| c.is_alphanumeric()).map(String::from));
        } else if !folded.is_empty() {
            out.push(folded);
        }
    }
    out
}

fn clean_words(text: &str) -> Vec<String> {
    let stripped = strip_brackets(text);
    let mut words = Vec::new();
    let mut current = String::new();
    for c in stripped.chars() {
        if matches!(c, '\'' | '’' | '‘' | '`' | '´' | 'ʼ') {
            continue;
        }
        if c.is_alphanumeric() {
            current.extend(c.to_lowercase());
        } else if !current.is_empty() {
            words.push(shorten_elongation(&current));
            current.clear();
        }
    }
    if !current.is_empty() {
        words.push(shorten_elongation(&current));
    }
    words.retain(|w| !HESITATION.contains(&w.as_str()));
    drop_filler_runs(words)
}

/// Remove balanced `(…)`, `[…]` and `{…}` spans; unbalanced brackets become spaces.
fn strip_brackets(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut keep = vec![true; chars.len()];
    let mut stack: Vec<(char, usize)> = Vec::new();
    for (i, &c) in chars.iter().enumerate() {
        match c {
            '(' | '[' | '{' => stack.push((c, i)),
            ')' | ']' | '}' => {
                let open = match c {
                    ')' => '(',
                    ']' => '[',
                    _ => '{',
                };
                if let Some(pos) = stack.iter().rposition(|&(o, _)| o == open) {
                    let start = stack[pos].1;
                    stack.truncate(pos);
                    keep[start..=i].iter_mut().for_each(|k| *k = false);
                }
            }
            _ => {}
        }
    }
    chars
        .iter()
        .zip(keep)
        .map(|(&c, k)| if !k || matches!(c, '(' | '[' | '{' | ')' | ']' | '}' | '♪' | '♫') { ' ' } else { c })
        .collect()
}

/// "sooo" → "so", "baaaby" → "baby": letters repeated three or more times.
fn shorten_elongation(word: &str) -> String {
    let mut out = String::with_capacity(word.len());
    let mut last = None;
    let mut run = 0;
    for c in word.chars() {
        if Some(c) == last {
            run += 1;
        } else {
            last = Some(c);
            run = 1;
        }
        if run <= 2 || !c.is_alphabetic() {
            out.push(c);
        } else if run == 3 {
            // The run turned out to be an elongation: keep a single letter.
            out.pop();
        }
    }
    out
}

fn drop_filler_runs(words: Vec<String>) -> Vec<String> {
    let is_filler: Vec<bool> = words.iter().map(|w| FILLER.contains(&w.as_str())).collect();
    let mut out = Vec::with_capacity(words.len());
    let mut i = 0;
    while i < words.len() {
        if is_filler[i] {
            let end = (i..words.len()).find(|&j| !is_filler[j]).unwrap_or(words.len());
            if end - i == 1 {
                out.push(words[i].clone());
            }
            i = end;
        } else {
            out.push(words[i].clone());
            i += 1;
        }
    }
    out
}

/// Keep at most two consecutive copies of any one- to four-word phrase.
fn collapse_repeats(words: &mut Vec<String>) {
    for n in (1..=4).rev() {
        let mut i = 0;
        while i + 3 * n <= words.len() {
            if words[i..i + n] == words[i + n..i + 2 * n] && words[i..i + n] == words[i + 2 * n..i + 3 * n] {
                words.drain(i + 2 * n..i + 3 * n);
            } else {
                i += 1;
            }
        }
    }
}

fn is_cjk(c: char) -> bool {
    matches!(c as u32, 0x3040..=0x30FF | 0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF)
}

/// Title without featured artists and version decorations
/// ("Bohemian Rhapsody - Remastered 2011" → "Bohemian Rhapsody").
pub(crate) fn base_title(title: &str) -> String {
    let mut working = cut_featuring(title).to_string();
    let without_brackets = strip_brackets(&working);
    if without_brackets.chars().any(char::is_alphanumeric) {
        working = without_brackets;
    }
    if let Some(pos) = working.find(" - ") {
        let suffix = fold(&working[pos + 3..]);
        let is_version =
            suffix.split(|c: char| !c.is_alphanumeric()).any(|word| VERSION_WORDS.iter().any(|v| word.starts_with(v)));
        if pos > 0 && is_version {
            working.truncate(pos);
        }
    }
    let collapsed = working.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed =
        collapsed.trim_matches(|c: char| c.is_whitespace() || matches!(c, '-' | '–' | '—' | ':' | ',' | ';' | '/'));
    if trimmed.is_empty() { title.trim().to_string() } else { trimmed.to_string() }
}

/// Compact merge key of a title: folded alphanumerics of [`base_title`], no leading "the".
pub(crate) fn title_key(title: &str) -> String {
    let folded = fold(&base_title(title));
    let folded = folded.strip_prefix("the ").unwrap_or(&folded);
    let key: String = folded.chars().filter(|c| c.is_alphanumeric()).collect();
    if key.is_empty() { fold(title).chars().filter(|c| c.is_alphanumeric()).collect() } else { key }
}

/// Individual artist names of a display string ("Elton John & Britney Spears").
pub(crate) fn artist_names(artist: &str) -> Vec<String> {
    let mut parts = vec![cut_featuring(artist).to_string()];
    let mut featured = featured_part(artist);
    for sep in [",", "&", ";", "/", "+", "×", " x ", " and ", " with ", " vs. ", " vs "] {
        parts = parts.iter().flat_map(|p| split_ci(p, sep)).collect();
        featured = featured.iter().flat_map(|p| split_ci(p, sep)).collect();
    }
    let mut names: Vec<String> = Vec::new();
    for name in parts.into_iter().chain(featured) {
        let name = name.trim().trim_matches(|c: char| c == '(' || c == ')' || c == '[' || c == ']').trim();
        if !name.is_empty() && !names.iter().any(|n| n.eq_ignore_ascii_case(name)) {
            names.push(name.to_string());
        }
    }
    names
}

/// Merge keys of every artist in a display string.
pub(crate) fn artist_keys(artist: &str) -> Vec<String> {
    let mut keys: Vec<String> = artist_names(artist).iter().map(|n| artist_key(n)).filter(|k| !k.is_empty()).collect();
    keys.dedup();
    keys
}

fn artist_key(name: &str) -> String {
    let folded = fold(name);
    let folded = folded.trim();
    let folded = folded.strip_prefix("the ").unwrap_or(folded);
    folded.chars().filter(|c| c.is_alphanumeric()).collect()
}

/// True when the title or artist marks a karaoke, instrumental or tribute release.
pub(crate) fn is_noise_release(title: &str, artist: &str, album: Option<&str>) -> bool {
    let haystack = fold(&format!("{title} | {artist} | {}", album.unwrap_or_default()));
    let haystack: String = haystack.chars().map(|c| if c.is_alphanumeric() { c } else { ' ' }).collect();
    let haystack = format!(" {} ", haystack.split_whitespace().collect::<Vec<_>>().join(" "));
    NOISE_MARKERS.iter().any(|m| haystack.contains(&format!(" {m} ")))
}

const FEATURING: &[&str] =
    &[" feat. ", " feat ", " ft. ", " featuring ", "(feat", "[feat", "(ft.", "[ft.", "(with ", "[with "];

fn featuring_pos(text: &str) -> Option<usize> {
    // ASCII lowercasing keeps byte offsets, and every marker is ASCII.
    let lower = text.to_ascii_lowercase();
    FEATURING.iter().filter_map(|m| lower.find(m)).filter(|&p| p > 0).min()
}

fn cut_featuring(text: &str) -> &str {
    match featuring_pos(text) {
        Some(pos) => text[..pos].trim_end(),
        None => text,
    }
}

fn featured_part(text: &str) -> Vec<String> {
    let Some(pos) = featuring_pos(text) else { return Vec::new() };
    let rest = &text[pos..];
    let rest = rest.trim_start_matches(['(', '[', ' ']);
    let rest = match rest.find(char::is_whitespace) {
        Some(space) => &rest[space..],
        None => "",
    };
    let rest = rest.split([')', ']']).next().unwrap_or_default();
    vec![rest.trim().to_string()]
}

/// Split on `sep`, ignoring ASCII case.
fn split_ci(text: &str, sep: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let mut out = Vec::new();
    let mut start = 0;
    while let Some(found) = lower[start..].find(sep) {
        out.push(text[start..start + found].to_string());
        start += found + sep.len();
    }
    out.push(text[start..].to_string());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(text: &str) -> Vec<String> {
        query_words(text)
    }

    #[test]
    fn query_words_strip_punctuation_and_annotations() {
        assert_eq!(
            words("Is this the real life? Is this just fantasy?").join(" "),
            "is this the real life is this just fantasy"
        );
        assert_eq!(words("[Music] I'm (ooh) caught ♪ in a landslide").join(" "), "im caught in a landslide");
        assert_eq!(
            words("An Tagen wie diesen, wünscht man sich Unendlichkeit").join(" "),
            "an tagen wie diesen wünscht man sich unendlichkeit"
        );
        assert_eq!(words("rock 'n' roll – twenty-one").join(" "), "rock n roll twenty one");
    }

    #[test]
    fn query_words_drop_filler_and_hesitations() {
        assert_eq!(words("la la la somebody once told me na na na").join(" "), "somebody once told me");
        assert_eq!(words("oh oh oh hmm I wanna dance uh with somebody").join(" "), "i wanna dance with somebody");
        // A single "oh" can be a real word.
        assert_eq!(words("oh say can you see").join(" "), "oh say can you see");
        assert_eq!(words("sooo baaaby yeahhh").join(" "), "so baby yeah");
    }

    #[test]
    fn query_words_collapse_loops() {
        assert_eq!(words("thank you thank you thank you thank you").join(" "), "thank you thank you");
        assert_eq!(words("hop hop hop the world so high").join(" "), "hop hop the world so high");
        assert_eq!(words("twinkle twinkle little star").join(" "), "twinkle twinkle little star");
    }

    #[test]
    fn unbalanced_brackets_do_not_swallow_text() {
        assert_eq!(words("hello (world").join(" "), "hello world");
        assert_eq!(words("a ) b").join(" "), "a b");
    }

    #[test]
    fn tokens_fold_and_split_cjk() {
        assert_eq!(transcript_tokens("Wünscht man sich Unendlichkeit"), ["wunscht", "man", "sich", "unendlichkeit"]);
        assert_eq!(transcript_tokens("Straße"), ["strasse"]);
        assert_eq!(transcript_tokens("夜に駆ける"), ["夜", "に", "駆", "け", "る"]);
        assert!(transcript_tokens("[Music]").is_empty());
    }

    #[test]
    fn base_titles_drop_versions() {
        assert_eq!(base_title("Bohemian Rhapsody - Remastered 2011"), "Bohemian Rhapsody");
        assert_eq!(base_title("Bohemian Rhapsody (Muppets Version)"), "Bohemian Rhapsody");
        assert_eq!(base_title("Hold Me Closer (feat. Britney Spears)"), "Hold Me Closer");
        assert_eq!(base_title("Something Just Like This feat. Coldplay"), "Something Just Like This");
        assert_eq!(base_title("Twinkle, Twinkle, Little Star"), "Twinkle, Twinkle, Little Star");
        assert_eq!(base_title("Don't Stop - Me Now"), "Don't Stop - Me Now");
        assert_eq!(base_title("(Untitled)"), "(Untitled)");
        assert_eq!(base_title("Stayin' Alive"), "Stayin' Alive");
    }

    #[test]
    fn title_keys_merge_variants() {
        assert_eq!(title_key("Bohemian Rhapsody"), title_key("Bohemian Rhapsody - Remastered 2011"));
        assert_eq!(title_key("Twinkle Twinkle Little Star"), title_key("Twinkle, Twinkle, Little Star"));
        assert_eq!(title_key("Tage wie diese"), "tagewiediese");
        assert_eq!(title_key("The Scientist"), title_key("Scientist"));
        assert_ne!(title_key("Tiny Dancer"), title_key("Hold Me Closer"));
    }

    #[test]
    fn artist_names_split_credits() {
        assert_eq!(artist_names("Elton John & Britney Spears"), ["Elton John", "Britney Spears"]);
        assert_eq!(artist_names("Glee Cast (Ft. Jonathan Groff)"), ["Glee Cast", "Jonathan Groff"]);
        assert_eq!(artist_names("The Chainsmokers, Coldplay"), ["The Chainsmokers", "Coldplay"]);
        assert_eq!(artist_names("Calvin Harris feat. Dua Lipa"), ["Calvin Harris", "Dua Lipa"]);
        assert_eq!(artist_keys("The Chainsmokers & Coldplay"), ["chainsmokers", "coldplay"]);
        assert_eq!(artist_keys("Beyoncé"), artist_keys("Beyonce"));
        assert_eq!(artist_keys("Panic! At the Disco"), ["panicatthedisco"]);
    }

    #[test]
    fn noise_releases() {
        assert!(is_noise_release("Bohemian Rhapsody (Karaoke Version)", "Sing2Piano", None));
        assert!(is_noise_release("All Star", "Party Tyme", Some("Karaoke Hits")));
        assert!(is_noise_release("All Star (In the Style of Smash Mouth)", "Ameritz", None));
        assert!(!is_noise_release("All Star", "Smash Mouth", Some("Astro Lounge")));
        assert!(!is_noise_release("Instrumentalist", "Someone", None));
    }
}
