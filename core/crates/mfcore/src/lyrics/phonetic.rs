//! Fuzzy token similarity: edit distance plus an English phonetic key, so that
//! misheard words ("tony danza" / "tiny dancer") still count as matches.

/// A folded token with its precomputed phonetic key.
#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub text: String,
    chars: Vec<char>,
    key: Vec<u8>,
}

impl Token {
    pub fn new(text: &str) -> Self {
        Self { text: text.to_string(), chars: text.chars().collect(), key: phonetic_key(text) }
    }
}

/// Similarity in 0..=1: 1 for identical tokens, high for near spellings and for
/// words that sound alike.
pub(crate) fn similarity(a: &Token, b: &Token) -> f32 {
    if a.text == b.text {
        return 1.0;
    }
    let longest = a.chars.len().max(b.chars.len());
    if longest == 0 {
        return 0.0;
    }
    let edit = 1.0 - levenshtein(&a.chars, &b.chars) as f32 / longest as f32;
    let sound = if a.key.is_empty() || b.key.is_empty() {
        0.0
    } else if a.key == b.key {
        // One-consonant keys ("a", "I", "oh") collide too easily to mean much.
        if a.key.len() == 1 { 0.55 } else { 0.9 }
    } else if a.key.len().min(b.key.len()) >= 2 && (a.key.starts_with(&b.key) || b.key.starts_with(&a.key)) {
        // A clipped ending ("danza" / "dancer") is a common mishearing.
        let (short, long) = (a.key.len().min(b.key.len()) as f32, a.key.len().max(b.key.len()) as f32);
        0.1 + 0.8 * short / long
    } else {
        let longest = a.key.len().max(b.key.len());
        0.85 * (1.0 - levenshtein(&a.key, &b.key) as f32 / longest as f32).powf(1.5)
    };
    edit.max(sound)
}

/// Levenshtein distance over any comparable symbols.
pub(crate) fn levenshtein<T: PartialEq>(a: &[T], b: &[T]) -> usize {
    if a.is_empty() {
        return b.len();
    }
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, x) in a.iter().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, y) in b.iter().enumerate() {
            let above = row[j + 1];
            row[j + 1] = if x == y { diagonal } else { 1 + diagonal.min(above).min(row[j]) };
            diagonal = above;
        }
    }
    row[b.len()]
}

/// Metaphone-style key for an English word. Voiced and unvoiced pairs are merged
/// (b/p, d/t, g/k, v/f, z/s) because sung consonants blur; vowels are dropped
/// except a leading one. Tokens outside a–z return an empty key.
pub(crate) fn phonetic_key(word: &str) -> Vec<u8> {
    if word.is_empty() || !word.bytes().all(|b| b.is_ascii_lowercase()) {
        return Vec::new();
    }
    let mut w = word.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(w.len());
    let push = |out: &mut Vec<u8>, code: u8| {
        if out.last() != Some(&code) {
            out.push(code);
        }
    };
    // Silent initial letters.
    if w.len() > 1 && matches!(&w[..2], b"kn" | b"gn" | b"pn" | b"wr" | b"ps") {
        w = &w[1..];
    }
    let at = |i: usize| w.get(i).copied().unwrap_or(0);
    let vowel = |c: u8| matches!(c, b'a' | b'e' | b'i' | b'o' | b'u');
    let front = |c: u8| matches!(c, b'e' | b'i' | b'y');
    let n = w.len();
    let mut i = 0;
    while i < n {
        let c = w[i];
        let next = at(i + 1);
        let mut step = 1;
        match c {
            b'a' | b'e' | b'i' | b'o' | b'u' => {
                if i == 0 {
                    push(&mut out, b'A');
                }
            }
            b'b' => {
                if !(i == n - 1 && i > 0 && w[i - 1] == b'm') {
                    push(&mut out, b'P');
                }
            }
            b'c' => {
                if next == b'h' {
                    let sch = i > 0 && w[i - 1] == b's';
                    push(&mut out, if sch { b'K' } else { b'X' });
                    step = 2;
                } else if front(next) {
                    push(&mut out, b'S');
                } else if next != b'k' {
                    push(&mut out, b'K');
                }
            }
            b'd' => {
                if next == b'g' && front(at(i + 2)) {
                    push(&mut out, b'J');
                    step = 3;
                } else {
                    push(&mut out, b'T');
                }
            }
            b'f' | b'v' => push(&mut out, b'F'),
            b'g' => {
                if next == b'h' {
                    // "ghost" keeps a hard g; "night" and "though" are silent.
                    if i == 0 {
                        push(&mut out, b'K');
                    }
                    step = 2;
                } else if next == b'n' && (i + 2 == n || (i + 4 == n && &w[i + 2..] == b"ed")) {
                    // "sign", "signed": silent.
                } else if front(next) {
                    push(&mut out, b'J');
                } else {
                    push(&mut out, b'K');
                }
            }
            b'h' => {
                let after_modifier = i > 0 && matches!(w[i - 1], b'c' | b'g' | b'p' | b's' | b't' | b'w');
                if vowel(next) && !after_modifier {
                    push(&mut out, b'H');
                }
            }
            b'j' => push(&mut out, b'J'),
            b'k' | b'q' => push(&mut out, b'K'),
            b'l' => push(&mut out, b'L'),
            b'm' => push(&mut out, b'M'),
            b'n' => push(&mut out, b'N'),
            b'p' => {
                if next == b'h' {
                    push(&mut out, b'F');
                    step = 2;
                } else {
                    push(&mut out, b'P');
                }
            }
            b'r' => push(&mut out, b'R'),
            b's' => {
                if next == b'h' {
                    push(&mut out, b'X');
                    step = 2;
                } else if next == b'i' && matches!(at(i + 2), b'o' | b'a') {
                    push(&mut out, b'X');
                } else {
                    push(&mut out, b'S');
                }
            }
            b't' => {
                if next == b'h' {
                    push(&mut out, b'0');
                    step = 2;
                } else if next == b'i' && matches!(at(i + 2), b'o' | b'a') {
                    push(&mut out, b'X');
                } else if !(next == b'c' && at(i + 2) == b'h') {
                    push(&mut out, b'T');
                }
            }
            b'w' | b'y' => {
                if i == 0 && vowel(next) {
                    push(&mut out, if c == b'w' { b'W' } else { b'Y' });
                }
            }
            b'x' => {
                if i == 0 {
                    push(&mut out, b'S');
                } else {
                    push(&mut out, b'K');
                    out.push(b'S');
                }
            }
            b'z' => push(&mut out, b'S'),
            _ => {}
        }
        i += step;
    }
    // Plural and verb "-s" endings are routinely lost or added by recognizers.
    if out.len() >= 3 && out.last() == Some(&b'S') && word.ends_with('s') && !word.ends_with("ss") {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(word: &str) -> String {
        String::from_utf8(phonetic_key(word)).unwrap_or_default()
    }

    fn sim(a: &str, b: &str) -> f32 {
        similarity(&Token::new(a), &Token::new(b))
    }

    #[test]
    fn levenshtein_basics() {
        assert_eq!(levenshtein(b"kitten", b"sitting"), 3);
        assert_eq!(levenshtein(b"", b"abc"), 3);
        assert_eq!(levenshtein(b"abc", b""), 3);
        assert_eq!(levenshtein(b"same", b"same"), 0);
    }

    #[test]
    fn keys_merge_sound_alikes() {
        assert_eq!(key("tony"), key("tiny"));
        assert_eq!(key("knight"), key("night"));
        assert_eq!(key("phone"), key("fone"));
        assert_eq!(key("legends"), key("legend"));
        assert_eq!(key("books"), key("book"));
        assert_eq!(key("thisguy"), key("thesky"));
        assert_eq!(key("kiss"), "KS");
        assert_eq!(key("the"), "0");
        assert!(key("夜").is_empty());
    }

    #[test]
    fn similarity_orders_sensibly() {
        assert_eq!(sim("fantasy", "fantasy"), 1.0);
        assert!(sim("tony", "tiny") >= 0.75);
        assert!(sim("danza", "dancer") >= 0.6);
        assert!(sim("commitments", "commitment") >= 0.9);
        assert!(sim("wiser", "wise") >= 0.75);
        assert!(sim("landslide", "fantasy") < 0.4);
        assert!(sim("a", "i") < 0.6);
    }
}
