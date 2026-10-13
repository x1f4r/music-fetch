//! The melody index file: song metadata plus reference phrases as notes, with
//! precomputed n-gram postings.
//!
//! Layout (little-endian; `.mfi`, format 1):
//!
//! ```text
//! header   160 B   magic "MFMELODY", format, counts, version string, 7 section (offset, len) pairs
//! STRS             UTF-8 string table (titles, artists, version)
//! SONG     40 B    title (off u32, len u16), artist (len u16, off u32), ISRC [12], Deezer id u64,
//!                  year u16, popularity u16 (×65535), first phrase u32
//! PHRS     16 B    song u32, start_ms u32, notes byte offset u32, note count u16, source u8, base pitch u8
//! NOTE             per note: pitch u8 (quarter tones above the phrase base), onset delta and
//!                  duration as LEB128 varints in 10 ms units
//! KEYS     12 B    n-gram key u32, postings byte offset u32, posting count u32 (sorted by key)
//! POST             per posting: phrase delta varint, collapsed-note position varint
//! TITL     12 B    (hash u64, song u32) sorted by hash: title+artist keys, then title-only keys
//! ```
//!
//! N-grams are taken over the phrase's notes with repeated pitches collapsed
//! ("pitch changes"): interval 4-grams (exact semitones, clipped to ±12) and
//! coarse contour 6-grams (±1–2, ±3–4, ±5+). Opening maps the file and checks
//! the header; nothing is decoded until used.

use std::fs::File;
use std::path::Path;

use crate::error::{MfError, Result};
use crate::melody::notes::Note;

pub(crate) const MAGIC: &[u8; 8] = b"MFMELODY";
pub(crate) const FORMAT: u32 = 1;
pub(crate) const HEADER_LEN: usize = 160;
pub(crate) const SECTIONS: usize = 7;
pub(crate) const SONG_LEN: usize = 40;
pub(crate) const PHRASE_LEN: usize = 16;
pub(crate) const KEY_LEN: usize = 12;
pub(crate) const TITLE_LEN: usize = 12;
/// Time unit of stored notes.
pub(crate) const NOTE_TICK_MS: u32 = 10;

pub(crate) const S_STRS: usize = 0;
pub(crate) const S_SONG: usize = 1;
pub(crate) const S_PHRS: usize = 2;
pub(crate) const S_NOTE: usize = 3;
pub(crate) const S_KEYS: usize = 4;
pub(crate) const S_POST: usize = 5;
pub(crate) const S_TITL: usize = 6;

/// Interval n-gram length (intervals; one more note).
pub(crate) const INTERVAL_N: usize = 4;
/// Contour n-gram length (intervals).
pub(crate) const CONTOUR_N: usize = 6;
const KIND_INTERVAL: u32 = 0;
const KIND_CONTOUR: u32 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct SongMeta {
    pub id: u32,
    pub title: String,
    pub artist: String,
    pub isrc: Option<String>,
    pub deezer_id: Option<u64>,
    pub year: Option<u16>,
    /// 0..=1, used as a prior.
    pub popularity: f32,
}

/// Where a phrase's notes come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhraseSource {
    Preview,
    Audio,
    Midi,
}

impl PhraseSource {
    pub(crate) fn code(self) -> u8 {
        match self {
            PhraseSource::Preview => 0,
            PhraseSource::Audio => 1,
            PhraseSource::Midi => 2,
        }
    }

    fn from_code(c: u8) -> Self {
        match c {
            1 => PhraseSource::Audio,
            2 => PhraseSource::Midi,
            _ => PhraseSource::Preview,
        }
    }
}

/// A reference phrase with its decoded notes.
#[derive(Debug, Clone)]
pub struct Phrase {
    pub id: u32,
    pub song: u32,
    pub start_ms: u32,
    pub source: PhraseSource,
    pub notes: Vec<Note>,
}

enum Bytes {
    Mapped(memmap2::Mmap),
    Owned(Vec<u8>),
}

impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Bytes::Mapped(m) => m,
            Bytes::Owned(v) => v,
        }
    }
}

/// An open melody index. All reads are bounds-checked; a damaged file yields
/// missing songs or phrases, never a panic.
pub struct MelodyIndex {
    bytes: Bytes,
    sections: [(usize, usize); SECTIONS],
    song_count: u32,
    phrase_count: u32,
    note_count: u32,
    key_count: u32,
    version: String,
}

impl MelodyIndex {
    /// Map an index file and check its header.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|_| MfError::IndexUnavailable)?;
        // Safety: the index is written once to a temp file and renamed into place;
        // nothing modifies it while mapped.
        let bytes = match unsafe { memmap2::Mmap::map(&file) } {
            Ok(map) => Bytes::Mapped(map),
            Err(_) => Bytes::Owned(std::fs::read(path).map_err(|_| MfError::IndexUnavailable)?),
        };
        Self::parse(bytes)
    }

    /// Open an index held in memory.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::parse(Bytes::Owned(bytes))
    }

    fn parse(bytes: Bytes) -> Result<Self> {
        let bad = || MfError::Internal {
            message: "the melody index is damaged".into(),
        };
        if bytes.len() < HEADER_LEN || &bytes[..8] != MAGIC {
            return Err(bad());
        }
        let format = le_u32(&bytes, 8).ok_or_else(bad)?;
        if format != FORMAT {
            return Err(MfError::Internal {
                message: "the melody index needs a newer app version".into(),
            });
        }
        let header_len = le_u32(&bytes, 12).ok_or_else(bad)? as usize;
        let song_count = le_u32(&bytes, 16).ok_or_else(bad)?;
        let phrase_count = le_u32(&bytes, 20).ok_or_else(bad)?;
        let note_count = le_u32(&bytes, 24).ok_or_else(bad)?;
        let key_count = le_u32(&bytes, 28).ok_or_else(bad)?;
        let version_off = le_u32(&bytes, 32).ok_or_else(bad)? as usize;
        let version_len = le_u32(&bytes, 36).ok_or_else(bad)? as usize;
        let mut sections = [(0usize, 0usize); SECTIONS];
        for (i, s) in sections.iter_mut().enumerate() {
            let off = le_u64(&bytes, 48 + 16 * i).ok_or_else(bad)?;
            let len = le_u64(&bytes, 56 + 16 * i).ok_or_else(bad)?;
            let end = off.checked_add(len).ok_or_else(bad)?;
            if (off as usize) < header_len.min(HEADER_LEN) || end > bytes.len() as u64 {
                return Err(bad());
            }
            *s = (off as usize, len as usize);
        }
        let expect = [
            (S_SONG, song_count as usize * SONG_LEN),
            (S_PHRS, phrase_count as usize * PHRASE_LEN),
            (S_KEYS, key_count as usize * KEY_LEN),
            (S_TITL, 2 * song_count as usize * TITLE_LEN),
        ];
        if expect.iter().any(|&(s, len)| sections[s].1 != len) {
            return Err(bad());
        }
        let strs = &bytes[sections[S_STRS].0..sections[S_STRS].0 + sections[S_STRS].1];
        let version = strs
            .get(version_off..version_off.saturating_add(version_len))
            .and_then(|b| std::str::from_utf8(b).ok())
            .ok_or_else(bad)?
            .to_string();
        Ok(Self {
            bytes,
            sections,
            song_count,
            phrase_count,
            note_count,
            key_count,
            version,
        })
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn song_count(&self) -> u32 {
        self.song_count
    }

    pub fn phrase_count(&self) -> u32 {
        self.phrase_count
    }

    pub fn note_count(&self) -> u32 {
        self.note_count
    }

    pub fn key_count(&self) -> u32 {
        self.key_count
    }

    /// File size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn section(&self, s: usize) -> &[u8] {
        let (off, len) = self.sections[s];
        &self.bytes[off..off + len]
    }

    fn string(&self, off: u32, len: usize) -> Option<String> {
        let strs = self.section(S_STRS);
        let b = strs.get(off as usize..(off as usize).checked_add(len)?)?;
        std::str::from_utf8(b).ok().map(str::to_string)
    }

    fn song_record(&self, id: u32) -> Option<&[u8]> {
        if id >= self.song_count {
            return None;
        }
        let at = id as usize * SONG_LEN;
        self.section(S_SONG).get(at..at + SONG_LEN)
    }

    pub fn song(&self, id: u32) -> Option<SongMeta> {
        let r = self.song_record(id)?;
        let title = self.string(le_u32(r, 0)?, le_u16(r, 4)? as usize)?;
        let artist = self.string(le_u32(r, 8)?, le_u16(r, 6)? as usize)?;
        let isrc = (r[12] != 0).then(|| {
            String::from_utf8_lossy(&r[12..24])
                .trim_end_matches('\0')
                .to_string()
        });
        let deezer_id = le_u64(r, 24).filter(|&d| d != 0);
        let year = le_u16(r, 32).filter(|&y| y != 0);
        let popularity = le_u16(r, 34)? as f32 / 65535.0;
        Some(SongMeta {
            id,
            title,
            artist,
            isrc,
            deezer_id,
            year,
            popularity,
        })
    }

    /// Phrase ids of a song.
    pub fn song_phrases(&self, id: u32) -> std::ops::Range<u32> {
        let first = |s: u32| self.song_record(s).and_then(|r| le_u32(r, 36));
        let Some(a) = first(id) else { return 0..0 };
        let b = if id + 1 < self.song_count {
            first(id + 1).unwrap_or(a)
        } else {
            self.phrase_count
        };
        a.min(self.phrase_count)..b.clamp(a, self.phrase_count)
    }

    /// Song of a phrase.
    pub fn phrase_song(&self, phrase: u32) -> Option<u32> {
        le_u32(self.phrase_record(phrase)?, 0)
    }

    fn phrase_record(&self, id: u32) -> Option<&[u8]> {
        if id >= self.phrase_count {
            return None;
        }
        let at = id as usize * PHRASE_LEN;
        self.section(S_PHRS).get(at..at + PHRASE_LEN)
    }

    /// Decode a phrase.
    pub fn phrase(&self, id: u32) -> Option<Phrase> {
        let r = self.phrase_record(id)?;
        let song = le_u32(r, 0)?;
        let start_ms = le_u32(r, 4)?;
        let off = le_u32(r, 8)? as usize;
        let count = le_u16(r, 12)? as usize;
        let source = PhraseSource::from_code(r[14]);
        let base = r[15] as f32;
        let blob = self.section(S_NOTE);
        let mut pos = off;
        let mut onset = 0u32;
        let mut notes = Vec::with_capacity(count);
        for _ in 0..count {
            let q = *blob.get(pos)?;
            pos += 1;
            let delta = read_varint(blob, &mut pos)?;
            let dur = read_varint(blob, &mut pos)?;
            onset = onset.saturating_add(delta.saturating_mul(NOTE_TICK_MS));
            notes.push(Note {
                pitch: base + q as f32 / 4.0,
                onset_ms: onset,
                dur_ms: dur.saturating_mul(NOTE_TICK_MS),
            });
        }
        Some(Phrase {
            id,
            song,
            start_ms,
            source,
            notes,
        })
    }

    /// Postings of an n-gram key: (phrase, collapsed-note position), and the count.
    pub(crate) fn postings(&self, key: u32) -> Option<(Postings<'_>, u32)> {
        let keys = self.section(S_KEYS);
        let (mut lo, mut hi) = (0usize, self.key_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let k = le_u32(keys, mid * KEY_LEN)?;
            match k.cmp(&key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let off = le_u32(keys, mid * KEY_LEN + 4)? as usize;
                    let count = le_u32(keys, mid * KEY_LEN + 8)?;
                    let post = self.section(S_POST);
                    return Some((
                        Postings {
                            blob: post,
                            pos: off,
                            left: count,
                            phrase: 0,
                        },
                        count,
                    ));
                }
            }
        }
        None
    }

    /// Total n-gram positions (for weighting keys by rarity).
    pub(crate) fn positions(&self) -> u64 {
        (self.note_count as u64).max(1)
    }

    /// A song whose normalized title and artist match; falls back to a title match
    /// whose artist overlaps. The most popular wins among equals.
    pub fn find_song(&self, title: &str, artist: &str) -> Option<SongMeta> {
        let t = normalize_title(title);
        let a = normalize_artist(artist);
        if t.is_empty() {
            return None;
        }
        let table = self.section(S_TITL);
        let n = self.song_count as usize;
        let full = self.lookup(&table[..n * TITLE_LEN], fnv1a(&format!("{t}\u{1f}{a}")));
        let mut best: Option<SongMeta> = None;
        let consider = |s: SongMeta, best: &mut Option<SongMeta>| {
            if best.as_ref().is_none_or(|b| s.popularity > b.popularity) {
                *best = Some(s);
            }
        };
        for id in full {
            if let Some(s) = self.song(id)
                && normalize_title(&s.title) == t
                && normalize_artist(&s.artist) == a
            {
                consider(s, &mut best);
            }
        }
        if best.is_some() {
            return best;
        }
        for id in self.lookup(&table[n * TITLE_LEN..], fnv1a(&t)) {
            if let Some(s) = self.song(id)
                && normalize_title(&s.title) == t
            {
                let sa = normalize_artist(&s.artist);
                if a.is_empty() || sa.contains(&a) || a.contains(&sa) || shares_word(&sa, &a) {
                    consider(s, &mut best);
                }
            }
        }
        best
    }

    /// Song ids stored under `hash` in a sorted (hash, song) table.
    fn lookup(&self, table: &[u8], hash: u64) -> Vec<u32> {
        let n = table.len() / TITLE_LEN;
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match le_u64(table, mid * TITLE_LEN) {
                Some(h) if h < hash => lo = mid + 1,
                Some(_) => hi = mid,
                None => return Vec::new(),
            }
        }
        let mut out = Vec::new();
        let mut i = lo;
        while i < n && le_u64(table, i * TITLE_LEN) == Some(hash) {
            if let Some(id) = le_u32(table, i * TITLE_LEN + 8) {
                out.push(id);
            }
            i += 1;
        }
        out
    }
}

/// Iterator over a key's postings.
pub(crate) struct Postings<'a> {
    blob: &'a [u8],
    pos: usize,
    left: u32,
    phrase: u32,
}

impl Iterator for Postings<'_> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<(u32, u32)> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let delta = read_varint(self.blob, &mut self.pos)?;
        let at = read_varint(self.blob, &mut self.pos)?;
        self.phrase = self.phrase.checked_add(delta)?;
        Some((self.phrase, at))
    }
}

/// A note of the collapsed sequence: consecutive notes of the same semitone merged.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Step {
    pub pitch: f32,
    pub onset_ms: u32,
    pub end_ms: u32,
}

/// Merge consecutive notes whose pitch rounds to the same semitone difference 0.
pub(crate) fn collapse(notes: &[Note]) -> Vec<Step> {
    let mut out: Vec<Step> = Vec::with_capacity(notes.len());
    for n in notes {
        if !n.pitch.is_finite() {
            continue;
        }
        let end = n.onset_ms.saturating_add(n.dur_ms);
        match out.last_mut() {
            Some(last) if (n.pitch - last.pitch).round() == 0.0 => {
                last.end_ms = last.end_ms.max(end)
            }
            _ => out.push(Step {
                pitch: n.pitch,
                onset_ms: n.onset_ms,
                end_ms: end,
            }),
        }
    }
    out
}

/// Semitone intervals between collapsed notes, clipped to ±12 (never 0).
pub(crate) fn intervals(steps: &[Step]) -> Vec<i8> {
    steps
        .windows(2)
        .map(|w| {
            let d = (w[1].pitch - w[0].pitch).round().clamp(-12.0, 12.0) as i8;
            if d == 0 {
                if w[1].pitch >= w[0].pitch { 1 } else { -1 }
            } else {
                d
            }
        })
        .collect()
}

/// Coarse contour class of an interval: ±1 (1–2 semitones), ±2 (3–4), ±3 (5+).
pub(crate) fn contour_class(d: i8) -> i8 {
    let m = match d.unsigned_abs() {
        0..=2 => 1,
        3..=4 => 2,
        _ => 3,
    };
    if d < 0 { -m } else { m }
}

pub(crate) fn interval_key(d: &[i8]) -> u32 {
    d.iter()
        .fold(0u32, |k, &x| k * 25 + (x.clamp(-12, 12) + 12) as u32)
        | (KIND_INTERVAL << 30)
}

pub(crate) fn contour_key(c: &[i8]) -> u32 {
    c.iter()
        .fold(0u32, |k, &x| k * 7 + (x.clamp(-3, 3) + 3) as u32)
        | (KIND_CONTOUR << 30)
}

/// Every (position, key) of a collapsed interval sequence, both kinds.
pub(crate) fn ngram_keys(iv: &[i8]) -> Vec<(u32, u32)> {
    let mut out = Vec::with_capacity(2 * iv.len());
    for (i, w) in iv.windows(INTERVAL_N).enumerate() {
        out.push((i as u32, interval_key(w)));
    }
    let classes: Vec<i8> = iv.iter().map(|&d| contour_class(d)).collect();
    for (i, w) in classes.windows(CONTOUR_N).enumerate() {
        out.push((i as u32, contour_key(w)));
    }
    out
}

/// Lowercase, accents folded, bracketed parts and "feat." credits removed,
/// punctuation collapsed to single spaces.
pub fn normalize_title(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut depth = 0i32;
    let lower = s.to_lowercase();
    let cut = [" feat. ", " feat ", " ft. ", " ft ", " featuring ", " - "]
        .iter()
        .filter_map(|p| lower.find(p))
        .min()
        .unwrap_or(lower.len());
    for c in lower[..cut].chars() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = (depth - 1).max(0),
            _ if depth > 0 => {}
            _ => push_folded(&mut out, c),
        }
    }
    collapse_spaces(&out)
}

/// Like [`normalize_title`] for the first credited artist only.
pub fn normalize_artist(s: &str) -> String {
    let lower = s.to_lowercase();
    let cut = [
        ",", " & ", " x ", " and ", " feat", " ft.", " with ", ";", "/",
    ]
    .iter()
    .filter_map(|p| lower.find(p))
    .min()
    .unwrap_or(lower.len());
    let mut out = String::with_capacity(cut);
    let first = lower[..cut].trim_start_matches("the ");
    for c in first.chars() {
        push_folded(&mut out, c);
    }
    collapse_spaces(&out)
}

fn push_folded(out: &mut String, c: char) {
    let folded = match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' => 'a',
        'ç' | 'ć' | 'č' => 'c',
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ę' => 'e',
        'ì' | 'í' | 'î' | 'ï' | 'ī' => 'i',
        'ñ' | 'ń' => 'n',
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' => 'o',
        'ù' | 'ú' | 'û' | 'ü' | 'ū' => 'u',
        'ý' | 'ÿ' => 'y',
        'ś' | 'š' => 's',
        'ź' | 'ż' | 'ž' => 'z',
        'ł' => 'l',
        'ß' => {
            out.push_str("ss");
            return;
        }
        '&' => {
            out.push_str(" and ");
            return;
        }
        '\'' | '’' | '`' => return,
        c if c.is_alphanumeric() => c,
        _ => ' ',
    };
    out.push(folded);
}

fn collapse_spaces(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn shares_word(a: &str, b: &str) -> bool {
    a.split(' ')
        .filter(|w| w.len() > 2)
        .any(|w| b.split(' ').any(|v| v == w))
}

/// FNV-1a, 64-bit.
pub(crate) fn fnv1a(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

pub(crate) fn read_varint(b: &[u8], pos: &mut usize) -> Option<u32> {
    let mut v = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = *b.get(*pos)?;
        *pos += 1;
        v |= ((byte & 0x7f) as u32).checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

pub(crate) fn write_varint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(crate) fn le_u16(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

pub(crate) fn le_u32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

pub(crate) fn le_u64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip() {
        let mut b = Vec::new();
        for v in [0, 1, 127, 128, 300, 70_000, u32::MAX] {
            write_varint(&mut b, v);
        }
        let mut pos = 0;
        for v in [0, 1, 127, 128, 300, 70_000, u32::MAX] {
            assert_eq!(read_varint(&b, &mut pos), Some(v));
        }
        assert_eq!(read_varint(&[0x80, 0x80], &mut 0), None);
    }

    #[test]
    fn collapse_merges_repeats_and_keys_are_distinct() {
        let n = |p: f32, t: u32| Note {
            pitch: p,
            onset_ms: t,
            dur_ms: 100,
        };
        let steps = collapse(&[
            n(60.0, 0),
            n(60.2, 100),
            n(62.0, 200),
            n(64.0, 300),
            n(64.0, 400),
            n(60.0, 500),
        ]);
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0].end_ms, 200);
        assert_eq!(intervals(&steps), vec![2, 2, -4]);
        assert_eq!(contour_class(-4), -2);
        assert_eq!(contour_class(7), 3);
        assert_ne!(interval_key(&[1, 2, 3, 4]), interval_key(&[4, 3, 2, 1]));
        assert_ne!(interval_key(&[1, 1, 1, 1]) & !(3 << 30), 0);
        assert_eq!(contour_key(&[1; 6]) >> 30, 1);
    }

    #[test]
    fn normalization() {
        assert_eq!(normalize_title("Hello (Remastered 2011) - Live"), "hello");
        assert_eq!(
            normalize_title("Café del Mar feat. Someone"),
            "cafe del mar"
        );
        assert_eq!(normalize_title("月亮代表我的心"), "月亮代表我的心");
        assert_eq!(normalize_artist("The Beatles"), "beatles");
        assert_eq!(
            normalize_artist("Calvin Harris & Dua Lipa"),
            "calvin harris"
        );
        assert_eq!(normalize_artist("Beyoncé, JAY-Z"), "beyonce");
    }

    #[test]
    fn damaged_files_are_rejected() {
        assert!(MelodyIndex::from_bytes(Vec::new()).is_err());
        assert!(MelodyIndex::from_bytes(vec![0; 400]).is_err());
        let mut b = vec![0u8; HEADER_LEN];
        b[..8].copy_from_slice(MAGIC);
        b[8..12].copy_from_slice(&FORMAT.to_le_bytes());
        b[48..56].copy_from_slice(&1_000_000u64.to_le_bytes());
        assert!(MelodyIndex::from_bytes(b).is_err());
    }
}
