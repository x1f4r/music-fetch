//! Builds a melody index file from the offline builder's JSONL output.
//!
//! See `docs/CORE_API.md` ("Melody builder input") for the input and
//! [`crate::melody::index`] for the file layout. Invalid lines and notes are
//! skipped and counted; songs without a usable phrase are left out.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::Deserialize;

use crate::error::{MfError, Result};
use crate::melody::index::{
    self, FORMAT, HEADER_LEN, KEY_LEN, MAGIC, NOTE_TICK_MS, PHRASE_LEN, PhraseSource, S_KEYS,
    S_NOTE, S_PHRS, S_POST, S_SONG, S_STRS, S_TITL, SECTIONS, SONG_LEN, TITLE_LEN,
};
use crate::melody::notes::Note;

#[derive(Debug, Clone, Default)]
pub struct BuildStats {
    pub songs: u32,
    pub phrases: u32,
    pub notes: u64,
    pub bytes: u64,
    /// Distinct n-gram keys.
    pub keys: u32,
    /// Input lines that were not a usable song.
    pub skipped: u32,
}

/// One input line.
#[derive(Debug, Clone, Deserialize)]
pub struct SongInput {
    pub title: String,
    #[serde(default)]
    pub artist: String,
    #[serde(default)]
    pub isrc: Option<String>,
    #[serde(default)]
    pub deezer_id: Option<u64>,
    #[serde(default)]
    pub year: Option<u16>,
    #[serde(default)]
    pub popularity: Option<f32>,
    #[serde(default)]
    pub phrases: Vec<PhraseInput>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PhraseInput {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub start_ms: u32,
    /// `[pitch, onset_ms, dur_ms]`.
    pub notes: Vec<[f64; 3]>,
}

/// Read `input_jsonl`, write the index to `output` (atomically) and report counts.
pub fn build_index(input_jsonl: &Path, output: &Path, version: &str) -> Result<BuildStats> {
    let file = std::fs::File::open(input_jsonl)
        .map_err(|e| MfError::invalid(format!("cannot read the builder input: {e}")))?;
    let mut songs = Vec::new();
    let mut skipped = 0u32;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<SongInput>(&line) {
            Ok(s) => songs.push(s),
            Err(_) => skipped += 1,
        }
    }
    let (bytes, mut stats) = build_bytes(&songs, version)?;
    stats.skipped += skipped;
    let tmp = output.with_extension("mfi.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, output)?;
    Ok(stats)
}

struct Song<'a> {
    input: &'a SongInput,
    phrases: Vec<(PhraseSource, u32, Vec<Note>)>,
}

/// Build an index in memory.
pub fn build_bytes(songs: &[SongInput], version: &str) -> Result<(Vec<u8>, BuildStats)> {
    let mut stats = BuildStats::default();
    let mut kept = Vec::with_capacity(songs.len());
    for s in songs {
        let phrases: Vec<_> = s.phrases.iter().filter_map(clean_phrase).collect();
        if s.title.trim().is_empty() || phrases.is_empty() {
            stats.skipped += 1;
            continue;
        }
        kept.push(Song { input: s, phrases });
    }
    if kept.len() > u32::MAX as usize / 2 {
        return Err(MfError::invalid("too many songs for one index"));
    }

    let mut strs = Vec::new();
    let mut add_str = |s: &str, cap: usize| -> (u32, u16) {
        let mut end = s.len().min(cap);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        let off = strs.len() as u32;
        strs.extend_from_slice(&s.as_bytes()[..end]);
        (off, end as u16)
    };
    let (version_off, version_len) = add_str(version, 255);
    let mut song_recs = Vec::with_capacity(kept.len() * SONG_LEN);
    let mut phrase_recs = Vec::new();
    let mut note_blob = Vec::new();
    let mut postings: Vec<(u32, u32, u32)> = Vec::new();
    let mut titles = Vec::with_capacity(kept.len());
    let mut phrase_id = 0u32;
    for (song_id, s) in kept.iter().enumerate() {
        let song_id = song_id as u32;
        let (title_off, title_len) = add_str(s.input.title.trim(), 512);
        let (artist_off, artist_len) = add_str(s.input.artist.trim(), 512);
        let mut rec = [0u8; SONG_LEN];
        rec[0..4].copy_from_slice(&title_off.to_le_bytes());
        rec[4..6].copy_from_slice(&title_len.to_le_bytes());
        rec[6..8].copy_from_slice(&artist_len.to_le_bytes());
        rec[8..12].copy_from_slice(&artist_off.to_le_bytes());
        if let Some(isrc) = s
            .input
            .isrc
            .as_deref()
            .map(str::trim)
            .filter(|i| i.len() == 12 && i.bytes().all(|b| b.is_ascii_alphanumeric()))
        {
            rec[12..24].copy_from_slice(&isrc.to_ascii_uppercase().as_bytes()[..12]);
        }
        rec[24..32].copy_from_slice(&s.input.deezer_id.unwrap_or(0).to_le_bytes());
        rec[32..34].copy_from_slice(&s.input.year.unwrap_or(0).to_le_bytes());
        let pop = s
            .input
            .popularity
            .filter(|p| p.is_finite())
            .unwrap_or(0.5)
            .clamp(0.0, 1.0);
        rec[34..36].copy_from_slice(&((pop * 65535.0).round() as u16).to_le_bytes());
        rec[36..40].copy_from_slice(&phrase_id.to_le_bytes());
        song_recs.extend_from_slice(&rec);
        let t = index::normalize_title(&s.input.title);
        let a = index::normalize_artist(&s.input.artist);
        titles.push((
            index::fnv1a(&format!("{t}\u{1f}{a}")),
            index::fnv1a(&t),
            song_id,
        ));

        for (source, start_ms, notes) in &s.phrases {
            let base = notes
                .iter()
                .map(|n| n.pitch)
                .fold(f32::INFINITY, f32::min)
                .floor()
                .clamp(0.0, 255.0);
            let off = note_blob.len() as u32;
            let mut prev_tick = 0u32;
            let mut stored = Vec::with_capacity(notes.len());
            for n in notes {
                let q = ((n.pitch - base) * 4.0).round().clamp(0.0, 255.0) as u8;
                let tick = (n.onset_ms + NOTE_TICK_MS / 2) / NOTE_TICK_MS;
                let dur = ((n.dur_ms + NOTE_TICK_MS / 2) / NOTE_TICK_MS).max(1);
                note_blob.push(q);
                index::write_varint(&mut note_blob, tick.saturating_sub(prev_tick));
                index::write_varint(&mut note_blob, dur);
                prev_tick = tick.max(prev_tick);
                // Keys come from the notes exactly as they will be decoded.
                stored.push(Note {
                    pitch: base + q as f32 / 4.0,
                    onset_ms: prev_tick * NOTE_TICK_MS,
                    dur_ms: dur * NOTE_TICK_MS,
                });
            }
            let mut rec = [0u8; PHRASE_LEN];
            rec[0..4].copy_from_slice(&song_id.to_le_bytes());
            rec[4..8].copy_from_slice(&start_ms.to_le_bytes());
            rec[8..12].copy_from_slice(&off.to_le_bytes());
            rec[12..14].copy_from_slice(&(notes.len() as u16).to_le_bytes());
            rec[14] = source.code();
            rec[15] = base as u8;
            phrase_recs.extend_from_slice(&rec);
            let steps = index::collapse(&stored);
            for (pos, key) in index::ngram_keys(&index::intervals(&steps)) {
                postings.push((key, phrase_id, pos));
            }
            stats.notes += notes.len() as u64;
            phrase_id += 1;
        }
    }
    if note_blob.len() > u32::MAX as usize {
        return Err(MfError::invalid("too many notes for one index"));
    }

    postings.sort_unstable();
    let mut keys = Vec::new();
    let mut post = Vec::new();
    let mut i = 0;
    while i < postings.len() {
        let key = postings[i].0;
        let off = post.len() as u32;
        let mut prev_phrase = 0u32;
        let mut count = 0u32;
        while i < postings.len() && postings[i].0 == key {
            let (_, p, pos) = postings[i];
            index::write_varint(&mut post, p - prev_phrase);
            index::write_varint(&mut post, pos);
            prev_phrase = p;
            count += 1;
            i += 1;
        }
        keys.extend_from_slice(&key.to_le_bytes());
        keys.extend_from_slice(&off.to_le_bytes());
        keys.extend_from_slice(&count.to_le_bytes());
    }
    if post.len() > u32::MAX as usize {
        return Err(MfError::invalid("too many postings for one index"));
    }

    let mut title_tab = Vec::with_capacity(2 * titles.len() * TITLE_LEN);
    for which in 0..2 {
        let mut entries: Vec<(u64, u32)> = titles
            .iter()
            .map(|&(full, title, id)| (if which == 0 { full } else { title }, id))
            .collect();
        entries.sort_unstable();
        for (h, id) in entries {
            title_tab.extend_from_slice(&h.to_le_bytes());
            title_tab.extend_from_slice(&id.to_le_bytes());
        }
    }

    let song_count = kept.len() as u32;
    let note_count =
        u32::try_from(stats.notes).map_err(|_| MfError::invalid("too many notes for one index"))?;
    let key_count = (keys.len() / KEY_LEN) as u32;
    let mut sections: [&[u8]; SECTIONS] = [&[]; SECTIONS];
    sections[S_STRS] = &strs;
    sections[S_SONG] = &song_recs;
    sections[S_PHRS] = &phrase_recs;
    sections[S_NOTE] = &note_blob;
    sections[S_KEYS] = &keys;
    sections[S_POST] = &post;
    sections[S_TITL] = &title_tab;

    let mut out = vec![0u8; HEADER_LEN];
    out[..8].copy_from_slice(MAGIC);
    out[8..12].copy_from_slice(&FORMAT.to_le_bytes());
    out[12..16].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    out[16..20].copy_from_slice(&song_count.to_le_bytes());
    out[20..24].copy_from_slice(&phrase_id.to_le_bytes());
    out[24..28].copy_from_slice(&note_count.to_le_bytes());
    out[28..32].copy_from_slice(&key_count.to_le_bytes());
    out[32..36].copy_from_slice(&version_off.to_le_bytes());
    out[36..40].copy_from_slice(&(version_len as u32).to_le_bytes());
    for (i, data) in sections.iter().enumerate() {
        while !out.len().is_multiple_of(8) {
            out.push(0);
        }
        let off = out.len() as u64;
        out[48 + 16 * i..56 + 16 * i].copy_from_slice(&off.to_le_bytes());
        out[56 + 16 * i..64 + 16 * i].copy_from_slice(&(data.len() as u64).to_le_bytes());
        out.extend_from_slice(data);
    }

    stats.songs = song_count;
    stats.phrases = phrase_id;
    stats.keys = key_count;
    stats.bytes = out.len() as u64;
    Ok((out, stats))
}

/// Valid, sorted, monophonic notes of a phrase; None if fewer than 2 remain.
fn clean_phrase(p: &PhraseInput) -> Option<(PhraseSource, u32, Vec<Note>)> {
    let source = match p.source.as_deref() {
        Some("audio") => PhraseSource::Audio,
        Some("midi") => PhraseSource::Midi,
        _ => PhraseSource::Preview,
    };
    let mut notes: Vec<Note> = p
        .notes
        .iter()
        .filter(|[pitch, onset, dur]| {
            pitch.is_finite()
                && (1.0..140.0).contains(pitch)
                && onset.is_finite()
                && *onset >= 0.0
                && dur.is_finite()
                && *dur > 0.0
        })
        .map(|&[pitch, onset, dur]| Note {
            pitch: pitch as f32,
            onset_ms: onset.min(u32::MAX as f64 / 2.0) as u32,
            dur_ms: dur.min(600_000.0) as u32,
        })
        .collect();
    notes.sort_by_key(|n| n.onset_ms);
    // Monophonic: a note ends where the next one starts.
    for i in 1..notes.len() {
        let next = notes[i].onset_ms;
        let prev = &mut notes[i - 1];
        if prev.onset_ms + prev.dur_ms > next {
            prev.dur_ms = next - prev.onset_ms;
        }
    }
    notes.retain(|n| n.dur_ms > 0);
    notes.truncate(u16::MAX as usize);
    (notes.len() >= 2).then_some((source, p.start_ms, notes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::melody::index::MelodyIndex;

    fn song(title: &str, artist: &str, pitches: &[f64]) -> SongInput {
        SongInput {
            title: title.into(),
            artist: artist.into(),
            isrc: Some("USUM71703861".into()),
            deezer_id: Some(42),
            year: Some(2017),
            popularity: Some(0.83),
            phrases: vec![PhraseInput {
                source: Some("preview".into()),
                start_ms: 48_000,
                notes: pitches
                    .iter()
                    .enumerate()
                    .map(|(i, &p)| [p, i as f64 * 250.0, 240.0])
                    .collect(),
            }],
        }
    }

    #[test]
    fn round_trip() {
        let songs = vec![
            song(
                "Song One (Remastered)",
                "Artist A & B",
                &[60.0, 62.0, 64.0, 65.0, 67.0, 65.0, 64.0, 62.0, 60.0],
            ),
            song(
                "Other",
                "Someone",
                &[70.0, 70.0, 72.25, 69.0, 67.0, 72.0, 74.0, 76.0],
            ),
            SongInput {
                title: " ".into(),
                ..song("x", "y", &[60.0, 61.0])
            },
        ];
        let (bytes, stats) = build_bytes(&songs, "2026.10.1").expect("build");
        assert_eq!(
            (stats.songs, stats.phrases, stats.notes, stats.skipped),
            (2, 2, 17, 1)
        );
        let idx = MelodyIndex::from_bytes(bytes).expect("open");
        assert_eq!(idx.version(), "2026.10.1");
        assert_eq!(idx.song_count(), 2);
        let s = idx.song(1).expect("song");
        assert_eq!(s.title, "Other");
        assert_eq!(s.isrc.as_deref(), Some("USUM71703861"));
        assert_eq!((s.deezer_id, s.year), (Some(42), Some(2017)));
        assert!((s.popularity - 0.83).abs() < 1e-4);
        let p = idx.phrase(1).expect("phrase");
        assert_eq!(p.start_ms, 48_000);
        assert_eq!(p.notes.len(), 8);
        assert!((p.notes[2].pitch - 72.25).abs() < 1e-6);
        assert_eq!((p.notes[3].onset_ms, p.notes[3].dur_ms), (750, 240));
        assert_eq!(idx.song_phrases(0), 0..1);
        assert_eq!(idx.find_song("song one", "Artist A").map(|s| s.id), Some(0));
        assert_eq!(
            idx.find_song("SONG ONE - 2011 Remaster", "artist a feat. c")
                .map(|s| s.id),
            Some(0)
        );
        assert!(idx.find_song("Song One", "Nobody").is_none());
        // Every key of the first phrase finds the phrase at the right position.
        let steps = index::collapse(&idx.phrase(0).expect("phrase").notes);
        for (pos, key) in index::ngram_keys(&index::intervals(&steps)) {
            let (postings, _) = idx.postings(key).expect("key");
            assert!(postings.into_iter().any(|(ph, at)| ph == 0 && at == pos));
        }
    }

    #[test]
    fn hostile_notes_are_cleaned() {
        let mut s = song("t", "a", &[]);
        s.phrases[0].notes = vec![
            [f64::NAN, 0.0, 10.0],
            [60.0, 500.0, 900.0],
            [62.0, 100.0, 1e12],
            [64.0, -5.0, 10.0],
            [65.0, 700.0, 100.0],
        ];
        let (bytes, stats) = build_bytes(&[s], "v").expect("build");
        assert_eq!(stats.notes, 3);
        let p = MelodyIndex::from_bytes(bytes)
            .expect("open")
            .phrase(0)
            .expect("phrase");
        assert_eq!(
            p.notes
                .iter()
                .map(|n| (n.onset_ms, n.dur_ms))
                .collect::<Vec<_>>(),
            vec![(100, 400), (500, 200), (700, 100)]
        );
    }
}
