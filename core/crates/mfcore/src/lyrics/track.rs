//! Lyrics of a known track from LRCLIB (`/api/get`, then `/api/search`), with
//! time-synced lines when LRCLIB has them.

use std::time::Duration;

use serde::Deserialize;

use super::fetch::{FetchError, Fetcher, Remote, Stats};
use super::text;
use crate::model::{LyricLine, Lyrics, Track};
use crate::util::cancel::CancelToken;

const GET_URL: &str = "https://lrclib.net/api/get";
const SEARCH_URL: &str = "https://lrclib.net/api/search";
/// Longest wait for a rate-limit slot.
const MAX_RATE_WAIT: Duration = Duration::from_secs(5);
/// Search results whose duration differs more than this are other versions.
const MAX_DURATION_GAP_S: f64 = 8.0;

#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase", default)]
struct Record {
    track_name: Option<String>,
    artist_name: Option<String>,
    duration: Option<f64>,
    instrumental: Option<bool>,
    plain_lyrics: Option<String>,
    synced_lyrics: Option<String>,
}

impl Record {
    fn has_text(&self) -> bool {
        let filled = |s: &Option<String>| s.as_deref().is_some_and(|t| t.chars().any(char::is_alphanumeric));
        filled(&self.plain_lyrics) || filled(&self.synced_lyrics)
    }

    fn into_lyrics(self) -> Option<Lyrics> {
        if !self.has_text() {
            return None;
        }
        let synced = self.synced_lyrics.as_deref().map(parse_synced).unwrap_or_default();
        let plain = match self.plain_lyrics.filter(|p| p.chars().any(char::is_alphanumeric)) {
            Some(plain) => plain.trim().to_string(),
            None => synced.iter().map(|l| l.text.as_str()).collect::<Vec<_>>().join("\n").trim().to_string(),
        };
        Some(Lyrics { plain, synced })
    }
}

/// Lyrics for `track`, or `None` when LRCLIB has none (or is unreachable).
pub async fn fetch_lyrics(http: &reqwest::Client, track: &Track) -> Option<Lyrics> {
    lookup_lyrics(http, track).await.ok().flatten()
}

/// Like [`fetch_lyrics`], but tells "no lyrics" (`Ok(None)`) apart from "LRCLIB
/// could not be asked" (`Err`), so callers only cache definite answers.
pub async fn lookup_lyrics(http: &reqwest::Client, track: &Track) -> Result<Option<Lyrics>, LyricsUnavailable> {
    let title = text::base_title(&track.title);
    if title.trim().is_empty() || track.artist.trim().is_empty() {
        return Ok(None);
    }
    let cancel = CancelToken::new();
    let stats = Stats::default();
    let fetcher = Fetcher { http, cancel: &cancel, cache_dir: None, max_wait: MAX_RATE_WAIT, stats: &stats };
    let seconds = track.duration_ms.map(|ms| ms.saturating_add(500) / 1000).filter(|&s| s > 0);

    let mut params = vec![("track_name", title.clone()), ("artist_name", track.artist.clone())];
    if let Some(album) = track.album.as_deref().filter(|a| !a.trim().is_empty()) {
        params.push(("album_name", album.to_string()));
    }
    if let Some(s) = seconds {
        params.push(("duration", s.to_string()));
    }
    match fetcher.fetch(Remote::Lrclib, "", || http.get(GET_URL).query(&params)).await {
        Ok(fetched) => {
            if let Ok(record) = serde_json::from_str::<Record>(&fetched.body) {
                if record.instrumental == Some(true) && !record.has_text() {
                    return Ok(None);
                }
                if let Some(lyrics) = record.into_lyrics() {
                    return Ok(Some(lyrics));
                }
            }
        }
        Err(FetchError::NotFound) => {}
        Err(e) => {
            tracing::debug!(?e, "lrclib get failed");
            return Err(LyricsUnavailable);
        }
    }

    let primary = text::artist_names(&track.artist).into_iter().next().unwrap_or_else(|| track.artist.clone());
    let params = [("track_name", title.clone()), ("artist_name", primary)];
    let fetched = match fetcher.fetch(Remote::Lrclib, "", || http.get(SEARCH_URL).query(&params)).await {
        Ok(fetched) => fetched,
        Err(FetchError::NotFound) => return Ok(None),
        Err(e) => {
            tracing::debug!(?e, "lrclib search failed");
            return Err(LyricsUnavailable);
        }
    };
    let records = serde_json::from_str::<Vec<Record>>(&fetched.body).map_err(|_| LyricsUnavailable)?;
    Ok(best_record(records, &title, &track.artist, track.duration_ms).and_then(Record::into_lyrics))
}

/// LRCLIB could not be reached or answered unexpectedly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LyricsUnavailable;

/// The search result with lyrics whose title and an artist match, closest in duration.
fn best_record(records: Vec<Record>, title: &str, artist: &str, duration_ms: Option<u64>) -> Option<Record> {
    let want_title = text::title_key(title);
    let want_artists = text::artist_keys(artist);
    records
        .into_iter()
        .filter(Record::has_text)
        .filter_map(|r| {
            let title_ok = r.track_name.as_deref().map(text::title_key).is_some_and(|t| t == want_title);
            let artists = r.artist_name.as_deref().map(text::artist_keys).unwrap_or_default();
            if !title_ok || !artists.iter().any(|a| want_artists.contains(a)) {
                return None;
            }
            let gap = match (duration_ms, r.duration) {
                (Some(ms), Some(s)) => (ms as f64 / 1000.0 - s).abs(),
                _ => MAX_DURATION_GAP_S / 2.0,
            };
            // Prefer records with synced lines among equally close ones.
            let synced_bonus = if r.synced_lyrics.as_deref().is_some_and(|s| !s.trim().is_empty()) { 0.0 } else { 0.5 };
            (gap <= MAX_DURATION_GAP_S).then_some((gap + synced_bonus, r))
        })
        .min_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, r)| r)
}

/// Parse LRC text: `[mm:ss.xx]` stamps (several per line allowed), `[offset:±ms]`,
/// other tags ignored. Lines are sorted by time; empty lines (instrumental breaks)
/// are kept.
pub(crate) fn parse_synced(lrc: &str) -> Vec<LyricLine> {
    let mut offset_ms: i64 = 0;
    let mut lines = Vec::new();
    for raw in lrc.lines() {
        let mut rest = raw.trim();
        let mut stamps = Vec::new();
        while let Some(body) = rest.strip_prefix('[') {
            let Some(end) = body.find(']') else { break };
            let tag = &body[..end];
            if let Some(ms) = parse_stamp(tag) {
                stamps.push(ms);
            } else if let Some(value) = tag.strip_prefix("offset:") {
                offset_ms = value.trim().parse().unwrap_or(0);
            }
            rest = body[end + 1..].trim_start();
        }
        let text = rest.trim().to_string();
        for ms in stamps {
            // A positive offset shows the lyrics earlier.
            let t = (ms as i64 - offset_ms).max(0) as u64;
            lines.push(LyricLine { t_ms: t, text: text.clone() });
        }
    }
    lines.sort_by_key(|l| l.t_ms);
    lines
}

/// `mm:ss`, `mm:ss.xx`, `mm:ss.xxx` or `mm:ss:xx` in milliseconds.
fn parse_stamp(tag: &str) -> Option<u64> {
    let (minutes, rest) = tag.split_once(':')?;
    let minutes: u64 = minutes.trim().parse().ok()?;
    let (seconds, fraction) = match rest.find(['.', ':']) {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };
    let seconds: u64 = seconds.trim().parse().ok()?;
    if seconds >= 60 {
        return None;
    }
    let fraction_ms = if fraction.is_empty() {
        0
    } else {
        if !fraction.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let digits: String = fraction.chars().chain("000".chars()).take(3).collect();
        digits.parse::<u64>().ok()?
    };
    Some(minutes * 60_000 + seconds * 1000 + fraction_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lrc() {
        let lrc = "[ar: Someone]\n[00:12.34]First line\n[01:02.5][00:30.00] Chorus\n[00:40]\n[02:00.123]Last";
        let lines = parse_synced(lrc);
        let got: Vec<(u64, &str)> = lines.iter().map(|l| (l.t_ms, l.text.as_str())).collect();
        assert_eq!(
            got,
            vec![(12_340, "First line"), (30_000, "Chorus"), (40_000, ""), (62_500, "Chorus"), (120_123, "Last")]
        );
    }

    #[test]
    fn applies_offset_and_ignores_garbage() {
        let lines = parse_synced("[offset:+500]\n[00:01.00]a\n[xx:yy]b\nplain line\n[00:00.20]c");
        let got: Vec<(u64, &str)> = lines.iter().map(|l| (l.t_ms, l.text.as_str())).collect();
        assert_eq!(got, vec![(0, "c"), (500, "a")]);
    }

    #[test]
    fn plain_from_synced_when_missing() {
        let record = Record { synced_lyrics: Some("[00:01.00]one\n[00:02.00]two".into()), ..Record::default() };
        let lyrics = record.into_lyrics().unwrap();
        assert_eq!(lyrics.plain, "one\ntwo");
        assert_eq!(lyrics.synced.len(), 2);
        assert!(Record { instrumental: Some(true), ..Record::default() }.into_lyrics().is_none());
    }

    #[test]
    fn picks_the_matching_record() {
        let record = |title: &str, artist: &str, duration: f64, synced: bool| Record {
            track_name: Some(title.into()),
            artist_name: Some(artist.into()),
            duration: Some(duration),
            instrumental: Some(false),
            plain_lyrics: Some("words here".into()),
            synced_lyrics: synced.then(|| "[00:01.00]words here".into()),
        };
        let records = vec![
            record("Take On Me", "a-ha", 300.0, true),
            record("Take On Me", "a-ha", 226.0, false),
            record("Take On Me", "a-ha", 225.0, true),
            record("Take On Me", "Someone Else", 225.0, true),
        ];
        let best = best_record(records, "Take On Me", "a-ha", Some(225_000)).unwrap();
        assert_eq!(best.duration, Some(225.0));
        assert!(best.synced_lyrics.is_some());
    }

    #[tokio::test]
    #[ignore = "live network: cargo test -p mfcore fetch_lyrics_live -- --ignored"]
    async fn fetch_lyrics_live() {
        let http = crate::util::http::build_client("1.0.0").unwrap();
        let track = Track {
            title: "Bohemian Rhapsody - Remastered 2011".into(),
            artist: "Queen".into(),
            duration_ms: Some(354_000),
            ..Track::default()
        };
        let lyrics = fetch_lyrics(&http, &track).await.expect("lyrics");
        assert!(lyrics.plain.to_lowercase().contains("real life"));
        assert!(lyrics.synced.len() > 20);
    }
}
