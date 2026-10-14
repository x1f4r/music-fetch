//! Lyrics from LRCLIB (free, keyless). `/api/get` looks up one track by its
//! signature; `/api/search` (metadata only) is the fallback when the exact
//! signature is unknown to it.

use serde::Deserialize;

use super::fetch::{FetchError, Fetcher, Remote};
use super::text;

const GET_URL: &str = "https://lrclib.net/api/get";
const SEARCH_URL: &str = "https://lrclib.net/api/search";
/// Lyrics with fewer words are placeholders or vandalism ("probe"), not lyrics.
const MIN_LYRIC_WORDS: usize = 12;

/// Lyrics of one track.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Found {
    Lyrics(String),
    /// LRCLIB knows the track and says it has no vocals.
    Instrumental,
}

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
    fn found(self) -> Option<Found> {
        if let Some(plain) = self.plain_lyrics.filter(|l| plausible(l)) {
            return Some(Found::Lyrics(plain));
        }
        if let Some(synced) = self.synced_lyrics.filter(|l| plausible(l)) {
            return Some(Found::Lyrics(synced));
        }
        self.instrumental.unwrap_or(false).then_some(Found::Instrumental)
    }

    fn has_lyrics(&self) -> bool {
        self.plain_lyrics.as_deref().is_some_and(plausible)
    }
}

fn plausible(lyrics: &str) -> bool {
    lyrics.split_whitespace().filter(|w| w.chars().any(char::is_alphanumeric)).count() >= MIN_LYRIC_WORDS
}

/// Lyrics for a candidate, or `None` when LRCLIB has nothing usable.
pub(crate) async fn lyrics(
    fetcher: &Fetcher<'_>,
    title: &str,
    artist: &str,
    duration_ms: Option<u64>,
) -> Result<Option<Found>, FetchError> {
    let title = text::base_title(title);
    let primary = text::artist_names(artist).into_iter().next().unwrap_or_else(|| artist.to_string());
    let seconds = duration_ms.map(|ms| ms.saturating_add(500) / 1000).filter(|&s| s > 0);

    let mut params = vec![("track_name", title.clone()), ("artist_name", artist.to_string())];
    if let Some(s) = seconds {
        params.push(("duration", s.to_string()));
    }
    let key = get_key(&title, artist, seconds);
    match fetcher.fetch(Remote::Lrclib, &key, || fetcher.http.get(GET_URL).query(&params)).await {
        Ok(fetched) => {
            if let Ok(record) = serde_json::from_str::<Record>(&fetched.body) {
                fetcher.remember(Remote::Lrclib, &key, &fetched).await;
                if let Some(found) = record.found() {
                    return Ok(Some(found));
                }
            }
        }
        Err(FetchError::NotFound) => {}
        Err(e) => return Err(e),
    }

    let params = [("track_name", title.clone()), ("artist_name", primary.clone())];
    let key = format!("search|{title}|{primary}");
    let fetched = match fetcher.fetch(Remote::Lrclib, &key, || fetcher.http.get(SEARCH_URL).query(&params)).await {
        Ok(fetched) => fetched,
        Err(FetchError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };
    let Ok(records) = serde_json::from_str::<Vec<Record>>(&fetched.body) else { return Ok(None) };
    fetcher.remember(Remote::Lrclib, &key, &fetched).await;
    Ok(best_record(records, &title, artist, duration_ms).and_then(Record::found))
}

/// Cache key of an `/api/get` lookup.
pub(crate) fn get_key(title: &str, artist: &str, seconds: Option<u64>) -> String {
    format!("get|{title}|{artist}|{}", seconds.unwrap_or(0))
}

/// The search record that best matches the candidate and has lyrics.
fn best_record(records: Vec<Record>, title: &str, artist: &str, duration_ms: Option<u64>) -> Option<Record> {
    let want_title = text::title_key(title);
    let want_artists = text::artist_keys(artist);
    records
        .into_iter()
        .filter(|r| r.has_lyrics() || r.instrumental == Some(true))
        .filter_map(|r| {
            let title_ok = r.track_name.as_deref().map(text::title_key).is_some_and(|t| t == want_title);
            let artists = r.artist_name.as_deref().map(text::artist_keys).unwrap_or_default();
            let artist_ok = artists.iter().any(|a| want_artists.contains(a));
            if !title_ok || !artist_ok {
                return None;
            }
            let duration_gap = match (duration_ms, r.duration) {
                (Some(ms), Some(s)) => ((ms as f64 / 1000.0) - s).abs(),
                _ => 30.0,
            };
            let lyric_bonus = if r.has_lyrics() { 0.0 } else { 60.0 };
            Some((duration_gap + lyric_bonus, r))
        })
        .min_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, r)| r)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORDS: &str = "is this the real life is this just fantasy caught in a landslide no escape";

    fn record(title: &str, artist: &str, duration: f64, lyrics: Option<&str>) -> Record {
        Record {
            track_name: Some(title.into()),
            artist_name: Some(artist.into()),
            duration: Some(duration),
            instrumental: Some(lyrics.is_none()),
            plain_lyrics: lyrics.map(str::to_string),
            synced_lyrics: None,
        }
    }

    #[test]
    fn picks_matching_record_closest_in_duration() {
        let a = format!("a {WORDS}");
        let b = format!("b {WORDS}");
        let records = vec![
            record("Bohemian Rhapsody", "Queen", 360.0, Some(&a)),
            record("Bohemian Rhapsody - Remastered 2011", "Queen", 355.0, Some(&b)),
            record("Bohemian Rhapsody", "Queen", 355.0, Some("probe")),
            record("Bohemian Rhapsody", "Panic! At the Disco", 355.0, Some(&a)),
            record("Killer Queen", "Queen", 355.0, Some(&a)),
        ];
        let best = best_record(records, "Bohemian Rhapsody", "Queen", Some(355_145));
        assert_eq!(best.and_then(|r| r.plain_lyrics), Some(b));
    }

    #[test]
    fn records_report_lyrics_or_instrumental() {
        assert_eq!(record("x", "y", 1.0, Some(WORDS)).found(), Some(Found::Lyrics(WORDS.into())));
        assert_eq!(record("x", "y", 1.0, None).found(), Some(Found::Instrumental));
        let synced_text = format!("[00:01.00] {WORDS}");
        let synced = Record { synced_lyrics: Some(synced_text.clone()), ..Record::default() };
        assert_eq!(synced.found(), Some(Found::Lyrics(synced_text)));
        assert_eq!(Record::default().found(), None);
        // Placeholder lyrics are not lyrics.
        let probe = Record { plain_lyrics: Some("probe".into()), ..Record::default() };
        assert_eq!(probe.found(), None);
    }
}
