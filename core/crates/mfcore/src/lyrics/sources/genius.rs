//! Genius public lyric search (undocumented, keyless). Very precise on exact
//! lyrics and returns the matched excerpt, but tolerates little deviation.
//!
//! Genius answers 401 to some generic HTTP-library user agents; the engine's own
//! user agent is accepted.

use serde::Deserialize;

use super::{Hit, ParseError, RESULTS_PER_QUERY, non_empty};

const URL: &str = "https://genius.com/api/search/lyric";

pub(crate) fn request(http: &reqwest::Client, query: &str) -> reqwest::RequestBuilder {
    let per_page = RESULTS_PER_QUERY.to_string();
    http.get(URL).query(&[("q", query), ("per_page", &per_page)]).header(reqwest::header::ACCEPT, "application/json")
}

#[derive(Deserialize)]
struct Envelope {
    response: Option<Body>,
}

#[derive(Deserialize)]
struct Body {
    #[serde(default)]
    sections: Vec<Section>,
}

#[derive(Deserialize)]
struct Section {
    #[serde(default)]
    hits: Vec<serde_json::Value>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SearchHit {
    highlights: Vec<Highlight>,
    matched_words: Option<u32>,
    nb_exact_words: Option<u32>,
    #[serde(rename = "type")]
    kind: Option<String>,
    result: Option<Song>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Highlight {
    value: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Song {
    id: Option<u64>,
    title: Option<String>,
    artist_names: Option<String>,
    primary_artist: Option<Artist>,
    primary_artists: Vec<Artist>,
    featured_artists: Vec<Artist>,
    url: Option<String>,
    song_art_image_url: Option<String>,
    release_date_components: Option<DateComponents>,
    stats: Option<Stats>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Stats {
    pageviews: Option<u64>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Artist {
    name: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct DateComponents {
    year: Option<u16>,
}

pub(crate) fn parse(body: &str) -> Result<Vec<Hit>, ParseError> {
    let envelope: Envelope = serde_json::from_str(body)?;
    let Some(response) = envelope.response else {
        return Err(ParseError::Malformed("missing response".into()));
    };
    let hits = response
        .sections
        .into_iter()
        .flat_map(|section| section.hits)
        .filter_map(|value| serde_json::from_value::<SearchHit>(value).ok())
        .filter(|hit| hit.kind.as_deref().is_none_or(|k| k == "song"))
        .filter_map(|hit| {
            let song = hit.result?;
            let mut artists: Vec<String> = song
                .primary_artists
                .iter()
                .chain(song.primary_artist.iter())
                .chain(song.featured_artists.iter())
                .filter_map(|a| a.name.as_deref().map(clean))
                .filter(|n| !n.is_empty())
                .collect();
            dedup_keep_order(&mut artists);
            let artist = song
                .artist_names
                .as_deref()
                .map(clean)
                .filter(|a| !a.is_empty())
                .or_else(|| artists.first().cloned())
                .unwrap_or_default();
            Some(Hit {
                title: song.title.as_deref().map(clean).unwrap_or_default(),
                artist,
                artists,
                release_year: song.release_date_components.and_then(|d| d.year),
                artwork_url: non_empty(song.song_art_image_url),
                id: song.id.map(|id| id.to_string()),
                url: non_empty(song.url),
                snippet: hit.highlights.into_iter().find_map(|h| non_empty(h.value)),
                matched_words: hit.matched_words,
                exact_words: hit.nb_exact_words,
                plays: song.stats.and_then(|s| s.pageviews),
                ..Hit::default()
            })
        })
        .collect();
    Ok(hits)
}

/// Genius uses non-breaking spaces in names ("Panic!\u{a0}at the Disco").
fn clean(text: &str) -> String {
    text.replace('\u{a0}', " ").split_whitespace().collect::<Vec<_>>().join(" ")
}

fn dedup_keep_order(names: &mut Vec<String>) {
    let mut seen = std::collections::HashSet::new();
    names.retain(|n| seen.insert(n.to_lowercase()));
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = r#"{"meta":{"status":200},"response":{"sections":[{"type":"lyric","hits":[
        {"highlights":[{"property":"lyrics","value":"Is this the real life? Is this just fantasy?\nCaught in a landslide","snippet":true}],
         "index":"lyric","matched_words":9,"nb_exact_words":9,"nb_typos":0,"type":"song",
         "result":{"id":1063,"title":"Bohemian Rhapsody","artist_names":"Queen","url":"https://genius.com/Queen-bohemian-rhapsody-lyrics",
                   "song_art_image_url":"https://images.genius.com/718de9.1000x1000x1.png","release_date_components":{"year":1975,"month":10,"day":31},
                   "primary_artist":{"name":"Queen"},"primary_artists":[{"name":"Queen"}],"featured_artists":[],
                   "stats":{"hot":false,"pageviews":11460848}}},
        {"highlights":[],"type":"song","matched_words":7,"nb_exact_words":6,
         "result":{"id":7,"title":"Bohemian Rhapsody","artist_names":"Glee Cast (Ft. Jonathan Groff)",
                   "primary_artist":{"name":"Glee Cast"},"featured_artists":[{"name":"Jonathan Groff"}]}},
        {"type":"album","result":{"id":9,"title":"Album"}}
    ]}]}}"#;

    #[test]
    fn parses_lyric_hits() {
        let hits = parse(BODY).unwrap_or_default();
        assert_eq!(hits.len(), 2);
        let h = &hits[0];
        assert_eq!(h.title, "Bohemian Rhapsody");
        assert_eq!(h.artist, "Queen");
        assert_eq!(h.artists, ["Queen"]);
        assert_eq!(h.id.as_deref(), Some("1063"));
        assert_eq!(h.release_year, Some(1975));
        assert_eq!(h.matched_words, Some(9));
        assert_eq!(h.exact_words, Some(9));
        assert_eq!(h.plays, Some(11_460_848));
        assert!(h.snippet.as_deref().unwrap_or_default().starts_with("Is this the real life?"));
        assert_eq!(hits[1].artist, "Glee Cast (Ft. Jonathan Groff)");
        assert_eq!(hits[1].artists, ["Glee Cast", "Jonathan Groff"]);
        assert_eq!(hits[1].snippet, None);
    }

    #[test]
    fn rejects_unexpected_bodies() {
        assert!(parse("{}").is_err());
        assert!(parse("not json").is_err());
        assert_eq!(parse(r#"{"response":{"sections":[]}}"#).map(|h| h.len()), Ok(0));
    }
}
