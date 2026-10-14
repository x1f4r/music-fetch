//! Merge backend hits into candidates by normalized title and overlapping artists.

use std::collections::{HashMap, HashSet};

use super::Backend;
use super::sources::Hit;
use super::text;
use crate::model::{Service, ServiceLink, Track};

/// One hit as returned for one query.
#[derive(Debug, Clone)]
pub(crate) struct Sighting {
    pub backend: Backend,
    /// Index of the request in the plan.
    pub request: usize,
    /// Words in that request's query.
    pub query_words: usize,
    /// 0-based position in the response.
    pub rank: usize,
    pub hit: Hit,
}

/// A song suggested by one or more backends.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub title_key: String,
    pub artist_keys: Vec<String>,
    pub sightings: Vec<Sighting>,
}

impl Candidate {
    fn accepts(&self, title_key: &str, artist_keys: &[String]) -> bool {
        self.title_key == title_key
            && (self.artist_keys.is_empty()
                || artist_keys.is_empty()
                || artist_keys.iter().any(|k| self.artist_keys.contains(k)))
    }

    /// True when `backend` returned this song.
    pub fn seen_on(&self, backend: Backend) -> bool {
        self.sightings.iter().any(|s| s.backend == backend)
    }

    /// Best 0-based rank given by `backend`.
    pub fn best_rank_on(&self, backend: Backend) -> Option<usize> {
        self.sightings.iter().filter(|s| s.backend == backend).map(|s| s.rank).min()
    }

    /// Distinct backends that returned this song.
    pub fn backends(&self) -> usize {
        self.sightings.iter().map(|s| s.backend).collect::<HashSet<_>>().len()
    }

    /// Reciprocal-rank fusion over every (backend, query) result list.
    pub fn rrf(&self) -> f32 {
        let mut best: HashMap<usize, (Backend, usize)> = HashMap::new();
        for s in &self.sightings {
            let entry = best.entry(s.request).or_insert((s.backend, s.rank));
            entry.1 = entry.1.min(s.rank);
        }
        best.values().map(|&(backend, rank)| backend_weight(backend) / (rank as f32 + 2.0)).sum()
    }

    /// Share of query words Genius found verbatim in this song's lyrics, 0..=1.
    pub fn genius_exactness(&self) -> f32 {
        self.sightings
            .iter()
            .filter(|s| s.backend == Backend::Genius && s.query_words > 0)
            .filter_map(|s| s.hit.exact_words.or(s.hit.matched_words).map(|w| w as f32 / s.query_words as f32))
            .fold(0.0, f32::max)
            .min(1.0)
    }

    /// Popularity hint in 0..=1 from YouTube Music plays, Deezer rank or Genius page views.
    pub fn popularity(&self) -> f32 {
        let score = |s: &Sighting| -> Option<f32> {
            let value = s.hit.plays? as f32;
            let (floor, span) = match s.backend {
                Backend::YouTubeMusic => (3.0, 6.0),
                Backend::Deezer => (5.0, 1.0),
                Backend::Genius => (3.0, 4.0),
                Backend::ITunes => return None,
            };
            Some(((value.max(1.0).log10() - floor) / span).clamp(0.0, 1.0))
        };
        self.sightings.iter().filter_map(score).fold(0.0, f32::max)
    }

    /// Seen on a music catalog (not only on Genius, which also lists poems,
    /// transcripts and unreleased songs).
    pub fn in_catalog(&self) -> bool {
        self.sightings.iter().any(|s| s.backend != Backend::Genius)
    }

    /// Lyric excerpts returned with the hits (Genius highlights).
    pub fn snippets(&self) -> Vec<&str> {
        let mut out: Vec<&str> = Vec::new();
        for s in &self.sightings {
            if let Some(snippet) = s.hit.snippet.as_deref()
                && !out.contains(&snippet)
            {
                out.push(snippet);
            }
        }
        out
    }

    /// The hit whose metadata represents the song: catalog services first, plain
    /// titles before remasters, mixes and live versions, then the best rank.
    pub fn primary(&self) -> Option<&Hit> {
        self.sightings
            .iter()
            .min_by_key(|s| (backend_priority(s.backend), is_decorated(&s.hit.title), s.rank))
            .map(|s| &s.hit)
    }

    /// True when every hit looks like karaoke, an instrumental or a tribute.
    pub fn is_noise(&self) -> bool {
        self.sightings.iter().all(|s| text::is_noise_release(&s.hit.title, &s.hit.artist, s.hit.album.as_deref()))
    }

    /// The first hit with `has`, preferring the primary's version of the song.
    fn preferred(&self, primary: &Hit, has: impl Fn(&Hit) -> bool) -> Option<&Hit> {
        let version = text::fold(&primary.title);
        self.sightings
            .iter()
            .filter(|s| has(&s.hit))
            .min_by_key(|s| (text::fold(&s.hit.title) != version, backend_priority(s.backend), s.rank))
            .map(|s| &s.hit)
    }

    /// A hit with `has` of exactly the primary's version, if any.
    fn same_version(&self, primary: &Hit, has: impl Fn(&Hit) -> bool) -> Option<&Hit> {
        let version = text::fold(&primary.title);
        self.preferred(primary, has).filter(|h| text::fold(&h.title) == version)
    }

    /// `backend`'s hit for this song, preferring the primary's version.
    fn best_from(&self, primary: &Hit, backend: Backend) -> Option<&Hit> {
        let version = text::fold(&primary.title);
        self.sightings
            .iter()
            .filter(|s| s.backend == backend)
            .min_by_key(|s| (text::fold(&s.hit.title) != version, s.rank))
            .map(|s| &s.hit)
    }

    /// The candidate as a `Track` (identity key left for the engine).
    pub fn track(&self) -> Track {
        let Some(primary) = self.primary() else { return Track::default() };
        let artists = self
            .preferred(primary, |h| !h.artists.is_empty())
            .map(|h| h.artists.clone())
            .unwrap_or_else(|| text::artist_names(&primary.artist));
        // Recording-specific fields only come from the same version; artwork, year
        // and genre may come from any.
        let mut track = Track {
            title: primary.title.clone(),
            artist: primary.artist.clone(),
            artists,
            album: self.same_version(primary, |h| h.album.is_some()).and_then(|h| h.album.clone()),
            isrc: self.same_version(primary, |h| h.isrc.is_some()).and_then(|h| h.isrc.clone()),
            duration_ms: self.same_version(primary, |h| h.duration_ms.is_some()).and_then(|h| h.duration_ms),
            release_year: self.preferred(primary, |h| h.release_year.is_some()).and_then(|h| h.release_year),
            genre: self.preferred(primary, |h| h.genre.is_some()).and_then(|h| h.genre.clone()),
            artwork_url: self.preferred(primary, |h| h.artwork_url.is_some()).and_then(|h| h.artwork_url.clone()),
            preview_url: self.same_version(primary, |h| h.preview_url.is_some()).and_then(|h| h.preview_url.clone()),
            ..Track::default()
        };
        // Streaming services first, the lyrics page last.
        for backend in [Backend::ITunes, Backend::YouTubeMusic, Backend::Deezer, Backend::Genius] {
            let Some(hit) = self.best_from(primary, backend) else { continue };
            let (id_key, service) = match backend {
                Backend::ITunes => ("apple", Service::AppleMusic),
                Backend::Deezer => ("deezer", Service::Deezer),
                Backend::YouTubeMusic => ("youtube", Service::YouTubeMusic),
                Backend::Genius => ("genius", Service::Genius),
            };
            if let Some(id) = &hit.id {
                track.ids.insert(id_key.to_string(), id.clone());
            }
            if let Some(url) = &hit.url {
                track.links.push(ServiceLink { service, url: url.clone(), is_search: false });
            }
        }
        track
    }
}

/// Group hits into candidates. Sightings keep their order inside each candidate.
pub(crate) fn merge(sightings: Vec<Sighting>) -> Vec<Candidate> {
    let mut candidates: Vec<Candidate> = Vec::new();
    for sighting in sightings {
        let title_key = text::title_key(&sighting.hit.title);
        let mut artist_keys = text::artist_keys(&sighting.hit.artist);
        for name in &sighting.hit.artists {
            for key in text::artist_keys(name) {
                if !artist_keys.contains(&key) {
                    artist_keys.push(key);
                }
            }
        }
        if title_key.is_empty() {
            continue;
        }
        match candidates.iter_mut().find(|c| c.accepts(&title_key, &artist_keys)) {
            Some(candidate) => {
                for key in artist_keys {
                    if !candidate.artist_keys.contains(&key) {
                        candidate.artist_keys.push(key);
                    }
                }
                candidate.sightings.push(sighting);
            }
            None => candidates.push(Candidate { title_key, artist_keys, sightings: vec![sighting] }),
        }
    }
    candidates
}

/// True for titles with version decorations ("(Live Aid)", "- 2011 Remaster").
fn is_decorated(title: &str) -> bool {
    text::base_title(title) != title.trim()
}

/// Relative trust in a backend's ranking for lyric queries.
fn backend_weight(backend: Backend) -> f32 {
    match backend {
        Backend::ITunes | Backend::YouTubeMusic => 1.0,
        Backend::Genius => 0.9,
        Backend::Deezer => 0.8,
    }
}

/// Which backend's metadata to show first.
fn backend_priority(backend: Backend) -> u8 {
    match backend {
        Backend::ITunes => 0,
        Backend::Deezer => 1,
        Backend::YouTubeMusic => 2,
        Backend::Genius => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sighting(backend: Backend, request: usize, rank: usize, title: &str, artist: &str) -> Sighting {
        Sighting {
            backend,
            request,
            query_words: 9,
            rank,
            hit: Hit { title: title.into(), artist: artist.into(), ..Hit::default() },
        }
    }

    #[test]
    fn merges_versions_and_artist_orders() {
        let candidates = merge(vec![
            sighting(Backend::ITunes, 0, 0, "Something Just Like This", "The Chainsmokers & Coldplay"),
            sighting(Backend::Genius, 1, 0, "Something Just Like This", "Coldplay & The Chainsmokers"),
            sighting(Backend::YouTubeMusic, 2, 1, "Something Just Like This (Remix)", "The Chainsmokers"),
            sighting(Backend::Deezer, 3, 0, "Something Just Like This", "Some Cover Band"),
        ]);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].sightings.len(), 3);
        assert_eq!(candidates[0].backends(), 3);
        assert_eq!(candidates[1].backends(), 1);
    }

    #[test]
    fn rrf_counts_each_list_once() {
        let candidates = merge(vec![
            sighting(Backend::ITunes, 0, 0, "Song", "Artist"),
            sighting(Backend::ITunes, 0, 3, "Song (Live)", "Artist"),
            sighting(Backend::Deezer, 1, 2, "Song", "Artist"),
        ]);
        assert_eq!(candidates.len(), 1);
        let expected = 1.0 / 2.0 + 0.8 / 4.0;
        assert!((candidates[0].rrf() - expected).abs() < 1e-6);
    }

    #[test]
    fn track_prefers_catalog_metadata_and_collects_links() {
        let mut itunes = sighting(Backend::ITunes, 0, 1, "Tiny Dancer", "Elton John");
        itunes.hit.id = Some("1".into());
        itunes.hit.url = Some("https://music.apple.com/x".into());
        itunes.hit.artwork_url = Some("https://apple/1000x1000bb.jpg".into());
        itunes.hit.duration_ms = Some(377_000);
        let mut ytm = sighting(Backend::YouTubeMusic, 1, 0, "Tiny Dancer", "Elton John");
        ytm.hit.id = Some("aqlGlaNlcWE".into());
        ytm.hit.url = Some("https://music.youtube.com/watch?v=aqlGlaNlcWE".into());
        ytm.hit.artists = vec!["Elton John".into()];
        ytm.hit.plays = Some(355_000_000);
        let mut deezer = sighting(Backend::Deezer, 2, 0, "Tiny Dancer", "Elton John");
        deezer.hit.isrc = Some("GBAMS0100090".into());
        let candidates = merge(vec![ytm, itunes, deezer]);
        let track = candidates[0].track();
        assert_eq!(track.title, "Tiny Dancer");
        assert_eq!(track.artists, ["Elton John"]);
        assert_eq!(track.duration_ms, Some(377_000));
        assert_eq!(track.isrc.as_deref(), Some("GBAMS0100090"));
        assert_eq!(track.artwork_url.as_deref(), Some("https://apple/1000x1000bb.jpg"));
        assert_eq!(track.ids.get("apple").map(String::as_str), Some("1"));
        assert_eq!(track.ids.get("youtube").map(String::as_str), Some("aqlGlaNlcWE"));
        assert_eq!(track.links.len(), 2);
        assert!(track.identity_key.is_empty());
        assert!(candidates[0].popularity() > 0.8);
    }

    #[test]
    fn track_prefers_the_plain_version_and_keeps_its_recording_fields() {
        let mut live = sighting(Backend::ITunes, 0, 0, "We Are The Champions (Live Aid)", "Queen");
        live.hit.duration_ms = Some(237_000);
        live.hit.id = Some("1".into());
        let mut studio = sighting(Backend::ITunes, 1, 2, "We Are the Champions", "Queen");
        studio.hit.duration_ms = Some(179_000);
        studio.hit.id = Some("2".into());
        let mut deezer = sighting(Backend::Deezer, 2, 0, "We Are The Champions (Live Aid)", "Queen");
        deezer.hit.isrc = Some("GBUM71900001".into());
        let candidates = merge(vec![live, studio, deezer]);
        assert_eq!(candidates.len(), 1);
        let track = candidates[0].track();
        assert_eq!(track.title, "We Are the Champions");
        assert_eq!(track.duration_ms, Some(179_000));
        assert_eq!(track.ids.get("apple").map(String::as_str), Some("2"));
        // The only ISRC belongs to the live recording.
        assert_eq!(track.isrc, None);
    }

    #[test]
    fn noise_needs_every_hit_to_be_noise() {
        let candidates = merge(vec![
            sighting(Backend::ITunes, 0, 0, "All Star (Karaoke Version)", "Smash Mouth"),
            sighting(Backend::Deezer, 1, 0, "All Star", "Smash Mouth"),
            sighting(Backend::ITunes, 0, 1, "All Star (Karaoke Version)", "Party Tyme Karaoke"),
        ]);
        assert_eq!(candidates.len(), 2);
        assert!(!candidates[0].is_noise());
        assert!(candidates[1].is_noise());
    }
}
