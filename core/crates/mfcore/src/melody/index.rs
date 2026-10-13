//! The melody index file: song metadata plus reference phrases as notes.

use std::path::Path;

use crate::error::Result;

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

pub struct MelodyIndex {}

impl MelodyIndex {
    pub fn open(path: &Path) -> Result<Self> {
        let _ = path;
        todo!("melody::index")
    }

    pub fn version(&self) -> &str {
        todo!("melody::index")
    }

    pub fn song_count(&self) -> u32 {
        todo!("melody::index")
    }

    pub fn song(&self, id: u32) -> Option<SongMeta> {
        let _ = id;
        todo!("melody::index")
    }

    /// Songs whose normalized title/artist match (used to fuse lyric candidates).
    pub fn find_song(&self, title: &str, artist: &str) -> Option<SongMeta> {
        let _ = (title, artist);
        todo!("melody::index")
    }
}
