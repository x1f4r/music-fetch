//! Track identity: fuzzy keys, tiered identity and the ISRC veto (ported from 0.4).

use crate::model::Track;

/// `fuzzy::<artists>::<title>` with feat./remaster/edit noise removed.
pub fn fuzzy_key(title: &str, artist: &str) -> String {
    let _ = (title, artist);
    todo!("identity")
}

/// `isrc::…`, else a provider id key, else the fuzzy key.
pub fn identity_key(track: &Track) -> String {
    let _ = track;
    todo!("identity")
}

/// Same recording? Different ISRCs veto; otherwise ids or fuzzy keys decide.
pub fn same_track(a: &Track, b: &Track) -> bool {
    let _ = (a, b);
    todo!("identity")
}
