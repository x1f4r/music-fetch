//! Records and enums shared by the engine and both apps.
//!
//! Everything here crosses the FFI boundary, so keep fields to UniFFI-supported
//! types. Times are milliseconds. Unix timestamps are `i64` milliseconds.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A service a track can be opened in, or a site a source came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum Service {
    Spotify,
    AppleMusic,
    YouTubeMusic,
    YouTube,
    Deezer,
    Tidal,
    AmazonMusic,
    SoundCloud,
    Bandcamp,
    Mixcloud,
    Vimeo,
    TikTok,
    Instagram,
    Facebook,
    X,
    Reddit,
    Twitch,
    Shazam,
    Genius,
    Qobuz,
    Pandora,
    Audiomack,
    Anghami,
    JioSaavn,
    Boomplay,
    Kkbox,
    YandexMusic,
    NetEaseMusic,
    LastFm,
    Web,
}

/// A link to a track on a service. `is_search` links open a search for the track
/// rather than the exact track page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct ServiceLink {
    pub service: Service,
    pub url: String,
    pub is_search: bool,
}

/// A recording: the unit every result points at.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, uniffi::Record)]
pub struct Track {
    pub title: String,
    /// Display string for all credited artists, e.g. "Calvin Harris & Dua Lipa".
    pub artist: String,
    /// Individual artist names when known; may be empty.
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub isrc: Option<String>,
    pub duration_ms: Option<u64>,
    pub release_year: Option<u16>,
    pub genre: Option<String>,
    /// Largest artwork available (ideally ≥ 600 px).
    pub artwork_url: Option<String>,
    /// Short audio preview; URLs from some services expire.
    pub preview_url: Option<String>,
    pub links: Vec<ServiceLink>,
    /// Provider ids, keyed by provider: "shazam", "apple", "spotify", "deezer",
    /// "acrcloud", "audd", "youtube", "melody".
    pub ids: HashMap<String, String>,
    /// Tiered identity key used for deduplication (see `identity`).
    pub identity_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum SessionKind {
    /// Microphone, recorded music expected.
    Listen,
    /// System or app audio on this device.
    SystemAudio,
    /// Humming, whistling or singing (explicit, or detected during `Listen`).
    Hum,
    /// A link: streaming track/album/playlist, video, page.
    Link,
    /// A local audio or video file.
    File,
    /// A typed lyric search.
    Lyrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum SessionStatus {
    Running,
    /// A live session found the recording.
    Matched,
    /// An impression or lyric search produced candidates.
    Candidates,
    /// A link or file analysis finished (see segments for what was found).
    Completed,
    NoMatch,
    Failed,
    Canceled,
    /// Captured while offline; identified automatically once a connection is back
    /// (see `Engine::retry_waiting`).
    Waiting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum SegmentKind {
    Track,
    /// Music that could not be identified.
    Unidentified,
    Speech,
    Silence,
}

/// Display tier for segments; drawn with fill style and text, never color alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, uniffi::Enum)]
pub enum Tier {
    Confirmed,
    Likely,
    Possible,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum EvidenceSource {
    Shazam,
    AudD,
    AcrCloud,
    /// Streaming-service page metadata (Spotify, Apple Music, ...).
    Metadata,
    /// Page attribution such as YouTube "Music" cards or a TikTok sound.
    PageHint,
    Melody,
    Lyrics,
    AcrHumming,
    User,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct Evidence {
    pub source: EvidenceSource,
    /// Position in the source media the evidence came from.
    pub at_ms: u64,
    /// Calibrated score in 0..=1.
    pub score: f32,
    /// Short human-readable detail, e.g. "offset 1:14, skew 0.3%".
    pub detail: Option<String>,
}

/// A stretch of a source: one identified track, or a gap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct Segment {
    /// Stable id within the session. Updates reuse the id.
    pub id: String,
    /// Playlist entry index (0 for single media).
    pub item_index: u32,
    pub start_ms: u64,
    pub end_ms: u64,
    pub kind: SegmentKind,
    pub track: Option<Track>,
    /// Calibrated confidence in 0..=1 (0 when `track` is None).
    pub confidence: f32,
    pub tier: Tier,
    pub evidence: Vec<Evidence>,
    pub alternates: Vec<Track>,
    /// Deep link to this moment in the source, when the site supports it.
    pub source_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, uniffi::Enum)]
pub enum CandidateTier {
    Strong,
    Possible,
    LongShot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum CandidateSourceKind {
    Melody,
    Lyrics,
    AcrHumming,
    Fingerprint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct CandidateSource {
    pub kind: CandidateSourceKind,
    pub score: f32,
    /// e.g. the matched lyric line, or "transposed −2, tempo ×0.9".
    pub detail: Option<String>,
}

/// "Your hum vs. the song": both contours in semitones relative to the query's
/// median, sampled every `frame_ms`. NaN marks unvoiced frames. The reference is
/// already transposed and time-aligned to the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct MelodyAlignment {
    pub frame_ms: u32,
    pub query: Vec<f32>,
    pub reference: Vec<f32>,
}

/// One possible answer to an impression (hum, whistle, singing) or lyric search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct Candidate {
    pub track: Track,
    /// Calibrated probability-like score in 0..=1.
    pub score: f32,
    pub tier: CandidateTier,
    pub sources: Vec<CandidateSource>,
    pub alignment: Option<MelodyAlignment>,
}

/// What a link or file turned out to be.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, uniffi::Record)]
pub struct SourceInfo {
    /// Canonical URL (None for local files).
    pub url: Option<String>,
    pub title: Option<String>,
    pub uploader: Option<String>,
    pub thumbnail_url: Option<String>,
    pub duration_ms: Option<u64>,
    pub service: Option<Service>,
    /// Site name for display, e.g. "YouTube", "Mixcloud", "example.com".
    pub site: Option<String>,
    /// Number of entries (1 for a single video/track).
    pub item_count: u32,
    /// Attribution hints from the page (YouTube "Music" cards, TikTok sound).
    pub hints: Vec<Track>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct PitchFrame {
    /// Time since the session started.
    pub t_ms: u64,
    /// MIDI note number (fractional). NaN when unvoiced.
    pub midi: f32,
    /// Voicing confidence 0..=1.
    pub confidence: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum Phase {
    Listening,
    Resolving,
    Downloading,
    Decoding,
    Segmenting,
    Recognizing,
    MatchingMelody,
    SearchingLyrics,
    Finishing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Enum)]
pub enum Notice {
    /// Listening for a while with a weak signal.
    MoveCloser,
    /// The input sounds like humming, whistling or singing.
    ImpressionDetected,
    /// Impression is too short so far; keep going.
    KeepGoing,
    /// Shazam asked us to slow down; queries resume after the delay.
    RateLimited { retry_in_ms: u64 },
    /// Captured audio is digital silence (permission denied or app blocks capture).
    CaptureSilent,
    /// Melody matching skipped because the melody index is not installed.
    MelodyIndexMissing,
    /// A provider failed; analysis continues without it.
    ProviderUnavailable { provider: String, message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum NoMatchReason {
    TooQuiet,
    TooShort,
    Noisy,
    MostlySpeech,
    CaptureSilent,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Enum)]
#[allow(clippy::large_enum_variant)] // FFI value type; boxing would change the bindings.
pub enum Outcome {
    /// A live session identified the recording.
    Matched {
        track: Track,
    },
    /// Candidates were produced (see the last `Candidates` event).
    Candidates {
        count: u32,
    },
    /// Link/file analysis finished.
    Completed {
        identified: u32,
        total: u32,
    },
    NoMatch {
        reason: NoMatchReason,
    },
    /// A live session could not reach the recognizer (no network). The capture is
    /// kept and the session is stored as `Waiting` until `Engine::retry_waiting`
    /// identifies it.
    Waiting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum ErrorCode {
    InvalidInput,
    UnsupportedLink,
    Network,
    RateLimited,
    MediaUnavailable,
    Decode,
    Platform,
    Storage,
    IndexUnavailable,
    Internal,
}

/// Events pushed to a `SessionObserver`, in order, from engine threads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Enum)]
pub enum SessionEvent {
    Started {
        session_id: String,
        kind: SessionKind,
    },
    /// `fraction` is set for phases with measurable progress (downloads).
    Phase {
        phase: Phase,
        fraction: Option<f32>,
    },
    /// Analysis progress through a link or file.
    Progress {
        analyzed_ms: u64,
        total_ms: u64,
        eta_ms: Option<u64>,
    },
    /// Live input level, about 30 per second. Perceptual 0..=1: −60 dBFS maps to
    /// 0 and 0 dBFS to 1 (linear in dB in between).
    Level {
        rms: f32,
        peak: f32,
    },
    /// Batched live pitch frames, about 10 batches per second.
    Pitch {
        frames: Vec<PitchFrame>,
    },
    Source {
        info: SourceInfo,
    },
    /// A playlist entry started.
    Item {
        index: u32,
        count: u32,
        title: Option<String>,
    },
    /// Insert or replace (by id) a segment.
    Segment {
        segment: Segment,
    },
    /// A segment was merged into another and should be removed.
    SegmentRemoved {
        id: String,
    },
    Candidates {
        candidates: Vec<Candidate>,
        is_final: bool,
    },
    Notice {
        notice: Notice,
    },
    Finished {
        outcome: Outcome,
    },
    Failed {
        code: ErrorCode,
        message: String,
    },
    /// The session was canceled by the app.
    Canceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum CaptureSource {
    Microphone,
    SystemAudio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum ListenMode {
    /// Recording first; switches to impression analysis when humming or singing is detected.
    Auto,
    /// Recording only (system audio always uses this).
    Recording,
    /// Humming, whistling or singing only.
    Hum,
}

/// Options for `Engine::start_listening_with`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Record)]
pub struct ListenOptions {
    /// Stop automatically after a hummed/sung phrase ends (default true). When false
    /// the session runs until `finish()` or the time limit.
    pub auto_stop: bool,
}

impl Default for ListenOptions {
    fn default() -> Self {
        Self { auto_stop: true }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum AnalyzeMode {
    /// Pick the strategy from the input.
    Auto,
    /// Treat the input as a recording (single or mix by duration).
    Recording,
    /// Treat the input as someone humming or singing.
    Impression,
    /// Force the long-mix strategy.
    Mix,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct LibraryQuery {
    /// Empty means all kinds.
    pub kinds: Vec<SessionKind>,
    pub search: Option<String>,
    pub pinned_only: bool,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct LibraryEntry {
    pub id: String,
    pub kind: SessionKind,
    pub status: SessionStatus,
    pub created_at_ms: i64,
    pub title: String,
    pub subtitle: Option<String>,
    pub artwork_url: Option<String>,
    /// The single answer, for sessions that have one.
    pub track: Option<Track>,
    /// Identified tracks (link/file sessions).
    pub track_count: u32,
    pub duration_ms: Option<u64>,
    pub source_url: Option<String>,
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct SessionDetail {
    pub entry: LibraryEntry,
    pub source: Option<SourceInfo>,
    pub segments: Vec<Segment>,
    pub candidates: Vec<Candidate>,
    /// What speech recognition heard (sung lyrics), if anything.
    pub transcript: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, uniffi::Enum)]
pub enum ExportFormat {
    /// "[mm:ss] Artist - Title" lines.
    Text,
    Csv,
    Json,
    Cue,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, uniffi::Record)]
pub struct MelodyIndexStatus {
    pub installed: bool,
    pub version: Option<String>,
    pub song_count: u32,
    pub size_bytes: u64,
    /// Newer version on the server (set after `check_melody_index`).
    pub available_version: Option<String>,
    pub download_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, uniffi::Record)]
pub struct StorageUsage {
    pub database_bytes: u64,
    pub cache_bytes: u64,
    pub melody_index_bytes: u64,
}

/// Lyrics for a track (from LRCLIB).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct Lyrics {
    pub plain: String,
    /// Time-synced lines when available; empty otherwise.
    pub synced: Vec<LyricLine>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct LyricLine {
    pub t_ms: u64,
    pub text: String,
}
