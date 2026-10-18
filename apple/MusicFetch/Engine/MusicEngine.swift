import Foundation
import MusicFetchCore

/// A running live session (microphone or system audio).
nonisolated protocol LiveSessionHandle: AnyObject, Sendable {
    var sessionID: String { get }
    /// Interleaved little-endian f32 PCM. Cheap; never blocks.
    func pushPcm(_ data: Data, sampleRate: UInt32, channels: UInt32)
    /// The user stopped capture: analyze what was captured.
    func finish()
    func cancel()
    /// What the on-device recognizer heard so far (sung words); each call replaces the
    /// last. Cheap; any thread.
    func pushTranscript(_ words: HeardWords)
}

/// A running analysis (link, file, lyric search, melody-index download).
nonisolated protocol AnalysisSessionHandle: AnyObject, Sendable {
    var sessionID: String { get }
    func cancel()
}

/// The engine surface the app uses. `CoreEngine` forwards to mfcore; `FakeEngine`
/// replays scripted sessions for previews and screenshots.
///
/// Methods marked "blocking" must not run on the main thread; use `offMain`.
nonisolated protocol MusicEngine: AnyObject, Sendable {
    /// Live sessions take the app's transcripts of sung words (`ListenOptions.transcripts`,
    /// `LiveSessionHandle.pushTranscript`); otherwise the engine asks its speech service.
    var acceptsTranscripts: Bool { get }
    func setCredentials(_ credentials: Credentials)
    /// Less CPU and memory for weaker Macs and Low Power Mode.
    func setLowPower(_ enabled: Bool)
    func startListening(source: CaptureSource, mode: ListenMode, options: ListenOptions, observer: SessionObserver) throws
        -> LiveSessionHandle
    func analyze(input: String, mode: AnalyzeMode, observer: SessionObserver) throws -> AnalysisSessionHandle
    func searchLyrics(text: String, observer: SessionObserver) throws -> AnalysisSessionHandle
    /// Blocking.
    func library(query: LibraryQuery) throws -> [LibraryEntry]
    /// Blocking.
    func session(id: String) throws -> SessionDetail?
    /// Blocking.
    func deleteSessions(ids: [String]) throws
    func setPinned(id: String, pinned: Bool) throws
    func correctSegment(sessionID: String, segmentID: String, track: Track) throws -> Segment
    /// "Not this": the segment becomes unidentified and its track an alternate.
    func rejectSegment(sessionID: String, segmentID: String) throws -> Segment
    /// Starts identifying sessions saved while offline; returns how many started.
    /// Blocking.
    func retryWaiting(observer: SessionObserver) throws -> UInt32
    func confirmCandidate(sessionID: String, track: Track) throws
    /// Blocking.
    func exportSession(id: String, format: ExportFormat) throws -> String
    /// Blocking.
    func storageUsage() -> StorageUsage
    /// Blocking.
    func clearCache() throws
    func melodyIndexStatus() -> MelodyIndexStatus
    /// Size of the installed index's encoder file, when there is one.
    func melodyEncoderBytes() -> UInt64?
    /// Blocking (network).
    func checkMelodyIndex() throws -> MelodyIndexStatus
    func installMelodyIndex(observer: SessionObserver) throws -> AnalysisSessionHandle
    /// Blocking.
    func removeMelodyIndex() throws
    func enrichTrack(_ track: Track) async -> Track
    func fetchLyrics(_ track: Track) async -> Lyrics?
}

/// Runs a blocking engine call on the cooperative pool instead of the caller's actor.
@concurrent
nonisolated func offMain<T: Sendable>(_ body: @Sendable () throws -> T) async throws -> T {
    try body()
}

/// Non-throwing variant of `offMain`.
@concurrent
nonisolated func offMain<T: Sendable>(_ body: @Sendable () -> T) async -> T {
    body()
}

// MARK: - mfcore adapter

/// `MusicEngine` backed by the Rust engine.
nonisolated final class CoreEngine: MusicEngine {
    private let engine: Engine
    private let dataDir: URL?

    init(engine: Engine, dataDir: URL? = nil) {
        self.engine = engine
        self.dataDir = dataDir
    }

    var acceptsTranscripts: Bool { true }

    func setCredentials(_ credentials: Credentials) {
        engine.setCredentials(credentials: credentials)
    }

    func setLowPower(_ enabled: Bool) {
        engine.setLowPower(enabled: enabled)
    }

    func startListening(source: CaptureSource, mode: ListenMode, options: ListenOptions, observer: SessionObserver) throws
        -> LiveSessionHandle
    {
        CoreLiveSession(try engine.startListeningWith(source: source, mode: mode, options: options, observer: observer))
    }

    func analyze(input: String, mode: AnalyzeMode, observer: SessionObserver) throws -> AnalysisSessionHandle {
        CoreAnalysisSession(try engine.analyze(input: input, mode: mode, observer: observer))
    }

    func searchLyrics(text: String, observer: SessionObserver) throws -> AnalysisSessionHandle {
        CoreAnalysisSession(try engine.searchLyrics(text: text, observer: observer))
    }

    func library(query: LibraryQuery) throws -> [LibraryEntry] {
        try engine.library(query: query)
    }

    func session(id: String) throws -> SessionDetail? {
        try engine.session(id: id)
    }

    func deleteSessions(ids: [String]) throws {
        try engine.deleteSessions(ids: ids)
    }

    func setPinned(id: String, pinned: Bool) throws {
        try engine.setPinned(id: id, pinned: pinned)
    }

    func correctSegment(sessionID: String, segmentID: String, track: Track) throws -> Segment {
        try engine.correctSegment(sessionId: sessionID, segmentId: segmentID, track: track)
    }

    func confirmCandidate(sessionID: String, track: Track) throws {
        try engine.confirmCandidate(sessionId: sessionID, track: track)
    }

    func rejectSegment(sessionID: String, segmentID: String) throws -> Segment {
        try engine.rejectSegment(sessionId: sessionID, segmentId: segmentID)
    }

    func retryWaiting(observer: SessionObserver) throws -> UInt32 {
        try engine.retryWaiting(observer: observer)
    }

    func exportSession(id: String, format: ExportFormat) throws -> String {
        try engine.exportSession(id: id, format: format)
    }

    func storageUsage() -> StorageUsage {
        engine.storageUsage()
    }

    func clearCache() throws {
        try engine.clearCache()
    }

    func melodyIndexStatus() -> MelodyIndexStatus {
        engine.melodyIndexStatus()
    }

    func checkMelodyIndex() throws -> MelodyIndexStatus {
        try engine.checkMelodyIndex()
    }

    /// `data_dir/melody/melody-encoder.mfe` (docs/CORE_API.md, "Melody index").
    func melodyEncoderBytes() -> UInt64? {
        guard let dataDir else { return nil }
        let url = dataDir.appending(path: "melody", directoryHint: .isDirectory).appending(path: "melody-encoder.mfe")
        return (try? FileManager.default.attributesOfItem(atPath: url.path)[.size] as? NSNumber)?.uint64Value
    }

    func installMelodyIndex(observer: SessionObserver) throws -> AnalysisSessionHandle {
        CoreAnalysisSession(try engine.installMelodyIndex(observer: observer))
    }

    func removeMelodyIndex() throws {
        try engine.removeMelodyIndex()
    }

    func enrichTrack(_ track: Track) async -> Track {
        await engine.enrichTrack(track: track)
    }

    func fetchLyrics(_ track: Track) async -> Lyrics? {
        await engine.fetchLyrics(track: track)
    }
}

nonisolated private final class CoreLiveSession: LiveSessionHandle {
    private let session: LiveSession
    let sessionID: String

    init(_ session: LiveSession) {
        self.session = session
        self.sessionID = session.id()
    }

    func pushPcm(_ data: Data, sampleRate: UInt32, channels: UInt32) {
        session.pushPcm(data: data, sampleRate: sampleRate, channels: channels)
    }

    func finish() {
        session.finish()
    }

    func cancel() {
        session.cancel()
    }

    func pushTranscript(_ words: HeardWords) {
        session.pushTranscript(
            transcript: SpeechTranscript(
                text: words.text, alternatives: words.alternatives, language: words.language, confidence: words.confidence,
                isFinal: words.isFinal))
    }
}

nonisolated private final class CoreAnalysisSession: AnalysisSessionHandle {
    private let session: AnalysisSession
    let sessionID: String

    init(_ session: AnalysisSession) {
        self.session = session
        self.sessionID = session.id()
    }

    func cancel() {
        session.cancel()
    }
}

// MARK: - Errors

extension MfError {
    /// The `ErrorCode` the UI maps to copy.
    nonisolated var code: ErrorCode {
        switch self {
        case .InvalidInput: .invalidInput
        case .UnsupportedLink: .unsupportedLink
        case .Network: .network
        case .RateLimited: .rateLimited
        case .MediaUnavailable: .mediaUnavailable
        case .Decode: .decode
        case .Platform: .platform
        case .Storage: .storage
        case .IndexUnavailable: .indexUnavailable
        case .Canceled, .Internal: .internal
        }
    }

    /// The engine's short message, when it has one.
    nonisolated var message: String? {
        switch self {
        case .InvalidInput(let message), .UnsupportedLink(let message), .Network(let message),
             .MediaUnavailable(let message), .Decode(let message), .Platform(let message),
             .Storage(let message), .Internal(let message):
            message
        case .RateLimited, .IndexUnavailable, .Canceled:
            nil
        }
    }
}
