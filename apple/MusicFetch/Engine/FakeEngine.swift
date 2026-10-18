import Foundation
import MusicFetchCore
import Synchronization

/// A scripted `MusicEngine` for SwiftUI previews and deterministic screenshots
/// (`-MFDemoState <state>`). Nothing touches the network or the disk.
nonisolated final class FakeEngine: MusicEngine {
    /// How live sessions and analyses behave.
    enum Scenario: String, Sendable {
        /// Listening for 5 s, then a match.
        case interactive
        /// Listening forever.
        case listening
        /// Humming forever (impression, pitch trace).
        case hum
        /// Humming forever while the progressive search finds answers (live candidates).
        case humAnswers
        /// Capture ends at once and matching never finishes.
        case matching
        /// No match after 2 s.
        case noMatch
        /// Silent capture.
        case captureSilent
        /// A link that keeps downloading.
        case downloading
        /// A mix that keeps streaming segments.
        case mix
        /// Live sessions only start; unit tests drive them through `lastLiveSession`.
        case manual
    }

    let scenario: Scenario
    private let pinned = Mutex<Set<String>>([DemoFixtures.mixID, DemoFixtures.nightsID])
    private let deleted = Mutex<Set<String>>([])
    private let live = Mutex<FakeSession?>(nil)
    private let melody = Mutex(DemoFixtures.melodyStatus)

    /// What Settings › Melody shows (demo states).
    func setMelodyStatus(_ status: MelodyIndexStatus) {
        melody.withLock { $0 = status }
    }

    /// The newest live session (unit tests emit its events with the `manual` scenario).
    var lastLiveSession: FakeSession? { live.withLock { $0 } }

    init(scenario: Scenario) {
        self.scenario = scenario
    }

    var acceptsTranscripts: Bool { true }

    func setCredentials(_ credentials: Credentials) {}

    func setLowPower(_ enabled: Bool) {}

    func startListening(source: CaptureSource, mode: ListenMode, options: ListenOptions, observer: SessionObserver) throws
        -> LiveSessionHandle
    {
        let session = FakeSession(observer: observer)
        session.listenOptions = options
        live.withLock { $0 = session }
        let kind: SessionKind = mode == .hum ? .hum : (source == .systemAudio ? .systemAudio : .listen)
        session.run { session in
            session.emit(.started(sessionId: session.sessionID, kind: kind))
            session.emit(.phase(phase: .listening, fraction: nil))
            if self.scenario == .manual { return }
            let humming = mode == .hum || self.scenario == .hum || self.scenario == .humAnswers
            if humming, mode != .hum { session.emit(.notice(notice: .impressionDetected)) }
            if self.scenario == .matching {
                for tick in 0..<30 { session.emit(.level(rms: 0.5 + 0.2 * Float(sin(Double(tick))), peak: 0.8)) }
                session.emit(.phase(phase: .recognizing, fraction: nil))
                while !session.isDone { session.sleep(0.2) }
                return
            }
            var tick = 0
            var t: UInt64 = 0
            while !session.isDone {
                let wave = Float(0.5 + 0.35 * sin(Double(tick) / 7) + 0.1 * sin(Double(tick) / 2.3))
                session.emit(.level(rms: self.scenario == .captureSilent ? 0 : wave, peak: min(wave + 0.2, 1)))
                if humming, tick % 3 == 0 {
                    let frames = (0..<2).map { offset -> PitchFrame in
                        let index = Int(t / 50) + offset
                        let note = DemoFixtures.contour(seed: 0, frames: index + 1)[index]
                        return PitchFrame(tMs: t + UInt64(offset * 50), midi: note.isNaN ? .nan : 62 + note, confidence: 0.9)
                    }
                    t += 100
                    session.emit(.pitch(frames: frames))
                }
                if self.scenario == .captureSilent, tick == 30 { session.emit(.notice(notice: .captureSilent)) }
                // A search every 2 s once 4 s are voiced (the demo hums from the start).
                if humming, options.liveCandidates, tick >= 120, tick % 60 == 0 {
                    let lists = DemoFixtures.liveLists
                    let list = self.scenario == .humAnswers ? lists[1] : lists[min(tick / 60 - 2, lists.count - 1)]
                    session.emit(.candidates(candidates: list, isFinal: false))
                }
                if self.scenario == .noMatch, tick == 60 {
                    session.emit(.finished(outcome: .noMatch(reason: .notFound)))
                    return
                }
                if self.scenario == .interactive, !humming, tick == 150 {
                    session.emit(.segment(segment: DemoFixtures.detail(DemoFixtures.resultID)!.segments[0]))
                    session.emit(.finished(outcome: .matched(track: DemoFixtures.houdini)))
                    return
                }
                tick += 1
                session.sleep(1.0 / 30)
            }
            if session.wasCanceled {
                session.emit(.canceled)
            } else if humming {
                session.emit(.phase(phase: .matchingMelody, fraction: nil))
                session.sleep(1)
                session.emit(.candidates(candidates: DemoFixtures.candidates, isFinal: true))
                session.emit(.finished(outcome: .candidates(count: UInt32(DemoFixtures.candidates.count))))
            } else {
                session.emit(.phase(phase: .recognizing, fraction: nil))
                session.sleep(1)
                session.emit(.segment(segment: DemoFixtures.detail(DemoFixtures.resultID)!.segments[0]))
                session.emit(.finished(outcome: .matched(track: DemoFixtures.houdini)))
            }
        }
        return session
    }

    func analyze(input: String, mode: AnalyzeMode, observer: SessionObserver) throws -> AnalysisSessionHandle {
        guard input.hasPrefix("http") || input.hasPrefix("/") else {
            throw MfError.UnsupportedLink(message: "This link isn't supported.")
        }
        let session = FakeSession(observer: observer)
        session.run { session in
            session.emit(.started(sessionId: session.sessionID, kind: input.hasPrefix("/") ? .file : .link))
            session.emit(.phase(phase: .resolving, fraction: nil))
            session.sleep(0.5)
            if input.contains("spotify") || input.contains("album") {
                session.emit(.source(info: DemoFixtures.collectionSource))
                for segment in DemoFixtures.collectionSegments { session.emit(.segment(segment: segment)) }
                let count = UInt32(DemoFixtures.collectionSegments.count)
                session.emit(.finished(outcome: .completed(identified: count, total: count)))
                return
            }
            session.emit(.source(info: DemoFixtures.mixSource))
            var fraction: Float = 0
            while fraction < 1, !session.isDone {
                session.emit(.phase(phase: .downloading, fraction: fraction))
                fraction += self.scenario == .downloading ? 0.004 : 0.05
                if self.scenario == .downloading, fraction > 0.38 { fraction = 0.38 }
                session.sleep(0.1)
            }
            session.emit(.phase(phase: .recognizing, fraction: nil))
            let total = DemoFixtures.mixSource.durationMs ?? 0
            for (index, segment) in DemoFixtures.mixSegments.enumerated() where !session.isDone {
                session.emit(.segment(segment: segment))
                let analyzed = min(segment.endMs + 40_000, total)
                session.emit(.progress(analyzedMs: analyzed, totalMs: total, etaMs: UInt64(240_000 - index * 20_000)))
                session.sleep(self.scenario == .mix && index >= 7 ? 3600 : 0.6)
            }
            if session.wasCanceled {
                session.emit(.canceled)
            } else {
                session.emit(.finished(outcome: .completed(identified: 7, total: 8)))
            }
        }
        return session
    }

    func searchLyrics(text: String, observer: SessionObserver) throws -> AnalysisSessionHandle {
        let session = FakeSession(observer: observer)
        session.run { session in
            session.emit(.started(sessionId: session.sessionID, kind: .lyrics))
            session.emit(.phase(phase: .searchingLyrics, fraction: nil))
            session.sleep(0.8)
            let results = Array(DemoFixtures.candidates.prefix(3)).map { candidate -> Candidate in
                var candidate = candidate
                candidate.alignment = nil
                candidate.sources = [CandidateSource(kind: .lyrics, score: candidate.score, detail: nil)]
                return candidate
            }
            let found = text.count > 3
            session.emit(.candidates(candidates: found ? results : [], isFinal: true))
            session.emit(.finished(outcome: found ? .candidates(count: UInt32(results.count)) : .noMatch(reason: .notFound)))
        }
        return session
    }

    func library(query: LibraryQuery) throws -> [LibraryEntry] {
        let pins = pinned.withLock { $0 }
        let removed = deleted.withLock { $0 }
        let search = query.search?.lowercased()
        return DemoFixtures.entries.compactMap { entry -> LibraryEntry? in
            var entry = entry
            entry.pinned = pins.contains(entry.id)
            guard !removed.contains(entry.id) else { return nil }
            if !query.kinds.isEmpty, !query.kinds.contains(entry.kind) { return nil }
            if query.pinnedOnly, !entry.pinned { return nil }
            if let search, !search.isEmpty,
                !"\(entry.title) \(entry.subtitle ?? "") \(entry.sourceUrl ?? "")".lowercased().contains(search)
            {
                return nil
            }
            return entry
        }
    }

    func session(id: String) throws -> SessionDetail? {
        guard var detail = DemoFixtures.detail(id) else { return nil }
        detail.entry.pinned = pinned.withLock { $0.contains(id) }
        return detail
    }

    func deleteSessions(ids: [String]) throws {
        deleted.withLock { $0.formUnion(ids) }
    }

    func setPinned(id: String, pinned value: Bool) throws {
        pinned.withLock { set in
            if value { set.insert(id) } else { set.remove(id) }
        }
    }

    func correctSegment(sessionID: String, segmentID: String, track: Track) throws -> Segment {
        guard var segment = DemoFixtures.detail(sessionID)?.segments.first(where: { $0.id == segmentID }) else {
            throw MfError.InvalidInput(message: "The segment no longer exists.")
        }
        segment.track = track
        segment.kind = .track
        segment.tier = .confirmed
        segment.confidence = 1
        segment.evidence.append(Evidence(source: .user, atMs: segment.startMs, score: 1, detail: nil))
        return segment
    }

    func confirmCandidate(sessionID: String, track: Track) throws {}

    func rejectSegment(sessionID: String, segmentID: String) throws -> Segment {
        guard var segment = DemoFixtures.detail(sessionID)?.segments.first(where: { $0.id == segmentID }) else {
            throw MfError.InvalidInput(message: "The segment no longer exists.")
        }
        if let track = segment.track { segment.alternates.insert(track, at: 0) }
        segment.track = nil
        segment.kind = .unidentified
        segment.tier = .unknown
        segment.confidence = 0
        return segment
    }

    func retryWaiting(observer: SessionObserver) throws -> UInt32 { 0 }

    func exportSession(id: String, format: ExportFormat) throws -> String {
        let segments = DemoFixtures.detail(id)?.segments ?? []
        return segments.compactMap { segment in
            segment.track.map { "[\(Format.cue(milliseconds: segment.startMs))] \($0.artist) - \($0.title)" }
        }.joined(separator: "\n")
    }

    func storageUsage() -> StorageUsage {
        StorageUsage(databaseBytes: 2_400_000, cacheBytes: 1_150_000_000, melodyIndexBytes: 61_000_000)
    }

    func clearCache() throws {}

    func melodyIndexStatus() -> MelodyIndexStatus {
        melody.withLock { $0 }
    }

    func melodyEncoderBytes() -> UInt64? {
        melody.withLock { $0.encoderInstalled } ? DemoFixtures.encoderBytes : nil
    }

    func checkMelodyIndex() throws -> MelodyIndexStatus {
        melody.withLock { $0 }
    }

    func installMelodyIndex(observer: SessionObserver) throws -> AnalysisSessionHandle {
        let session = FakeSession(observer: observer)
        session.run { session in
            session.emit(.started(sessionId: session.sessionID, kind: .hum))
            var fraction: Float = 0
            while fraction < 1, !session.isDone {
                session.emit(.phase(phase: .downloading, fraction: fraction))
                fraction += 0.005
                session.sleep(0.1)
            }
            if !session.wasCanceled {
                self.melody.withLock { status in
                    status.encoderInstalled = true
                    status.availableVersion = nil
                    status.downloadBytes = nil
                }
            }
            session.emit(session.wasCanceled ? .canceled : .finished(outcome: .completed(identified: 1, total: 1)))
        }
        return session
    }

    func removeMelodyIndex() throws {}

    func enrichTrack(_ track: Track) async -> Track {
        track
    }

    func fetchLyrics(_ track: Track) async -> Lyrics? {
        DemoFixtures.lyrics
    }
}

/// A scripted session running on its own thread.
nonisolated final class FakeSession: LiveSessionHandle, AnalysisSessionHandle, @unchecked Sendable {
    let sessionID = UUID().uuidString.lowercased()
    private let observer: SessionObserver
    private let state = Mutex<(finished: Bool, canceled: Bool)>((false, false))
    private let heard = Mutex<[HeardWords]>([])
    private let options = Mutex<ListenOptions?>(nil)

    init(observer: SessionObserver) {
        self.observer = observer
    }

    var isDone: Bool { state.withLock { $0.finished || $0.canceled } }
    var wasCanceled: Bool { state.withLock { $0.canceled } }

    func run(_ script: @escaping @Sendable (FakeSession) -> Void) {
        let thread = Thread { script(self) }
        thread.name = "FakeSession"
        thread.start()
    }

    func emit(_ event: SessionEvent) {
        observer.onEvent(event: event)
    }

    func sleep(_ seconds: TimeInterval) {
        let end = Date.now.addingTimeInterval(seconds)
        while Date.now < end, !isDone {
            Thread.sleep(forTimeInterval: min(0.05, end.timeIntervalSinceNow))
        }
    }

    func pushPcm(_ data: Data, sampleRate: UInt32, channels: UInt32) {}

    func pushTranscript(_ words: HeardWords) {
        heard.withLock { $0.append(words) }
    }

    /// Transcripts the app pushed, oldest first (tests).
    var transcripts: [HeardWords] { heard.withLock { $0 } }

    /// The options the session was started with (tests).
    var listenOptions: ListenOptions? {
        get { options.withLock { $0 } }
        set { options.withLock { $0 = newValue } }
    }

    func finish() {
        state.withLock { $0.finished = true }
    }

    func cancel() {
        state.withLock { $0.canceled = true }
    }
}
