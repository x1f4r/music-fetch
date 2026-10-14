import AppKit
import Foundation
import MusicFetchCore
import Observation

/// One session as the UI sees it: a live result, hum candidates, a link or file
/// analysis, a lyric search, or a library entry. Fed by engine events and/or the
/// stored `SessionDetail`.
@Observable
final class SessionModel: Identifiable {
    /// Which screen shows the session.
    enum Presentation: Equatable {
        /// Still working and nothing to list yet (the Listen stage shows progress).
        case running
        /// One track: §6.3.
        case result
        /// Ranked hum or lyric candidates: §6.4.
        case candidates
        /// Long media with a timeline strip: §6.5.
        case timeline
        /// Album or playlist rows without a timeline: §6.6.
        case collection
        /// Finished without anything to show (no match, failure, canceled).
        case empty
    }

    struct Progress: Equatable {
        var analyzedMs: UInt64
        var totalMs: UInt64
        var etaMs: UInt64?
    }

    struct Failure: Equatable {
        var code: ErrorCode
        var message: String?
    }

    struct Item: Equatable {
        var index: UInt32
        var count: UInt32
        var title: String?
    }

    let id: String
    private(set) var kind: SessionKind
    private(set) var status: SessionStatus
    private(set) var createdAt: Date
    private(set) var entry: LibraryEntry?
    private(set) var source: SourceInfo?
    private(set) var phase: Phase?
    private(set) var fraction: Double?
    private(set) var progress: Progress?
    private(set) var segments: [Segment] = []
    private(set) var candidates: [Candidate] = []
    private(set) var candidatesFinal = false
    private(set) var item: Item?
    private(set) var matchedTrack: Track?
    private(set) var confirmedTrack: Track?
    private(set) var noMatchReason: NoMatchReason?
    private(set) var failure: Failure?
    private(set) var transcript: String?
    private(set) var completed: (identified: UInt32, total: UInt32)?
    private(set) var isRunning: Bool
    private(set) var wasStopped = false
    var pinned: Bool
    /// The analysis mode the session was started with (links and files).
    var mode: AnalyzeMode = .auto
    /// Where a live session listened (the inspector's "Source").
    var origin: ListenSource?
    /// The live pitch trace of a hum, frozen when capture ended (MIDI, NaN = unvoiced).
    var humTrace: [Float] = []
    var humSeconds: Int?
    /// Lyrics of the result track, loaded on demand.
    var lyrics: Lyrics?
    var lyricsLoaded = false

    // Selection shared by the list, the strip and the inspector.
    var selectedSegmentID: String?
    var selectedCandidateID: String?
    /// Timeline zoom, 1…40.
    var zoom: Double = 1

    /// Called on every event after the model applied it.
    @ObservationIgnored var onEvent: ((SessionModel, SessionEvent) -> Void)?
    @ObservationIgnored private var handle: AnalysisSessionHandle?
    @ObservationIgnored private var consumer: Task<Void, Never>?

    init(id: String, kind: SessionKind, createdAt: Date = .now, isRunning: Bool) {
        self.id = id
        self.kind = kind
        self.status = isRunning ? .running : .completed
        self.createdAt = createdAt
        self.isRunning = isRunning
        self.pinned = false
    }

    /// A stored session.
    convenience init(detail: SessionDetail) {
        self.init(id: detail.entry.id, kind: detail.entry.kind, createdAt: Format.date(milliseconds: detail.entry.createdAtMs),
                  isRunning: detail.entry.status == .running)
        apply(detail: detail)
    }

    // MARK: - Engine input

    /// Starts consuming `bridge` for an analysis started by the app.
    func attach(_ handle: AnalysisSessionHandle?, events: AsyncStream<SessionEvent>) {
        self.handle = handle
        isRunning = true
        status = .running
        consumer?.cancel()
        consumer = Task { [weak self] in
            for await event in events {
                guard let self else { return }
                self.apply(event)
            }
        }
    }

    /// Replaces stored state with `detail` (library open, refresh after finishing).
    func apply(detail: SessionDetail) {
        entry = detail.entry
        kind = detail.entry.kind
        if !isRunning || detail.entry.status != .running {
            status = detail.entry.status
            isRunning = detail.entry.status == .running && handle != nil
        }
        createdAt = Format.date(milliseconds: detail.entry.createdAtMs)
        pinned = detail.entry.pinned
        if let stored = detail.source { source = stored }
        if !detail.segments.isEmpty || !isRunning { segments = Self.sorted(detail.segments) }
        if !detail.candidates.isEmpty || !isRunning { candidates = detail.candidates }
        candidatesFinal = !isRunning
        transcript = detail.transcript
        if let track = detail.entry.track {
            if kind == .hum || kind == .lyrics {
                confirmedTrack = track
            } else if kind == .listen || kind == .systemAudio {
                matchedTrack = track
            }
        }
        if !isRunning, status == .completed, completed == nil {
            let identified = UInt32(segments.filter { $0.track != nil }.count)
            completed = (identified, max(detail.entry.trackCount, identified))
        }
    }

    /// Applies one engine event.
    func apply(_ event: SessionEvent) {
        switch event {
        case .started(_, let kind):
            self.kind = kind
            isRunning = true
            status = .running
        case .phase(let phase, let fraction):
            self.phase = phase
            self.fraction = fraction.map(Double.init)
        case .progress(let analyzed, let total, let eta):
            progress = Progress(analyzedMs: analyzed, totalMs: total, etaMs: eta)
        case .source(let info):
            source = info
        case .item(let index, let count, let title):
            item = Item(index: index, count: count, title: title)
        case .segment(let segment):
            upsert(segment)
        case .segmentRemoved(let id):
            segments.removeAll { $0.id == id }
            if selectedSegmentID == id { selectedSegmentID = nil }
        case .candidates(let list, let isFinal):
            mergeCandidates(list)
            candidatesFinal = isFinal
        case .finished(let outcome):
            isRunning = false
            phase = nil
            handle = nil
            switch outcome {
            case .matched(let track):
                matchedTrack = track
                status = .matched
            case .candidates:
                status = .candidates
                candidatesFinal = true
            case .completed(let identified, let total):
                completed = (identified, total)
                status = .completed
                if let total = progress?.totalMs { progress?.analyzedMs = total }
            case .noMatch(let reason):
                noMatchReason = reason
                status = .noMatch
            }
        case .failed(let code, let message):
            isRunning = false
            phase = nil
            handle = nil
            failure = Failure(code: code, message: message)
            status = .failed
        case .canceled:
            isRunning = false
            phase = nil
            handle = nil
            status = .canceled
        case .level, .pitch, .notice:
            break
        }
        onEvent?(self, event)
    }

    private func upsert(_ segment: Segment) {
        if let index = segments.firstIndex(where: { $0.id == segment.id }) {
            segments[index] = segment
        } else {
            let position = segments.firstIndex { Self.precedes(segment, $0) } ?? segments.endIndex
            segments.insert(segment, at: position)
        }
    }

    /// Rows update in place and never reorder after the first final list; new
    /// candidates append.
    private func mergeCandidates(_ list: [Candidate]) {
        guard candidatesFinal else {
            candidates = list
            return
        }
        var merged: [Candidate] = candidates.map { existing in
            list.first { $0.track.identityKey == existing.track.identityKey } ?? existing
        }
        for candidate in list where !merged.contains(where: { $0.track.identityKey == candidate.track.identityKey }) {
            merged.append(candidate)
        }
        candidates = merged
    }

    static func sorted(_ segments: [Segment]) -> [Segment] {
        segments.sorted(by: precedes)
    }

    private static func precedes(_ a: Segment, _ b: Segment) -> Bool {
        if a.itemIndex != b.itemIndex { return a.itemIndex < b.itemIndex }
        return a.startMs < b.startMs
    }

    // MARK: - Derived state

    /// Segments that are listed (speech and silence are only drawn on the strip).
    var listedSegments: [Segment] {
        segments.filter { $0.kind == .track || $0.kind == .unidentified }
    }

    var identifiedCount: Int {
        segments.filter { $0.kind == .track && $0.track != nil }.count
    }

    /// The single answer of the session, if it has one.
    var track: Track? {
        if let confirmedTrack { return confirmedTrack }
        if let matchedTrack { return matchedTrack }
        if kind == .link || kind == .file, presentation == .result {
            return listedSegments.first(where: { $0.track != nil })?.track ?? entry?.track
        }
        return nil
    }

    /// The segment the single answer came from (live match or a short link).
    var resultSegment: Segment? {
        segments.first { $0.track != nil && ($0.track?.identityKey == track?.identityKey) }
    }

    var durationMs: UInt64? {
        source?.durationMs ?? progress?.totalMs ?? entry?.durationMs
    }

    var isCollection: Bool {
        if (source?.itemCount ?? 1) > 1 { return true }
        let tracks = segments.filter { $0.kind == .track }
        return tracks.count >= 2 && tracks.allSatisfy { $0.startMs == 0 && $0.endMs == 0 }
    }

    var isLongMedia: Bool {
        if mode == .mix { return true }
        if let duration = durationMs, duration >= 240_000 { return true }
        return listedSegments.count >= 2
    }

    var presentation: Presentation {
        switch kind {
        case .hum, .lyrics:
            if confirmedTrack != nil { return .result }
            if !candidates.isEmpty { return .candidates }
            return isRunning ? .running : .empty
        case .listen, .systemAudio:
            if matchedTrack != nil || confirmedTrack != nil { return .result }
            if !candidates.isEmpty { return .candidates }
            return isRunning ? .running : .empty
        case .link, .file:
            if isCollection { return .collection }
            if isLongMedia { return .timeline }
            if listedSegments.contains(where: { $0.track != nil }) || entry?.track != nil {
                return isRunning ? .running : .result
            }
            return isRunning ? .running : .empty
        }
    }

    /// Title for the window, library and headers.
    var title: String {
        if presentation == .result, let track { return track.title }
        if let title = source?.title, !title.isEmpty { return title }
        if let title = entry?.title, !title.isEmpty { return title }
        if let best = candidates.first { return best.track.title }
        return Copy.untitled
    }

    /// "YouTube · 1:00:33" under a mix title.
    var mediaSubtitle: String? {
        let site = source?.site ?? Format.host(source?.url ?? entry?.sourceUrl)
        let duration = durationMs.map { Format.cue(milliseconds: $0) }
        let parts = [site, duration].compactMap { $0 }
        return parts.isEmpty ? nil : parts.joined(separator: " · ")
    }

    /// "Sabrina Carpenter · 12 tracks · open.spotify.com" under a collection title.
    var collectionSubtitle: String? {
        let artists = Set(listedSegments.compactMap { $0.track?.artist })
        let artist = artists.count == 1 ? artists.first : (source?.uploader)
        let count = max(Int(source?.itemCount ?? 0), listedSegments.count)
        let host = Format.host(source?.url ?? entry?.sourceUrl)
        let parts = [artist, count > 0 ? Copy.trackCount(count) : nil, host].compactMap { $0 }
        return parts.isEmpty ? nil : parts.joined(separator: " · ")
    }

    var artworkURL: String? {
        source?.thumbnailUrl ?? entry?.artworkUrl
    }

    /// The best candidate's hum contour, or the live trace.
    var humContour: [Float] {
        if let query = candidates.first(where: { $0.alignment != nil })?.alignment?.query, !query.isEmpty {
            return query
        }
        return humTrace
    }

    var humDurationSeconds: Int {
        if let humSeconds { return humSeconds }
        if let alignment = candidates.first(where: { $0.alignment != nil })?.alignment {
            return Int((Double(alignment.query.count) * Double(alignment.frameMs) / 1000).rounded())
        }
        return 0
    }

    var selectedSegment: Segment? {
        guard let selectedSegmentID else { return nil }
        return segments.first { $0.id == selectedSegmentID }
    }

    var selectedCandidate: Candidate? {
        if let selectedCandidateID, let match = candidates.first(where: { $0.track.identityKey == selectedCandidateID }) {
            return match
        }
        return candidates.first
    }

    /// Status text in the mix header.
    var statusText: String {
        if wasStopped || status == .canceled {
            return Copy.stopped(identifiedCount)
        }
        if isRunning {
            return Copy.eta(milliseconds: progress?.etaMs)
        }
        if let completed, completed.total > 0, completed.total >= completed.identified {
            let total = max(Int(completed.total), listedSegments.count)
            return Copy.identified(identifiedCount, of: total)
        }
        return Copy.identifiedOnly(identifiedCount)
    }

    // MARK: - Actions

    /// Stops a running analysis; identified segments stay.
    func cancel() {
        guard isRunning else { return }
        wasStopped = true
        handle?.cancel()
    }

    func markPinned(_ value: Bool) {
        pinned = value
        entry?.pinned = value
    }

    /// Records a user correction returned by the engine.
    func replace(_ segment: Segment) {
        upsert(segment)
    }

    /// "This is it": the session now has a single answer.
    func confirm(_ track: Track) {
        confirmedTrack = track
        if let index = candidates.firstIndex(where: { $0.track.identityKey == track.identityKey }) {
            var candidate = candidates.remove(at: index)
            candidate.tier = .strong
            candidates.insert(candidate, at: 0)
        }
    }

    func setLyrics(_ lyrics: Lyrics?) {
        self.lyrics = lyrics
        lyricsLoaded = true
    }
}
