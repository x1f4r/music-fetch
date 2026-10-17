import MusicFetchCore
import Testing

@testable import MusicFetch

@MainActor
struct SessionModelTests {
    private func segment(_ id: String, start: UInt64, end: UInt64, track: Track?, item: UInt32 = 0, tier: Tier = .confirmed)
        -> Segment
    {
        Segment(
            id: id, itemIndex: item, startMs: start, endMs: end, kind: track == nil ? .unidentified : .track, track: track,
            confidence: track == nil ? 0 : 0.95, tier: tier, evidence: [], alternates: [], sourceUrl: nil)
    }

    @Test func segmentsUpsertInCueOrderAndRemove() {
        let model = SessionModel(id: "s", kind: .link, isRunning: true)
        model.apply(.started(sessionId: "s", kind: .link))
        model.apply(.segment(segment: segment("b", start: 60_000, end: 120_000, track: DemoFixtures.nights)))
        model.apply(.segment(segment: segment("a", start: 0, end: 60_000, track: DemoFixtures.houdini)))
        model.apply(.segment(segment: segment("c", start: 120_000, end: 180_000, track: nil)))
        #expect(model.segments.map(\.id) == ["a", "b", "c"])

        model.apply(.segment(segment: segment("b", start: 60_000, end: 130_000, track: DemoFixtures.dreams)))
        #expect(model.segments.map(\.id) == ["a", "b", "c"])
        #expect(model.segments[1].track?.title == "Dreams")

        model.selectedSegmentID = "c"
        model.apply(.segmentRemoved(id: "c"))
        #expect(model.segments.map(\.id) == ["a", "b"])
        #expect(model.selectedSegmentID == nil)
        #expect(model.identifiedCount == 2)
    }

    @Test func candidatesKeepOrderAfterTheFirstFinalList() {
        let model = SessionModel(id: "h", kind: .hum, isRunning: true)
        let list = DemoFixtures.candidates
        model.apply(.candidates(candidates: [list[1], list[0]], isFinal: false))
        #expect(model.candidates.map(\.track.title) == ["Dancing Queen", "Take On Me"])
        // Before the first final list the order follows the engine.
        model.apply(.candidates(candidates: [list[0], list[1]], isFinal: true))
        #expect(model.candidates.map(\.track.title) == ["Take On Me", "Dancing Queen"])
        // After it, rows update in place and new ones append.
        var updated = list[1]
        updated.tier = .strong
        model.apply(.candidates(candidates: [updated, list[2], list[0]], isFinal: true))
        #expect(model.candidates.map(\.track.title) == ["Take On Me", "Dancing Queen", "Africa"])
        #expect(model.candidates[1].tier == .strong)
    }

    @Test func aCaptureSavedOfflineWaits() {
        let live = SessionModel(id: "w", kind: .listen, isRunning: true)
        live.apply(.started(sessionId: "w", kind: .listen))
        live.apply(.finished(outcome: .waiting))
        #expect(live.status == .waiting)
        #expect(!live.isRunning)
        #expect(live.presentation == .empty)
    }

    @Test func presentationFollowsTheSession() {
        let live = SessionModel(id: "l", kind: .listen, isRunning: true)
        #expect(live.presentation == .running)
        live.apply(.finished(outcome: .matched(track: DemoFixtures.houdini)))
        #expect(live.presentation == .result)
        #expect(live.track?.title == "Houdini")

        let hum = SessionModel(id: "h", kind: .hum, isRunning: true)
        hum.apply(.candidates(candidates: DemoFixtures.candidates, isFinal: true))
        hum.apply(.finished(outcome: .candidates(count: 4)))
        #expect(hum.presentation == .candidates)
        hum.confirm(DemoFixtures.candidateTracks[2])
        #expect(hum.presentation == .result)
        #expect(hum.candidates.first?.track.title == "Africa")
        #expect(hum.candidates.first?.tier == .strong)

        let short = SessionModel(id: "s", kind: .link, isRunning: true)
        short.apply(.source(info: SourceInfo(url: "https://example.com/v", title: "Clip", uploader: nil, thumbnailUrl: nil,
                                             durationMs: 30_000, service: nil, site: "Example", itemCount: 1, hints: [])))
        short.apply(.segment(segment: segment("x", start: 0, end: 30_000, track: DemoFixtures.espresso)))
        #expect(short.presentation == .running)
        short.apply(.finished(outcome: .completed(identified: 1, total: 1)))
        #expect(short.presentation == .result)

        let mix = SessionModel(id: "m", kind: .link, isRunning: true)
        mix.apply(.source(info: DemoFixtures.mixSource))
        #expect(mix.presentation == .timeline)

        let album = SessionModel(id: "a", kind: .link, isRunning: true)
        album.apply(.source(info: DemoFixtures.collectionSource))
        #expect(album.presentation == .collection)

        let nothing = SessionModel(id: "n", kind: .link, isRunning: true)
        nothing.apply(.failed(code: .unsupportedLink, message: "This link isn't supported."))
        #expect(nothing.presentation == .empty)
        #expect(nothing.failure?.code == .unsupportedLink)
    }

    @Test func statusTextCountsIdentifiedSegments() {
        let model = SessionModel(id: "m", kind: .link, isRunning: true)
        model.apply(.source(info: DemoFixtures.mixSource))
        for segment in DemoFixtures.mixSegments { model.apply(.segment(segment: segment)) }
        model.apply(.progress(analyzedMs: 1_000_000, totalMs: 3_633_000, etaMs: 200_000))
        #expect(model.statusText == "About 4 min left")
        model.apply(.finished(outcome: .completed(identified: 7, total: 8)))
        #expect(model.statusText == "7 of 8 identified")
    }

    @Test func storedDetailLoadsAsAResult() throws {
        let detail = try #require(DemoFixtures.detail(DemoFixtures.resultID))
        let model = SessionModel(detail: detail)
        #expect(model.presentation == .result)
        #expect(Copy.matchedBy(model) == "Shazam, after 4 s")
        #expect(Copy.sourceName(model) == "Microphone")
    }
}
