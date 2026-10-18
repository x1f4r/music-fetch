import Foundation
import MusicFetchCore
import Testing

@testable import MusicFetch

/// Answers while humming: what the stage shows from the progressive search's interim
/// lists, and how honest the final candidates screen is.
@MainActor
struct LiveAnswersTests {
    private func candidate(_ title: String, _ score: Float, _ tier: CandidateTier) -> Candidate {
        var track = DemoFixtures.candidateTracks[0]
        track.title = title
        track.identityKey = title.lowercased()
        return Candidate(track: track, score: score, tier: tier, sources: [], alignment: nil)
    }

    @Test func showsTheBestOneOrTwoStrongOrPossibleAnswers() {
        let answers = LiveAnswers(list: [
            candidate("A", 0.9, .strong), candidate("B", 0.5, .possible), candidate("C", 0.4, .possible),
        ])
        #expect(answers.candidates.map(\.track.title) == ["A", "B"])
        #expect(answers.candidate(at: 1)?.track.title == "B")
        #expect(answers.candidate(at: 2) == nil)
    }

    @Test func longShotsAreNotShownWhileHumming() {
        #expect(LiveAnswers(list: [candidate("A", 0.2, .longShot), candidate("B", 0.1, .longShot)]).isEmpty)
        let one = LiveAnswers(list: [candidate("A", 0.6, .possible), candidate("B", 0.2, .longShot)])
        #expect(one.candidates.map(\.track.title) == ["A"])
        #expect(LiveAnswers(list: []).isEmpty)
    }

    @Test func onlySongsOrderAndTiersRedrawTheRows() {
        let before = LiveAnswers(list: [candidate("A", 0.6, .possible), candidate("B", 0.4, .possible)])
        let rescored = LiveAnswers(list: [candidate("A", 0.62, .possible), candidate("B", 0.38, .possible)])
        let promoted = LiveAnswers(list: [candidate("A", 0.8, .strong), candidate("B", 0.4, .possible)])
        let swapped = LiveAnswers(list: [candidate("B", 0.6, .possible), candidate("A", 0.4, .possible)])
        #expect(!rescored.differs(from: before))
        #expect(promoted.differs(from: before))
        #expect(swapped.differs(from: before))
    }

    @Test func confidenceFollowsTheBestCandidate() {
        #expect(CandidateConfidence([candidate("A", 0.9, .strong)]) == .strong)
        #expect(CandidateConfidence([candidate("A", 0.5, .possible), candidate("B", 0.2, .longShot)]) == .possible)
        #expect(CandidateConfidence([candidate("A", 0.2, .longShot)]) == .unsure)
        #expect(CandidateConfidence([]) == .unsure)
    }

    @Test func announcementsSayWhenTheMatcherIsNotSure() {
        let unsure = [candidate("A", 0.2, .longShot), candidate("B", 0.1, .longShot)]
        #expect(Copy.candidatesAnnouncement(unsure, kind: .hum) == Copy.notSureHum)
        #expect(Copy.candidatesAnnouncement(unsure, kind: .lyrics) == Copy.notSureLyrics)
        let sure = [candidate("A", 0.9, .strong)]
        #expect(Copy.candidatesAnnouncement(sure, kind: .hum) == Copy.possibleMatches(sure))
    }

    @Test func liveCandidatesAreRequestedForEveryLiveSession() throws {
        let suite = "local.musicfetch.tests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        Preferences.registerDefaults(defaults)
        #expect(Preferences.listenOptions(defaults).liveCandidates)
        defaults.set(false, forKey: PreferenceKey.autoStopAfterPause)
        #expect(Preferences.listenOptions(defaults).liveCandidates)
    }

    // MARK: - On the stage

    private func humming() async throws -> (AppModel, FakeSession) {
        let app = AppModel(demo: DemoState(name: "tests-manual"))
        await app.start()
        // No speech recognizer in these tests.
        app.listen.makeTranscriber = { language, lowPower, handler in
            SungWordsTranscriber(language: language, lowPower: lowPower, handler: handler) { _ in nil }
        }
        app.listen.start(.hum)
        let engine = try #require(app.engine as? FakeEngine)
        let session = try #require(engine.lastLiveSession)
        try await until { app.listen.stage == .listening && app.listen.liveSession?.kind == .hum }
        return (app, session)
    }

    /// Polls the main actor until `condition` holds (events hop from engine threads).
    private func until(_ condition: () -> Bool) async throws {
        for _ in 0..<200 {
            if condition() { return }
            try await Task.sleep(for: .milliseconds(10))
        }
        Issue.record("condition not met in time")
    }

    @Test func interimListsArePreviewsNotResults() async throws {
        let (app, session) = try await humming()
        let listen = app.listen!
        let lists = DemoFixtures.liveLists

        session.emit(.candidates(candidates: lists[0], isFinal: false))
        try await Task.sleep(for: .milliseconds(100))
        #expect(listen.liveAnswers.isEmpty, "long shots stay hidden")
        #expect(!listen.showsLiveAnswers)

        session.emit(.candidates(candidates: lists[1], isFinal: false))
        try await until { !listen.liveAnswers.isEmpty }
        #expect(listen.liveAnswers.candidates.map(\.track.title) == ["Take On Me", "Dancing Queen"])
        #expect(listen.showsLiveAnswers)
        #expect(listen.stage == .listening)
        #expect(listen.shown == nil, "an interim list never replaces the stage")

        session.emit(.candidates(candidates: [], isFinal: false))
        try await until { listen.liveAnswers.isEmpty }
        listen.cancel()
    }

    @Test func clickingAnAnswerStopsAndSelectsIt() async throws {
        let (app, session) = try await humming()
        let listen = app.listen!
        session.emit(.candidates(candidates: DemoFixtures.liveLists[1], isFinal: false))
        try await until { listen.liveAnswers.candidates.count == 2 }

        let second = try #require(listen.liveAnswers.candidate(at: 1))
        listen.choose(second)
        #expect(listen.stage == .matching)
        #expect(listen.showsLiveAnswers, "the answers stay while matching")

        session.emit(.phase(phase: .matchingMelody, fraction: nil))
        session.emit(.candidates(candidates: DemoFixtures.candidates, isFinal: true))
        session.emit(.finished(outcome: .candidates(count: UInt32(DemoFixtures.candidates.count))))
        try await until { listen.shown != nil }
        #expect(listen.shown?.selectedCandidateID == second.track.identityKey)
        #expect(listen.liveAnswers.isEmpty)
        #expect(listen.stage == .idle)
    }

    @Test func anEarlyStopByTheEngineEndsCaptureAndShowsTheList() async throws {
        let (app, session) = try await humming()
        let listen = app.listen!
        session.emit(.candidates(candidates: DemoFixtures.liveLists[2], isFinal: false))
        try await until { listen.liveAnswers.candidate(at: 0)?.tier == .strong }
        // The engine's stop rule: two agreeing confident searches end the capture.
        session.emit(.phase(phase: .matchingMelody, fraction: nil))
        try await until { listen.stage == .matching }
        session.emit(.candidates(candidates: DemoFixtures.candidates, isFinal: true))
        session.emit(.finished(outcome: .candidates(count: UInt32(DemoFixtures.candidates.count))))
        try await until { listen.shown != nil }
        #expect(listen.shown?.candidates.first?.track.title == "Take On Me")
    }

    @Test func noMatchClearsTheAnswers() async throws {
        let (app, session) = try await humming()
        let listen = app.listen!
        session.emit(.candidates(candidates: DemoFixtures.liveLists[1], isFinal: false))
        try await until { !listen.liveAnswers.isEmpty }
        session.emit(.finished(outcome: .noMatch(reason: .notFound)))
        try await until { listen.stage == .noMatch(.notFound) }
        #expect(listen.liveAnswers.isEmpty)
        #expect(!listen.showsLiveAnswers)
    }
}
