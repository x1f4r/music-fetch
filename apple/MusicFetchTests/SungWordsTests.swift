import Foundation
import MusicFetchCore
import Synchronization
import Testing

@testable import MusicFetch

/// A recognizer that reports what the test tells it to.
nonisolated final class ScriptedBackend: SpeechBackend, @unchecked Sendable {
    let language: String? = "en-US"
    let onText: SpeechBackendHandler
    /// Sent with `isLast` when `finish()` is called; nil = never answers.
    let finalText: RecognizedText?
    private let counts = Mutex((samples: 0, finished: false, canceled: false))

    init(finalText: RecognizedText?, onText: @escaping SpeechBackendHandler) {
        self.finalText = finalText
        self.onText = onText
    }

    var samples: Int { counts.withLock { $0.samples } }
    var finished: Bool { counts.withLock { $0.finished } }
    var canceled: Bool { counts.withLock { $0.canceled } }

    func append(_ samples: [Float], rate: Double) {
        counts.withLock { $0.samples += samples.count }
    }

    func finish() {
        counts.withLock { $0.finished = true }
        if let finalText { onText(finalText, true) }
    }

    func cancel() {
        counts.withLock { $0.canceled = true }
    }

    func say(_ text: String) {
        onText(RecognizedText(text: text), false)
    }
}

/// The backend a transcriber made, once it made one.
nonisolated final class BackendSlot: Sendable {
    private let slot = Mutex<ScriptedBackend?>(nil)
    var value: ScriptedBackend? { slot.withLock { $0 } }
    func set(_ backend: ScriptedBackend) { slot.withLock { $0 = backend } }
}

/// Collects what a transcriber reports.
nonisolated final class HeardLog: Sendable {
    private let items = Mutex<[HeardWords]>([])
    func add(_ words: HeardWords) { items.withLock { $0.append(words) } }
    var all: [HeardWords] { items.withLock { $0 } }
    var finals: [HeardWords] { all.filter(\.isFinal) }
}

/// Sung words: the live recognizer around the capture, and how the stage asks for
/// speech recognition on macOS 15.
@MainActor
struct SungWordsTests {
    /// One second of stereo audio at 16 kHz, as the capture hands it over.
    private func stereoSecond(rate: UInt32 = 16_000) -> Data {
        let samples = [Float](repeating: 0.25, count: Int(rate) * 2)
        return samples.withUnsafeBufferPointer { Data(buffer: $0) }
    }

    private func transcriber(final: RecognizedText?, log: HeardLog) -> (SungWordsTranscriber, BackendSlot) {
        let made = BackendSlot()
        let transcriber = SungWordsTranscriber(language: "en-US", lowPower: false, handler: { log.add($0) }) { onText in
            let backend = ScriptedBackend(finalText: final, onText: onText)
            made.set(backend)
            return backend
        }
        return (transcriber, made)
    }

    private func until(_ condition: () -> Bool) async throws {
        for _ in 0..<300 {
            if condition() { return }
            try await Task.sleep(for: .milliseconds(10))
        }
        Issue.record("condition not met in time")
    }

    @Test func downmixesInterleavedStereo() {
        let data = [Float]([1, 0, 0.5, 0.5, -1, 1]).withUnsafeBufferPointer { Data(buffer: $0) }
        #expect(SungWordsTranscriber.downmix(data, channels: 2) == [0.5, 0.5, 0])
        #expect(SungWordsTranscriber.downmix(data, channels: 1).count == 6)
    }

    @Test func keepsTheLastSecondsUntilActivatedThenFeedsThem() async throws {
        let log = HeardLog()
        let (words, made) = transcriber(final: RecognizedText(text: "never gonna give you up"), log: log)
        for _ in 0..<20 { words.append(stereoSecond(), sampleRate: 16_000, channels: 2) }
        words.activate()
        try await until { made.value != nil }
        let backend = try #require(made.value)
        try await until { backend.samples > 0 }
        // Only the newest 12 s of the 20 before activation are recognized.
        #expect(backend.samples == Int(SungWordsTranscriber.prefixSeconds) * 16_000)
        words.append(stereoSecond(), sampleRate: 16_000, channels: 2)
        #expect(backend.samples == 13 * 16_000)
    }

    @Test func partialsGoOutOnceAndTheFinalExactlyOnce() async throws {
        let log = HeardLog()
        let (words, made) = transcriber(final: RecognizedText(text: "never gonna give you up", confidence: 0.8), log: log)
        words.activate()
        try await until { made.value != nil }
        let backend = try #require(made.value)
        backend.say("never")
        backend.say("never")
        backend.say("  ")
        backend.say("never gonna")
        words.finish()
        try await until { !log.finals.isEmpty }
        backend.say("late")
        words.finish()
        #expect(log.all.map(\.text) == ["never", "never gonna", "never gonna give you up"])
        #expect(log.finals.count == 1)
        #expect(log.finals.first?.confidence == 0.8)
        #expect(log.finals.first?.language == "en-US")
        #expect(backend.finished)
    }

    @Test func nothingRecognizedFinishesAtOnceWithoutWords() {
        let log = HeardLog()
        let (words, made) = transcriber(final: nil, log: log)
        words.append(stereoSecond(), sampleRate: 16_000, channels: 2)
        words.finish()
        // Never activated (a recording, not a hum): the engine gets an empty final now.
        #expect(log.finals.map(\.text) == [""])
        #expect(made.value == nil)
    }

    @Test func aSilentRecognizerIsCutOffWithItsNewestWords() async throws {
        let log = HeardLog()
        let (words, made) = transcriber(final: nil, log: log)
        words.activate()
        try await until { made.value != nil }
        let backend = try #require(made.value)
        backend.say("is this the real life")
        let start = ContinuousClock.now
        words.finish()
        try await until { !log.finals.isEmpty }
        #expect(ContinuousClock.now - start < .seconds(2), "the engine waits at most 2 s")
        #expect(log.finals.first?.text == "is this the real life")
        try await until { backend.canceled }
    }

    @Test func stoppingWhileTheRecognizerStartsStillReadsTheHum() async throws {
        let log = HeardLog()
        let made = BackendSlot()
        let words = SungWordsTranscriber(language: "en-US", lowPower: false, handler: { log.add($0) }) { onText in
            try? await Task.sleep(for: .milliseconds(200))
            let backend = ScriptedBackend(finalText: RecognizedText(text: "hello from the other side"), onText: onText)
            made.set(backend)
            return backend
        }
        words.activate()
        for _ in 0..<3 { words.append(stereoSecond(), sampleRate: 16_000, channels: 2) }
        words.finish()
        try await until { !log.finals.isEmpty }
        #expect(made.value?.samples == 3 * 16_000)
        #expect(log.finals.map(\.text) == ["hello from the other side"])
    }

    @Test func cancelReportsNothingMore() async throws {
        let log = HeardLog()
        let (words, made) = transcriber(final: RecognizedText(text: "too late"), log: log)
        words.activate()
        try await until { made.value != nil }
        let backend = try #require(made.value)
        words.cancel()
        backend.say("hello there")
        words.finish()
        #expect(log.all.isEmpty)
        #expect(backend.canceled)
    }

    @Test func noRecognizerMeansAnEmptyFinal() async throws {
        let log = HeardLog()
        let words = SungWordsTranscriber(language: "en-US", lowPower: false, handler: { log.add($0) }) { _ in nil }
        words.activate()
        try await Task.sleep(for: .milliseconds(50))
        words.finish()
        try await until { !log.finals.isEmpty }
        #expect(log.finals.map(\.text) == [""])
    }

    @Test func accumulatorJoinsSegmentsAndWeighsConfidence() {
        var transcript = TranscriptAccumulator()
        transcript.add(text: "never gonna", alternatives: ["never gone a"], confidences: [(0.9, 11)], isFinal: true)
        transcript.add(text: "give you", alternatives: [], confidences: [], isFinal: false)
        #expect(transcript.partial.text == "never gonna give you")
        transcript.add(text: "give you up", alternatives: [], confidences: [(0.5, 11)], isFinal: true)
        let final = transcript.final
        #expect(final.text == "never gonna give you up")
        #expect(final.alternatives == ["never gone a give you up"])
        #expect(abs((final.confidence ?? 0) - 0.7) < 0.001)
    }

    // MARK: - On the stage

    private func makeApp(speech status: SpeechAccess.Status) async -> (AppModel, HeardLog) {
        let app = AppModel(demo: DemoState(name: "tests-manual"))
        await app.start()
        app.speech.showForDemo(status)
        let log = HeardLog()
        app.listen.makeTranscriber = { language, lowPower, handler in
            SungWordsTranscriber(language: language, lowPower: lowPower, handler: { log.add($0); handler($0) }) { onText in
                ScriptedBackend(finalText: RecognizedText(text: "never gonna give you up"), onText: onText)
            }
        }
        return (app, log)
    }

    @Test func aHumSendsTheRecognizedWordsToTheEngine() async throws {
        let (app, log) = await makeApp(speech: .ready)
        app.listen.start(.hum)
        let engine = try #require(app.engine as? FakeEngine)
        let session = try #require(engine.lastLiveSession)
        try await until { app.listen.stage == .listening }
        app.listen.stop()
        try await until { session.transcripts.contains(where: \.isFinal) }
        #expect(session.transcripts.last?.text == "never gonna give you up")
        #expect(log.finals.count == 1)
    }

    @Test func thisMacNeverReadsWords() async throws {
        let (app, log) = await makeApp(speech: .ready)
        app.listen.start(.thisMac)
        let engine = try #require(app.engine as? FakeEngine)
        let session = try #require(engine.lastLiveSession)
        try await until { app.listen.stage == .listening }
        app.listen.stop()
        try await Task.sleep(for: .milliseconds(100))
        #expect(session.transcripts.isEmpty)
        #expect(log.all.isEmpty)
    }

    @Test func theFirstHumExplainsSpeechRecognitionOnce() async throws {
        let (app, _) = await makeApp(speech: .needsPermission)
        let listen = app.listen!
        listen.start(.hum)
        #expect(listen.stage == .speechPrePrompt)
        #expect(listen.recoveryActions == [.allowSpeech, .skipSpeech])
        #expect(listen.stateLine == Copy.speechPrePrompt)

        listen.perform(.skipSpeech)
        #expect(listen.stage == .listening, "Not Now hums without words")
        listen.cancel()

        listen.start(.hum)
        #expect(listen.stage == .listening, "asked once")
        listen.cancel()
    }

    @Test func choosingAnotherMethodLeavesTheExplanation() async {
        let (app, _) = await makeApp(speech: .needsPermission)
        let listen = app.listen!
        listen.start(.hum)
        #expect(listen.stage == .speechPrePrompt)
        listen.select(.microphone)
        #expect(listen.stage == .idle)
        #expect(listen.source == .microphone)
    }

    @Test func noExplanationWhenNothingNeedsAsking() async {
        for status in [SpeechAccess.Status.ready, .denied, .unsupported, .needsModel] {
            let (app, _) = await makeApp(speech: status)
            app.listen.start(.hum)
            #expect(app.listen.stage == .listening, "\(status)")
            app.listen.cancel()
        }
    }

    @Test func speechNotesExplainWhyWordsAreNotRead() {
        #expect(Copy.speechNote(.ready, language: "en-US") == Copy.speechNote(nil, language: "en-US"))
        #expect(Copy.speechNote(.denied, language: "en-US").contains("System Settings"))
        #expect(Copy.speechNote(.unsupported, language: "en-US") != Copy.speechNote(.ready, language: "en-US"))
    }
}
