import AVFoundation
import Foundation
import MusicFetchCore
import Speech
import Synchronization

/// What the on-device recognizer heard during a hum or sing session, for the engine's
/// transcript input (docs/CORE_API.md, "Sung words").
nonisolated struct HeardWords: Equatable, Sendable {
    /// The best hypothesis.
    var text: String
    /// Further hypotheses, best first (final results only).
    var alternatives: [String] = []
    /// BCP-47 tag of the recognizer's language.
    var language: String
    /// 0…1 when the recognizer reports it.
    var confidence: Float?
    /// The last result for this capture; nothing follows it.
    var isFinal: Bool
}

/// One recognizer update: the whole capture's text so far.
nonisolated struct RecognizedText: Equatable, Sendable {
    var text: String
    var alternatives: [String] = []
    var confidence: Float?

    static let empty = RecognizedText(text: "")
}

/// Streams a live session's microphone audio to the on-device speech recognizer while
/// someone hums or sings, and reports what it hears: partial results while capturing,
/// then exactly one final result soon after capture ends (the engine waits up to 2 s
/// for it). Nothing leaves the Mac.
///
/// Audio is recognized only after `activate()`: at the start of a Hum session, or when
/// a microphone session turns out to be humming. Until then the last seconds are kept,
/// so the first words aren't lost.
nonisolated final class SungWordsTranscriber: Sendable {
    typealias Handler = @Sendable (HeardWords) -> Void

    /// Audio kept before activation.
    static let prefixSeconds: Double = 12
    /// Audio kept while the recognizer starts.
    static let startupSeconds: Double = 30
    /// After capture ends the final result goes out at the latest after this long.
    static let finalDeadline: Duration = .milliseconds(1700)

    private struct State {
        var active = false
        var finished = false
        var canceled = false
        var sentFinal = false
        var backendFailed = false
        var pending: [[Float]] = []
        var pendingRate: Double = 0
        var pendingFrames = 0
        var backend: (any SpeechBackend)?
        var latest = RecognizedText.empty
        var language: String
    }

    private let lowPower: Bool
    private let handler: Handler
    private let state: Mutex<State>
    private let makeBackend: @Sendable (_ onText: @escaping SpeechBackendHandler) async -> (any SpeechBackend)?

    /// `language`: the BCP-47 tag to recognize (the Mac's language).
    convenience init(language: String, lowPower: Bool, handler: @escaping Handler) {
        self.init(language: language, lowPower: lowPower, handler: handler) { onText in
            await SpeechBackends.make(language: language, lowPower: lowPower, onText: onText)
        }
    }

    /// With a custom recognizer (tests).
    init(
        language: String, lowPower: Bool, handler: @escaping Handler,
        makeBackend: @escaping @Sendable (_ onText: @escaping SpeechBackendHandler) async -> (any SpeechBackend)?
    ) {
        self.lowPower = lowPower
        self.handler = handler
        self.makeBackend = makeBackend
        self.state = Mutex(State(language: language))
    }

    var isActive: Bool { state.withLock { $0.active } }

    /// Starts recognizing, beginning with the audio kept so far. Idempotent.
    func activate() {
        let start = state.withLock { state -> Bool in
            guard !state.active, !state.canceled, !state.finished else { return false }
            state.active = true
            return true
        }
        guard start else { return }
        Task.detached(priority: lowPower ? .utility : .userInitiated) { [self] in
            let backend = await makeBackend { [weak self] text, isLast in self?.receive(text, isLast: isLast) }
            let finishNow = state.withLock { state -> Bool? in
                guard !state.canceled else { return nil }
                guard let backend else {
                    state.backendFailed = true
                    return state.finished
                }
                for chunk in state.pending { backend.append(chunk, rate: state.pendingRate) }
                state.pending = []
                state.pendingFrames = 0
                state.backend = backend
                if let tag = backend.language { state.language = tag }
                return state.finished
            }
            guard let finishNow else {
                backend?.cancel()
                return
            }
            if backend == nil {
                // No recognizer: say so at once if capture already ended.
                if finishNow { sendFinal() }
            } else if finishNow {
                backend?.finish()
                scheduleDeadline()
            }
        }
    }

    /// Captured audio (interleaved little-endian f32). Called on the capture thread;
    /// cheap, never blocks on recognition.
    func append(_ data: Data, sampleRate: UInt32, channels: UInt32) {
        let mono = Self.downmix(data, channels: Int(channels))
        guard !mono.isEmpty, sampleRate > 0 else { return }
        let rate = Double(sampleRate)
        state.withLock { state in
            guard !state.canceled, !state.finished, !state.backendFailed else { return }
            if let backend = state.backend {
                backend.append(mono, rate: rate)
                return
            }
            if state.pendingRate != rate {
                state.pending = []
                state.pendingFrames = 0
                state.pendingRate = rate
            }
            state.pending.append(mono)
            state.pendingFrames += mono.count
            let limit = Int(rate * (state.active ? Self.startupSeconds : Self.prefixSeconds))
            while state.pendingFrames > limit, !state.pending.isEmpty {
                state.pendingFrames -= state.pending.removeFirst().count
            }
        }
    }

    /// Capture ended. The final result follows within `finalDeadline`; when nothing was
    /// being recognized it goes out at once (empty), so the engine never waits for it.
    func finish() {
        let (wasActive, backend, failed) = state.withLock { state -> (Bool, (any SpeechBackend)?, Bool) in
            guard !state.finished, !state.canceled else { return (false, nil, true) }
            state.finished = true
            state.pending = []
            return (state.active, state.backend, state.backendFailed)
        }
        guard wasActive, !failed else {
            sendFinal()
            return
        }
        if let backend {
            backend.finish()
            scheduleDeadline()
        }
        // Still starting: the start task finishes the backend.
    }

    /// Stops at once; nothing more is reported.
    func cancel() {
        let backend = state.withLock { state -> (any SpeechBackend)? in
            state.canceled = true
            state.pending = []
            return state.backend
        }
        backend?.cancel()
    }

    // MARK: - Results

    private func receive(_ text: RecognizedText, isLast: Bool) {
        if isLast {
            sendFinal(text)
            return
        }
        let words = state.withLock { state -> HeardWords? in
            guard !state.canceled, !state.sentFinal else { return nil }
            let trimmed = text.text.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, trimmed != state.latest.text else { return nil }
            state.latest = RecognizedText(text: trimmed, alternatives: text.alternatives, confidence: text.confidence)
            return HeardWords(text: trimmed, language: state.language, isFinal: false)
        }
        if let words { handler(words) }
    }

    /// The one final result: `text`, or the newest partial when the recognizer gave up.
    private func sendFinal(_ text: RecognizedText? = nil) {
        let (words, backend) = state.withLock { state -> (HeardWords?, (any SpeechBackend)?) in
            guard !state.canceled, !state.sentFinal else { return (nil, nil) }
            state.sentFinal = true
            let best = text.flatMap { $0.text.isEmpty ? nil : $0 } ?? state.latest
            let words = HeardWords(
                text: best.text.trimmingCharacters(in: .whitespacesAndNewlines), alternatives: best.alternatives,
                language: state.language, confidence: best.confidence, isFinal: true)
            return (words, text == nil ? state.backend : nil)
        }
        guard let words else { return }
        handler(words)
        // The deadline passed before the recognizer finished: stop it.
        backend?.cancel()
    }

    private func scheduleDeadline() {
        Task.detached { [weak self] in
            try? await Task.sleep(for: Self.finalDeadline)
            self?.sendFinal()
        }
    }

    // MARK: - Audio

    /// Mono samples from interleaved f32 bytes.
    static func downmix(_ data: Data, channels: Int) -> [Float] {
        guard channels > 0 else { return [] }
        let frames = data.count / (MemoryLayout<Float>.size * channels)
        guard frames > 0 else { return [] }
        return data.withUnsafeBytes { raw -> [Float] in
            let samples = raw.bindMemory(to: Float.self)
            if channels == 1 { return Array(samples.prefix(frames)) }
            let scale = 1 / Float(channels)
            return (0..<frames).map { frame in
                var sum: Float = 0
                for channel in 0..<channels { sum += samples[frame * channels + channel] }
                return sum * scale
            }
        }
    }
}

/// Text so far (`isLast` false) or the capture's last result (`isLast` true).
typealias SpeechBackendHandler = @Sendable (RecognizedText, _ isLast: Bool) -> Void

/// A running on-device recognizer fed with mono audio.
nonisolated protocol SpeechBackend: AnyObject, Sendable {
    /// The recognizer's language (BCP-47), when it differs from the requested one.
    var language: String? { get }
    /// Mono f32 at `rate`. Cheap; never blocks on recognition.
    func append(_ samples: [Float], rate: Double)
    /// No more audio: finalize; the last result follows with `isLast`.
    func finish()
    func cancel()
}

nonisolated enum SpeechBackends {
    /// `SpeechAnalyzer` on macOS 26 when the language's model is installed, else
    /// `SFSpeechRecognizer` on-device when allowed; nil when neither can run now.
    static func make(language: String, lowPower: Bool, onText: @escaping SpeechBackendHandler) async -> (any SpeechBackend)? {
        let locale = Locale(identifier: language)
        if #available(macOS 26, *), !Compat.forcesLegacy, let analyzer = await SpeechAccess.analyzerModule(for: locale) {
            guard await AssetInventory.status(forModules: [analyzer.module]) == .installed else { return nil }
            do {
                return try await AnalyzerBackend.start(
                    module: analyzer.module, locale: analyzer.locale, lowPower: lowPower, onText: onText)
            } catch {
                Log.speech.error("Live transcription did not start: \(error.localizedDescription, privacy: .public)")
                return nil
            }
        }
        guard SpeechAccess.legacyStatus(locale: locale) == .ready else { return nil }
        return RecognizerBackend(locale: locale, onText: onText)
    }
}

/// Joins a recognizer's finalized segments and its current volatile text.
nonisolated struct TranscriptAccumulator: Sendable {
    private(set) var pieces: [SpeechTranscriberService.Piece] = []
    private var weighted: (sum: Double, characters: Int) = (0, 0)
    private var volatile = ""

    /// A volatile (still changing) or finalized segment.
    mutating func add(text: String, alternatives: [String], confidences: [(Double, Int)], isFinal: Bool) {
        if isFinal {
            pieces.append(SpeechTranscriberService.Piece(text: text, alternatives: alternatives))
            for (confidence, characters) in confidences {
                weighted.sum += confidence * Double(characters)
                weighted.characters += characters
            }
            volatile = ""
        } else {
            volatile = text
        }
    }

    /// Everything heard so far, finalized or not.
    var partial: RecognizedText {
        let parts = pieces.map(\.text) + [volatile]
        let text = parts.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }.joined(separator: " ")
        return RecognizedText(text: text)
    }

    /// The finalized text with its alternatives and mean confidence.
    var final: RecognizedText {
        let hypotheses = SpeechTranscriberService.hypotheses(from: pieces)
        let confidence = weighted.characters > 0 ? Float(weighted.sum / Double(weighted.characters)) : nil
        return RecognizedText(text: hypotheses.first ?? "", alternatives: Array(hypotheses.dropFirst().prefix(4)), confidence: confidence)
    }
}

// MARK: - SpeechAnalyzer (macOS 26)

@available(macOS 26, *)
nonisolated final class AnalyzerBackend: SpeechBackend, @unchecked Sendable {
    let language: String?
    private let analyzer: SpeechAnalyzer
    private let input: AsyncStream<AnalyzerInput>.Continuation
    private let format: AVAudioFormat
    private let lock = NSLock()
    private var converter: AVAudioConverter?
    private var converterRate: Double = 0
    private var collector: Task<Void, Never>?

    private init(analyzer: SpeechAnalyzer, input: AsyncStream<AnalyzerInput>.Continuation, format: AVAudioFormat, language: String) {
        self.analyzer = analyzer
        self.input = input
        self.format = format
        self.language = language
    }

    static func start(module: any SpeechModule, locale: Locale, lowPower: Bool, onText: @escaping SpeechBackendHandler)
        async throws -> AnalyzerBackend
    {
        guard let format = await SpeechAnalyzer.bestAvailableAudioFormat(compatibleWith: [module]) else {
            throw PlatformError.NotSupported
        }
        let (stream, continuation) = AsyncStream.makeStream(of: AnalyzerInput.self, bufferingPolicy: .unbounded)
        let options = SpeechAnalyzer.Options(
            priority: lowPower ? .utility : .userInitiated, modelRetention: lowPower ? .whileInUse : .lingering)
        let analyzer = SpeechAnalyzer(modules: [module], options: options)
        let backend = AnalyzerBackend(analyzer: analyzer, input: continuation, format: format, language: locale.identifier(.bcp47))
        backend.collector = Task.detached {
            var transcript = TranscriptAccumulator()
            do {
                if let transcriber = module as? SpeechTranscriber {
                    for try await result in transcriber.results {
                        transcript.add(
                            text: String(result.text.characters), alternatives: result.alternatives.map { String($0.characters) },
                            confidences: confidences(in: result.text), isFinal: result.isFinal)
                        onText(transcript.partial, false)
                    }
                } else if let transcriber = module as? DictationTranscriber {
                    for try await result in transcriber.results {
                        transcript.add(
                            text: String(result.text.characters), alternatives: result.alternatives.map { String($0.characters) },
                            confidences: confidences(in: result.text), isFinal: result.isFinal)
                        onText(transcript.partial, false)
                    }
                }
            } catch {
                if !Task.isCancelled {
                    Log.speech.info("Live transcription ended: \(error.localizedDescription, privacy: .public)")
                }
            }
            if !Task.isCancelled { onText(transcript.final, true) }
        }
        try await analyzer.prepareToAnalyze(in: format)
        try await analyzer.start(inputSequence: stream)
        return backend
    }

    func append(_ samples: [Float], rate: Double) {
        lock.withLock {
            guard let buffer = convert(samples, rate: rate) else { return }
            input.yield(AnalyzerInput(buffer: buffer))
        }
    }

    func finish() {
        input.finish()
        let analyzer = analyzer
        Task.detached {
            do {
                try await analyzer.finalizeAndFinishThroughEndOfInput()
            } catch {
                Log.speech.info("Finalizing the transcription failed: \(error.localizedDescription, privacy: .public)")
                await analyzer.cancelAndFinishNow()
            }
        }
    }

    func cancel() {
        input.finish()
        collector?.cancel()
        let analyzer = analyzer
        Task.detached { await analyzer.cancelAndFinishNow() }
    }

    /// `samples` in the analyzer's format. Called under `lock`.
    private func convert(_ samples: [Float], rate: Double) -> AVAudioPCMBuffer? {
        guard let buffer = PCMConversion.monoBuffer(samples, rate: rate) else { return nil }
        if buffer.format == format { return buffer }
        if converter == nil || converterRate != rate {
            converter = AVAudioConverter(from: buffer.format, to: format)
            converterRate = rate
        }
        guard let converter else { return nil }
        let capacity = AVAudioFrameCount((Double(samples.count) * format.sampleRate / rate).rounded(.up)) + 64
        guard let output = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: capacity) else { return nil }
        let source = UncheckedSendable(buffer)
        let supplied = Mutex(false)
        var error: NSError?
        let status = converter.convert(to: output, error: &error) { _, inputStatus in
            if supplied.withLock({ $0 }) {
                inputStatus.pointee = .noDataNow
                return nil
            }
            supplied.withLock { $0 = true }
            inputStatus.pointee = .haveData
            return source.value
        }
        guard status != .error, output.frameLength > 0 else { return nil }
        return output
    }
}

/// (confidence, characters) per run of a recognizer result.
@available(macOS 26, *)
nonisolated func confidences(in text: AttributedString) -> [(Double, Int)] {
    text.runs.compactMap { run in
        guard let confidence = run[AttributeScopes.SpeechAttributes.ConfidenceAttribute.self] else { return nil }
        return (confidence, text[run.range].characters.count)
    }
}

// MARK: - SFSpeechRecognizer (macOS 15; on-device only)

nonisolated final class RecognizerBackend: SpeechBackend, @unchecked Sendable {
    let language: String?
    private let request = SFSpeechAudioBufferRecognitionRequest()
    private let onText: SpeechBackendHandler
    private let done = Mutex(false)
    private let lock = NSLock()
    private var task: SFSpeechRecognitionTask?

    /// Nil unless on-device recognition for `locale` is available and allowed.
    init?(locale: Locale, onText: @escaping SpeechBackendHandler) {
        guard let recognizer = SFSpeechRecognizer(locale: locale), recognizer.supportsOnDeviceRecognition,
            SFSpeechRecognizer.authorizationStatus() == .authorized
        else { return nil }
        self.onText = onText
        language = recognizer.locale.identifier(.bcp47)
        let queue = OperationQueue()
        queue.maxConcurrentOperationCount = 1
        queue.qualityOfService = .userInitiated
        recognizer.queue = queue
        request.requiresOnDeviceRecognition = true
        request.shouldReportPartialResults = true
        request.taskHint = .dictation
        request.addsPunctuation = false
        task = recognizer.recognitionTask(with: request) { [weak self] result, error in
            self?.handle(result, error: error)
        }
    }

    private func handle(_ result: SFSpeechRecognitionResult?, error: Error?) {
        guard !done.withLock({ $0 }) else { return }
        if let result {
            let text = result.bestTranscription.formattedString
            guard result.isFinal else {
                onText(RecognizedText(text: text), false)
                return
            }
            done.withLock { $0 = true }
            let segments = result.bestTranscription.segments
            let confidence = segments.isEmpty ? nil : segments.map(\.confidence).reduce(0, +) / Float(segments.count)
            var seen = Set([text.lowercased()])
            let alternatives = result.transcriptions.dropFirst().map(\.formattedString)
                .filter { !$0.isEmpty && seen.insert($0.lowercased()).inserted }
            onText(RecognizedText(text: text, alternatives: Array(alternatives.prefix(4)), confidence: confidence), true)
        } else if error != nil {
            // Includes "no speech detected": the newest partial (or nothing) is final.
            done.withLock { $0 = true }
            onText(.empty, true)
        }
    }

    func append(_ samples: [Float], rate: Double) {
        guard let buffer = PCMConversion.monoBuffer(samples, rate: rate) else { return }
        lock.withLock { request.append(buffer) }
    }

    func finish() {
        lock.withLock { request.endAudio() }
    }

    func cancel() {
        lock.withLock { task?.cancel() }
    }
}
