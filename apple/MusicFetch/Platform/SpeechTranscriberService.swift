import AVFoundation
import Foundation
import MusicFetchCore
import OSLog
import Speech

/// `SpeechToText` for sung lyrics, entirely on this Mac.
///
/// On macOS 26 prefers `SpeechAnalyzer` + `SpeechTranscriber`, and `DictationTranscriber`
/// for locales it lacks. On macOS 15, and for locales neither supports, uses
/// `SFSpeechRecognizer` with on-device recognition only. Nothing is sent to a server.
nonisolated final class SpeechTranscriberService: SpeechToText {
    /// Overall budget per call; the engine gives up after 15 s.
    static let timeout: Duration = .seconds(14)

    private let isEnabled: @Sendable () -> Bool

    /// `isEnabled` is read on every call (Settings › Melody › "Also search lyrics you sing").
    init(isEnabled: @escaping @Sendable () -> Bool) {
        self.isEnabled = isEnabled
    }

    func transcribe(wavPath: String, language: String, task: TaskHandle) throws -> [String] {
        guard isEnabled() else { return [] }
        let url = URL(filePath: wavPath)
        do {
            return try BlockingBridge.run(timeout: Self.timeout, isCanceled: { task.isCanceled() }) {
                try await Self.transcribe(file: url, language: language)
            }
        } catch BlockingBridge.Failure.canceled {
            throw PlatformError.Canceled
        } catch BlockingBridge.Failure.timedOut {
            throw PlatformError.Failed(message: "Speech recognition took too long.")
        } catch let error as PlatformError {
            throw error
        } catch {
            Log.speech.error("Transcription failed: \(error.localizedDescription, privacy: .public)")
            throw PlatformError.Failed(message: "Speech recognition failed.")
        }
    }

    /// Starts downloading the on-device model for `language` if it is supported but not
    /// installed yet, so the first hum doesn't wait for it. Errors are ignored.
    static func prepare(language: String) async {
        guard #available(macOS 26, *), !Compat.forcesLegacy else { return }
        let locale = Locale(identifier: language)
        guard let match = matchingLocale(locale, in: await SpeechTranscriber.supportedLocales) else { return }
        let transcriber = makeTranscriber(locale: match)
        guard await AssetInventory.status(forModules: [transcriber]) < .installed else { return }
        do {
            try await AssetInventory.assetInstallationRequest(supporting: [transcriber])?.downloadAndInstall()
        } catch {
            Log.speech.info("Speech model download did not finish: \(error.localizedDescription, privacy: .public)")
        }
    }

    /// A file transcriber that reports alternatives. Built from options rather than a
    /// preset: the preset names differ between the macOS 26.0 SDK and later systems.
    @available(macOS 26, *)
    private static func makeTranscriber(locale: Locale) -> SpeechTranscriber {
        SpeechTranscriber(locale: locale, transcriptionOptions: [], reportingOptions: [.alternativeTranscriptions], attributeOptions: [])
    }

    /// Up to five hypotheses, best first; empty when nothing was said.
    static func transcribe(file: URL, language: String) async throws -> [String] {
        let locale = Locale(identifier: language)
        if #available(macOS 26, *), !Compat.forcesLegacy,
            let hypotheses = try await transcribeWithSpeechAnalyzer(file: file, locale: locale)
        {
            return hypotheses
        }
        return try await recognizeWithLegacyRecognizer(file: file, locale: locale)
    }

    // MARK: - SpeechAnalyzer (macOS 26)

    /// Hypotheses from `SpeechTranscriber` or `DictationTranscriber`; nil when neither
    /// supports `locale` on this Mac.
    @available(macOS 26, *)
    private static func transcribeWithSpeechAnalyzer(file: URL, locale: Locale) async throws -> [String]? {
        if let match = matchingLocale(locale, in: await SpeechTranscriber.supportedLocales) {
            let transcriber = makeTranscriber(locale: match)
            if try await installAssets(for: transcriber) {
                let pieces = try await analyze(file: file, module: transcriber) {
                    var pieces: [Piece] = []
                    for try await result in transcriber.results {
                        pieces.append(
                            Piece(text: String(result.text.characters), alternatives: result.alternatives.map { String($0.characters) }))
                    }
                    return pieces
                }
                return hypotheses(from: pieces)
            }
        }

        if let match = matchingLocale(locale, in: await DictationTranscriber.supportedLocales) {
            let transcriber = DictationTranscriber(locale: match, preset: .longDictation)
            if try await installAssets(for: transcriber) {
                let pieces = try await analyze(file: file, module: transcriber) {
                    var pieces: [Piece] = []
                    for try await result in transcriber.results {
                        pieces.append(
                            Piece(text: String(result.text.characters), alternatives: result.alternatives.map { String($0.characters) }))
                    }
                    return pieces
                }
                return hypotheses(from: pieces)
            }
        }
        return nil
    }

    /// One finalized result: its best text and alternatives.
    struct Piece: Sendable, Equatable {
        var text: String
        var alternatives: [String]
    }

    @available(macOS 26, *)
    private static func installAssets(for module: any SpeechModule) async throws -> Bool {
        switch await AssetInventory.status(forModules: [module]) {
        case .installed:
            return true
        case .unsupported:
            return false
        case .supported, .downloading:
            guard let request = try await AssetInventory.assetInstallationRequest(supporting: [module]) else { return true }
            try await request.downloadAndInstall()
            return await AssetInventory.status(forModules: [module]) == .installed
        @unknown default:
            return false
        }
    }

    /// Runs the analyzer over `file` while `collect` drains the module's results.
    @available(macOS 26, *)
    private static func analyze(
        file: URL,
        module: any SpeechModule,
        collect: @escaping @Sendable () async throws -> [Piece]
    ) async throws -> [Piece] {
        let collector = Task { try await collect() }
        do {
            let audio = try AVAudioFile(forReading: file)
            let analyzer = try await SpeechAnalyzer(inputAudioFile: audio, modules: [module], finishAfterFile: true)
            let pieces = try await withTaskCancellationHandler {
                try await collector.value
            } onCancel: {
                collector.cancel()
                Task { await analyzer.cancelAndFinishNow() }
            }
            try Task.checkCancellation()
            return pieces
        } catch {
            collector.cancel()
            throw error
        }
    }

    /// Joins the per-segment texts into whole-clip hypotheses: the best text first, then
    /// one hypothesis per alternative rank. Deduplicated, at most five.
    static func hypotheses(from pieces: [Piece]) -> [String] {
        let join: ([String]) -> String = { parts in
            parts.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { !$0.isEmpty }
                .joined(separator: " ")
        }
        guard !pieces.isEmpty else { return [] }
        var hypotheses = [join(pieces.map(\.text))]
        let ranks = pieces.map(\.alternatives.count).max() ?? 0
        for rank in 0..<ranks {
            hypotheses.append(join(pieces.map { rank < $0.alternatives.count ? $0.alternatives[rank] : $0.text }))
        }
        var seen = Set<String>()
        return hypotheses.filter { !$0.isEmpty && seen.insert($0.lowercased()).inserted }.prefix(5).map { $0 }
    }

    /// The supported locale closest to `locale`: same identifier, then same language
    /// and region, then the same language.
    static func matchingLocale(_ locale: Locale, in supported: [Locale]) -> Locale? {
        let wanted = locale.identifier(.bcp47).lowercased()
        if let exact = supported.first(where: { $0.identifier(.bcp47).lowercased() == wanted }) {
            return exact
        }
        let language = locale.language.languageCode?.identifier
        let region = locale.region?.identifier
        let sameLanguage = supported.filter { $0.language.languageCode?.identifier == language }
        if let regional = sameLanguage.first(where: { $0.region?.identifier == region }) {
            return regional
        }
        return sameLanguage.first
    }

    // MARK: - SFSpeechRecognizer (macOS 15; on-device only)

    private static func recognizeWithLegacyRecognizer(file: URL, locale: Locale) async throws -> [String] {
        guard let recognizer = SFSpeechRecognizer(locale: locale), recognizer.supportsOnDeviceRecognition else {
            throw PlatformError.NotSupported
        }
        guard await authorizeLegacyRecognizer() else { throw PlatformError.NotSupported }

        let request = SFSpeechURLRecognitionRequest(url: file)
        request.requiresOnDeviceRecognition = true
        request.shouldReportPartialResults = false
        request.taskHint = .dictation

        let holder = RecognitionTaskHolder()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<[String], Error>) in
                let once = ResumeOnce(continuation)
                holder.task = recognizer.recognitionTask(with: request) { result, error in
                    if let result, result.isFinal {
                        let texts = result.transcriptions.map(\.formattedString)
                        var seen = Set<String>()
                        once.resume(returning: texts.filter { !$0.isEmpty && seen.insert($0.lowercased()).inserted }.prefix(5).map { $0 })
                    } else if let error {
                        // "No speech detected" ends the task with an error; that is an empty result.
                        let nsError = error as NSError
                        if nsError.domain == "kAFAssistantErrorDomain" && nsError.code == 1110 {
                            once.resume(returning: [])
                        } else {
                            once.resume(throwing: error)
                        }
                    }
                }
            }
        } onCancel: {
            holder.task?.cancel()
        }
    }

    /// Never asks from here: this runs on an engine thread in the middle of a session.
    /// The Listen stage asks before the first hum (`SpeechModel`), Settings when the
    /// user turns lyric search back on.
    private static func authorizeLegacyRecognizer() async -> Bool {
        SFSpeechRecognizer.authorizationStatus() == .authorized
    }
}

nonisolated private final class RecognitionTaskHolder: @unchecked Sendable {
    var task: SFSpeechRecognitionTask?
}

/// Resumes a continuation at most once (recognition callbacks can fire repeatedly).
nonisolated private final class ResumeOnce<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<T, Error>?

    init(_ continuation: CheckedContinuation<T, Error>) {
        self.continuation = continuation
    }

    func resume(returning value: T) {
        take()?.resume(returning: value)
    }

    func resume(throwing error: Error) {
        take()?.resume(throwing: error)
    }

    private func take() -> CheckedContinuation<T, Error>? {
        lock.lock()
        let value = continuation
        continuation = nil
        lock.unlock()
        return value
    }
}
