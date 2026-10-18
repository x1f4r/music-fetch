import Foundation
import Speech

/// Whether this Mac can turn sung words into text on its own, and the permission that
/// takes. Recognition never uses a server.
///
/// macOS 26 runs `SpeechAnalyzer`, whose transcribers keep audio on the Mac and need no
/// permission; the model for a language may have to download first. macOS 15 (and
/// languages the new transcribers lack) uses `SFSpeechRecognizer` with on-device
/// recognition only, which needs the user's permission.
nonisolated enum SpeechAccess {
    nonisolated enum Status: Equatable, Sendable {
        /// Words are recognized live while the user sings.
        case ready
        /// macOS 26: the language's model is not installed yet.
        case needsModel
        /// macOS 15: speech recognition needs the user's permission first.
        case needsPermission
        /// macOS 15: the user (or a profile) turned speech recognition off.
        case denied
        /// No on-device recognizer for the language on this Mac.
        case unsupported
    }

    /// The recognizer that will run for `language`, and what it still needs.
    static func status(language: String) async -> Status {
        let locale = Locale(identifier: language)
        if #available(macOS 26, *), !Compat.forcesLegacy, let analyzer = await analyzerModule(for: locale) {
            return await AssetInventory.status(forModules: [analyzer.module]) == .installed ? .ready : .needsModel
        }
        return legacyStatus(locale: locale)
    }

    /// `SFSpeechRecognizer` with on-device recognition: supported, then authorized.
    static func legacyStatus(locale: Locale) -> Status {
        guard let recognizer = SFSpeechRecognizer(locale: locale), recognizer.supportsOnDeviceRecognition else {
            return .unsupported
        }
        return status(for: SFSpeechRecognizer.authorizationStatus())
    }

    static func status(for authorization: SFSpeechRecognizerAuthorizationStatus) -> Status {
        switch authorization {
        case .authorized: .ready
        case .notDetermined: .needsPermission
        case .denied, .restricted: .denied
        @unknown default: .denied
        }
    }

    /// Shows the system prompt (once per install); true when allowed.
    static func requestPermission() async -> Bool {
        await withCheckedContinuation { continuation in
            SFSpeechRecognizer.requestAuthorization { continuation.resume(returning: $0 == .authorized) }
        }
    }

    /// `SpeechTranscriber` for the language, or `DictationTranscriber` when only it has
    /// one, with the locale it runs in; nil when neither has the language.
    @available(macOS 26, *)
    static func analyzerModule(for locale: Locale) async -> (module: any SpeechModule, locale: Locale)? {
        if let match = SpeechTranscriberService.matchingLocale(locale, in: await SpeechTranscriber.supportedLocales) {
            let module = SpeechTranscriber(
                locale: match, transcriptionOptions: [], reportingOptions: [.volatileResults, .alternativeTranscriptions],
                attributeOptions: [.transcriptionConfidence])
            return (module, match)
        }
        if let match = SpeechTranscriberService.matchingLocale(locale, in: await DictationTranscriber.supportedLocales) {
            let module = DictationTranscriber(
                locale: match, contentHints: [], transcriptionOptions: [],
                reportingOptions: [.volatileResults, .alternativeTranscriptions], attributeOptions: [.transcriptionConfidence])
            return (module, match)
        }
        return nil
    }
}
