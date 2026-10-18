import Foundation
import Observation

/// Sung lyrics on this Mac: whether words can be recognized while someone hums or sings,
/// the one-time explanation before macOS 15 asks for speech recognition, and what the
/// Settings row says. Everything stays on the Mac; only the recognized words go to the
/// lyric search.
@Observable
final class SpeechModel {
    /// Nil until the first check.
    private(set) var status: SpeechAccess.Status?
    /// BCP-47 tag of the language to recognize (the Mac's).
    let language: String

    @ObservationIgnored private let isDemo: Bool
    @ObservationIgnored private let defaults: UserDefaults
    @ObservationIgnored private var downloadingModel = false
    /// Demo states never write the user's preferences.
    @ObservationIgnored private var askedInDemo = false

    init(language: String = EngineHost.language(for: .current), demoStatus: SpeechAccess.Status? = nil,
         defaults: UserDefaults = .standard)
    {
        self.language = language
        self.defaults = defaults
        isDemo = demoStatus != nil
        status = demoStatus
    }

    /// Checks again (at launch, when the app becomes active, after a hum).
    func refresh() async {
        guard !isDemo else { return }
        let status = await SpeechAccess.status(language: language)
        if status != self.status { self.status = status }
    }

    /// Lyric search is on and the recognizer can run live during capture.
    var transcribesLive: Bool {
        defaults.bool(forKey: PreferenceKey.searchSungLyrics) && status == .ready
    }

    /// Before the first hum on macOS 15: say what the permission is for, then let macOS
    /// ask. Only once; declining keeps humming working without words.
    var asksBeforeHumming: Bool {
        defaults.bool(forKey: PreferenceKey.searchSungLyrics) && status == .needsPermission && !wasAsked
    }

    /// "Not Now" on the stage: don't explain again.
    func declinePermission() {
        markAsked()
    }

    /// Shows the system prompt (the first time) and checks again.
    func requestPermission() async {
        markAsked()
        guard !isDemo else { return }
        _ = await SpeechAccess.requestPermission()
        await refresh()
    }

    /// macOS 26: the language's speech model isn't installed. A hum asked for words, so
    /// install it now (from the system's asset store) for the next one; this hum's words
    /// are still read after capture by the engine's speech service.
    func installModelIfNeeded() {
        guard status == .needsModel, !downloadingModel, !isDemo else { return }
        downloadingModel = true
        Task {
            await SpeechTranscriberService.prepare(language: language)
            downloadingModel = false
            await refresh()
        }
    }

    private var wasAsked: Bool {
        isDemo ? askedInDemo : defaults.bool(forKey: PreferenceKey.speechPermissionAsked)
    }

    private func markAsked() {
        if isDemo {
            askedInDemo = true
        } else {
            defaults.set(true, forKey: PreferenceKey.speechPermissionAsked)
        }
    }
}
