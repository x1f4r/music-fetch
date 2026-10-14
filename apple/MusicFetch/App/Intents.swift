import AppIntents
import Foundation

/// "Listen": identify what's playing with the microphone.
nonisolated struct ListenIntent: AppIntent {
    static let title: LocalizedStringResource = "Listen"
    static let description = IntentDescription("Identifies the song that is playing.")
    static let openAppWhenRun = true

    @MainActor
    func perform() async throws -> some IntentResult {
        await AppModel.shared?.start()
        AppModel.shared?.listenNow(.microphone)
        return .result()
    }
}

/// "Hum": identify a tune you hum, whistle or sing.
nonisolated struct HumIntent: AppIntent {
    static let title: LocalizedStringResource = "Hum"
    static let description = IntentDescription("Identifies a tune you hum, whistle or sing.")
    static let openAppWhenRun = true

    @MainActor
    func perform() async throws -> some IntentResult {
        await AppModel.shared?.start()
        AppModel.shared?.listenNow(.hum)
        return .result()
    }
}

/// "Identify Link": the songs in a link (video, track, album, playlist).
struct IdentifyLinkIntent: @preconcurrency AppIntent {
    static let title: LocalizedStringResource = "Identify Link"
    static let description = IntentDescription("Finds the songs in a link.")
    static let openAppWhenRun = true

    @Parameter(title: "Link")
    var link: URL

    @MainActor
    func perform() async throws -> some IntentResult {
        await AppModel.shared?.start()
        AppModel.shared?.identify(link.isFileURL ? link.path : link.absoluteString)
        return .result()
    }
}

/// "Open Last Result".
nonisolated struct OpenLastResultIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Last Result"
    static let description = IntentDescription("Shows the last song Music Fetch identified.")
    static let openAppWhenRun = true

    @MainActor
    func perform() async throws -> some IntentResult {
        guard let app = AppModel.shared else { return .result() }
        await app.start()
        await app.library.refresh()
        if let recent = app.library.recent { app.showOnListen(recent.id) }
        return .result()
    }
}

nonisolated struct MusicFetchShortcuts: AppShortcutsProvider {
    static var appShortcuts: [AppShortcut] {
        AppShortcut(
            intent: ListenIntent(), phrases: ["Identify with \(.applicationName)", "Listen with \(.applicationName)"],
            shortTitle: "Listen", systemImageName: "mic")
        AppShortcut(intent: HumIntent(), phrases: ["Hum with \(.applicationName)"], shortTitle: "Hum", systemImageName: "waveform")
        AppShortcut(
            intent: OpenLastResultIntent(), phrases: ["Show my last \(.applicationName) result"], shortTitle: "Last Result",
            systemImageName: "music.note")
    }
}
