import AppKit
import Foundation
import MusicFetchCore

/// Deterministic app states for screenshots: launch with `-MFDemoState <state>`.
/// The app then runs on `FakeEngine` and never touches the real library.
///
/// States: idle, listening, hum, matching, result, candidates, mix, collection, library,
/// session, noMatch, captureSilent, downloading, prePrompt, micDenied, melodyPrompt,
/// lyrics, failed, settings-general, settings-services, settings-melody,
/// settings-storage, settings-about.
nonisolated struct DemoState: Sendable {
    let name: String

    static func fromLaunchArguments(_ defaults: UserDefaults = .standard) -> DemoState? {
        // Unit tests host the app: keep them away from the real library.
        if ProcessInfo.processInfo.environment["XCTestConfigurationFilePath"] != nil { return DemoState(name: "tests") }
        guard let name = defaults.string(forKey: "MFDemoState"), !name.isEmpty else { return nil }
        return DemoState(name: name)
    }

    var scenario: FakeEngine.Scenario {
        switch name {
        case "listening": .listening
        case "hum": .hum
        case "matching": .matching
        case "noMatch": .noMatch
        case "captureSilent": .captureSilent
        case "downloading": .downloading
        case "mix": .mix
        default: .interactive
        }
    }

    /// The settings pane to open, for `settings-<pane>`.
    var settingsPane: String? {
        name.hasPrefix("settings-") ? String(name.dropFirst("settings-".count)) : nil
    }

    @MainActor
    func apply(to app: AppModel) async {
        switch name {
        case "listening", "matching", "noMatch":
            app.listenNow(.microphone)
        case "hum":
            app.listenNow(.hum)
        case "captureSilent":
            app.listenNow(.thisMac)
        case "result":
            app.showOnListen(DemoFixtures.resultID)
        case "candidates":
            app.showOnListen(DemoFixtures.humID)
        case "mix", "downloading":
            app.identify("https://www.youtube.com/watch?v=demo")
        case "collection":
            app.identify("https://open.spotify.com/album/demo")
        case "library":
            app.route = .library
        case "session":
            app.openInLibrary(DemoFixtures.mixID)
        case "prePrompt":
            app.listen.showForDemo(.systemAudioPrePrompt)
        case "micDenied":
            app.listen.showForDemo(.microphoneDenied)
        case "failed":
            app.listen.showForDemo(.failed(.mediaUnavailable, nil))
        case "melodyPrompt":
            app.showMelodyPrompt = true
        case "lyrics":
            app.presentLyricSearch()
            app.lyricSearch?.text = "a few words I half remember"
            app.lyricSearch?.search()
        default:
            break
        }
        if let pane = settingsPane {
            UserDefaults.standard.set(pane, forKey: PreferenceKey.settingsPane)
            try? await Task.sleep(for: .milliseconds(300))
            NSApp.activate()
            app.openSettingsAction?()
        }
    }
}
