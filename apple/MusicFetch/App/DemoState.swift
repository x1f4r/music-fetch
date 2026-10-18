import AppKit
import Foundation
import MusicFetchCore

/// Deterministic app states for screenshots: launch with `-MFDemoState <state>`.
/// The app then runs on `FakeEngine` and never touches the real library, and nothing
/// it changes is written to the app's preferences.
///
/// States: idle, listening, hum, hum-answers, matching, result, candidates, candidates-unsure, mix, collection, library,
/// library-selection, library-pinned, session, noMatch, captureSilent, downloading,
/// prePrompt, speechPrePrompt, micDenied, melodyPrompt, lyrics, failed, providers, providers-custom,
/// settings-general, settings-general-custom, settings-services, settings-melody, settings-melody-model,
/// settings-melody-speech-off,
/// settings-melody-downloading, settings-storage, settings-about.
///
/// Options for the screenshot tests (apple/MusicFetchUITests):
/// - `-MFWindowSize 1180x760`: the main window's content size.
/// - `-MFAppearance light|dark`: the app's appearance, whatever the system uses.
/// - `-MFReduceMotion YES`: crossfades instead of springs, so states settle quickly.
/// - `-MFInspector YES|NO`: whether the inspector starts open (default open).
nonisolated struct DemoState: Sendable {
    let name: String
    var windowSize: CGSize?
    var appearance: String?
    var reduceMotion = false
    var inspector: Bool?
    /// Laid out in a window that is never shown (`-MFRender`): no activation, no
    /// window moves, no Settings window.
    var offscreen = false

    init(name: String) {
        self.name = name
    }

    static func fromLaunchArguments(_ defaults: UserDefaults = .standard) -> DemoState? {
        // Unit tests host the app: keep them away from the real library. (Tests that drive
        // live sessions themselves create `DemoState(name: "tests-manual")`.)
        if ProcessInfo.processInfo.environment["XCTestConfigurationFilePath"] != nil { return DemoState(name: "tests") }
        // The offscreen harness builds its own states; the app's model stays on the fake engine.
        if RenderHarness.isActive {
            var state = DemoState(name: "render")
            state.offscreen = true
            return state
        }
        guard let name = defaults.string(forKey: "MFDemoState"), !name.isEmpty else { return nil }
        var state = DemoState(name: name)
        state.windowSize = defaults.string(forKey: "MFWindowSize").flatMap(Self.size)
        state.appearance = defaults.string(forKey: "MFAppearance")?.lowercased()
        state.reduceMotion = defaults.bool(forKey: "MFReduceMotion")
        state.inspector = defaults.object(forKey: "MFInspector") != nil ? defaults.bool(forKey: "MFInspector") : nil
        return state
    }

    /// Unit tests host the app (`tests`, `tests-manual`): no windows, no settling.
    var isTest: Bool { name.hasPrefix("tests") }

    /// "1180x760".
    static func size(_ text: String) -> CGSize? {
        let parts = text.lowercased().split(separator: "x").compactMap { Double($0) }
        guard parts.count == 2, parts[0] > 0, parts[1] > 0 else { return nil }
        return CGSize(width: parts[0], height: parts[1])
    }

    var scenario: FakeEngine.Scenario {
        switch name {
        case "listening": .listening
        case "hum": .hum
        case "hum-answers": .humAnswers
        case "matching": .matching
        case "noMatch": .noMatch
        case "captureSilent": .captureSilent
        case "downloading": .downloading
        case "mix": .mix
        case "tests-manual": .manual
        default: .interactive
        }
    }

    /// The settings pane to open, for `settings-<pane>`.
    var settingsPane: String? {
        guard name.hasPrefix("settings-") else { return nil }
        return name.dropFirst("settings-".count).split(separator: "-").first.map(String.init)
    }

    /// How long the state needs to settle before a screenshot (scripted sessions,
    /// reveal animations).
    private var settleTime: Duration {
        switch name {
        case "noMatch": .seconds(3)
        case "mix": .seconds(6)
        case "collection", "downloading": .seconds(2)
        case "listening", "hum", "matching", "captureSilent": .seconds(2)
        case "hum-answers": .seconds(5)
        case "lyrics": .seconds(1.5)
        default: .seconds(1)
        }
    }

    /// `-MFAppearance`, before any window appears.
    @MainActor
    func applyAppearance() {
        switch appearance {
        case "dark": NSApp.appearance = NSAppearance(named: .darkAqua)
        case "light": NSApp.appearance = NSAppearance(named: .aqua)
        default: break
        }
    }

    @MainActor
    func apply(to app: AppModel) async {
        // Screenshots show the window in front, active when the system allows it.
        if !isTest, !offscreen {
            NSApp.activate()
            NSApp.windows.filter(\.isVisible).forEach { $0.orderFrontRegardless() }
        }
        if !offscreen, let windowSize, let window = NSApp.windows.first(where: { $0.identifier?.rawValue.hasPrefix("main") == true }) {
            window.setContentSize(windowSize)
            if let screen = window.screen ?? NSScreen.main {
                window.setFrameTopLeftPoint(NSPoint(x: screen.visibleFrame.minX + 16, y: screen.visibleFrame.maxY - 16))
            }
        }
        switch name {
        case "listening", "matching", "noMatch":
            app.listenNow(.microphone)
        case "hum", "hum-answers":
            app.listenNow(.hum)
        case "captureSilent":
            app.listenNow(.thisMac)
        case "result":
            app.showOnListen(DemoFixtures.resultID)
        case "candidates":
            app.showOnListen(DemoFixtures.humID)
        case "candidates-unsure":
            app.showOnListen(DemoFixtures.unsureHumID)
        case "mix", "downloading":
            app.identify("https://www.youtube.com/watch?v=demo")
        case "collection":
            app.identify("https://open.spotify.com/album/demo")
        case "library":
            app.route = .library
        case "library-selection":
            app.route = .library
            app.library.selection = [DemoFixtures.resultID, DemoFixtures.mixID, DemoFixtures.humID]
        case "library-pinned":
            app.route = .library
            app.library.filter = .pinned
        case "session":
            app.openInLibrary(DemoFixtures.mixID)
        case "prePrompt":
            // The explanation comes with choosing This Mac.
            app.listen.select(.thisMac)
            app.listen.showForDemo(.systemAudioPrePrompt)
        case "micDenied":
            app.listen.showForDemo(.microphoneDenied)
        case "speechPrePrompt":
            // The first hum on macOS 15 explains speech recognition before macOS asks.
            app.listen.start(.hum)
        case "failed":
            app.listen.showForDemo(.failed(.mediaUnavailable, nil))
        case "melodyPrompt":
            app.showMelodyPrompt = true
        case "providers":
            app.presentProviderPanel()
        case "providers-custom":
            app.presentProviderPanel(startingWithCustom: true)
        case "settings-general-custom":
            app.setPreferredProvider(.custom, custom: CustomProvider(name: "Radio Archive", template: "https://radio.example.org/find?q="))
        case "lyrics":
            app.presentLyricSearch()
            app.lyricSearch?.text = "a few words I half remember"
            app.lyricSearch?.search()
        default:
            break
        }
        if let pane = settingsPane, !offscreen {
            // In memory only: the Settings window reads it instead of the stored pane.
            app.demoSettingsPane = pane
            try? await Task.sleep(for: .milliseconds(300))
            NSApp.activate()
            app.openSettingsAction?()
            if name == "settings-melody-model" {
                (app.engine as? FakeEngine)?.setMelodyStatus(DemoFixtures.melodyStatusWithoutEncoder)
                app.settings.refreshMelody()
            }
            if name == "settings-melody-downloading" {
                try? await Task.sleep(for: .milliseconds(200))
                app.settings.downloadMelody()
            }
        }
        if !isTest {
            try? await Task.sleep(for: settleTime)
            app.demoReady = true
        }
    }
}
