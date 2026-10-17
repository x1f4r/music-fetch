import Foundation
import MusicFetchCore

/// `UserDefaults` keys (also used with `@AppStorage`).
nonisolated enum PreferenceKey {
    /// The provider songs open in (`Provider.storageKey`). Never registered as a default:
    /// written only when the user chooses or skips, so its absence means "ask".
    static let preferredProvider = "preferredProvider"
    static let customProviderName = "customProviderName"
    static let customProviderTemplate = "customProviderTemplate"
    /// "Open songs in" of builds before the provider panel; migrated once.
    static let legacyDefaultService = "defaultService"
    static let showMenuBarExtra = "showMenuBarExtra"
    static let notifyInBackground = "notifyInBackground"
    static let searchSungLyrics = "searchSungLyrics"
    /// Settings › Melody › "Stop automatically after a pause" (`ListenOptions.autoStop`).
    static let autoStopAfterPause = "autoStopAfterPause"
    static let systemAudioExplained = "systemAudioExplained"
    static let listenSource = "listenSource"
    static let settingsPane = "settingsPane"
    static let showInspector = "showInspector"
    /// Developer override for the melody-index manifest (e.g. a local test server).
    static let melodyManifestOverride = "MFMelodyIndexManifestURL"
}

nonisolated enum Preferences {
    static func registerDefaults(_ defaults: UserDefaults = .standard) {
        defaults.register(defaults: [
            PreferenceKey.showMenuBarExtra: true,
            PreferenceKey.notifyInBackground: true,
            PreferenceKey.searchSungLyrics: true,
            PreferenceKey.autoStopAfterPause: true,
        ])
    }

    /// Read from engine threads by the speech service.
    static var searchSungLyrics: Bool {
        UserDefaults.standard.bool(forKey: PreferenceKey.searchSungLyrics)
    }

    /// Options for live sessions.
    static func listenOptions(_ defaults: UserDefaults = .standard) -> ListenOptions {
        ListenOptions(autoStop: defaults.bool(forKey: PreferenceKey.autoStopAfterPause))
    }
}
