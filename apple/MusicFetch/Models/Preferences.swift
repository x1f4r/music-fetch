import Foundation
import MusicFetchCore

/// `UserDefaults` keys (also used with `@AppStorage`).
nonisolated enum PreferenceKey {
    static let defaultService = "defaultService"
    static let showMenuBarExtra = "showMenuBarExtra"
    static let notifyInBackground = "notifyInBackground"
    static let searchSungLyrics = "searchSungLyrics"
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
            PreferenceKey.defaultService: ServiceKey.spotify.rawValue,
            PreferenceKey.showMenuBarExtra: true,
            PreferenceKey.notifyInBackground: true,
            PreferenceKey.searchSungLyrics: true,
        ])
    }

    /// Read from engine threads by the speech service.
    static var searchSungLyrics: Bool {
        UserDefaults.standard.bool(forKey: PreferenceKey.searchSungLyrics)
    }
}

/// A `Service` that can be stored in `UserDefaults`.
nonisolated enum ServiceKey: String, CaseIterable, Sendable {
    case spotify, appleMusic, youTubeMusic, youTube, deezer, tidal, amazonMusic, soundCloud

    init(_ service: Service) {
        self =
            switch service {
            case .appleMusic: .appleMusic
            case .youTubeMusic: .youTubeMusic
            case .youTube: .youTube
            case .deezer: .deezer
            case .tidal: .tidal
            case .amazonMusic: .amazonMusic
            case .soundCloud: .soundCloud
            default: .spotify
            }
    }

    var service: Service {
        switch self {
        case .spotify: .spotify
        case .appleMusic: .appleMusic
        case .youTubeMusic: .youTubeMusic
        case .youTube: .youTube
        case .deezer: .deezer
        case .tidal: .tidal
        case .amazonMusic: .amazonMusic
        case .soundCloud: .soundCloud
        }
    }
}
