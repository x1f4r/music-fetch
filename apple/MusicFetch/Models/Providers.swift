import AppKit
import Foundation
import MusicFetchCore

/// Where "Open in …" sends a track (docs/APPS.md, "Opening links"): a service the engine
/// builds links for, or the user's custom provider.
nonisolated enum Provider: Hashable, Sendable {
    case service(Service)
    case custom

    /// The value stored in preferences ("spotify", "custom", …).
    var storageKey: String {
        switch self {
        case .service(let service): service.storageKey
        case .custom: "custom"
        }
    }

    init?(storageKey: String) {
        if storageKey == "custom" {
            self = .custom
        } else if let service = Service(storageKey: storageKey) {
            self = .service(service)
        } else {
            return nil
        }
    }
}

/// A name and a URL template with `{artist}`, `{title}`, `{query}` and `{isrc}`
/// placeholders, for any app or site the built-in providers miss.
nonisolated struct CustomProvider: Equatable, Sendable {
    static let placeholders = ["{artist}", "{title}", "{query}", "{isrc}"]

    var name: String
    var template: String

    var trimmedName: String { name.trimmingCharacters(in: .whitespacesAndNewlines) }

    /// Usable: a name, at least one placeholder, and an http(s) URL once filled in.
    var isValid: Bool {
        !trimmedName.isEmpty && Self.isValidTemplate(template)
    }

    static func isValidTemplate(_ template: String) -> Bool {
        let trimmed = template.trimmingCharacters(in: .whitespacesAndNewlines)
        guard placeholders.contains(where: trimmed.contains) else { return false }
        return customProviderLink(track: sampleTrack, template: trimmed) != nil
    }

    /// A track with every field the placeholders read, for validating templates.
    private static let sampleTrack = Track(
        title: "Title", artist: "Artist", artists: ["Artist"], album: nil, isrc: "USAAA0000001", durationMs: nil,
        releaseYear: nil, genre: nil, artworkUrl: nil, previewUrl: nil, links: [], ids: [:], identityKey: "")
}

/// One provider's link for a track.
nonisolated struct ProviderLink: Hashable, Sendable {
    var provider: Provider
    var url: String
    /// A search for "artist title" rather than the track's own page.
    var isSearch: Bool
}

/// The providers the app offers, in display order.
nonisolated enum Providers {
    /// Every service `providerLinks` covers (docs/APPS.md).
    static let all: [Service] = [
        .spotify, .appleMusic, .youTubeMusic, .youTube, .deezer, .tidal, .amazonMusic, .soundCloud, .qobuz, .pandora,
        .audiomack, .anghami, .jioSaavn, .boomplay, .kkbox, .yandexMusic, .netEaseMusic, .bandcamp, .shazam, .lastFm,
        .genius,
    ]

    /// The first-launch panel's tiles; the rest are under "More…".
    static let common: [Service] = [.spotify, .youTubeMusic, .appleMusic, .tidal, .amazonMusic, .deezer, .soundCloud]

    /// Providers that are not tiles in the first-launch panel.
    static var more: [Service] { all.filter { !common.contains($0) } }

    /// What Skip and an undecided preference mean.
    static let fallback: Provider = .service(.spotify)

    /// One link per provider in `all` order (the track's exact link when known, else the
    /// engine's search link), then the custom provider when it is set up.
    static func links(for track: Track, custom: CustomProvider?) -> [ProviderLink] {
        let known = track.links + providerLinks(track: track)
        var result: [ProviderLink] = []
        for service in all {
            let candidates = known.filter { $0.service == service }
            guard let link = candidates.first(where: { !$0.isSearch }) ?? candidates.first else { continue }
            result.append(ProviderLink(provider: .service(service), url: link.url, isSearch: link.isSearch))
        }
        if let custom, custom.isValid,
            let url = customProviderLink(track: track, template: custom.template.trimmingCharacters(in: .whitespacesAndNewlines))
        {
            result.append(ProviderLink(provider: .custom, url: url, isSearch: false))
        }
        return result
    }

    /// The link the big button opens: the preferred provider's, else the first exact
    /// link, else the first link.
    static func primary(in links: [ProviderLink], preferred: Provider) -> ProviderLink? {
        links.first { $0.provider == preferred } ?? links.first { !$0.isSearch && $0.provider != .custom } ?? links.first
    }

    /// The chevron menu: every other provider, exact links first, then searches, then the
    /// custom provider.
    static func others(in links: [ProviderLink], excluding primary: ProviderLink?) -> [ProviderLink] {
        let rest = links.filter { $0 != primary }
        let services = rest.filter { $0.provider != .custom }
        return services.filter { !$0.isSearch } + services.filter(\.isSearch) + rest.filter { $0.provider == .custom }
    }
}

/// A track's links resolved for the user's provider: what the big button opens and
/// what its chevron menu lists.
nonisolated struct TrackLinks: Sendable {
    let primary: ProviderLink?
    /// Every other provider: exact links, then searches, then the custom provider.
    let others: [ProviderLink]

    init(track: Track, preference: ProviderPreference) {
        let links = Providers.links(for: track, custom: preference.usableCustom)
        primary = Providers.primary(in: links, preferred: preference.provider)
        others = Providers.others(in: links, excluding: primary)
    }

    /// `others` laid out for menus: exact links, then searches on the common providers,
    /// then the custom provider; searches on the remaining providers go into a "More…"
    /// submenu so the menu stays short.
    var menu: ProviderMenu { ProviderMenu(others) }
}

/// The chevron menu's sections (see `TrackLinks.menu`).
nonisolated struct ProviderMenu: Equatable, Sendable {
    var exact: [ProviderLink]
    var searches: [ProviderLink]
    /// Searches on providers outside `Providers.common`.
    var more: [ProviderLink]
    var custom: [ProviderLink]

    init(_ links: [ProviderLink]) {
        let services = links.filter { $0.provider != .custom }
        let isCommon: (ProviderLink) -> Bool = { link in
            if case .service(let service) = link.provider { return Providers.common.contains(service) }
            return false
        }
        exact = services.filter { !$0.isSearch }
        searches = services.filter { $0.isSearch && isCommon($0) }
        more = services.filter { $0.isSearch && !isCommon($0) }
        custom = links.filter { $0.provider == .custom }
    }

    /// Every link, in menu order.
    var all: [ProviderLink] { exact + searches + more + custom }
}

// MARK: - Preference

/// The stored provider decision: `preferredProvider` is written only when the user
/// chooses or skips in the first-launch panel (or changes it in Settings), so its
/// absence means "ask".
nonisolated struct ProviderPreference: Equatable, Sendable {
    /// Raw `preferredProvider` value; nil while undecided.
    var key: String?
    var custom: CustomProvider

    init(key: String?, customName: String, customTemplate: String) {
        self.key = key
        self.custom = CustomProvider(name: customName, template: customTemplate)
    }

    static func load(_ defaults: UserDefaults = .standard) -> ProviderPreference {
        ProviderPreference(
            key: defaults.string(forKey: PreferenceKey.preferredProvider),
            customName: defaults.string(forKey: PreferenceKey.customProviderName) ?? "",
            customTemplate: defaults.string(forKey: PreferenceKey.customProviderTemplate) ?? "")
    }

    var isDecided: Bool { key.flatMap(Provider.init(storageKey:)) != nil }

    /// The provider links open in. A custom provider without a usable template falls
    /// back to Spotify.
    var provider: Provider {
        guard let key, let provider = Provider(storageKey: key) else { return Providers.fallback }
        if provider == .custom, !custom.isValid { return Providers.fallback }
        return provider
    }

    /// The custom provider when it is usable.
    var usableCustom: CustomProvider? {
        custom.isValid ? custom : nil
    }

    var name: String { Copy.name(of: provider, custom: custom) }

    /// Records the decision (first-launch panel or Settings).
    static func store(_ provider: Provider, custom: CustomProvider? = nil, in defaults: UserDefaults = .standard) {
        if let custom {
            defaults.set(custom.trimmedName, forKey: PreferenceKey.customProviderName)
            defaults.set(custom.template.trimmingCharacters(in: .whitespacesAndNewlines), forKey: PreferenceKey.customProviderTemplate)
        }
        defaults.set(provider.storageKey, forKey: PreferenceKey.preferredProvider)
    }

    /// Carries a service picked in an earlier build's Settings over as the decision.
    static func migrate(_ defaults: UserDefaults = .standard) {
        guard defaults.string(forKey: PreferenceKey.preferredProvider) == nil,
            let legacy = defaults.string(forKey: PreferenceKey.legacyDefaultService), Service(storageKey: legacy) != nil
        else { return }
        defaults.set(legacy, forKey: PreferenceKey.preferredProvider)
        defaults.removeObject(forKey: PreferenceKey.legacyDefaultService)
    }
}

// MARK: - Installed apps

/// Finds music apps on this Mac to pre-select in the first-launch panel.
nonisolated enum InstalledProviders {
    /// Bundle identifiers of each service's Mac app or installed web app.
    static let bundleIDs: [Service: [String]] = [
        .spotify: ["com.spotify.client"],
        .youTubeMusic: [
            "com.github.th-ch.youtube-music",
            // The YouTube Music web app installed from Chrome, Brave or Edge.
            "com.google.Chrome.app.cinhimbnkkaeohfgghhklpknlkffjgod",
            "com.brave.Browser.app.cinhimbnkkaeohfgghhklpknlkffjgod",
            "com.microsoft.edgemac.app.cinhimbnkkaeohfgghhklpknlkffjgod",
        ],
        .tidal: ["com.tidal.desktop"],
        .deezer: ["com.deezer.deezer-desktop"],
        .amazonMusic: ["com.amazon.music"],
        .appleMusic: ["com.apple.Music"],
    ]

    /// App names looked up in the Applications folders (Safari web apps get generated
    /// bundle identifiers).
    static let appNames: [Service: [String]] = [
        .youTubeMusic: ["YouTube Music"]
    ]

    /// Third-party apps first: someone who installed one listens there. Music is on
    /// every Mac and comes last.
    static let preference: [Service] = [.spotify, .youTubeMusic, .tidal, .deezer, .amazonMusic, .appleMusic]

    /// The service to pre-select.
    static func suggestion(isInstalled: (Service) -> Bool) -> Service {
        preference.first(where: isInstalled) ?? .appleMusic
    }

    @MainActor
    static func suggestion() -> Service {
        suggestion(isInstalled: isInstalled)
    }

    @MainActor
    static func isInstalled(_ service: Service) -> Bool {
        let workspace = NSWorkspace.shared
        if bundleIDs[service, default: []].contains(where: { workspace.urlForApplication(withBundleIdentifier: $0) != nil }) {
            return true
        }
        let fileManager = FileManager.default
        let home = fileManager.homeDirectoryForCurrentUser
        let folders = [
            URL(filePath: "/Applications"),
            home.appending(path: "Applications"),
            home.appending(path: "Applications/Chrome Apps.localized"),
            home.appending(path: "Applications/Brave Browser Apps.localized"),
            home.appending(path: "Applications/Edge Apps.localized"),
        ]
        return appNames[service, default: []].contains { name in
            folders.contains { fileManager.fileExists(atPath: $0.appending(path: "\(name).app").path) }
        }
    }
}

// MARK: - Service keys

extension Service {
    /// Every case, in declaration order (UniFFI enums are not `CaseIterable`).
    nonisolated static let allCases: [Service] = [
        .spotify, .appleMusic, .youTubeMusic, .youTube, .deezer, .tidal, .amazonMusic, .soundCloud, .bandcamp, .mixcloud,
        .vimeo, .tikTok, .instagram, .facebook, .x, .reddit, .twitch, .shazam, .genius, .qobuz, .pandora, .audiomack,
        .anghami, .jioSaavn, .boomplay, .kkbox, .yandexMusic, .netEaseMusic, .lastFm, .web,
    ]

    /// Stable name for preferences (never shown).
    nonisolated var storageKey: String {
        switch self {
        case .spotify: "spotify"
        case .appleMusic: "appleMusic"
        case .youTubeMusic: "youTubeMusic"
        case .youTube: "youTube"
        case .deezer: "deezer"
        case .tidal: "tidal"
        case .amazonMusic: "amazonMusic"
        case .soundCloud: "soundCloud"
        case .bandcamp: "bandcamp"
        case .mixcloud: "mixcloud"
        case .vimeo: "vimeo"
        case .tikTok: "tikTok"
        case .instagram: "instagram"
        case .facebook: "facebook"
        case .x: "x"
        case .reddit: "reddit"
        case .twitch: "twitch"
        case .shazam: "shazam"
        case .genius: "genius"
        case .qobuz: "qobuz"
        case .pandora: "pandora"
        case .audiomack: "audiomack"
        case .anghami: "anghami"
        case .jioSaavn: "jioSaavn"
        case .boomplay: "boomplay"
        case .kkbox: "kkbox"
        case .yandexMusic: "yandexMusic"
        case .netEaseMusic: "netEaseMusic"
        case .lastFm: "lastFm"
        case .web: "web"
        }
    }

    nonisolated init?(storageKey: String) {
        guard let service = Self.allCases.first(where: { $0.storageKey == storageKey }) else { return nil }
        self = service
    }
}
