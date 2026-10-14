import Foundation
import MusicFetchCore

/// Builds the engine configuration, owns the platform services and creates the engine
/// off the main thread.
actor EngineHost {
    /// Melody-index manifest published with the app's releases.
    nonisolated static let melodyIndexManifestURL =
        "https://github.com/x1f4r/music-fetch/releases/download/melody-index-1/manifest.json"

    private var engine: (any MusicEngine)?

    /// Creates the engine once; later calls return the same instance.
    func start(credentials: Credentials) throws -> any MusicEngine {
        if let engine { return engine }
        let config = try Self.makeConfig()
        let core = try Engine(
            config: config,
            media: nil,
            decoder: AVFoundationDecoder(),
            speech: SpeechTranscriberService(isEnabled: { Preferences.searchSungLyrics })
        )
        let engine = CoreEngine(engine: core)
        engine.setCredentials(credentials)
        self.engine = engine
        return engine
    }

    /// `EngineConfig` for this Mac: Application Support and Caches directories, the
    /// storefront and language from the current locale, and the bundled media runtime.
    nonisolated static func makeConfig(bundle: Bundle = .main, locale: Locale = .current) throws -> EngineConfig {
        let dataDir = try directory(.applicationSupportDirectory)
        let cacheDir = try directory(.cachesDirectory)
        var mediaTools: MediaToolsConfig?
        do {
            mediaTools = try MediaRuntime.locate(bundle: bundle).mediaToolsConfig()
        } catch {
            // Links that need yt-dlp fail with a clear error; everything else works.
            Log.engine.error("Media runtime unavailable: \(error.localizedDescription, privacy: .public)")
        }
        return EngineConfig(
            dataDir: dataDir.path,
            cacheDir: cacheDir.path,
            country: country(for: locale),
            language: language(for: locale),
            appVersion: appVersion(bundle: bundle),
            mediaTools: mediaTools,
            melodyIndexManifestUrl: melodyIndexManifestURL
        )
    }

    /// `~/Library/Application Support/Music Fetch` (or `Caches/Music Fetch`), created if needed.
    nonisolated static func directory(_ kind: FileManager.SearchPathDirectory) throws -> URL {
        let base = try FileManager.default.url(for: kind, in: .userDomainMask, appropriateFor: nil, create: true)
        let url = base.appending(path: "Music Fetch", directoryHint: .isDirectory)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    /// ISO 3166-1 alpha-2 storefront; "US" when the locale has no region.
    nonisolated static func country(for locale: Locale) -> String {
        guard let region = locale.region?.identifier, region.count == 2, region.allSatisfy(\.isLetter) else { return "US" }
        return region.uppercased()
    }

    /// BCP-47 language tag such as "en-US".
    nonisolated static func language(for locale: Locale) -> String {
        let language = locale.language.languageCode?.identifier ?? "en"
        if let region = locale.region?.identifier, region.count == 2 {
            return "\(language)-\(region.uppercased())"
        }
        return language
    }

    nonisolated static func appVersion(bundle: Bundle) -> String {
        bundle.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String ?? "1.0.0"
    }
}
