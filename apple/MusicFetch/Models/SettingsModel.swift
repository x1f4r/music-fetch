import AppKit
import Foundation
import MusicFetchCore
import Observation
import ServiceManagement

/// Settings: credentials, melody library, storage and login item (§6.9).
@Observable
final class SettingsModel {
    /// Melody library row states.
    enum MelodyState: Equatable {
        case unknown
        case notInstalled
        case installed
        case downloading(fraction: Double)
        case failed
    }

    // MARK: Credentials

    /// What the fields show; saved to the Keychain on commit.
    var credentials = CredentialFields()
    private(set) var savedCredentials = CredentialFields()
    private(set) var credentialError: String?

    // MARK: Melody library

    private(set) var melody = MelodyIndexStatus(
        installed: false, version: nil, songCount: 0, sizeBytes: 0, availableVersion: nil, downloadBytes: nil)
    private(set) var melodyState: MelodyState = .unknown
    private(set) var isCheckingMelody = false
    /// Size of the published index (from the manifest), for "Not installed · 48 MB".
    private(set) var publishedBytes: UInt64?

    // MARK: Storage

    private(set) var storage: StorageUsage?
    private(set) var isClearingCache = false

    // MARK: Login item

    private(set) var launchAtLogin = false
    private(set) var loginItemError: String?

    @ObservationIgnored private unowned let app: AppModel
    @ObservationIgnored private let keychain = KeychainStore()
    @ObservationIgnored private var install: AnalysisSessionHandle?
    @ObservationIgnored private var installTask: Task<Void, Never>?

    init(app: AppModel) {
        self.app = app
        launchAtLogin = SMAppService.mainApp.status == .enabled
    }

    // MARK: - Credentials

    /// Reads the Keychain (on first use; the engine gets them at launch too).
    func loadCredentials() -> Credentials {
        let fields = app.isDemo ? CredentialFields() : CredentialFields.load(from: keychain)
        credentials = fields
        savedCredentials = fields
        return fields.credentials
    }

    var hasUnsavedCredentials: Bool { credentials != savedCredentials }

    func saveCredentials() {
        guard credentials != savedCredentials else { return }
        do {
            if !app.isDemo { try credentials.save(to: keychain) }
            savedCredentials = credentials
            credentialError = nil
            app.engine?.setCredentials(credentials.credentials)
        } catch {
            credentialError = error.localizedDescription
        }
    }

    func isSet(_ account: CredentialFields.Account) -> Bool {
        !savedCredentials[account].isEmpty
    }

    // MARK: - Melody library

    /// Local status; checks the manifest when nothing is installed so the size is known.
    func refreshMelody() {
        guard let engine = app.engine else { return }
        if case .downloading = melodyState { return }
        melody = engine.melodyIndexStatus()
        melodyState = melody.installed ? .installed : .notInstalled
        if !melody.installed, publishedBytes == nil {
            checkForUpdates(quietly: true)
        }
    }

    var isMelodyInstalled: Bool { melody.installed }

    /// True when a newer index than the installed one is published.
    var hasMelodyUpdate: Bool {
        melody.installed && melody.availableVersion != nil
    }

    func checkForUpdates(quietly: Bool = false) {
        guard let engine = app.engine, !isCheckingMelody else { return }
        isCheckingMelody = true
        Task {
            defer { isCheckingMelody = false }
            do {
                let status = try await offMain { try engine.checkMelodyIndex() }
                melody = status
                if let bytes = status.downloadBytes { publishedBytes = bytes }
                if case .downloading = melodyState { return }
                melodyState = status.installed ? .installed : .notInstalled
            } catch {
                Log.engine.info("Melody manifest check failed: \(String(describing: error), privacy: .public)")
                if !quietly { melodyState = .failed }
            }
        }
    }

    /// Downloads (or updates) the index; progress arrives as `Phase.downloading`.
    func downloadMelody() {
        guard let engine = app.engine else { return }
        if case .downloading = melodyState { return }
        let bridge = ObserverBridge()
        do {
            install = try engine.installMelodyIndex(observer: bridge)
        } catch {
            bridge.close()
            Log.engine.error("Melody download failed to start: \(String(describing: error), privacy: .public)")
            melodyState = .failed
            return
        }
        melodyState = .downloading(fraction: 0)
        installTask = Task { [weak self] in
            for await event in bridge.events {
                guard let self else { return }
                switch event {
                case .phase(.downloading, let fraction):
                    self.melodyState = .downloading(fraction: Double(fraction ?? 0))
                case .finished:
                    self.install = nil
                    self.melody = engine.melodyIndexStatus()
                    self.melodyState = self.melody.installed ? .installed : .failed
                    self.refreshStorage()
                case .failed(let code, _):
                    self.install = nil
                    Log.engine.error("Melody download failed: \(String(describing: code), privacy: .public)")
                    self.melodyState = .failed
                case .canceled:
                    self.install = nil
                    self.melody = engine.melodyIndexStatus()
                    self.melodyState = self.melody.installed ? .installed : .notInstalled
                default:
                    break
                }
            }
        }
    }

    func cancelMelodyDownload() {
        install?.cancel()
    }

    func removeMelody() {
        guard let engine = app.engine else { return }
        do {
            try engine.removeMelodyIndex()
        } catch {
            Log.engine.error("Removing the melody index failed: \(String(describing: error), privacy: .public)")
        }
        melody = engine.melodyIndexStatus()
        melodyState = melody.installed ? .installed : .notInstalled
        refreshStorage()
    }

    /// "48 MB" for prompts: the published size, else the installed size, else nil.
    var melodyDownloadSize: UInt64? {
        melody.downloadBytes ?? publishedBytes ?? (melody.sizeBytes > 0 ? melody.sizeBytes : nil)
    }

    // MARK: - Storage

    func refreshStorage() {
        guard let engine = app.engine else { return }
        Task {
            storage = await offMain { engine.storageUsage() }
        }
    }

    var totalBytes: UInt64 {
        guard let storage else { return 0 }
        return storage.databaseBytes + storage.cacheBytes + storage.melodyIndexBytes
    }

    func clearCache() {
        guard let engine = app.engine, !isClearingCache else { return }
        isClearingCache = true
        Task {
            defer { isClearingCache = false }
            do {
                try await offMain { try engine.clearCache() }
            } catch {
                Log.engine.error("Clearing the cache failed: \(String(describing: error), privacy: .public)")
            }
            storage = await offMain { engine.storageUsage() }
        }
    }

    /// The data folder shown under "Location".
    var dataDirectory: URL? {
        try? EngineHost.directory(.applicationSupportDirectory)
    }

    // MARK: - Login item

    func setLaunchAtLogin(_ enabled: Bool) {
        do {
            if enabled {
                try SMAppService.mainApp.register()
            } else {
                try SMAppService.mainApp.unregister()
            }
            loginItemError = nil
        } catch {
            loginItemError = error.localizedDescription
        }
        launchAtLogin = SMAppService.mainApp.status == .enabled
        if enabled, SMAppService.mainApp.status == .requiresApproval {
            SMAppService.openSystemSettingsLoginItems()
        }
    }
}
