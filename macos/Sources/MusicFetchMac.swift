import AppKit
import SwiftUI
import UniformTypeIdentifiers

private let backendCommandDefaultsKey = "musicFetch.backendCommand"
private let recentAnalysesDefaultsKey = "musicFetch.recentAnalyses"
private let openExternalLinksDefaultsKey = "musicFetch.openExternalLinks"
private let languageDefaultsKey = "musicFetch.language"

private func defaultUILanguageCode() -> String {
    let preferred = Locale.preferredLanguages.first?.lowercased() ?? "en"
    if preferred.hasPrefix("de") { return "de" }
    if preferred.hasPrefix("es") { return "es" }
    if preferred.hasPrefix("fr") { return "fr" }
    return "en"
}

enum UILanguage: String, CaseIterable {
    case en
    case de
    case es
    case fr

    var label: String {
        switch self {
        case .en: return "English"
        case .de: return "Deutsch"
        case .es: return "Español"
        case .fr: return "Français"
        }
    }
}

private func loc(_ code: String, _ en: String, _ de: String, _ es: String, _ fr: String) -> String {
    switch UILanguage(rawValue: code) ?? .en {
    case .en: return en
    case .de: return de
    case .es: return es
    case .fr: return fr
    }
}

extension Notification.Name {
    static let musicFetchAnalyze = Notification.Name("musicFetch.analyze")
    static let musicFetchFocusInput = Notification.Name("musicFetch.focusInput")
}

struct DoctorCheck: Codable, Identifiable {
    var id: String { name }
    let name: String
    let ok: Bool
    let detail: String
}

struct TrackMatchPayload: Codable, Hashable {
    let title: String
    let artist: String?
    let album: String?
    let isrc: String?
    let provider_ids: [String: String]
    let external_links: [String: String]
}

struct SegmentPayload: Codable, Identifiable, Hashable {
    let source_item_id: String
    let start_ms: Int
    let end_ms: Int
    let kind: String?
    let confidence: Double
    let providers: [String]
    let evidence_count: Int
    let track: TrackMatchPayload?
    let alternates: [TrackMatchPayload]
    let repeat_group_id: String?
    let probe_count: Int?
    let provider_attempts: Int?
    let metadata_hints: [String]?
    var id: String { "\(source_item_id)-\(start_ms)-\(end_ms)-\(track?.title ?? kind ?? "segment")" }
}

struct ItemPayload: Codable, Identifiable {
    let id: String
    let input_value: String
    let status: String
    let error: String?
}

struct JobPayload: Codable, Hashable {
    let id: String
    let status: String
    let created_at: String
    let updated_at: String
}

struct JobEventPayload: Codable, Hashable, Identifiable {
    let id: Int
    let job_id: String
    let level: String
    let message: String
    let created_at: String
}

struct AnalyzeResponse: Codable {
    let job: JobPayload
    let items: [ItemPayload]
    let segments: [SegmentPayload]
    let events: [JobEventPayload]?
}

struct InstallResponse: Codable {
    let installed: [String]
    let skipped: [String]
    let failed: [String]
    let checks: [DoctorCheck]
}

struct LibraryEntryPayload: Codable, Identifiable, Hashable {
    let job_id: String
    let title: String
    let input_value: String
    let status: String
    let created_at: String
    let updated_at: String
    let item_count: Int
    let segment_count: Int
    let matched_count: Int
    let pinned: Bool
    let artifact_size_bytes: Int

    var id: String { job_id }
}

struct ArtifactCategorySummaryPayload: Codable, Hashable {
    let category: String
    let count: Int
    let size_bytes: Int
}

struct ArtifactEntryPayload: Codable, Identifiable, Hashable {
    let id: String
    let category: String
    let label: String
    let path: String
    let size_bytes: Int
    let exists: Bool
    let temporary: Bool
    let job_id: String?
    let source_item_id: String?
    let pinned: Bool
}

struct StorageSummaryPayload: Codable, Hashable {
    let job_id: String?
    let auto_clean: Bool
    let total_size_bytes: Int
    let categories: [ArtifactCategorySummaryPayload]
    let entries: [ArtifactEntryPayload]
    let locations: [String: String]
}

enum WorkspaceSection: String, CaseIterable, Identifiable {
    case analyze
    case library
    case storage
    case settings

    var id: String { rawValue }

    var title: String {
        switch self {
        case .analyze:
            return "Analyze"
        case .library:
            return "Library"
        case .storage:
            return "Storage"
        case .settings:
            return "Settings"
        }
    }

    var icon: String {
        switch self {
        case .analyze:
            return "waveform.and.magnifyingglass"
        case .library:
            return "books.vertical"
        case .storage:
            return "internaldrive"
        case .settings:
            return "gearshape"
        }
    }
}

struct PlatformLink: Identifiable, Hashable {
    let id: String
    let label: String
    let icon: String
    let color: Color
    let url: URL
}

struct RecentAnalysis: Codable, Identifiable, Hashable {
    let id: String
    let input: String
    let title: String
    let status: String
    let segmentCount: Int
    let createdAt: Date
}

enum AppViewState: Equatable {
    case idle
    case analyzing(String)
    case recordingMic
    case recordingSystem
    case showingResults
    case error(String)
}

struct SegmentViewModel: Identifiable, Hashable {
    let id: String
    let payload: SegmentPayload
    let title: String
    let subtitle: String
    let detailLabel: String
    let qualityLabel: String?
    let accentColor: Color
    let timelineColor: Color
    let statusBadge: String?
    let metadataHint: String?
    let primaryLinks: [PlatformLink]
    let overflowLinks: [PlatformLink]
    let isInteractive: Bool
    let repeatGroupID: String?

    var startMs: Int { payload.start_ms }
    var endMs: Int { payload.end_ms }
}

@MainActor
final class AppModel: ObservableObject {
    @Published var backendCommand: String
    @Published var inputValue = ""
    @Published var providerChecks: [DoctorCheck] = []
    @Published var result: AnalyzeResponse?
    @Published var recentAnalyses: [RecentAnalysis] = []
    @Published var libraryEntries: [LibraryEntryPayload] = []
    @Published var storageSummary: StorageSummaryPayload?
    @Published var viewState: AppViewState = .idle
    @Published var selectedSegmentID: String?
    @Published var selectedWorkspace: WorkspaceSection = .analyze
    @Published var selectedLibraryJobID: String?
    @Published var selectedStorageJobID: String?
    @Published var languageCode: String

    private let microphoneRecorder = MicrophoneRecorder()
    private let systemAudioRecorder = SystemAudioRecorder()
    private var analysisPhaseTask: Task<Void, Never>?
    private var pollingTask: Task<Void, Never>?
    private var cachedResults: [String: AnalyzeResponse] = [:]
    private var openedPrimaryLinkJobIDs: Set<String> = []

    init() {
        let savedBackend = UserDefaults.standard.string(forKey: backendCommandDefaultsKey)
        self.backendCommand = savedBackend?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false ? savedBackend! : Self.defaultBackendCommand()
        self.recentAnalyses = Self.loadRecentAnalyses()
        self.languageCode = UserDefaults.standard.string(forKey: languageDefaultsKey) ?? defaultUILanguageCode()
    }

    var isBusy: Bool {
        switch viewState {
        case .recordingMic, .recordingSystem:
            return true
        default:
            return false
        }
    }

    var statusTitle: String {
        switch viewState {
        case .idle:
            return loc(languageCode, "Find music", "Musik finden", "Buscar música", "Trouver la musique")
        case .recordingMic:
            return loc(languageCode, "Mic on", "Mikro an", "Micrófono activo", "Micro activé")
        case .recordingSystem:
            return loc(languageCode, "System on", "System an", "Sistema activo", "Système actif")
        case let .analyzing(phase):
            return phase
        case .showingResults:
            return loc(languageCode, "Done", "Fertig", "Listo", "Terminé")
        case let .error(message):
            return message
        }
    }

    func setLanguage(_ code: String) {
        languageCode = code
        UserDefaults.standard.set(code, forKey: languageDefaultsKey)
    }

    var timelineModels: [SegmentViewModel] {
        guard let result else { return [] }
        return result.segments.map(makeSegmentViewModel)
    }

    var selectedSegment: SegmentViewModel? {
        let models = timelineModels
        if let selectedSegmentID, let match = models.first(where: { $0.id == selectedSegmentID }) {
            return match
        }
        return models.first
    }

    func updateBackendCommand(_ value: String) {
        backendCommand = value
        UserDefaults.standard.set(value, forKey: backendCommandDefaultsKey)
    }

    func refreshDoctor() {
        Task {
            do {
                providerChecks = try await runJSON(arguments: ["doctor", "--json"], as: [DoctorCheck].self)
            } catch {
                viewState = .error(loc(languageCode, "Diagnostics failed", "Diagnose fehlgeschlagen", "Falló el diagnóstico", "Échec du diagnostic"))
            }
        }
    }

    func bootstrap() {
        Task {
            await refreshLibrary()
            await refreshStorage()
        }
        startPolling()
    }

    func analyze() {
        let trimmed = inputValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        guard currentRecordingState == nil else { return }
        Task {
            do {
                let response = try await runJSON(arguments: ["submit", trimmed, "--json"], as: AnalyzeResponse.self)
                inputValue = ""
                result = response
                selectedSegmentID = response.segments.first?.id
                cache(response: response, source: trimmed)
                selectedLibraryJobID = response.job.id
                selectedStorageJobID = response.job.id
                selectedWorkspace = .analyze
                updateViewState(for: response)
                await refreshLibrary()
                _ = try await refreshJobSnapshot(response.job.id)
                await refreshStorage(jobID: response.job.id)
            } catch {
                viewState = .error(loc(languageCode, "Analysis failed", "Analyse fehlgeschlagen", "Falló el análisis", "Échec de l’analyse"))
            }
        }
    }

    func chooseFile() {
        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = false
        panel.allowedContentTypes = [
            UTType.audio,
            UTType.movie,
            UTType.mpeg4Movie,
            UTType.quickTimeMovie,
            UTType(filenameExtension: "wav"),
            UTType(filenameExtension: "mp3"),
            UTType(filenameExtension: "flac"),
            UTType(filenameExtension: "m4a")
        ].compactMap { $0 }
        if panel.runModal() == .OK, let url = panel.url {
            inputValue = url.path
        }
    }

    func restoreRecent(_ analysis: RecentAnalysis) {
        inputValue = analysis.input
        if let cached = cachedResults[analysis.id] {
            result = cached
            selectedSegmentID = cached.segments.first?.id
            viewState = .showingResults
        }
    }

    func startNewAnalysis() {
        inputValue = ""
        result = nil
        selectedSegmentID = nil
        viewState = .idle
        selectedWorkspace = .analyze
    }

    func loadLibraryJob(_ jobID: String) {
        Task {
            do {
                let response = try await refreshJobSnapshot(jobID)
                selectedLibraryJobID = jobID
                selectedStorageJobID = jobID
                cache(response: response, source: response.items.first?.input_value ?? jobID)
                await refreshStorage(jobID: jobID)
            } catch {
                viewState = .error(loc(languageCode, "Could not load analysis", "Analyse konnte nicht geladen werden", "No se pudo cargar el análisis", "Impossible de charger l’analyse"))
            }
        }
    }

    func refreshLibrary() async {
        do {
            libraryEntries = try await runJSON(arguments: ["library", "--limit", "60", "--json"], as: [LibraryEntryPayload].self)
            if selectedLibraryJobID == nil {
                selectedLibraryJobID = libraryEntries.first?.job_id
            }
        } catch {
            libraryEntries = []
        }
    }

    func refreshStorage(jobID: String? = nil) async {
        do {
            var args = ["storage", "summary", "--json"]
            if let jobID {
                args += ["--job-id", jobID]
            }
            storageSummary = try await runJSON(arguments: args, as: StorageSummaryPayload.self)
        } catch {
            storageSummary = nil
        }
    }

    func selectStorageScope(_ jobID: String?) {
        selectedStorageJobID = jobID
        Task {
            await refreshStorage(jobID: jobID)
        }
    }

    func cleanupArtifacts(jobID: String? = nil) {
        viewState = .analyzing("Temporäre Dateien werden entfernt")
        Task {
            defer {
                if case .analyzing = viewState {
                    viewState = .idle
                }
            }
            do {
                var args = ["storage", "cleanup", "--json"]
                if let jobID {
                    args += ["--job-id", jobID]
                }
                let summary = try await runJSON(arguments: args, as: StorageSummaryPayload.self)
                storageSummary = summary
                await refreshLibrary()
                if let jobID, result?.job.id == jobID {
                    await refreshStorage(jobID: jobID)
                }
                viewState = .idle
            } catch {
                viewState = .error(loc(languageCode, "Cleanup failed", "Cleanup fehlgeschlagen", "Falló la limpieza", "Échec du nettoyage"))
            }
        }
    }

    func setPinned(jobID: String, pinned: Bool) {
        Task {
            do {
                struct PinResponse: Codable { let job_id: String; let pinned: Bool }
                _ = try await runJSON(arguments: ["storage", "pin", jobID, pinned ? "--pinned" : "--unpinned", "--json"], as: PinResponse.self)
                await refreshLibrary()
                await refreshStorage(jobID: selectedStorageJobID)
            } catch {
                viewState = .error(loc(languageCode, "Pin failed", "Pinnen fehlgeschlagen", "Falló el anclaje", "Échec de l’épinglage"))
            }
        }
    }

    func reveal(_ path: String) {
        NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: path)])
    }

    func selectSegment(_ segmentID: String) {
        selectedSegmentID = segmentID
    }

    func installMissingCoreDependencies() {
        viewState = .analyzing(loc(languageCode, "Installing tools", "Werkzeuge werden eingerichtet", "Instalando herramientas", "Installation des outils"))
        Task {
            defer {
                if case .analyzing = viewState {
                    viewState = .idle
                }
            }
            do {
                let response = try await runJSON(arguments: ["install-deps", "--json"], as: InstallResponse.self)
                providerChecks = response.checks
                viewState = .idle
            } catch {
                viewState = .error(loc(languageCode, "Install failed", "Installation fehlgeschlagen", "Falló la instalación", "Échec de l’installation"))
            }
        }
    }

    func toggleMicrophoneRecording() {
        if currentRecordingState == .recordingMic {
            stopMicrophoneRecording()
            return
        }
        guard currentRecordingState == nil else { return }
        Task {
            do {
                _ = try await microphoneRecorder.start()
                viewState = .recordingMic
            } catch {
                viewState = .error(error.localizedDescription)
            }
        }
    }

    func toggleSystemAudioRecording() {
        if currentRecordingState == .recordingSystem {
            stopSystemAudioRecording()
            return
        }
        guard currentRecordingState == nil else { return }
        Task {
            do {
                _ = try await systemAudioRecorder.start()
                viewState = .recordingSystem
            } catch {
                viewState = .error(error.localizedDescription)
            }
        }
    }

    func copy(_ value: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(value, forType: .string)
    }

    private var currentRecordingState: AppViewState? {
        switch viewState {
        case .recordingMic:
            return .recordingMic
        case .recordingSystem:
            return .recordingSystem
        default:
            return nil
        }
    }

    private func stopMicrophoneRecording() {
        guard let url = microphoneRecorder.stop() else {
            viewState = .idle
            return
        }
        inputValue = url.path
        analyze()
    }

    private func stopSystemAudioRecording() {
        Task {
            do {
                let url = try await systemAudioRecorder.stop()
                if let url {
                    inputValue = url.path
                    analyze()
                } else {
                    viewState = .idle
                }
            } catch {
                viewState = .error(error.localizedDescription)
            }
        }
    }

    private func startAnalysisPhases() {
        analysisPhaseTask?.cancel()
        let phases = [
            loc(languageCode, "Preparing audio", "Audio wird vorbereitet", "Preparando audio", "Préparation audio"),
            loc(languageCode, "Matching music", "Musik wird erkannt", "Detectando música", "Identification de la musique"),
            loc(languageCode, "Building timeline", "Timeline wird aufgebaut", "Creando línea de tiempo", "Création de la timeline"),
        ]
        viewState = .analyzing(phases[0])
        analysisPhaseTask = Task { [weak self] in
            var index = 0
            while !Task.isCancelled {
                try? await Task.sleep(for: .seconds(1.4))
                guard let self else { return }
                index = (index + 1) % phases.count
                if case .analyzing = self.viewState {
                    self.viewState = .analyzing(phases[index])
                } else {
                    return
                }
            }
        }
    }

    private func stopAnalysisPhases() {
        analysisPhaseTask?.cancel()
        analysisPhaseTask = nil
    }

    private func startPolling() {
        pollingTask?.cancel()
        pollingTask = Task { [weak self] in
            while let self, !Task.isCancelled {
                await self.pollLiveState()
                try? await Task.sleep(for: .seconds(2))
            }
        }
    }

    private func pollLiveState() async {
        await refreshLibrary()
        if selectedWorkspace == .storage {
            await refreshStorage(jobID: selectedStorageJobID)
        }
        let jobID: String?
        switch selectedWorkspace {
        case .analyze:
            jobID = result?.job.id
        case .library:
            jobID = selectedLibraryJobID
        case .storage:
            jobID = selectedStorageJobID
        case .settings:
            jobID = nil
        }
        guard let jobID else { return }
        guard let job = libraryEntries.first(where: { $0.job_id == jobID }) else { return }
        if ["queued", "running"].contains(job.status) || result?.job.id == jobID {
            _ = try? await refreshJobSnapshot(jobID)
        }
    }

    @discardableResult
    private func refreshJobSnapshot(_ jobID: String) async throws -> AnalyzeResponse {
        let response = try await runJSON(arguments: ["job", jobID, "--json"], as: AnalyzeResponse.self)
        result = response
        if selectedSegmentID == nil || !response.segments.contains(where: { $0.id == selectedSegmentID }) {
            selectedSegmentID = response.segments.first?.id
        }
        updateViewState(for: response)
        if response.job.status == "succeeded" || response.job.status == "partial_failed" {
            maybeOpenPrimaryLinkIfEnabled(response)
        }
        return response
    }

    private func updateViewState(for response: AnalyzeResponse) {
        switch response.job.status {
        case "queued":
            viewState = .analyzing(response.events?.last?.message ?? loc(languageCode, "Queued", "In Warteschlange", "En cola", "En file"))
        case "running":
            viewState = .analyzing(response.events?.last?.message ?? loc(languageCode, "Working", "Läuft", "Procesando", "En cours"))
        case "failed":
            viewState = .error(response.events?.last?.message ?? loc(languageCode, "Analysis failed", "Analyse fehlgeschlagen", "Falló el análisis", "Échec de l’analyse"))
        default:
            viewState = .showingResults
        }
    }

    private func cache(response: AnalyzeResponse, source: String) {
        cachedResults[response.job.id] = response
        let title = response.items.first?.input_value ?? source
        let entry = RecentAnalysis(
            id: response.job.id,
            input: source,
            title: title,
            status: response.job.status,
            segmentCount: response.segments.count,
            createdAt: Date(),
        )
        recentAnalyses.removeAll { $0.id == entry.id || $0.input == entry.input }
        recentAnalyses.insert(entry, at: 0)
        recentAnalyses = Array(recentAnalyses.prefix(8))
        Self.saveRecentAnalyses(recentAnalyses)
    }

    private func maybeOpenPrimaryLinkIfEnabled(_ response: AnalyzeResponse) {
        guard UserDefaults.standard.bool(forKey: openExternalLinksDefaultsKey) else { return }
        guard !openedPrimaryLinkJobIDs.contains(response.job.id) else { return }
        guard let first = response.segments.first(where: { $0.track != nil }),
              let urlString = first.track?.external_links["spotify"] ?? first.track?.external_links["apple_music"] ?? first.track?.external_links["youtube_music"],
              let url = URL(string: urlString) else { return }
        openedPrimaryLinkJobIDs.insert(response.job.id)
        NSWorkspace.shared.open(url)
    }

    private func runProcess(arguments: [String]) async throws -> Data {
        let command = backendCommand.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !command.isEmpty else {
            throw NSError(domain: "MusicFetchMac", code: 1, userInfo: [NSLocalizedDescriptionKey: "Backend command is empty"])
        }
        return try await withCheckedThrowingContinuation { continuation in
            let process = Process()
            if command.contains("/") || command.hasPrefix("~") {
                let expanded = NSString(string: command).expandingTildeInPath
                process.executableURL = URL(fileURLWithPath: expanded)
                process.arguments = arguments
            } else {
                process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
                process.arguments = [command] + arguments
            }
            process.currentDirectoryURL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
            var environment = ProcessInfo.processInfo.environment
            let backendDir: String? = {
                let command = backendCommand.trimmingCharacters(in: .whitespacesAndNewlines)
                if command.contains("/") || command.hasPrefix("~") {
                    let expanded = NSString(string: command).expandingTildeInPath
                    return URL(fileURLWithPath: expanded).deletingLastPathComponent().path
                }
                return nil
            }()
            let pathParts = [
                backendDir,
                "/opt/homebrew/bin",
                "/opt/homebrew/sbin",
                "/usr/local/bin",
                "/usr/local/sbin",
                environment["PATH"]
            ].compactMap { $0 }.filter { !$0.isEmpty }
            environment["PATH"] = pathParts.joined(separator: ":")
            process.environment = environment
            let stdout = Pipe()
            let stderr = Pipe()
            process.standardOutput = stdout
            process.standardError = stderr
            process.terminationHandler = { task in
                let data = stdout.fileHandleForReading.readDataToEndOfFile()
                let errorData = stderr.fileHandleForReading.readDataToEndOfFile()
                if task.terminationStatus == 0 {
                    continuation.resume(returning: data)
                } else {
                    let message = String(data: errorData, encoding: .utf8) ?? "Unknown process error"
                    continuation.resume(throwing: NSError(domain: "MusicFetchMac", code: Int(task.terminationStatus), userInfo: [NSLocalizedDescriptionKey: message]))
                }
            }
            do {
                try process.run()
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    private func runJSON<T: Decodable>(arguments: [String], as type: T.Type) async throws -> T {
        let data = try await runProcess(arguments: arguments)
        return try JSONDecoder().decode(T.self, from: data)
    }

    private static func defaultBackendCommand() -> String {
        if
            let resourceURL = Bundle.main.url(forResource: "backend-command", withExtension: "txt", subdirectory: "Resources"),
            let value = try? String(contentsOf: resourceURL).trimmingCharacters(in: .whitespacesAndNewlines),
            !value.isEmpty
        {
            return value
        }

        let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
        let repoCandidate = cwd.appendingPathComponent(".venv/bin/music-fetch").path
        if FileManager.default.isExecutableFile(atPath: repoCandidate) {
            return repoCandidate
        }
        return "/usr/local/bin/music-fetch"
    }

    private static func loadRecentAnalyses() -> [RecentAnalysis] {
        guard let data = UserDefaults.standard.data(forKey: recentAnalysesDefaultsKey) else { return [] }
        return (try? JSONDecoder().decode([RecentAnalysis].self, from: data)) ?? []
    }

    private static func saveRecentAnalyses(_ analyses: [RecentAnalysis]) {
        if let data = try? JSONEncoder().encode(analyses) {
            UserDefaults.standard.set(data, forKey: recentAnalysesDefaultsKey)
        }
    }

    func makeSegmentViewModel(from payload: SegmentPayload) -> SegmentViewModel {
        let label = (payload.kind ?? "matched_track")
        let quality: String?
        switch payload.confidence {
        case 0.85...:
            quality = loc(languageCode, "High", "Sicher", "Alta", "Élevée")
        case 0.65..<0.85:
            quality = loc(languageCode, "Good", "Wahrscheinlich", "Buena", "Bonne")
        case 0.01..<0.65:
            quality = loc(languageCode, "Low", "Unsicher", "Baja", "Faible")
        default:
            quality = nil
        }

        let primaryLinks: [PlatformLink]
        let overflowLinks: [PlatformLink]
        if let track = payload.track {
            let links = Self.platformLinks(for: track)
            primaryLinks = Array(links.prefix(3))
            overflowLinks = Array(links.dropFirst(3))
        } else {
            primaryLinks = []
            overflowLinks = []
        }

        let accent: Color
        let timeline: Color
        let title: String
        let subtitle: String
        let detail: String

        switch label {
        case "speech_only":
            accent = .gray
            timeline = .gray.opacity(0.65)
            title = loc(languageCode, "Speech only", "Sprache / kein Song", "Solo voz", "Voix seule")
            subtitle = loc(languageCode, "No song here.", "Hier ist kein Song.", "Sin canción.", "Pas de musique.")
            detail = loc(languageCode, "Speech", "Kein Song erkannt", "Voz", "Voix")
        case "music_unresolved":
            accent = .orange
            timeline = .orange.opacity(0.75)
            title = payload.track.map { Self.displayTitle(for: $0) } ?? loc(languageCode, "Music found", "Musik erkannt", "Música detectada", "Musique détectée")
            subtitle = payload.track?.album ?? loc(languageCode, "No solid match.", "Nicht sicher zugeordnet.", "Sin coincidencia firme.", "Pas de correspondance sûre.")
            detail = loc(languageCode, "Unresolved", "Nicht sicher zugeordnet", "Sin resolver", "Non résolu")
        case "silence_or_fx":
            accent = .secondary
            timeline = .secondary.opacity(0.28)
            title = loc(languageCode, "No audio", "Kein relevanter Audioinhalt", "Sin audio útil", "Pas d’audio utile")
            subtitle = loc(languageCode, "Silence or FX.", "Stille oder Effekte.", "Silencio o FX.", "Silence ou FX.")
            detail = loc(languageCode, "Silence", "Kein Audioinhalt", "Silencio", "Silence")
        default:
            accent = .accentColor
            timeline = .accentColor.opacity(0.85)
            title = payload.track.map { Self.displayTitle(for: $0) } ?? loc(languageCode, "Song found", "Song erkannt", "Canción detectada", "Titre détecté")
            subtitle = payload.track?.album ?? loc(languageCode, "Matched song", "Song erkannt", "Canción detectada", "Titre détecté")
            detail = loc(languageCode, "Matched", "Song erkannt", "Coincidencia", "Correspondance")
        }

        return SegmentViewModel(
            id: payload.id,
            payload: payload,
            title: title,
            subtitle: subtitle,
            detailLabel: detail,
            qualityLabel: quality,
            accentColor: accent,
            timelineColor: timeline,
            statusBadge: payload.repeat_group_id == nil ? nil : loc(languageCode, "Repeat", "Wiederholt", "Repite", "Répété"),
            metadataHint: payload.metadata_hints?.first,
            primaryLinks: primaryLinks,
            overflowLinks: overflowLinks,
            isInteractive: payload.track != nil,
            repeatGroupID: payload.repeat_group_id
        )
    }

    private static func displayTitle(for track: TrackMatchPayload) -> String {
        if let artist = track.artist?.trimmingCharacters(in: .whitespacesAndNewlines), !artist.isEmpty {
            return "\(artist) - \(track.title)"
        }
        return track.title
    }

    private static func platformLinks(for track: TrackMatchPayload) -> [PlatformLink] {
        let defs: [(String, String, String, Color)] = [
            ("spotify", "Spotify", "music.note.list", .green),
            ("youtube_music", "YouTube Music", "play.circle.fill", .red),
            ("apple_music", "Apple Music", "apple.logo", .pink),
            ("deezer", "Deezer", "waveform", .purple),
            ("shazam", "Shazam", "magnifyingglass.circle.fill", .blue),
        ]
        return defs.compactMap { key, label, icon, color in
            guard let raw = track.external_links[key], let url = URL(string: raw), !raw.isEmpty else {
                return nil
            }
            return PlatformLink(id: key, label: label, icon: icon, color: color, url: url)
        }
    }
}

struct ContentView: View {
    @ObservedObject var model: AppModel
    @State private var columnVisibility: NavigationSplitViewVisibility = .all

    var body: some View {
        NavigationSplitView(columnVisibility: $columnVisibility) {
            SidebarView(model: model)
                .navigationSplitViewColumnWidth(min: 220, ideal: 250, max: 300)
        } detail: {
            WorkspaceDetailView(model: model)
        }
        .navigationSplitViewStyle(.balanced)
        .background(
            LinearGradient(
                colors: [Color(nsColor: .windowBackgroundColor), Color(nsColor: .underPageBackgroundColor)],
                startPoint: .top,
                endPoint: .bottom
            )
        )
        .task {
            model.bootstrap()
        }
    }
}

struct SidebarView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            VStack(alignment: .leading, spacing: 14) {
                Button {
                    model.startNewAnalysis()
                } label: {
                    Label(loc(model.languageCode, "New", "Neu", "Nuevo", "Nouveau"), systemImage: "plus.circle.fill")
                        .font(.headline.weight(.semibold))
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                VStack(alignment: .leading, spacing: 4) {
                    ForEach(WorkspaceSection.allCases) { section in
                        Button {
                            model.selectedWorkspace = section
                            if section == .library, let jobID = model.selectedLibraryJobID ?? model.libraryEntries.first?.job_id {
                                model.selectedLibraryJobID = jobID
                                model.loadLibraryJob(jobID)
                            }
                            if section == .storage {
                                model.selectStorageScope(model.selectedStorageJobID)
                            }
                        } label: {
                            HStack(spacing: 12) {
                                Image(systemName: section.icon)
                                    .frame(width: 18)
                                Text(loc(model.languageCode, section.title, section.title == "Analyze" ? "Analysieren" : section.title == "Library" ? "Bibliothek" : section.title == "Storage" ? "Speicher" : "Einstellungen", section.title == "Analyze" ? "Analizar" : section.title == "Library" ? "Biblioteca" : section.title == "Storage" ? "Almacenamiento" : "Ajustes", section.title == "Analyze" ? "Analyser" : section.title == "Library" ? "Bibliothèque" : section.title == "Storage" ? "Stockage" : "Réglages"))
                                Spacer()
                            }
                            .padding(.horizontal, 12)
                            .padding(.vertical, 10)
                        }
                        .buttonStyle(SidebarRowButtonStyle(isSelected: model.selectedWorkspace == section))
                    }
                }
            }
            .padding(18)

            Divider()
                .padding(.horizontal, 12)

            Group {
                switch model.selectedWorkspace {
                case .library:
                    SidebarLibraryPanel(model: model)
                case .storage:
                    SidebarStoragePanel(model: model)
                case .analyze, .settings:
                    Spacer(minLength: 0)
                }
            }
        }
        .background(.regularMaterial)
    }
}

struct SidebarLibraryPanel: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 8) {
                Text(loc(model.languageCode, "Library", "Bibliothek", "Biblioteca", "Bibliothèque"))
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.secondary)
                Spacer()
                Button(loc(model.languageCode, "Refresh", "Aktualisieren", "Actualizar", "Actualiser")) {
                    Task { await model.refreshLibrary() }
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
            }

            ScrollView {
                LazyVStack(spacing: 8) {
                    ForEach(model.libraryEntries) { entry in
                        Button {
                            model.selectedLibraryJobID = entry.job_id
                            model.loadLibraryJob(entry.job_id)
                        } label: {
                            LibraryEntryRowView(entry: entry, isSelected: model.selectedLibraryJobID == entry.job_id)
                        }
                        .buttonStyle(.plain)
                        .contextMenu {
                            Button(entry.pinned ? loc(model.languageCode, "Unpin", "Lösen", "Desfijar", "Détacher") : loc(model.languageCode, "Pin", "Pinnen", "Fijar", "Épingler")) {
                                model.setPinned(jobID: entry.job_id, pinned: !entry.pinned)
                            }
                            Button(loc(model.languageCode, "Open in Storage", "Im Speicher öffnen", "Abrir en almacenamiento", "Ouvrir dans stockage")) {
                                model.selectedWorkspace = .storage
                                model.selectStorageScope(entry.job_id)
                            }
                        }
                    }
                }
                .padding(.vertical, 2)
            }
        }
        .padding(18)
    }
}

struct SidebarStoragePanel: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 8) {
                Text(loc(model.languageCode, "Storage", "Speicher", "Almacenamiento", "Stockage"))
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.secondary)
                Spacer()
            }

            Button {
                model.selectStorageScope(nil)
            } label: {
                StorageScopeRow(
                    title: loc(model.languageCode, "All temp files", "Alle Temp-Dateien", "Todos los temporales", "Tous les temporaires"),
                    subtitle: loc(model.languageCode, "Across all runs", "Über alle Läufe", "En todos los análisis", "Sur tous les runs"),
                    isSelected: model.selectedStorageJobID == nil
                )
            }
            .buttonStyle(.plain)

            ScrollView {
                LazyVStack(spacing: 8) {
                    ForEach(model.libraryEntries) { entry in
                        Button {
                            model.selectStorageScope(entry.job_id)
                        } label: {
                            StorageScopeRow(
                                title: entry.title,
                                subtitle: "\(formatBytes(entry.artifact_size_bytes)) • \(entry.pinned ? loc(model.languageCode, "Pinned", "Gepinnt", "Fijado", "Épinglé") : loc(model.languageCode, "Temp", "Temporär", "Temp.", "Temp."))",
                                isSelected: model.selectedStorageJobID == entry.job_id
                            )
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.vertical, 2)
            }
        }
        .padding(18)
    }

    private func formatBytes(_ size: Int) -> String {
        ByteCountFormatter.string(fromByteCount: Int64(size), countStyle: .file)
    }
}

struct SidebarRowButtonStyle: ButtonStyle {
    let isSelected: Bool

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .background(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .fill(isSelected ? Color.accentColor.opacity(configuration.isPressed ? 0.18 : 0.14) : Color.primary.opacity(configuration.isPressed ? 0.06 : 0.03))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .strokeBorder(isSelected ? Color.accentColor.opacity(0.25) : Color.clear, lineWidth: 1)
            )
    }
}

struct WorkspaceDetailView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        Group {
            switch model.selectedWorkspace {
            case .analyze:
                AnalyzeWorkspaceView(model: model)
            case .library:
                LibraryWorkspaceView(model: model)
            case .storage:
                StorageWorkspaceView(model: model)
            case .settings:
                SettingsWorkspaceView(model: model)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding(18)
    }
}

struct SearchHeroView: View {
    @ObservedObject var model: AppModel
    @FocusState private var searchFocused: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            VStack(alignment: .leading, spacing: 6) {
                Text(loc(model.languageCode, "Find music", "Musik finden", "Buscar música", "Trouver la musique"))
                    .font(.system(size: 30, weight: .semibold, design: .rounded))
                Text(loc(model.languageCode, "Paste a link or choose a file.", "Link einfügen oder Datei wählen.", "Pega un enlace o elige un archivo.", "Collez un lien ou choisissez un fichier."))
                    .font(.system(size: 14))
                    .foregroundStyle(.secondary)
            }

            HStack(alignment: .top, spacing: 14) {
                TextField(loc(model.languageCode, "Link or local file", "Link oder lokale Datei", "Enlace o archivo local", "Lien ou fichier local"), text: $model.inputValue, axis: .vertical)
                    .textFieldStyle(.roundedBorder)
                    .font(.system(size: 16))
                    .focused($searchFocused)

                Button(loc(model.languageCode, "Analyze", "Analysieren", "Analizar", "Analyser")) { model.analyze() }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.large)
                    .keyboardShortcut(.return, modifiers: [.command])
                    .disabled(model.isBusy)
            }

            HStack(spacing: 10) {
                Button(loc(model.languageCode, "File", "Datei", "Archivo", "Fichier")) { model.chooseFile() }
                    .buttonStyle(.bordered)
                    .controlSize(.large)
                    .disabled(model.isBusy)

                RecordingButton(
                    title: model.viewState == .recordingMic ? loc(model.languageCode, "Stop Mic", "Mikro stoppen", "Detener mic", "Arrêter micro") : loc(model.languageCode, "Mic", "Mikro", "Mic", "Micro"),
                    systemImage: "mic.fill",
                    isActive: model.viewState == .recordingMic,
                    action: model.toggleMicrophoneRecording
                )
                RecordingButton(
                    title: model.viewState == .recordingSystem ? loc(model.languageCode, "Stop Sys", "System stoppen", "Detener sist.", "Arrêter sys.") : loc(model.languageCode, "System", "System", "Sistema", "Système"),
                    systemImage: "waveform",
                    isActive: model.viewState == .recordingSystem,
                    action: model.toggleSystemAudioRecording
                )
            }

            StatusBannerView(state: model.viewState)
        }
        .padding(22)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .strokeBorder(Color.primary.opacity(0.06))
        )
        .onReceive(NotificationCenter.default.publisher(for: .musicFetchAnalyze)) { _ in
            model.analyze()
        }
        .onReceive(NotificationCenter.default.publisher(for: .musicFetchFocusInput)) { _ in
            searchFocused = true
        }
    }
}

struct AnalyzeWorkspaceView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            SearchHeroView(model: model)
            ResultsSectionView(model: model)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }
}

struct LibraryWorkspaceView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            if let selected = model.libraryEntries.first(where: { $0.job_id == model.selectedLibraryJobID }) {
                HStack {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(selected.title)
                            .font(.title2.weight(.semibold))
                        Text(selected.input_value)
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    Spacer()
                    Button(selected.pinned ? loc(model.languageCode, "Unpin", "Lösen", "Desfijar", "Détacher") : loc(model.languageCode, "Pin", "Pinnen", "Fijar", "Épingler")) {
                        model.setPinned(jobID: selected.job_id, pinned: !selected.pinned)
                    }
                    .buttonStyle(.bordered)
                }
            } else {
                HStack {
                    Text(loc(model.languageCode, "Library", "Bibliothek", "Biblioteca", "Bibliothèque"))
                        .font(.title2.weight(.semibold))
                    Spacer()
                }
            }
            ResultsSectionView(model: model)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .padding(20)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }
}

struct StorageWorkspaceView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text(model.selectedStorageJobID == nil ? loc(model.languageCode, "Temp storage", "Temp-Speicher", "Almacén temporal", "Stockage temporaire") : loc(model.languageCode, "Job artifacts", "Job-Artefakte", "Artefactos del análisis", "Artefacts du job"))
                        .font(.title2.weight(.semibold))
                    Text(model.storageSummary?.auto_clean == true ? loc(model.languageCode, "Auto-clean on.", "Auto-Clean an.", "Auto-limpieza activa.", "Nettoyage auto activé.") : loc(model.languageCode, "Files kept.", "Dateien bleiben.", "Se guardan archivos.", "Fichiers conservés."))
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button(loc(model.languageCode, "Clean All", "Alles löschen", "Limpiar todo", "Tout nettoyer")) {
                    model.cleanupArtifacts()
                }
                .buttonStyle(.bordered)
                if let jobID = model.selectedStorageJobID {
                    Button(loc(model.languageCode, "Clean Job", "Job löschen", "Limpiar análisis", "Nettoyer le job")) {
                        model.cleanupArtifacts(jobID: jobID)
                    }
                    .buttonStyle(.borderedProminent)
                }
            }

            if let summary = model.storageSummary {
                StorageSummaryHeaderView(summary: summary)
                StorageArtifactsListView(summary: summary, onReveal: model.reveal)
            } else {
                ContentUnavailableView(
                    loc(model.languageCode, "No storage yet", "Noch kein Speicher", "Sin almacenamiento", "Pas de stockage"),
                    systemImage: "internaldrive",
                    description: Text(loc(model.languageCode, "Run analysis or pick a job.", "Analyse starten oder Job wählen.", "Ejecuta un análisis o elige un job.", "Lancez une analyse ou choisissez un job."))
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .padding(20)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }
}

struct SettingsWorkspaceView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        SettingsView(model: model)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }
}

struct LibraryEntryRowView: View {
    let entry: LibraryEntryPayload
    let isSelected: Bool

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: entry.status == "succeeded" ? "checkmark.circle.fill" : "clock")
                .foregroundStyle(entry.status == "succeeded" ? .green : .secondary)
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Text(entry.title)
                        .font(.subheadline.weight(.semibold))
                        .lineLimit(1)
                    if entry.pinned {
                        Image(systemName: "pin.fill")
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }
                Text(entry.input_value)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                Text("\(entry.segment_count) segments • \(ByteCountFormatter.string(fromByteCount: Int64(entry.artifact_size_bytes), countStyle: .file))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .fill(isSelected ? Color.accentColor.opacity(0.12) : Color.primary.opacity(0.035))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .strokeBorder(isSelected ? Color.accentColor.opacity(0.25) : Color.clear, lineWidth: 1)
        )
    }
}

struct StorageScopeRow: View {
    let title: String
    let subtitle: String
    let isSelected: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.primary)
                .lineLimit(1)
            Text(subtitle)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(1)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .fill(isSelected ? Color.accentColor.opacity(0.12) : Color.primary.opacity(0.035))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .strokeBorder(isSelected ? Color.accentColor.opacity(0.25) : Color.clear, lineWidth: 1)
        )
    }
}

struct StorageSummaryHeaderView: View {
    let summary: StorageSummaryPayload

    var body: some View {
        HStack(spacing: 14) {
            SummaryChip(title: "Artifacts", value: "\(summary.entries.count)")
            SummaryChip(title: "Size", value: ByteCountFormatter.string(fromByteCount: Int64(summary.total_size_bytes), countStyle: .file))
            SummaryChip(title: "Policy", value: summary.auto_clean ? "Auto-clean" : "Retained")
            Spacer()
        }
    }
}

struct SummaryChip: View {
    let title: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.subheadline.weight(.semibold))
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(Color.primary.opacity(0.04), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
    }
}

struct StorageArtifactsListView: View {
    let summary: StorageSummaryPayload
    let onReveal: (String) -> Void

    var body: some View {
        ScrollView {
            LazyVStack(alignment: .leading, spacing: 12) {
                if !summary.categories.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Categories")
                            .font(.headline)
                        ForEach(summary.categories, id: \.category) { category in
                            HStack {
                                Text(category.category.replacingOccurrences(of: "_", with: " ").capitalized)
                                Spacer()
                                Text("\(category.count)")
                                    .foregroundStyle(.secondary)
                                Text(ByteCountFormatter.string(fromByteCount: Int64(category.size_bytes), countStyle: .file))
                                    .foregroundStyle(.secondary)
                            }
                            .font(.subheadline)
                        }
                    }
                    .padding(18)
                    .background(Color.primary.opacity(0.035), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
                }

                VStack(alignment: .leading, spacing: 8) {
                    Text("Files & folders")
                        .font(.headline)
                    ForEach(summary.entries) { entry in
                        HStack(alignment: .top, spacing: 12) {
                            Image(systemName: entry.temporary ? "folder.badge.gearshape" : "externaldrive")
                                .foregroundStyle(entry.temporary ? Color.accentColor : Color.secondary)
                            VStack(alignment: .leading, spacing: 3) {
                                HStack {
                                    Text(entry.label)
                                        .font(.subheadline.weight(.semibold))
                                    if entry.pinned {
                                        Text("Pinned")
                                            .font(.caption2.weight(.semibold))
                                            .padding(.horizontal, 7)
                                            .padding(.vertical, 4)
                                            .background(Color.orange.opacity(0.14), in: Capsule())
                                            .foregroundStyle(.orange)
                                    }
                                }
                                Text(entry.path)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .textSelection(.enabled)
                                Text(ByteCountFormatter.string(fromByteCount: Int64(entry.size_bytes), countStyle: .file))
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer()
                            Button("Reveal") {
                                onReveal(entry.path)
                            }
                            .buttonStyle(.bordered)
                        }
                        .padding(14)
                        .background(Color.primary.opacity(0.03), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                }
                .padding(18)
                .background(Color.primary.opacity(0.035), in: RoundedRectangle(cornerRadius: 16, style: .continuous))

                VStack(alignment: .leading, spacing: 8) {
                    Text("Locations")
                        .font(.headline)
                    ForEach(summary.locations.keys.sorted(), id: \.self) { key in
                        HStack {
                            Text(key.capitalized)
                            Spacer()
                            Text(summary.locations[key] ?? "")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .textSelection(.enabled)
                        }
                    }
                }
                .padding(18)
                .background(Color.primary.opacity(0.035), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
            }
        }
    }
}

struct RecordingButton: View {
    let title: String
    let systemImage: String
    let isActive: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Label(title, systemImage: systemImage)
                .font(.subheadline.weight(.medium))
        }
        .buttonStyle(.bordered)
        .tint(isActive ? .red : .accentColor)
    }
}

struct StatusBannerView: View {
    let state: AppViewState

    var body: some View {
        HStack(spacing: 10) {
            switch state {
            case .idle:
                Image(systemName: "sparkles")
                    .foregroundStyle(.secondary)
                Text(loc(modelLanguage(), "Paste a link or choose a file.", "Link einfügen oder Datei wählen.", "Pega un enlace o elige un archivo.", "Collez un lien ou choisissez un fichier."))
                    .foregroundStyle(.secondary)
            case let .analyzing(phase):
                ProgressView()
                    .controlSize(.small)
                Text(phase)
                    .foregroundStyle(.secondary)
            case .recordingMic:
                Image(systemName: "mic.fill")
                    .foregroundStyle(.red)
                Text(loc(modelLanguage(), "Mic is recording. Tap again to stop.", "Mikro läuft. Erneut tippen zum Stoppen.", "El micrófono graba. Pulsa otra vez para parar.", "Le micro enregistre. Touchez encore pour arrêter."))
                    .foregroundStyle(.secondary)
            case .recordingSystem:
                Image(systemName: "waveform")
                    .foregroundStyle(.orange)
                Text(loc(modelLanguage(), "System audio is recording. Tap again to stop.", "Systemaudio läuft. Erneut tippen zum Stoppen.", "El audio del sistema graba. Pulsa otra vez para parar.", "L’audio système enregistre. Touchez encore pour arrêter."))
                    .foregroundStyle(.secondary)
            case .showingResults:
                Image(systemName: "checkmark.circle.fill")
                    .foregroundStyle(.green)
                Text(loc(modelLanguage(), "Analysis done.", "Analyse fertig.", "Análisis listo.", "Analyse terminée."))
                    .foregroundStyle(.secondary)
            case let .error(message):
                Image(systemName: "exclamationmark.triangle.fill")
                    .foregroundStyle(.orange)
                Text(message)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
        .font(.subheadline)
        .padding(.horizontal, 2)
    }

    private func modelLanguage() -> String {
        UserDefaults.standard.string(forKey: languageDefaultsKey) ?? defaultUILanguageCode()
    }
}

struct ResultsSectionView: View {
    @ObservedObject var model: AppModel
    @State private var showOnlySongs = true

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            HStack {
                Text(loc(model.languageCode, "Results", "Ergebnisse", "Resultados", "Résultats"))
                    .font(.headline)
                Spacer()
                if let result = model.result {
                    Text("\(result.segments.count) \(loc(model.languageCode, "segments", "Segmente", "segmentos", "segments"))")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
            }

            if case let .analyzing(phase) = model.viewState {
                LoadingResultsView(phase: phase)
            } else if let result = model.result {
                let viewModels = result.segments.map(model.makeSegmentViewModel)
                let hasSongs = viewModels.contains(where: { $0.payload.kind == "matched_track" })
                let filteredModels = filteredSegments(from: viewModels, showOnlySongs: showOnlySongs && hasSongs)

                ResultsToolbarView(
                    totalCount: viewModels.count,
                    songCount: viewModels.filter { $0.payload.kind == "matched_track" }.count,
                    showOnlySongs: Binding(
                        get: { showOnlySongs && hasSongs },
                        set: { showOnlySongs = $0 }
                    ),
                    hasSongs: hasSongs
                )

                ResultsTimelineView(segments: filteredModels, selectedSegmentID: model.selectedSegmentID) { segmentID in
                    model.selectSegment(segmentID)
                }

                CompactResultsView(
                    segments: filteredModels,
                    selectedSegmentID: Binding(
                        get: { model.selectedSegmentID },
                        set: { if let value = $0 { model.selectSegment(value) } }
                    ),
                    onCopy: { text in model.copy(text) }
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .onAppear {
                    if model.selectedSegmentID == nil {
                        model.selectedSegmentID = filteredModels.first?.id
                    }
                }
                .onChange(of: filteredModels.map(\.id)) { _, ids in
                    guard !ids.isEmpty else {
                        model.selectedSegmentID = nil
                        return
                    }
                    if let selected = model.selectedSegmentID, ids.contains(selected) {
                        return
                    }
                    model.selectedSegmentID = ids.first
                }
            } else {
                EmptyStateView()
            }
        }
        .padding(24)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 26, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 26, style: .continuous)
                .strokeBorder(Color.primary.opacity(0.05))
        )
    }

    private func filteredSegments(from segments: [SegmentViewModel], showOnlySongs: Bool) -> [SegmentViewModel] {
        if showOnlySongs {
            return segments.filter { $0.payload.kind == "matched_track" }
        }
        return segments
    }
}

struct ResultsToolbarView: View {
    let totalCount: Int
    let songCount: Int
    @Binding var showOnlySongs: Bool
    let hasSongs: Bool
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()

    var body: some View {
        HStack(spacing: 12) {
            if hasSongs {
                Picker(loc(languageCode, "Filter", "Filter", "Filtro", "Filtre"), selection: $showOnlySongs) {
                    Text(loc(languageCode, "Songs", "Songs", "Canciones", "Titres")).tag(true)
                    Text(loc(languageCode, "All", "Alle", "Todo", "Tout")).tag(false)
                }
                .pickerStyle(.segmented)
                .frame(width: 180)
            }

            Spacer()

            if hasSongs && showOnlySongs {
                Text("\(songCount) \(loc(languageCode, "songs", "Songs", "canciones", "titres"))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("\(totalCount) \(loc(languageCode, "items", "Abschnitte", "bloques", "blocs"))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
    }
}

struct LoadingResultsView: View {
    let phase: String
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            ProgressView()
                .controlSize(.regular)
            Text(phase)
                .font(.title3.weight(.semibold))
            Text(loc(languageCode, "Long mixes are segmented first, then matched.", "Lange Mixe werden erst segmentiert, dann zugeordnet.", "Los mixes largos se segmentan primero y luego se identifican.", "Les longs mixes sont d’abord segmentés puis identifiés."))
                .foregroundStyle(.secondary)
            RoundedRectangle(cornerRadius: 10)
                .fill(Color.primary.opacity(0.05))
                .frame(height: 14)
            ForEach(0..<3, id: \.self) { _ in
                RoundedRectangle(cornerRadius: 18)
                    .fill(Color.primary.opacity(0.04))
                    .frame(height: 92)
            }
        }
    }
}

struct EmptyStateView: View {
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()

    var body: some View {
        ContentUnavailableView(
            loc(languageCode, "No analysis", "Noch keine Analyse", "Sin análisis", "Pas d’analyse"),
            systemImage: "waveform.and.magnifyingglass",
            description: Text(loc(languageCode, "Run analysis to see the timeline.", "Starte eine Analyse, um die Timeline zu sehen.", "Ejecuta un análisis para ver la línea de tiempo.", "Lancez une analyse pour voir la timeline."))
        )
        .frame(maxWidth: .infinity, minHeight: 260)
    }
}

struct ResultsTimelineView: View {
    let segments: [SegmentViewModel]
    let selectedSegmentID: String?
    let onSelect: (String) -> Void
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(loc(languageCode, "Timeline", "Timeline", "Timeline", "Timeline"))
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)
            GeometryReader { geometry in
                let totalDuration = max(1, segments.map(\.endMs).max() ?? 1)
                ZStack(alignment: .leading) {
                    RoundedRectangle(cornerRadius: 10, style: .continuous)
                        .fill(Color.primary.opacity(0.06))
                    ForEach(segments) { segment in
                        Button {
                            onSelect(segment.id)
                        } label: {
                            RoundedRectangle(cornerRadius: 8, style: .continuous)
                                .fill(segment.timelineColor)
                                .overlay(
                                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                                        .strokeBorder(selectedSegmentID == segment.id ? Color.primary.opacity(0.28) : .clear, lineWidth: 2)
                                )
                        }
                        .buttonStyle(.plain)
                        .frame(
                            width: max(6, width(for: segment, totalDuration: totalDuration, width: geometry.size.width)),
                            height: selectedSegmentID == segment.id ? 24 : 18
                        )
                        .offset(x: offset(for: segment, totalDuration: totalDuration, width: geometry.size.width))
                        .help(segment.title)
                    }
                }
            }
            .frame(height: 28)
        }
    }

    private func width(for segment: SegmentViewModel, totalDuration: Int, width: CGFloat) -> CGFloat {
        let fraction = CGFloat(segment.endMs - segment.startMs) / CGFloat(totalDuration)
        return width * fraction
    }

    private func offset(for segment: SegmentViewModel, totalDuration: Int, width: CGFloat) -> CGFloat {
        CGFloat(segment.startMs) / CGFloat(totalDuration) * width
    }
}

struct CompactResultsView: View {
    let segments: [SegmentViewModel]
    @Binding var selectedSegmentID: String?
    let onCopy: (String) -> Void

    var body: some View {
        HSplitView {
            SegmentListPane(segments: segments, selectedSegmentID: $selectedSegmentID)
                .frame(minWidth: 260, idealWidth: 300, maxWidth: 340)

            SegmentInspectorPane(
                viewModel: segments.first(where: { $0.id == selectedSegmentID }) ?? segments.first,
                onCopy: onCopy
            )
            .frame(minWidth: 360, maxWidth: .infinity)
        }
        .frame(minHeight: 360, idealHeight: 440)
        .background(Color.primary.opacity(0.02), in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }
}

struct SegmentListPane: View {
    let segments: [SegmentViewModel]
    @Binding var selectedSegmentID: String?

    var body: some View {
        ScrollView {
            LazyVStack(spacing: 8) {
                ForEach(segments) { segment in
                    Button {
                        selectedSegmentID = segment.id
                    } label: {
                        HStack(alignment: .center, spacing: 10) {
                            RoundedRectangle(cornerRadius: 3, style: .continuous)
                                .fill(segment.timelineColor)
                                .frame(width: 6, height: 34)

                            VStack(alignment: .leading, spacing: 3) {
                                Text(segment.title)
                                    .font(.subheadline.weight(.medium))
                                    .foregroundStyle(.primary)
                                    .lineLimit(1)
                                HStack(spacing: 6) {
                                    Text(timeRange(segment))
                                    Text("•")
                                    Text(segment.detailLabel)
                                }
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            }

                            Spacer(minLength: 8)

                            if let badge = segment.statusBadge {
                                Text(badge)
                                    .font(.caption2.weight(.semibold))
                                    .padding(.horizontal, 8)
                                    .padding(.vertical, 4)
                                    .background(segment.accentColor.opacity(0.12), in: Capsule())
                                    .foregroundStyle(segment.accentColor)
                            }
                        }
                        .padding(.horizontal, 12)
                        .padding(.vertical, 10)
                        .background(
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .fill(selectedSegmentID == segment.id ? Color.accentColor.opacity(0.12) : Color.primary.opacity(0.035))
                        )
                        .overlay(
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .strokeBorder(selectedSegmentID == segment.id ? Color.accentColor.opacity(0.35) : Color.clear, lineWidth: 1)
                        )
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(12)
        }
    }

    private func timeRange(_ segment: SegmentViewModel) -> String {
        "\(formatTime(segment.startMs))–\(formatTime(segment.endMs))"
    }

    private func formatTime(_ milliseconds: Int) -> String {
        let totalSeconds = milliseconds / 1000
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        let seconds = totalSeconds % 60
        if hours > 0 {
            return String(format: "%02d:%02d:%02d", hours, minutes, seconds)
        }
        return String(format: "%02d:%02d", minutes, seconds)
    }
}

struct SegmentInspectorPane: View {
    let viewModel: SegmentViewModel?
    let onCopy: (String) -> Void
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()

    var body: some View {
        Group {
            if let viewModel {
                VStack(alignment: .leading, spacing: 18) {
                    HStack(alignment: .top, spacing: 14) {
                        Circle()
                            .fill(viewModel.timelineColor)
                            .frame(width: 14, height: 14)
                            .padding(.top, 6)

                        VStack(alignment: .leading, spacing: 6) {
                            Text(viewModel.title)
                                .font(.system(size: 28, weight: .semibold, design: .rounded))
                                .fixedSize(horizontal: false, vertical: true)
                            Text(viewModel.subtitle)
                                .font(.title3)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }

                        Spacer()

                        if let badge = viewModel.statusBadge {
                            Text(badge)
                                .font(.caption.weight(.semibold))
                                .padding(.horizontal, 10)
                                .padding(.vertical, 6)
                                .background(viewModel.accentColor.opacity(0.14), in: Capsule())
                                .foregroundStyle(viewModel.accentColor)
                        }
                    }

                    HStack(spacing: 12) {
                        InfoPill(systemImage: "clock", text: "\(formatTime(viewModel.startMs)) – \(formatTime(viewModel.endMs))")
                        InfoPill(systemImage: "sparkles", text: viewModel.detailLabel)
                        if let quality = viewModel.qualityLabel {
                            InfoPill(systemImage: "checkmark.seal", text: quality)
                        }
                    }

                    if let hint = viewModel.metadataHint, !hint.isEmpty {
                        Text(cleanHint(hint))
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }

                    Divider()

                    if viewModel.isInteractive {
                        VStack(alignment: .leading, spacing: 12) {
                            Text(loc(languageCode, "Actions", "Aktionen", "Acciones", "Actions"))
                                .font(.headline)

                            HStack(spacing: 10) {
                                Button(loc(languageCode, "Copy", "Kopieren", "Copiar", "Copier")) {
                                    onCopy(viewModel.title)
                                }
                                .buttonStyle(.bordered)

                                ForEach(viewModel.primaryLinks) { link in
                                    Link(destination: link.url) {
                                        Label(link.label, systemImage: link.icon)
                                            .font(.subheadline.weight(.semibold))
                                            .padding(.horizontal, 12)
                                            .padding(.vertical, 8)
                                            .foregroundStyle(.white)
                                            .background(link.color.gradient, in: Capsule())
                                    }
                                    .buttonStyle(.plain)
                                }

                                if !viewModel.overflowLinks.isEmpty {
                                    Menu(loc(languageCode, "More", "Mehr", "Más", "Plus")) {
                                        ForEach(viewModel.overflowLinks) { link in
                                            Link(destination: link.url) {
                                                Label(link.label, systemImage: link.icon)
                                            }
                                        }
                                    }
                                    .menuStyle(.borderlessButton)
                                }
                            }
                        }
                    } else {
                        Button(loc(languageCode, "Copy", "Kopieren", "Copiar", "Copier")) {
                            onCopy(viewModel.title)
                        }
                        .buttonStyle(.bordered)
                    }

                    if !viewModel.payload.alternates.isEmpty {
                        VStack(alignment: .leading, spacing: 10) {
                            Text(loc(languageCode, "Alternates", "Weitere Kandidaten", "Alternativas", "Alternatives"))
                                .font(.headline)
                            ForEach(viewModel.payload.alternates, id: \.self) { alternate in
                                Text(alternate.artist.map { "\($0) – \(alternate.title)" } ?? alternate.title)
                                    .font(.subheadline)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }

                    Spacer(minLength: 0)
                }
                .padding(24)
            } else {
                ContentUnavailableView(
                    loc(languageCode, "No segment", "Kein Abschnitt gewählt", "Sin segmento", "Aucun segment"),
                    systemImage: "line.3.horizontal.decrease.circle",
                    description: Text(loc(languageCode, "Pick a segment to inspect it.", "Wähle links einen Abschnitt.", "Elige un segmento.", "Choisissez un segment."))
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
    }

    private func cleanHint(_ hint: String) -> String {
        hint.replacingOccurrences(of: "tracklist:", with: "")
            .replacingOccurrences(of: "chapter:", with: "")
    }

    private func formatTime(_ milliseconds: Int) -> String {
        let totalSeconds = milliseconds / 1000
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        let seconds = totalSeconds % 60
        if hours > 0 {
            return String(format: "%02d:%02d:%02d", hours, minutes, seconds)
        }
        return String(format: "%02d:%02d", minutes, seconds)
    }
}

struct InfoPill: View {
    let systemImage: String
    let text: String

    var body: some View {
        Label(text, systemImage: systemImage)
            .font(.caption.weight(.medium))
            .padding(.horizontal, 10)
            .padding(.vertical, 7)
            .background(Color.primary.opacity(0.06), in: Capsule())
            .foregroundStyle(.secondary)
    }
}

struct SegmentCardView: View {
    let viewModel: SegmentViewModel
    let onCopy: (String) -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack(alignment: .top) {
                VStack(alignment: .leading, spacing: 6) {
                    Text(viewModel.title)
                        .font(.title3.weight(.semibold))
                    Text(viewModel.subtitle)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                if let badge = viewModel.statusBadge {
                    Text(badge)
                        .font(.caption.weight(.semibold))
                        .padding(.horizontal, 10)
                        .padding(.vertical, 5)
                        .background(viewModel.accentColor.opacity(0.12), in: Capsule())
                        .foregroundStyle(viewModel.accentColor)
                }
            }

            HStack(spacing: 14) {
                Label("\(formatTime(viewModel.startMs)) – \(formatTime(viewModel.endMs))", systemImage: "clock")
                Label(viewModel.detailLabel, systemImage: "sparkles")
                if let quality = viewModel.qualityLabel {
                    Label(quality, systemImage: "checkmark.seal")
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)

            if let hint = viewModel.metadataHint, !hint.isEmpty {
                Text(hint.replacingOccurrences(of: "tracklist:", with: "").replacingOccurrences(of: "chapter:", with: ""))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 8) {
                Button("Kopieren") {
                    onCopy(viewModel.title)
                }
                .buttonStyle(.bordered)

                ForEach(viewModel.primaryLinks) { link in
                    Link(destination: link.url) {
                        Label(link.label, systemImage: link.icon)
                            .font(.caption.weight(.semibold))
                            .padding(.horizontal, 10)
                            .padding(.vertical, 6)
                            .foregroundStyle(.white)
                            .background(link.color.gradient, in: Capsule())
                    }
                    .buttonStyle(.plain)
                }

                if !viewModel.overflowLinks.isEmpty {
                    Menu("Mehr") {
                        ForEach(viewModel.overflowLinks) { link in
                            Link(destination: link.url) {
                                Label(link.label, systemImage: link.icon)
                            }
                        }
                    }
                    .menuStyle(.borderlessButton)
                }
            }
        }
        .padding(18)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.primary.opacity(0.035), in: RoundedRectangle(cornerRadius: 22, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 22, style: .continuous)
                .strokeBorder(viewModel.accentColor.opacity(0.12))
        )
    }

    private func formatTime(_ milliseconds: Int) -> String {
        let totalSeconds = milliseconds / 1000
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        let seconds = totalSeconds % 60
        if hours > 0 {
            return String(format: "%02d:%02d:%02d", hours, minutes, seconds)
        }
        return String(format: "%02d:%02d", minutes, seconds)
    }
}

struct SettingsView: View {
    @ObservedObject var model: AppModel
    @AppStorage(openExternalLinksDefaultsKey) private var openExternalLinks = false
    @AppStorage(languageDefaultsKey) private var languageCode = UILanguage.en.rawValue
    @AppStorage("musicFetch.casualMode") private var casualMode = true
    @AppStorage("musicFetch.analysisMode") private var analysisMode = "auto"
    @AppStorage("musicFetch.recallProfile") private var recallProfile = "max_recall"
    @AppStorage("musicFetch.metadataHints") private var metadataHints = true
    @AppStorage("musicFetch.repeatDetection") private var repeatDetection = true
    @AppStorage("musicFetch.preferSeparation") private var preferSeparation = true
    @AppStorage("musicFetch.preferredInput") private var preferredInput = "link"
    @AppStorage("musicFetch.recordingTarget") private var recordingTarget = "microphone"
    @AppStorage("musicFetch.debugDetails") private var debugDetails = false

    var body: some View {
        TabView {
            Form {
                Picker("Language", selection: Binding(
                    get: { languageCode },
                    set: {
                        languageCode = $0
                        model.setLanguage($0)
                    }
                )) {
                    ForEach(UILanguage.allCases, id: \.rawValue) { language in
                        Text(language.label).tag(language.rawValue)
                    }
                }
                Picker("Standardanalyse", selection: $analysisMode) {
                    Text("Auto").tag("auto")
                    Text("Single").tag("single_track")
                    Text("Playlist").tag("playlist_entry")
                    Text("Long mix").tag("long_mix")
                }
                Toggle("Simple UI", isOn: $casualMode)
                Toggle("Open links", isOn: $openExternalLinks)
            }
            .padding(20)
            .tabItem { Label("General", systemImage: "gearshape") }

            Form {
                Picker("Default input", selection: $preferredInput) {
                    Text("Link").tag("link")
                    Text("File").tag("file")
                    Text("Mic").tag("microphone")
                    Text("System").tag("system")
                }
                Picker("Record target", selection: $recordingTarget) {
                    Text("Mic").tag("microphone")
                    Text("System").tag("system")
                }
            }
            .padding(20)
            .tabItem { Label("Input", systemImage: "square.and.arrow.down") }

            Form {
                Picker("Recall-Profil", selection: $recallProfile) {
                    Text("Max Recall").tag("max_recall")
                    Text("Balanced").tag("balanced")
                    Text("Fast First").tag("fast_first")
                }
                Toggle("Use hints", isOn: $metadataHints)
                Toggle("Detect repeats", isOn: $repeatDetection)
                Toggle("Prefer stems", isOn: $preferSeparation)
            }
            .padding(20)
            .tabItem { Label("Recognition", systemImage: "waveform.path.ecg") }

            Form {
                LabeledContent("Local catalog", value: "CLI/API")
                LabeledContent("AudD / ACRCloud", value: "Backend config")
                Text("Advanced connection setup stays out of the main workspace.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(20)
            .tabItem { Label("Connections", systemImage: "link") }

            VStack(alignment: .leading, spacing: 16) {
                HStack {
                    Button("Refresh") { model.refreshDoctor() }
                    Button("Install tools") { model.installMissingCoreDependencies() }
                }
                TextField("Backend path", text: Binding(
                    get: { model.backendCommand },
                    set: { model.updateBackendCommand($0) }
                ))
                .textFieldStyle(.roundedBorder)

                Toggle("Show debug", isOn: $debugDetails)

                List(model.providerChecks) { check in
                    HStack {
                        Image(systemName: check.ok ? "checkmark.circle.fill" : "exclamationmark.triangle.fill")
                            .foregroundStyle(check.ok ? .green : .orange)
                        VStack(alignment: .leading, spacing: 3) {
                            Text(check.name)
                            Text(check.detail)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }
                }
                .frame(minHeight: 240)
            }
            .padding(20)
            .tabItem { Label("Diagnostics", systemImage: "stethoscope") }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .onAppear {
            if model.providerChecks.isEmpty {
                model.refreshDoctor()
            }
        }
    }
}

@main
struct MusicFetchMacApp: App {
    @StateObject private var model = AppModel()

    var body: some Scene {
        WindowGroup {
            ContentView(model: model)
        }
        .defaultSize(width: 1440, height: 920)
        .commands {
            CommandGroup(after: .newItem) {
                Button("Run Analysis") {
                    NotificationCenter.default.post(name: .musicFetchAnalyze, object: nil)
                }
                .keyboardShortcut(.return, modifiers: [.command])

                Button("Focus Input") {
                    NotificationCenter.default.post(name: .musicFetchFocusInput, object: nil)
                }
                .keyboardShortcut("l", modifiers: [.command])
            }
        }

        Settings {
            SettingsView(model: model)
        }
    }
}
