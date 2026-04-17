import AppKit
import SwiftUI
import UniformTypeIdentifiers
import UserNotifications

@MainActor
final class AppModel: ObservableObject {
    @Published var backendCommand: String
    @Published var inputValue = ""
    @Published var providerChecks: [DoctorCheck] = []
    @Published var result: AnalyzeResponse?
    @Published var libraryEntries: [LibraryEntryPayload] = []
    @Published var storageSummary: StorageSummaryPayload?
    @Published var viewState: AppViewState = .idle
    @Published var selectedSegmentID: String?
    @Published var route = ShellRoutingState()
    @Published var languageCode: String
    @Published var captureState: CaptureState = .idle
    @Published var latestQuickCaptureSummary = ""

    @Published var jobProgress: [String: JobProgress] = [:]
    @Published var systemResources: SystemResources?

    private let microphoneRecorder = MicrophoneRecorder()
    private let systemAudioRecorder = SystemAudioRecorder()
    private let backend = BackendController()
    private var analysisPhaseTask: Task<Void, Never>?
    private var pollingTask: Task<Void, Never>?
    private var cachedResults: [String: AnalyzeResponse] = [:]
    private var openedPrimaryLinkJobIDs: Set<String> = []
    private var notifiedCompletionJobIDs: Set<String> = []
    private var submittedJobIDs: Set<String> = []
    private var eventStreamTasks: [String: Task<Void, Never>] = [:]
    private var terminationObserver: NSObjectProtocol?
    private var lastStorageFetchAt: Date?
    private var lastStorageFetchJobID: String?
    private var lastLibraryFetchAt: Date?
    private static let storageCacheWindow: TimeInterval = 8
    private static let libraryCacheWindow: TimeInterval = 4

    init() {
        let savedBackend = UserDefaults.standard.string(forKey: backendCommandDefaultsKey)
        let resolvedBackend = Self.resolveInitialBackendCommand(saved: savedBackend)
        backendCommand = resolvedBackend
        UserDefaults.standard.set(resolvedBackend, forKey: backendCommandDefaultsKey)
        languageCode = UserDefaults.standard.string(forKey: languageDefaultsKey) ?? defaultUILanguageCode()
        terminationObserver = NotificationCenter.default.addObserver(
            forName: NSApplication.willTerminateNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task {
                await self.backend.stop()
            }
        }
    }

    var isBusy: Bool {
        captureState.isBusy
    }

    var selectedWorkspace: WorkspaceSection {
        route.workspace
    }

    var selectedLibraryJobID: String? {
        route.libraryJobID
    }

    var selectedStorageJobID: String? {
        route.storageJobID
    }

    var orderedLibraryEntries: [LibraryEntryPayload] {
        libraryEntries.sorted {
            if $0.pinned != $1.pinned {
                return $0.pinned && !$1.pinned
            }
            return $0.updated_at > $1.updated_at
        }
    }

    var quickCaptureSummary: String {
        if !latestQuickCaptureSummary.isEmpty {
            return latestQuickCaptureSummary
        }
        guard let response = result else {
            return loc(languageCode, "No quick result yet", "Noch kein Schnelltreffer", "Sin resultado rapido", "Pas encore de resultat rapide")
        }
        return Self.topMatchSummary(from: response) ?? loc(languageCode, "No quick result yet", "Noch kein Schnelltreffer", "Sin resultado rapido", "Pas encore de resultat rapide")
    }

    var statusTitle: String {
        switch captureState {
        case .startingMic:
            return loc(languageCode, "Starting mic", "Mikro startet", "Iniciando mic", "Demarrage micro")
        case .recordingMic:
            return loc(languageCode, "Mic on", "Mikro an", "Microfono activo", "Micro actif")
        case .stoppingMic:
            return loc(languageCode, "Stopping mic", "Mikro stoppt", "Deteniendo mic", "Arret micro")
        case .startingSystem:
            return loc(languageCode, "Starting system", "System startet", "Iniciando sistema", "Demarrage systeme")
        case .recordingSystem:
            return loc(languageCode, "System on", "System an", "Sistema activo", "Systeme actif")
        case .stoppingSystem:
            return loc(languageCode, "Stopping system", "System stoppt", "Deteniendo sistema", "Arret systeme")
        case .idle:
            break
        }

        switch viewState {
        case .idle:
            return loc(languageCode, "Ready", "Bereit", "Listo", "Pret")
        case .recordingMic:
            return loc(languageCode, "Mic on", "Mikro an", "Microfono activo", "Micro actif")
        case .recordingSystem:
            return loc(languageCode, "System on", "System an", "Sistema activo", "Systeme actif")
        case let .analyzing(phase):
            return phase
        case .showingResults:
            return loc(languageCode, "Done", "Fertig", "Listo", "Termine")
        case let .error(message):
            return message
        }
    }

    var activeJobID: String? {
        switch route.workspace {
        case .analyze:
            return result?.job.id
        case .library:
            return route.libraryJobID
        case .storage:
            return route.storageJobID
        }
    }

    func setLanguage(_ code: String) {
        languageCode = code
        UserDefaults.standard.set(code, forKey: languageDefaultsKey)
    }

    func updateBackendCommand(_ value: String) {
        backendCommand = value
        UserDefaults.standard.set(value, forKey: backendCommandDefaultsKey)
        Task {
            await backend.stop()
        }
    }

    func activateWorkspace(_ section: WorkspaceSection) {
        let alreadyThere = route.workspace == section
        route.workspace = section
        switch section {
        case .analyze:
            break
        case .library:
            if route.libraryJobID == nil {
                route.libraryJobID = orderedLibraryEntries.first?.job_id
            }
            if let jobID = route.libraryJobID {
                if let cached = cachedResults[jobID] {
                    applyCachedSnapshot(cached)
                }
                if !alreadyThere || shouldRefreshLibrary() {
                    loadLibraryJob(jobID)
                }
            }
        case .storage:
            if route.storageJobID == nil {
                route.storageJobID = route.libraryJobID ?? orderedLibraryEntries.first?.job_id
            }
            if shouldRefreshStorage(for: route.storageJobID) || !alreadyThere && storageSummary == nil {
                Task { await refreshStorage(jobID: route.storageJobID) }
            }
        }
    }

    private func applyCachedSnapshot(_ cached: AnalyzeResponse) {
        result = cached
        if selectedSegmentID == nil || !cached.segments.contains(where: { $0.id == selectedSegmentID }) {
            selectedSegmentID = cached.segments.first?.id
        }
        updateViewState(for: cached)
    }

    private func shouldRefreshStorage(for jobID: String?) -> Bool {
        if storageSummary == nil { return true }
        if lastStorageFetchJobID != jobID { return true }
        guard let at = lastStorageFetchAt else { return true }
        return Date().timeIntervalSince(at) > Self.storageCacheWindow
    }

    private func shouldRefreshLibrary() -> Bool {
        guard let at = lastLibraryFetchAt else { return true }
        return Date().timeIntervalSince(at) > Self.libraryCacheWindow
    }

    func refreshCurrentWorkspace() {
        Task {
            await refreshLibrary()
            switch route.workspace {
            case .analyze:
                if let jobID = result?.job.id {
                    _ = try? await refreshJobSnapshot(jobID)
                }
                await refreshStorage(jobID: route.storageJobID)
            case .library:
                if let jobID = route.libraryJobID {
                    _ = try? await refreshJobSnapshot(jobID)
                }
            case .storage:
                await refreshStorage(jobID: route.storageJobID)
            }
        }
    }

    func showDiagnostics() {
        UserDefaults.standard.set(SettingsTab.diagnostics.rawValue, forKey: settingsTabDefaultsKey)
        if providerChecks.isEmpty {
            refreshDoctor()
        }
        NotificationCenter.default.post(name: .musicFetchShowDiagnostics, object: nil)
    }

    func analyze() {
        let trimmed = inputValue.trimmingCharacters(in: .whitespacesAndNewlines)
        submitInput(trimmed, switchToAnalyze: true)
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
            UTType(filenameExtension: "m4a"),
        ].compactMap { $0 }
        if panel.runModal() == .OK, let url = panel.url {
            inputValue = url.path
        }
    }

    func startNewAnalysis() {
        inputValue = ""
        result = nil
        selectedSegmentID = nil
        viewState = .idle
        route.workspace = .analyze
    }

    func loadLibraryJob(_ jobID: String) {
        if let cached = cachedResults[jobID] {
            route.libraryJobID = jobID
            applyCachedSnapshot(cached)
        }
        Task {
            do {
                let response = try await refreshJobSnapshot(jobID)
                route.libraryJobID = jobID
                cache(response: response, source: response.items.first?.input_value ?? jobID)
            } catch {
                if cachedResults[jobID] == nil {
                    viewState = .error(loc(languageCode, "Could not load analysis", "Analyse konnte nicht geladen werden", "No se pudo cargar el analisis", "Impossible de charger l'analyse"))
                }
            }
        }
    }

    func refreshDoctor() {
        Task {
            do {
                providerChecks = try await runJSON(arguments: ["doctor", "--json"], as: [DoctorCheck].self)
            } catch {
                viewState = .error(loc(languageCode, "Diagnostics failed", "Diagnose fehlgeschlagen", "Fallo el diagnostico", "Echec du diagnostic"))
            }
        }
    }

    func bootstrap() {
        Task {
            var startupFailed = false
            do {
                _ = try await backend.ensureServer(command: backendCommand)
            } catch {
                startupFailed = true
                viewState = .error(error.localizedDescription)
            }
            await refreshLibrary()
            await refreshStorage()
            await fetchSystemResources()
            reconcileStreams()
            if startupFailed,
               case let .error(message) = viewState,
               Self.isRecoverableBackendStartupError(message)
            {
                viewState = result == nil ? .idle : .showingResults
            }
            await requestNotificationAuthorization()
        }
        startPolling()
    }

    func refreshLibrary() async {
        do {
            let payload = try await backend.getJSON(
                "/v1/library",
                command: backendCommand,
                queryItems: [URLQueryItem(name: "limit", value: "60")],
                type: LibraryEnvelope.self
            )
            let incoming = payload.entries
            if incoming != libraryEntries {
                libraryEntries = incoming
            }
            if route.libraryJobID == nil || !libraryEntries.contains(where: { $0.job_id == route.libraryJobID }) {
                route.libraryJobID = orderedLibraryEntries.first?.job_id
            }
            if let storageJobID = route.storageJobID,
               !libraryEntries.contains(where: { $0.job_id == storageJobID })
            {
                route.storageJobID = nil
            }
            lastLibraryFetchAt = Date()
        } catch {
            if !libraryEntries.isEmpty { return }
            libraryEntries = []
        }
    }

    func refreshStorage(jobID: String? = nil) async {
        do {
            var queryItems: [URLQueryItem] = []
            if let jobID {
                queryItems.append(URLQueryItem(name: "job_id", value: jobID))
            }
            let payload = try await backend.getJSON("/v1/storage", command: backendCommand, queryItems: queryItems, type: StorageEnvelope.self)
            storageSummary = payload.storage
            lastStorageFetchAt = Date()
            lastStorageFetchJobID = jobID
        } catch {
            if storageSummary == nil { return }
        }
    }

    func selectStorageScope(_ jobID: String?) {
        route.storageJobID = jobID
        Task {
            await refreshStorage(jobID: jobID)
        }
    }

    func cleanupArtifacts(jobID: String? = nil) {
        viewState = .analyzing(loc(languageCode, "Removing temporary files", "Temporare Dateien werden entfernt", "Eliminando archivos temporales", "Suppression des fichiers temporaires"))
        Task {
            defer {
                if case .analyzing = viewState {
                    viewState = .idle
                }
            }
            do {
                var queryItems: [URLQueryItem] = []
                if let jobID {
                    queryItems.append(URLQueryItem(name: "job_id", value: jobID))
                }
                let payload = try await backend.deleteJSON("/v1/storage", command: backendCommand, queryItems: queryItems, type: StorageEnvelope.self)
                storageSummary = payload.storage
                await refreshLibrary()
                if let jobID, result?.job.id == jobID {
                    await refreshStorage(jobID: jobID)
                }
                viewState = .idle
            } catch {
                viewState = .error(loc(languageCode, "Cleanup failed", "Cleanup fehlgeschlagen", "Fallo la limpieza", "Echec du nettoyage"))
            }
        }
    }

    func setPinned(jobID: String, pinned: Bool) {
        Task {
            do {
                _ = try await backend.putJSON(
                    "/v1/storage/jobs/\(jobID)/pin",
                    command: backendCommand,
                    body: ["pinned": pinned],
                    type: PinResponse.self
                )
                await refreshLibrary()
                await refreshStorage(jobID: route.storageJobID)
            } catch {
                viewState = .error(loc(languageCode, "Pin failed", "Pinnen fehlgeschlagen", "Fallo al fijar", "Echec de l'epinglage"))
            }
        }
    }

    func cancelActiveJob() {
        guard let jobID = result?.job.id ?? route.libraryJobID ?? route.storageJobID else { return }
        Task {
            do {
                _ = try await backend.postJSON(
                    "/v1/jobs/\(jobID)/cancel",
                    command: backendCommand,
                    body: [String: String](),
                    type: CancelResponse.self
                )
                _ = try await refreshJobSnapshot(jobID)
                await refreshLibrary()
            } catch {
                viewState = .error(loc(languageCode, "Cancel failed", "Abbruch fehlgeschlagen", "Fallo la cancelacion", "Echec de l'annulation"))
            }
        }
    }

    func retryUnresolvedSegments() {
        guard let jobID = result?.job.id else { return }
        viewState = .analyzing(loc(languageCode, "Retrying unresolved segments", "Unklare Segmente werden erneut geprueft", "Reintentando segmentos sin resolver", "Nouvelle tentative sur les segments non resolus"))
        Task {
            do {
                let payload = RetrySegmentsRequest(source_item_id: nil, options: currentJobOptions())
                _ = try await backend.postJSON(
                    "/v1/jobs/\(jobID)/segments/retry",
                    command: backendCommand,
                    body: payload,
                    type: RetryResponse.self
                )
                let response = try await refreshJobSnapshot(jobID)
                cache(response: response, source: response.items.first?.input_value ?? jobID)
                await refreshLibrary()
            } catch {
                viewState = .error(loc(languageCode, "Retry failed", "Erneuter Versuch fehlgeschlagen", "Fallo el reintento", "Nouvelle tentative echouee"))
            }
        }
    }

    func correctSegment(_ segment: SegmentPayload, title: String, artist: String?, album: String?) {
        guard let jobID = result?.job.id else { return }
        Task {
            do {
                let payload = SegmentCorrectionRequest(
                    source_item_id: segment.source_item_id,
                    start_ms: segment.start_ms,
                    end_ms: segment.end_ms,
                    title: title,
                    artist: artist,
                    album: album
                )
                _ = try await backend.postJSON(
                    "/v1/jobs/\(jobID)/segments/correct",
                    command: backendCommand,
                    body: payload,
                    type: SegmentCorrectionResponse.self
                )
                let response = try await refreshJobSnapshot(jobID)
                cache(response: response, source: response.items.first?.input_value ?? jobID)
                await refreshLibrary()
            } catch {
                viewState = .error(loc(languageCode, "Correction failed", "Korrektur fehlgeschlagen", "Fallo la correccion", "Echec de la correction"))
            }
        }
    }

    func exportCurrentResults(format: String) {
        guard let jobID = result?.job.id else { return }
        Task {
            do {
                let response = try await backend.getJSON(
                    "/v1/jobs/\(jobID)/export",
                    command: backendCommand,
                    queryItems: [URLQueryItem(name: "format", value: format)],
                    type: ExportResponse.self
                )
                let panel = NSSavePanel()
                panel.nameFieldStringValue = response.filename
                if panel.runModal() == .OK, let url = panel.url {
                    try response.content.write(to: url, atomically: true, encoding: .utf8)
                }
            } catch {
                viewState = .error(loc(languageCode, "Export failed", "Export fehlgeschlagen", "Fallo la exportacion", "Echec de l'export"))
            }
        }
    }

    func showMainWindow() {
        NSApp.activate(ignoringOtherApps: true)
        NotificationCenter.default.post(name: .musicFetchFocusInput, object: nil)
    }

    func reveal(_ path: String) {
        NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: path)])
    }

    func copy(_ value: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(value, forType: .string)
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
                viewState = .error(loc(languageCode, "Install failed", "Installation fehlgeschlagen", "Fallo la instalacion", "Echec de l'installation"))
            }
        }
    }

    func toggleMicrophoneRecording() {
        if captureState == .recordingMic {
            stopMicrophoneRecording()
            return
        }
        guard captureState == .idle else { return }
        captureState = .startingMic
        Task {
            do {
                _ = try await microphoneRecorder.start()
                captureState = .recordingMic
            } catch {
                captureState = .idle
                viewState = .error(error.localizedDescription)
            }
        }
    }

    func toggleSystemAudioRecording() {
        if captureState == .recordingSystem {
            stopSystemAudioRecording()
            return
        }
        guard captureState == .idle else { return }
        captureState = .startingSystem
        Task {
            do {
                _ = try await systemAudioRecorder.start()
                captureState = .recordingSystem
            } catch {
                captureState = .idle
                viewState = .error(error.localizedDescription)
            }
        }
    }

    func makeSegmentViewModel(from payload: SegmentPayload) -> SegmentViewModel {
        let label = payload.kind ?? "matched_track"
        let quality: String?
        switch payload.confidence {
        case 0.85...:
            quality = loc(languageCode, "High", "Sicher", "Alta", "Elevee")
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
            subtitle = loc(languageCode, "No song here.", "Hier ist kein Song.", "Sin cancion.", "Pas de musique.")
            detail = loc(languageCode, "Speech", "Kein Song erkannt", "Voz", "Voix")
        case "music_unresolved":
            accent = .orange
            timeline = .orange.opacity(0.75)
            title = payload.track.map { Self.displayTitle(for: $0) } ?? loc(languageCode, "Music found", "Musik erkannt", "Musica detectada", "Musique detectee")
            subtitle = payload.track?.album ?? loc(languageCode, "No solid match.", "Nicht sicher zugeordnet.", "Sin coincidencia firme.", "Pas de correspondance sure.")
            detail = loc(languageCode, "Unresolved", "Nicht sicher zugeordnet", "Sin resolver", "Non resolu")
        case "silence_or_fx":
            accent = .secondary
            timeline = .secondary.opacity(0.28)
            title = loc(languageCode, "No audio", "Kein relevanter Audioinhalt", "Sin audio util", "Pas d'audio utile")
            subtitle = loc(languageCode, "Silence or FX.", "Stille oder Effekte.", "Silencio o FX.", "Silence ou FX.")
            detail = loc(languageCode, "Silence", "Kein Audioinhalt", "Silencio", "Silence")
        default:
            accent = .accentColor
            timeline = .accentColor.opacity(0.85)
            title = payload.track.map { Self.displayTitle(for: $0) } ?? loc(languageCode, "Song found", "Song erkannt", "Cancion detectada", "Titre detecte")
            subtitle = payload.track?.album ?? loc(languageCode, "Matched song", "Song erkannt", "Cancion detectada", "Titre detecte")
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
            statusBadge: payload.repeat_group_id == nil ? nil : loc(languageCode, "Repeat", "Wiederholt", "Repite", "Repete"),
            metadataHint: payload.metadata_hints?.first,
            primaryLinks: primaryLinks,
            overflowLinks: overflowLinks,
            isInteractive: payload.track != nil,
            repeatGroupID: payload.repeat_group_id
        )
    }

    private func stopMicrophoneRecording() {
        captureState = .stoppingMic
        guard let url = microphoneRecorder.stop() else {
            captureState = .idle
            viewState = .idle
            return
        }
        let stagedURL = stageCapturedFile(url, prefix: "mic")
        inputValue = stagedURL.path
        captureState = .idle
        submitInput(stagedURL.path, switchToAnalyze: true)
    }

    private func stopSystemAudioRecording() {
        captureState = .stoppingSystem
        Task {
            do {
                let url = try await systemAudioRecorder.stop()
                captureState = .idle
                if let url {
                    let stagedURL = stageCapturedFile(url, prefix: "system")
                    inputValue = stagedURL.path
                    submitInput(stagedURL.path, switchToAnalyze: true)
                } else {
                    viewState = .idle
                }
            } catch {
                captureState = .idle
                viewState = .error(error.localizedDescription)
            }
        }
    }

    private func submitInput(_ rawValue: String, switchToAnalyze: Bool) {
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        guard captureState == .idle else { return }
        startAnalysisPhases()
        Task {
            do {
                let options = currentJobOptions()
                let existsAsLocalFile = FileManager.default.fileExists(atPath: trimmed)
                let jobResponse: BackendCreateJobResponse
                if existsAsLocalFile {
                    jobResponse = try await backend.uploadFile("/v1/uploads", command: backendCommand, fileURL: URL(fileURLWithPath: trimmed), options: options, type: BackendCreateJobResponse.self)
                } else {
                    let payload = BackendJobCreateRequest(inputs: [trimmed], options: options)
                    jobResponse = try await backend.postJSON("/v1/jobs", command: backendCommand, body: payload, type: BackendCreateJobResponse.self)
                }
                inputValue = ""
                route.libraryJobID = jobResponse.job_id
                route.storageJobID = jobResponse.job_id
                submittedJobIDs.insert(jobResponse.job_id)
                if switchToAnalyze {
                    route.workspace = .analyze
                }
                subscribeToJob(jobResponse.job_id)
                await refreshLibrary()
                let response = try await refreshJobSnapshot(jobResponse.job_id)
                cache(response: response, source: trimmed)
                await refreshStorage(jobID: jobResponse.job_id)
            } catch {
                stopAnalysisPhases()
                viewState = .error(error.localizedDescription)
            }
        }
    }

    private func startAnalysisPhases() {
        analysisPhaseTask?.cancel()
        let phases = [
            loc(languageCode, "Preparing audio", "Audio wird vorbereitet", "Preparando audio", "Preparation audio"),
            loc(languageCode, "Matching music", "Musik wird erkannt", "Detectando musica", "Identification de la musique"),
            loc(languageCode, "Building timeline", "Timeline wird aufgebaut", "Creando timeline", "Creation de la timeline"),
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
                let interval: Duration = self.hasActiveJob ? .seconds(2) : .seconds(6)
                try? await Task.sleep(for: interval)
            }
        }
    }

    private var hasActiveJob: Bool {
        libraryEntries.contains { ["queued", "running"].contains($0.status) }
    }

    private func pollLiveState() async {
        await refreshLibrary()
        reconcileStreams()
        if hasActiveJob {
            await fetchSystemResources()
        }
        if route.workspace == .storage, shouldRefreshStorage(for: route.storageJobID) {
            await refreshStorage(jobID: route.storageJobID)
        }
        guard let jobID = activeJobID else { return }
        guard let job = libraryEntries.first(where: { $0.job_id == jobID }) else { return }
        if ["queued", "running"].contains(job.status) {
            _ = try? await refreshJobSnapshot(jobID)
        }
    }

    @discardableResult
    private func refreshJobSnapshot(_ jobID: String) async throws -> AnalyzeResponse {
        let response = try await backend.getJSON("/v1/jobs/\(jobID)/snapshot", command: backendCommand, type: AnalyzeResponse.self)
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
            viewState = .analyzing(response.events?.last?.message ?? loc(languageCode, "Working", "Laeuft", "Procesando", "En cours"))
        case "failed":
            stopAnalysisPhases()
            let failureMessage =
                response.job.error ??
                response.events?.last(where: { $0.level == "error" })?.message ??
                response.events?.last?.message ??
                loc(languageCode, "Analysis failed", "Analyse fehlgeschlagen", "Fallo el analisis", "Echec de l'analyse")
            viewState = .error(failureMessage)
        case "canceled":
            stopAnalysisPhases()
            viewState = .error(loc(languageCode, "Analysis canceled", "Analyse abgebrochen", "Analisis cancelado", "Analyse annulee"))
        default:
            stopAnalysisPhases()
            viewState = .showingResults
        }
    }

    private func cache(response: AnalyzeResponse, source: String) {
        cachedResults[response.job.id] = response
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

    private func currentJobOptions() -> BackendJobOptions {
        let analysisMode = AnalysisMode(rawValue: UserDefaults.standard.string(forKey: analysisModeDefaultsKey) ?? AnalysisMode.auto.rawValue) ?? .auto
        let recallProfile = RecallProfile(rawValue: UserDefaults.standard.string(forKey: recallProfileDefaultsKey) ?? RecallProfile.maxRecall.rawValue) ?? .maxRecall
        return BackendJobOptions(
            prefer_separation: UserDefaults.standard.object(forKey: preferSeparationDefaultsKey) as? Bool ?? true,
            analysis_mode: analysisMode.rawValue,
            recall_profile: recallProfile.rawValue,
            enable_metadata_hints: UserDefaults.standard.object(forKey: metadataHintsDefaultsKey) as? Bool ?? true,
            enable_repeat_detection: UserDefaults.standard.object(forKey: repeatDetectionDefaultsKey) as? Bool ?? true,
            max_windows: 24,
            max_segments: 360,
            max_probes_per_segment: 3,
            max_provider_calls: 420
        )
    }

    private func stageCapturedFile(_ sourceURL: URL, prefix: String) -> URL {
        let cachesRoot = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first
            ?? FileManager.default.temporaryDirectory
        let directory = cachesRoot
            .appendingPathComponent("Music Fetch", isDirectory: true)
            .appendingPathComponent("Recordings", isDirectory: true)
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let destination = directory
            .appendingPathComponent("music-fetch-\(prefix)-\(UUID().uuidString)")
            .appendingPathExtension(sourceURL.pathExtension.isEmpty ? "m4a" : sourceURL.pathExtension)
        do {
            if FileManager.default.fileExists(atPath: destination.path) {
                try FileManager.default.removeItem(at: destination)
            }
            try FileManager.default.copyItem(at: sourceURL, to: destination)
            return destination
        } catch {
            return sourceURL
        }
    }

    private func requestNotificationAuthorization() async {
        _ = try? await UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound])
    }

    private func notifyCompletion(response: AnalyzeResponse) async {
        let content = UNMutableNotificationContent()
        let matched = response.segments.filter { $0.track != nil }.count
        switch response.job.status {
        case "succeeded":
            content.title = loc(languageCode, "Analysis complete", "Analyse fertig", "Analisis listo", "Analyse terminee")
            content.body = Self.topMatchSummary(from: response)
                ?? "\(matched) " + loc(languageCode, "tracks matched", "Treffer", "coincidencias", "correspondances")
        case "partial_failed":
            content.title = loc(languageCode, "Analysis finished with issues", "Analyse mit Hinweisen fertig", "Analisis con avisos", "Analyse avec avis")
            content.body = "\(matched) " + loc(languageCode, "tracks matched", "Treffer", "coincidencias", "correspondances")
        case "failed":
            content.title = loc(languageCode, "Analysis failed", "Analyse fehlgeschlagen", "Fallo el analisis", "Echec de l'analyse")
            content.body = response.job.error ?? loc(languageCode, "Open Music Fetch for details.", "Oeffne Music Fetch fuer Details.", "Abre Music Fetch para detalles.", "Ouvrez Music Fetch pour les details.")
        case "canceled":
            return
        default:
            return
        }
        content.sound = .default
        let request = UNNotificationRequest(identifier: "music-fetch-\(response.job.id)", content: content, trigger: nil)
        try? await UNUserNotificationCenter.current().add(request)
    }

    private func subscribeToJob(_ jobID: String) {
        submittedJobIDs.insert(jobID)
        guard eventStreamTasks[jobID] == nil else { return }
        let task = Task { [weak self] in
            guard let self else { return }
            await self.backend.streamEvents(jobID: jobID, command: self.backendCommand) { payload in
                let segmentCount: Int
                let matchedCount: Int
                if let response = self.cachedResults[jobID] {
                    segmentCount = response.segments.count
                    matchedCount = response.segments.filter { $0.track != nil }.count
                } else {
                    segmentCount = self.jobProgress[jobID]?.segmentCount ?? 0
                    matchedCount = self.jobProgress[jobID]?.matchedCount ?? 0
                }

                if let data = payload.data(using: .utf8),
                   let event = try? JSONDecoder().decode(JobEventPayload.self, from: data)
                {
                    let progress = JobProgress(
                        status: "running",
                        message: event.message,
                        updatedAt: Date(),
                        segmentCount: segmentCount,
                        matchedCount: matchedCount
                    )
                    self.jobProgress[jobID] = progress
                    if self.activeJobID == jobID {
                        self.viewState = .analyzing(event.message)
                    }
                }

                Task {
                    if let response = try? await self.refreshJobSnapshot(jobID) {
                        self.cache(response: response, source: response.items.first?.input_value ?? jobID)
                        if self.activeJobID == jobID {
                            self.latestQuickCaptureSummary = Self.topMatchSummary(from: response) ?? self.latestQuickCaptureSummary
                        }
                        let matched = response.segments.filter { $0.track != nil }.count
                        self.jobProgress[jobID] = JobProgress(
                            status: response.job.status,
                            message: response.events?.last?.message ?? "",
                            updatedAt: Date(),
                            segmentCount: response.segments.count,
                            matchedCount: matched
                        )
                        if Self.isTerminalStatus(response.job.status) {
                            await self.maybeNotify(response: response)
                        }
                    }
                }
            }
            _ = await MainActor.run {
                self.eventStreamTasks.removeValue(forKey: jobID)
            }
        }
        eventStreamTasks[jobID] = task
    }

    private static func isTerminalStatus(_ status: String) -> Bool {
        ["succeeded", "partial_failed", "failed", "canceled"].contains(status)
    }

    private func maybeNotify(response: AnalyzeResponse) async {
        guard !notifiedCompletionJobIDs.contains(response.job.id) else { return }
        notifiedCompletionJobIDs.insert(response.job.id)
        await notifyCompletion(response: response)
    }

    func snapshot(for jobID: String) async -> AnalyzeResponse? {
        if let cached = cachedResults[jobID] { return cached }
        do {
            let response = try await backend.getJSON(
                "/v1/jobs/\(jobID)/snapshot",
                command: backendCommand,
                type: AnalyzeResponse.self
            )
            cachedResults[jobID] = response
            return response
        } catch {
            return nil
        }
    }

    func retrySegments(jobID: String) {
        Task {
            do {
                let payload = RetrySegmentsRequest(source_item_id: nil, options: currentJobOptions())
                _ = try await backend.postJSON(
                    "/v1/jobs/\(jobID)/segments/retry",
                    command: backendCommand,
                    body: payload,
                    type: RetryResponse.self
                )
                _ = try await refreshJobSnapshot(jobID)
                await refreshLibrary()
            } catch {
                // surfaced in the view's own error path
            }
        }
    }

    func cancelJob(_ jobID: String) {
        Task {
            do {
                _ = try await backend.postJSON(
                    "/v1/jobs/\(jobID)/cancel",
                    command: backendCommand,
                    body: [String: String](),
                    type: CancelResponse.self
                )
                _ = try? await refreshJobSnapshot(jobID)
                await refreshLibrary()
            } catch {
                // ignored
            }
        }
    }

    /// Permanently delete a library run (files + history). Mirrors the
    /// "Delete permanently" context-menu action.
    ///
    /// Cache invalidation invariants:
    ///   1. Clear our in-memory caches for the job FIRST so no stale UI
    ///      snapshot lingers after the backend drops the row.
    ///   2. Refresh BOTH library and storage — they're derived from the same
    ///      DB rows, so a delete invalidates both. (Previously, cleanup
    ///      refreshed the library but not storage, leading to stale counts.)
    ///   3. If the deleted job was the currently-focused library/storage
    ///      scope, move the selection to whatever's left (or clear it).
    func deleteJob(_ jobID: String) {
        let wasFocusedLibrary = route.libraryJobID == jobID
        let wasFocusedStorage = route.storageJobID == jobID
        Task {
            do {
                _ = try await backend.deleteJSON(
                    "/v1/jobs/\(jobID)",
                    command: backendCommand,
                    type: DeleteJobResponse.self
                )
                // 1. Purge every in-memory cache for this job.
                cachedResults.removeValue(forKey: jobID)
                jobProgress.removeValue(forKey: jobID)
                openedPrimaryLinkJobIDs.remove(jobID)
                notifiedCompletionJobIDs.remove(jobID)
                submittedJobIDs.remove(jobID)
                eventStreamTasks[jobID]?.cancel()
                eventStreamTasks.removeValue(forKey: jobID)
                // 2. Reload library + storage atomically so UI counts stay
                //    coherent. Both calls hit the same backend; cheap.
                await refreshLibrary()
                await refreshStorage(jobID: nil)
                // 3. Move selection if the deleted run was focused.
                if wasFocusedLibrary {
                    route.libraryJobID = orderedLibraryEntries.first?.job_id
                }
                if wasFocusedStorage {
                    route.storageJobID = orderedLibraryEntries.first?.job_id
                }
                if result?.job.id == jobID {
                    result = nil
                    selectedSegmentID = nil
                    viewState = .idle
                }
            } catch {
                viewState = .error(loc(languageCode,
                                        "Delete failed",
                                        "Loeschen fehlgeschlagen",
                                        "Fallo al eliminar",
                                        "Echec de la suppression"))
            }
        }
    }

    func exportResults(jobID: String, format: String) {
        Task {
            do {
                let response = try await backend.getJSON(
                    "/v1/jobs/\(jobID)/export",
                    command: backendCommand,
                    queryItems: [URLQueryItem(name: "format", value: format)],
                    type: ExportResponse.self
                )
                let panel = NSSavePanel()
                panel.nameFieldStringValue = response.filename
                if panel.runModal() == .OK, let url = panel.url {
                    try response.content.write(to: url, atomically: true, encoding: .utf8)
                }
            } catch {
                viewState = .error(loc(languageCode, "Export failed", "Export fehlgeschlagen", "Fallo la exportacion", "Echec de l'export"))
            }
        }
    }

    func fetchSystemResources() async {
        do {
            systemResources = try await backend.getJSON(
                "/v1/system/resources",
                command: backendCommand,
                type: SystemResources.self
            )
        } catch {
            // keep the previous value; surfaces render a fallback
        }
    }

    private func reconcileStreams() {
        for entry in libraryEntries where ["queued", "running"].contains(entry.status) {
            if eventStreamTasks[entry.job_id] == nil {
                subscribeToJob(entry.job_id)
            }
        }
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
            process.currentDirectoryURL = BackendController.defaultWorkingDirectory()
            var environment = BackendController.baseEnvironment()
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
                environment["PATH"],
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

    private static func resolveInitialBackendCommand(saved: String?) -> String {
        let bundled = defaultBackendCommand()
        let trimmedSaved = saved?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !trimmedSaved.isEmpty else {
            return bundled
        }
        if shouldReplaceSavedBackend(trimmedSaved, bundled: bundled) {
            return bundled
        }
        return trimmedSaved
    }

    private static func shouldReplaceSavedBackend(_ saved: String, bundled: String) -> Bool {
        guard bundled != saved else {
            return false
        }
        guard bundled.hasPrefix("/"), FileManager.default.isExecutableFile(atPath: bundled) else {
            return false
        }
        return saved == "music-fetch" || saved == "/usr/local/bin/music-fetch" || saved.contains("/.local/bin/music-fetch")
    }

    private static func topMatchSummary(from response: AnalyzeResponse) -> String? {
        guard let first = response.segments.first(where: { $0.track != nil }), let track = first.track else {
            return nil
        }
        if let artist = track.artist, !artist.isEmpty {
            return "\(artist) - \(track.title)"
        }
        return track.title
    }

    private static func isRecoverableBackendStartupError(_ message: String) -> Bool {
        let knownFragments = [
            "Backend server did not start in time",
            "Could not launch backend command",
            "NSURLErrorDomain Code=-1004",
            "could not be completed. (NSURLErrorDomain error -1004)",
        ]
        return knownFragments.contains { message.localizedCaseInsensitiveContains($0) }
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
