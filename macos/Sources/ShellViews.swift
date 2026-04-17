import AppKit
import SwiftUI

// MARK: - Root

struct ContentView: View {
    @ObservedObject var model: AppModel
    @State private var columnVisibility: NavigationSplitViewVisibility = .all
    @State private var inspectorVisible: Bool = true
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        NavigationSplitView(columnVisibility: $columnVisibility) {
            SidebarView(model: model)
                .navigationSplitViewColumnWidth(min: 208, ideal: 224, max: 260)
        } detail: {
            WorkspaceContainer(model: model, inspectorVisible: $inspectorVisible)
        }
        .navigationSplitViewStyle(.balanced)
        .frame(minWidth: 1080, minHeight: 720)
        .task { model.bootstrap() }
        .onReceive(NotificationCenter.default.publisher(for: .musicFetchAnalyze)) { _ in
            model.analyze()
        }
        .onReceive(NotificationCenter.default.publisher(for: .musicFetchShowDiagnostics)) { _ in
            UserDefaults.standard.set(SettingsTab.diagnostics.rawValue, forKey: settingsTabDefaultsKey)
            openSettings()
        }
    }
}

// MARK: - Sidebar

struct SidebarView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
                .padding(.horizontal, Theme.Space.m)
                .padding(.top, Theme.Space.m)
                .padding(.bottom, Theme.Space.s)

            List(selection: workspaceBinding) {
                Section {
                    ForEach(WorkspaceSection.allCases) { section in
                        NavigationLink(value: section) {
                            SidebarRow(
                                section: section,
                                code: model.languageCode,
                                runningCount: runningCount,
                                libraryCount: model.libraryEntries.count
                            )
                        }
                    }
                }
            }
            .listStyle(.sidebar)
            .scrollContentBackground(.hidden)

            Divider().opacity(0.6)

            SidebarFooter(model: model)
        }
    }

    private var header: some View {
        HStack(spacing: Theme.Space.xs) {
            ZStack {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(Color.accentColor.opacity(0.16))
                    .frame(width: 30, height: 30)
                Image(systemName: "waveform")
                    .font(.system(size: 14, weight: .semibold, design: .rounded))
                    .foregroundStyle(.tint)
            }
            VStack(alignment: .leading, spacing: 1) {
                Text("Music Fetch")
                    .font(Theme.Font.title)
                Text(statusLine)
                    .font(Theme.Font.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer(minLength: 0)
        }
    }

    private var runningCount: Int {
        model.libraryEntries.filter { ["queued", "running"].contains($0.status) }.count
    }

    private var statusLine: String {
        if runningCount > 0 {
            return "\(runningCount) " + loc(model.languageCode, "running", "laufend", "en curso", "en cours")
        }
        return loc(model.languageCode, "Ready", "Bereit", "Listo", "Pret")
    }

    private var workspaceBinding: Binding<WorkspaceSection?> {
        Binding(
            get: { model.selectedWorkspace },
            set: { value in
                guard let value else { return }
                model.activateWorkspace(value)
            }
        )
    }
}

private struct SidebarRow: View {
    let section: WorkspaceSection
    let code: String
    let runningCount: Int
    let libraryCount: Int

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: section.icon)
                .font(.system(size: 13, weight: .medium))
                .frame(width: 18)
                .foregroundStyle(.secondary)
            Text(section.title(code))
                .font(Theme.Font.rowTitle)
            Spacer(minLength: 0)
            if let badge {
                Text(badge)
                    .font(.system(size: 10, weight: .semibold))
                    .monospacedDigit()
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(Color.primary.opacity(0.08), in: Capsule())
            }
        }
    }

    private var badge: String? {
        switch section {
        case .analyze:
            return runningCount > 0 ? "\(runningCount)" : nil
        case .library:
            return libraryCount > 0 ? "\(libraryCount)" : nil
        case .storage:
            return nil
        }
    }
}

private struct SidebarFooter: View {
    @ObservedObject var model: AppModel

    var body: some View {
        HStack(spacing: Theme.Space.xs) {
            StatusDot(color: dotColor, pulsing: pulsing)
            VStack(alignment: .leading, spacing: 1) {
                Text(titleLine)
                    .font(Theme.Font.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                if let resources = model.systemResources {
                    Text(resourceLine(resources))
                        .font(.system(size: 10, weight: .regular))
                        .foregroundStyle(Theme.Palette.textTertiary)
                        .monospacedDigit()
                        .lineLimit(1)
                }
            }
            Spacer(minLength: 4)
            SettingsLink {
                Image(systemName: "gearshape")
                    .font(.system(size: 13, weight: .medium))
                    .foregroundStyle(.secondary)
                    .frame(width: 22, height: 22)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help(loc(model.languageCode, "Settings", "Einstellungen", "Ajustes", "Reglages"))
        }
        .padding(.horizontal, Theme.Space.m)
        .padding(.vertical, Theme.Space.s)
    }

    private var dotColor: Color {
        switch model.captureState {
        case .recordingMic: return Theme.Palette.micTint
        case .recordingSystem: return Theme.Palette.systemTint
        case .startingMic, .stoppingMic, .startingSystem, .stoppingSystem: return Theme.Palette.warningTint
        case .idle:
            if model.libraryEntries.contains(where: { ["queued", "running"].contains($0.status) }) {
                return Theme.Palette.accent
            }
            return Theme.Palette.successTint
        }
    }

    private var pulsing: Bool {
        if case .recordingMic = model.captureState { return true }
        if case .recordingSystem = model.captureState { return true }
        return model.libraryEntries.contains(where: { ["queued", "running"].contains($0.status) })
    }

    private var titleLine: String {
        model.statusTitle
    }

    private func resourceLine(_ resources: SystemResources) -> String {
        let active = resources.active_jobs
        let max = resources.max_workers
        let parallel = "\(active)/\(max)"
        let ram = resources.ram_gb > 0 ? String(format: "%.0f GB", resources.ram_gb) : "\(resources.cpu_count) CPU"
        return "\(parallel) · \(ram)"
    }
}

// MARK: - Workspace container

struct WorkspaceContainer: View {
    @ObservedObject var model: AppModel
    @Binding var inspectorVisible: Bool

    var body: some View {
        Group {
            switch model.selectedWorkspace {
            case .analyze:
                AnalyzeView(model: model)
                    .inspector(isPresented: analyzeInspectorBinding) {
                        InspectorView(model: model)
                            .inspectorColumnWidth(min: 300, ideal: 340, max: 440)
                    }
            case .library:
                LibraryView(model: model)
            case .storage:
                StorageView(model: model)
            }
        }
        .navigationTitle(model.selectedWorkspace.title(model.languageCode))
        .navigationSubtitle(model.selectedWorkspace.subtitle(model.languageCode))
        .toolbar {
            if model.selectedWorkspace == .analyze {
                ToolbarItem(placement: .primaryAction) {
                    Button {
                        withAnimation(.easeInOut(duration: 0.18)) {
                            inspectorVisible.toggle()
                        }
                    } label: {
                        Label(loc(model.languageCode, "Inspector", "Inspector", "Inspector", "Inspecteur"),
                              systemImage: "sidebar.right")
                    }
                    .help(loc(model.languageCode, "Toggle inspector", "Inspector umschalten", "Alternar inspector", "Basculer l'inspecteur"))
                    .keyboardShortcut("i", modifiers: [.command, .option])
                }
            }
        }
    }

    private var analyzeInspectorBinding: Binding<Bool> {
        Binding(
            get: { inspectorVisible && model.selectedWorkspace == .analyze },
            set: { inspectorVisible = $0 }
        )
    }
}

// MARK: - Analyze workspace

struct AnalyzeView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: Theme.Space.l) {
                InputCard(model: model)
                ResultsSectionView(model: model)
            }
            .padding(.horizontal, Theme.Space.xl)
            .padding(.vertical, Theme.Space.l)
            .frame(maxWidth: 940, alignment: .topLeading)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .background(Theme.Palette.surface)
    }
}

struct InputCard: View {
    @ObservedObject var model: AppModel
    @AppStorage(preferredInputDefaultsKey) private var preferredInputRaw = PreferredInput.link.rawValue
    @FocusState private var focused: Bool

    var body: some View {
        Panel(padding: Theme.Space.l, radius: Theme.Radius.panel) {
            VStack(alignment: .leading, spacing: Theme.Space.m) {
                HStack(alignment: .firstTextBaseline) {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(heroTitle)
                            .font(Theme.Font.display)
                        Text(subtitle)
                            .font(Theme.Font.body)
                            .foregroundStyle(.secondary)
                            .lineLimit(2, reservesSpace: false)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                    Spacer(minLength: 0)
                    sourcePicker
                }

                sourceRow
                    .frame(height: 44)

                if case let .analyzing(phase) = model.viewState {
                    phaseBar(phase)
                } else if case let .error(message) = model.viewState {
                    errorBar(message)
                }
            }
        }
        .onReceive(NotificationCenter.default.publisher(for: .musicFetchFocusInput)) { _ in
            focused = true
        }
        .onAppear {
            if preferredInput == .link { focused = true }
        }
    }

    private var sourcePicker: some View {
        Picker("", selection: preferredBinding) {
            ForEach(PreferredInput.allCases) { input in
                Label(input.title(model.languageCode), systemImage: input.systemImage)
                    .tag(input)
            }
        }
        .pickerStyle(.segmented)
        .labelsHidden()
        .controlSize(.regular)
        .fixedSize()
    }

    @ViewBuilder
    private var sourceRow: some View {
        switch preferredInput {
        case .link:
            linkRow
        case .file:
            fileRow
        case .microphone:
            captureRow(icon: "mic.fill", status: micStatus, tint: Theme.Palette.micTint)
        case .system:
            captureRow(icon: "speaker.wave.2.fill", status: systemStatus, tint: Theme.Palette.systemTint)
        }
    }

    private var linkRow: some View {
        HStack(spacing: Theme.Space.xs) {
            leadingIcon("link", tint: Color.accentColor)
            TextField(
                loc(model.languageCode, "Paste a YouTube, TikTok, or Vimeo link",
                    "YouTube-, TikTok- oder Vimeo-Link einfuegen",
                    "Pega un enlace de YouTube, TikTok o Vimeo",
                    "Collez un lien YouTube, TikTok ou Vimeo"),
                text: $model.inputValue
            )
            .textFieldStyle(.plain)
            .font(.system(size: 15))
            .focused($focused)
            .submitLabel(.go)
            .onSubmit { runPrimary() }

            primaryButton(Color.accentColor)
        }
        .padding(.horizontal, Theme.Space.s)
        .padding(.vertical, 6)
        .background(
            RoundedRectangle(cornerRadius: Theme.Radius.card, style: .continuous)
                .fill(Theme.Palette.surfaceSunken.opacity(0.55))
        )
        .overlay(
            RoundedRectangle(cornerRadius: Theme.Radius.card, style: .continuous)
                .strokeBorder(Theme.Palette.hairline, lineWidth: 0.5)
        )
    }

    private var fileRow: some View {
        HStack(spacing: Theme.Space.xs) {
            leadingIcon("doc", tint: Color.accentColor)
            TextField(
                loc(model.languageCode, "File path on this Mac",
                    "Dateipfad auf diesem Mac",
                    "Ruta de archivo en este Mac",
                    "Chemin du fichier sur ce Mac"),
                text: $model.inputValue
            )
            .textFieldStyle(.plain)
            .font(.system(size: 15))
            .focused($focused)
            .submitLabel(.go)
            .onSubmit { runPrimary() }

            Button {
                model.chooseFile()
            } label: {
                Image(systemName: "folder")
                    .font(.system(size: 13, weight: .semibold))
                    .foregroundStyle(.secondary)
                    .frame(width: 32, height: 32)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help(loc(model.languageCode, "Choose file", "Datei waehlen", "Elegir archivo", "Choisir un fichier"))

            primaryButton(Color.accentColor)
        }
        .padding(.horizontal, Theme.Space.s)
        .padding(.vertical, 6)
        .background(
            RoundedRectangle(cornerRadius: Theme.Radius.card, style: .continuous)
                .fill(Theme.Palette.surfaceSunken.opacity(0.55))
        )
        .overlay(
            RoundedRectangle(cornerRadius: Theme.Radius.card, style: .continuous)
                .strokeBorder(Theme.Palette.hairline, lineWidth: 0.5)
        )
    }

    private func captureRow(icon: String, status: String, tint: Color) -> some View {
        HStack(spacing: Theme.Space.s) {
            leadingIcon(icon, tint: tint)
            Text(status)
                .font(.system(size: 15, weight: .medium))
                .foregroundStyle(.primary)
            Spacer(minLength: 4)
            primaryButton(tint)
        }
        .padding(.horizontal, Theme.Space.s)
        .padding(.vertical, 6)
        .background(
            RoundedRectangle(cornerRadius: Theme.Radius.card, style: .continuous)
                .fill(tint.opacity(0.06))
        )
        .overlay(
            RoundedRectangle(cornerRadius: Theme.Radius.card, style: .continuous)
                .strokeBorder(tint.opacity(0.18), lineWidth: 0.5)
        )
    }

    @ViewBuilder
    private func leadingIcon(_ name: String, tint: Color) -> some View {
        Image(systemName: name)
            .font(.system(size: 13, weight: .semibold))
            .foregroundStyle(tint)
            .frame(width: 28, height: 28)
            .background(tint.opacity(0.14), in: RoundedRectangle(cornerRadius: 7, style: .continuous))
    }

    private func primaryButton(_ tint: Color) -> some View {
        Button {
            runPrimary()
        } label: {
            Text(primaryLabel)
                .font(.system(size: 13, weight: .semibold))
                .frame(minWidth: 90)
        }
        .buttonStyle(.borderedProminent)
        .controlSize(.regular)
        .tint(tint)
        .keyboardShortcut(.return, modifiers: [.command])
        .disabled(!canRun)
    }

    private func phaseBar(_ phase: String) -> some View {
        HStack(spacing: Theme.Space.s) {
            ProgressView().controlSize(.small)
            Text(phase)
                .font(Theme.Font.body)
                .foregroundStyle(.secondary)
                .lineLimit(1)
            Spacer()
        }
    }

    private func errorBar(_ message: String) -> some View {
        HStack(alignment: .top, spacing: Theme.Space.xs) {
            Image(systemName: "exclamationmark.triangle.fill")
                .foregroundStyle(Theme.Palette.dangerTint)
                .font(.system(size: 12, weight: .semibold))
            Text(message)
                .font(Theme.Font.body)
                .foregroundStyle(.primary)
                .lineLimit(3)
                .fixedSize(horizontal: false, vertical: true)
            Spacer()
        }
        .padding(Theme.Space.s)
        .background(Theme.Palette.dangerTint.opacity(0.08), in: RoundedRectangle(cornerRadius: Theme.Radius.row, style: .continuous))
    }

    // MARK: computed

    private var heroTitle: String {
        loc(model.languageCode, "Find music", "Musik finden", "Buscar musica", "Trouver la musique")
    }

    private var micStatus: String {
        switch model.captureState {
        case .recordingMic:
            return loc(model.languageCode, "Recording from microphone",
                       "Aufnahme vom Mikrofon",
                       "Grabando del microfono",
                       "Enregistrement microphone")
        case .stoppingMic:
            return loc(model.languageCode, "Stopping", "Stoppt", "Deteniendo", "Arret")
        default:
            return loc(model.languageCode, "Tap start to listen",
                       "Tippe Start zum Hoeren",
                       "Pulsa iniciar para escuchar",
                       "Appuyez pour ecouter")
        }
    }

    private var systemStatus: String {
        switch model.captureState {
        case .recordingSystem:
            return loc(model.languageCode, "Capturing system audio",
                       "System-Audio wird aufgenommen",
                       "Capturando audio del sistema",
                       "Capture audio systeme")
        case .stoppingSystem:
            return loc(model.languageCode, "Stopping", "Stoppt", "Deteniendo", "Arret")
        default:
            return loc(model.languageCode, "Tap start to capture",
                       "Tippe Start zum Aufnehmen",
                       "Pulsa iniciar para capturar",
                       "Appuyez pour capturer")
        }
    }

    private var subtitle: String {
        switch preferredInput {
        case .link:
            return loc(model.languageCode,
                       "Identify every song in a video, mix, or set.",
                       "Jeden Song in Video, Mix oder Set erkennen.",
                       "Identifica cada cancion en video, mezcla o set.",
                       "Identifiez chaque titre d'une video ou d'un mix.")
        case .file:
            return loc(model.languageCode,
                       "Any audio or video file on this Mac.",
                       "Beliebige Audio- oder Videodatei auf diesem Mac.",
                       "Cualquier archivo de audio o video de este Mac.",
                       "Tout fichier audio ou video de ce Mac.")
        case .microphone:
            return loc(model.languageCode,
                       "Listen to music playing nearby.",
                       "Hoere Musik in deiner Umgebung.",
                       "Escucha la musica del entorno.",
                       "Ecoutez la musique autour de vous.")
        case .system:
            return loc(model.languageCode,
                       "Capture whatever is playing on this Mac.",
                       "Nimm auf, was auf diesem Mac laeuft.",
                       "Captura lo que suena en este Mac.",
                       "Capturez ce qui joue sur ce Mac.")
        }
    }

    private var preferredInput: PreferredInput {
        PreferredInput(rawValue: preferredInputRaw) ?? .link
    }

    private var preferredBinding: Binding<PreferredInput> {
        Binding(
            get: { preferredInput },
            set: { newValue in
                preferredInputRaw = newValue.rawValue
                if newValue == .link || newValue == .file { focused = true }
            }
        )
    }

    private var canRun: Bool {
        switch preferredInput {
        case .link, .file:
            return !model.isBusy && !model.inputValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        case .microphone, .system:
            return true
        }
    }

    private var primaryLabel: String {
        switch preferredInput {
        case .link, .file:
            return loc(model.languageCode, "Analyze", "Analysieren", "Analizar", "Analyser")
        case .microphone:
            return (model.captureState == .recordingMic || model.captureState == .stoppingMic)
                ? loc(model.languageCode, "Stop", "Stopp", "Detener", "Arreter")
                : loc(model.languageCode, "Start", "Start", "Iniciar", "Demarrer")
        case .system:
            return (model.captureState == .recordingSystem || model.captureState == .stoppingSystem)
                ? loc(model.languageCode, "Stop", "Stopp", "Detener", "Arreter")
                : loc(model.languageCode, "Start", "Start", "Iniciar", "Demarrer")
        }
    }

    private func runPrimary() {
        switch preferredInput {
        case .link:
            model.analyze()
        case .file:
            let trimmed = model.inputValue.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty { model.chooseFile() } else { model.analyze() }
        case .microphone:
            model.toggleMicrophoneRecording()
        case .system:
            model.toggleSystemAudioRecording()
        }
    }
}

// MARK: - Library workspace

struct LibraryView: View {
    @ObservedObject var model: AppModel
    @State private var searchText: String = ""
    @AppStorage(libraryScopeDefaultsKey) private var scopeRaw = LibraryScope.all.rawValue
    @State private var path: [String] = []

    var body: some View {
        NavigationStack(path: $path) {
            content
                .navigationDestination(for: String.self) { jobID in
                    LibraryJobDetailView(model: model, jobID: jobID)
                }
        }
    }

    private var content: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: Theme.Space.m) {
                scopeRow
                    .padding(.horizontal, Theme.Space.xl)
                    .padding(.top, Theme.Space.l)

                if filteredEntries.isEmpty {
                    emptyState
                } else {
                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 280, maximum: 360), spacing: Theme.Space.s)],
                              spacing: Theme.Space.s) {
                        ForEach(filteredEntries) { entry in
                            NavigationLink(value: entry.job_id) {
                                LibraryRunCard(
                                    entry: entry,
                                    progress: model.jobProgress[entry.job_id],
                                    languageCode: model.languageCode
                                )
                            }
                            .buttonStyle(.plain)
                            .contextMenu {
                                Button(entry.pinned
                                    ? loc(model.languageCode, "Unpin", "Losen", "Desfijar", "Detacher")
                                    : loc(model.languageCode, "Pin", "Pinnen", "Fijar", "Epingler")) {
                                    model.setPinned(jobID: entry.job_id, pinned: !entry.pinned)
                                }
                                if ["running", "queued"].contains(entry.status) {
                                    Button(loc(model.languageCode, "Cancel", "Abbrechen", "Cancelar", "Annuler"),
                                           role: .destructive) {
                                        model.cancelJob(entry.job_id)
                                    }
                                }
                            }
                        }
                    }
                    .padding(.horizontal, Theme.Space.xl)
                    .padding(.bottom, Theme.Space.l)
                }
            }
        }
        .background(Theme.Palette.surface)
        .searchable(text: $searchText, prompt: loc(model.languageCode, "Search runs", "Laeufe suchen", "Buscar analisis", "Rechercher"))
        .refreshable { await model.refreshLibrary() }
    }

    private var scopeRow: some View {
        HStack(spacing: Theme.Space.s) {
            Picker("", selection: scopeBinding) {
                ForEach(LibraryScope.allCases) { scope in
                    Text(scope.title(model.languageCode)).tag(scope)
                }
            }
            .pickerStyle(.segmented)
            .frame(maxWidth: 460)
            Spacer()
            if let resources = model.systemResources, resources.active_jobs > 0 {
                Pill("\(resources.active_jobs)/\(resources.max_workers) " + loc(model.languageCode, "parallel", "parallel", "en paralelo", "en parallele"),
                     icon: "square.stack.3d.up",
                     tint: Theme.Palette.accent)
            }
        }
    }

    private var emptyState: some View {
        VStack(spacing: Theme.Space.m) {
            Image(systemName: "rectangle.stack")
                .font(.system(size: 36, weight: .light))
                .foregroundStyle(.tertiary)
            Text(loc(model.languageCode, "No runs yet", "Noch keine Laeufe", "Sin analisis", "Aucune analyse"))
                .font(Theme.Font.title)
            Text(loc(model.languageCode,
                     "Start an analysis in the Analyze workspace. Runs stream here live.",
                     "Starte eine Analyse im Bereich Analysieren. Laeufe erscheinen hier live.",
                     "Inicia un analisis en Analizar. Los resultados aparecen aqui en vivo.",
                     "Lancez une analyse dans Analyser. Les resultats apparaissent ici en direct."))
                .font(Theme.Font.body)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(maxWidth: 420)
        .frame(maxWidth: .infinity)
        .padding(.vertical, 60)
    }

    private var scope: LibraryScope {
        LibraryScope(rawValue: scopeRaw) ?? .all
    }

    private var scopeBinding: Binding<LibraryScope> {
        Binding(
            get: { scope },
            set: { scopeRaw = $0.rawValue }
        )
    }

    private var filteredEntries: [LibraryEntryPayload] {
        model.orderedLibraryEntries.filter { entry in
            switch scope {
            case .all: break
            case .running:
                if !["running", "queued"].contains(entry.status) { return false }
            case .pinned:
                if !entry.pinned { return false }
            }
            if searchText.isEmpty { return true }
            return entry.title.localizedCaseInsensitiveContains(searchText)
                || entry.input_value.localizedCaseInsensitiveContains(searchText)
        }
    }
}

enum LibraryScope: String, CaseIterable, Identifiable {
    case all
    case running
    case pinned

    var id: String { rawValue }

    func title(_ code: String) -> String {
        switch self {
        case .all: return loc(code, "All", "Alle", "Todos", "Tout")
        case .running: return loc(code, "Running", "Laufend", "En curso", "En cours")
        case .pinned: return loc(code, "Pinned", "Gepinnt", "Fijados", "Epingles")
        }
    }
}

struct LibraryRunCard: View {
    let entry: LibraryEntryPayload
    let progress: JobProgress?
    let languageCode: String

    private var isRunning: Bool {
        ["running", "queued"].contains(entry.status)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: Theme.Space.s) {
            HStack(alignment: .top, spacing: Theme.Space.xs) {
                statusIcon
                VStack(alignment: .leading, spacing: 2) {
                    Text(entry.title)
                        .font(Theme.Font.rowTitle)
                        .lineLimit(2)
                        .multilineTextAlignment(.leading)
                        .fixedSize(horizontal: false, vertical: true)
                    Text(entry.input_value)
                        .font(Theme.Font.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .truncationMode(.middle)
                }
                Spacer(minLength: 0)
                if entry.pinned {
                    Image(systemName: "pin.fill")
                        .font(.system(size: 10, weight: .semibold))
                        .foregroundStyle(.orange)
                }
            }

            if isRunning {
                VStack(alignment: .leading, spacing: 6) {
                    if let message = progress?.message, !message.isEmpty {
                        Text(message)
                            .font(Theme.Font.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    } else {
                        Text(loc(languageCode, "Analyzing", "Analysiere", "Analizando", "Analyse"))
                            .font(Theme.Font.caption)
                            .foregroundStyle(.secondary)
                    }
                    LiveProgressBar(progress: nil, tint: Theme.Palette.accent)
                        .frame(height: 3)
                }
            } else {
                HStack(spacing: Theme.Space.xs) {
                    Pill("\(entry.segment_count)", icon: "rectangle.split.3x1")
                    Pill("\(entry.matched_count)", icon: "checkmark.seal", tint: Theme.Palette.successTint)
                    Pill(formatBytes(entry.artifact_size_bytes), icon: "internaldrive")
                    Spacer()
                    statusBadge
                }
            }
        }
        .padding(Theme.Space.m)
        .frame(maxWidth: .infinity, minHeight: 116, alignment: .topLeading)
        .panelBackground(radius: Theme.Radius.card, elevated: true)
        .overlay(alignment: .topTrailing) {
            if isRunning {
                runningChip
                    .padding(.top, 10)
                    .padding(.trailing, 10)
            }
        }
    }

    private var statusIcon: some View {
        let (name, color) = iconDescriptor
        return ZStack {
            RoundedRectangle(cornerRadius: 6, style: .continuous)
                .fill(color.opacity(0.16))
                .frame(width: 26, height: 26)
            Image(systemName: name)
                .font(.system(size: 11, weight: .semibold))
                .foregroundStyle(color)
        }
    }

    private var iconDescriptor: (String, Color) {
        switch entry.status {
        case "succeeded":
            return ("waveform", Theme.Palette.successTint)
        case "running", "queued":
            return ("waveform.badge.magnifyingglass", Theme.Palette.accent)
        case "failed":
            return ("exclamationmark.triangle.fill", Theme.Palette.dangerTint)
        case "partial_failed":
            return ("exclamationmark.circle.fill", Theme.Palette.warningTint)
        case "canceled":
            return ("xmark.circle", Theme.Palette.textSecondary)
        default:
            return ("music.note", Theme.Palette.textSecondary)
        }
    }

    private var statusBadge: some View {
        let color: Color
        let text: String
        switch entry.status {
        case "succeeded":
            color = Theme.Palette.successTint
            text = loc(languageCode, "Done", "Fertig", "Listo", "Termine")
        case "failed":
            color = Theme.Palette.dangerTint
            text = loc(languageCode, "Failed", "Fehler", "Fallo", "Echec")
        case "partial_failed":
            color = Theme.Palette.warningTint
            text = loc(languageCode, "Partial", "Teilweise", "Parcial", "Partiel")
        case "canceled":
            color = Theme.Palette.textSecondary
            text = loc(languageCode, "Canceled", "Abgebrochen", "Cancelado", "Annule")
        default:
            color = Theme.Palette.textSecondary
            text = entry.status.capitalized
        }
        return Text(text)
            .font(.system(size: 10, weight: .semibold))
            .foregroundStyle(color)
    }

    private var runningChip: some View {
        HStack(spacing: 4) {
            StatusDot(color: Theme.Palette.accent, pulsing: true)
            Text(loc(languageCode, "Live", "Live", "En vivo", "Direct"))
                .font(.system(size: 10, weight: .semibold))
                .foregroundStyle(Theme.Palette.accent)
        }
        .padding(.horizontal, 7)
        .padding(.vertical, 3)
        .background(Theme.Palette.accentSoft, in: Capsule())
    }
}

// MARK: - Library detail

struct LibraryJobDetailView: View {
    @ObservedObject var model: AppModel
    let jobID: String

    @State private var response: AnalyzeResponse?
    @State private var selectedSegmentID: String?
    @State private var zoom: Double = 1.0
    @State private var showOnlySongs: Bool = true
    @State private var refreshTask: Task<Void, Never>?

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: Theme.Space.l) {
                header
                if let response {
                    detail(response)
                } else {
                    ProgressView()
                        .controlSize(.large)
                        .frame(maxWidth: .infinity, minHeight: 220)
                }
            }
            .padding(.horizontal, Theme.Space.xl)
            .padding(.vertical, Theme.Space.l)
            .frame(maxWidth: 940, alignment: .topLeading)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .background(Theme.Palette.surface)
        .task(id: jobID) { await load() }
        .onDisappear {
            refreshTask?.cancel()
            refreshTask = nil
        }
    }

    @ViewBuilder
    private var header: some View {
        if let entry = model.libraryEntries.first(where: { $0.job_id == jobID }) {
            HStack(alignment: .top, spacing: Theme.Space.m) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(entry.title)
                        .font(Theme.Font.largeTitle)
                        .lineLimit(2)
                        .fixedSize(horizontal: false, vertical: true)
                    Text(entry.input_value)
                        .font(Theme.Font.body)
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)
                        .lineLimit(2)
                }
                Spacer()
                headerActions(entry: entry)
            }
        }
    }

    @ViewBuilder
    private func headerActions(entry: LibraryEntryPayload) -> some View {
        HStack(spacing: Theme.Space.xs) {
            Button {
                model.setPinned(jobID: entry.job_id, pinned: !entry.pinned)
            } label: {
                Label(entry.pinned
                      ? loc(model.languageCode, "Unpin", "Losen", "Desfijar", "Detacher")
                      : loc(model.languageCode, "Pin", "Pinnen", "Fijar", "Epingler"),
                      systemImage: entry.pinned ? "pin.slash" : "pin")
            }
            .buttonStyle(.bordered)

            Menu {
                Button("JSON") { model.exportResults(jobID: jobID, format: "json") }
                Button("CSV") { model.exportResults(jobID: jobID, format: "csv") }
                Button(loc(model.languageCode, "Chapters", "Kapitel", "Capitulos", "Chapitres")) {
                    model.exportResults(jobID: jobID, format: "chapters")
                }
            } label: {
                Label(loc(model.languageCode, "Export", "Export", "Exportar", "Exporter"),
                      systemImage: "square.and.arrow.up")
            }
            .menuStyle(.button)
            .fixedSize()

            if ["queued", "running"].contains(entry.status) {
                Button(role: .destructive) {
                    model.cancelJob(jobID)
                } label: {
                    Label(loc(model.languageCode, "Cancel", "Abbrechen", "Cancelar", "Annuler"),
                          systemImage: "stop.circle")
                }
                .buttonStyle(.bordered)
                .tint(.orange)
            }
        }
    }

    @ViewBuilder
    private func detail(_ response: AnalyzeResponse) -> some View {
        let models = response.segments.map(model.makeSegmentViewModel)
        let hasSongs = models.contains { $0.payload.kind == "matched_track" }
        let filtered = (showOnlySongs && hasSongs)
            ? models.filter { $0.payload.kind == "matched_track" }
            : models

        Panel(padding: Theme.Space.m, radius: Theme.Radius.panel) {
            VStack(alignment: .leading, spacing: Theme.Space.s) {
                summaryRow(response: response, models: models)
                if hasSongs {
                    Picker("", selection: $showOnlySongs) {
                        Text("\(loc(model.languageCode, "Songs", "Songs", "Canciones", "Titres")) (\(models.filter { $0.payload.kind == "matched_track" }.count))").tag(true)
                        Text("\(loc(model.languageCode, "All", "Alle", "Todo", "Tout")) (\(models.count))").tag(false)
                    }
                    .pickerStyle(.segmented)
                    .labelsHidden()
                    .frame(maxWidth: 260)
                }
                if !filtered.isEmpty {
                    HStack(spacing: Theme.Space.xs) {
                        ResultsTimelineView(
                            segments: filtered,
                            selectedID: selectedSegmentID,
                            onSelect: { selectedSegmentID = $0 },
                            zoom: $zoom
                        )
                        .frame(height: 36)
                        TimelineZoomControl(zoom: $zoom)
                    }
                    SegmentList(
                        segments: filtered,
                        selectedID: $selectedSegmentID
                    )
                    .frame(minHeight: 280)
                }
            }
        }
    }

    private func summaryRow(response: AnalyzeResponse, models: [SegmentViewModel]) -> some View {
        let matched = models.filter { $0.payload.kind == "matched_track" }.count
        let unresolved = models.filter { $0.payload.kind == "music_unresolved" }.count
        return HStack(spacing: Theme.Space.xs) {
            Pill("\(matched)", icon: "checkmark.seal", tint: Theme.Palette.successTint)
            Pill("\(models.count)", icon: "rectangle.split.3x1")
            if unresolved > 0 {
                Pill("\(unresolved) " + loc(model.languageCode, "unresolved", "unklar", "sin resolver", "non resolu"),
                     icon: "questionmark.circle", tint: Theme.Palette.warningTint)
            }
            Spacer()
            if unresolved > 0 {
                Button {
                    model.retrySegments(jobID: jobID)
                } label: {
                    Label(loc(model.languageCode, "Retry unresolved", "Unklare erneut pruefen", "Reintentar", "Reessayer"),
                          systemImage: "arrow.clockwise")
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
            }
        }
    }

    private func load() async {
        response = await model.snapshot(for: jobID)
        if let first = response?.segments.first {
            selectedSegmentID = first.id
        }
    }
}

// MARK: - Storage workspace

struct StorageView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: Theme.Space.l) {
                header
                if let summary = model.storageSummary {
                    StorageHeroPanel(summary: summary, languageCode: model.languageCode)
                    if !summary.categories.isEmpty {
                        Panel {
                            VStack(alignment: .leading, spacing: Theme.Space.xs) {
                                SectionLabel(loc(model.languageCode, "Categories", "Kategorien", "Categorias", "Categories"))
                                StorageCategoriesList(categories: summary.categories)
                            }
                        }
                    }
                    Panel {
                        VStack(alignment: .leading, spacing: Theme.Space.xs) {
                            HStack {
                                SectionLabel(loc(model.languageCode, "Artifacts", "Artefakte", "Artefactos", "Artefacts"))
                                Spacer()
                                Text("\(summary.entries.count) " + loc(model.languageCode, "items", "Eintraege", "elementos", "elements"))
                                    .font(Theme.Font.caption)
                                    .foregroundStyle(.secondary)
                            }
                            StorageArtifactsList(entries: summary.entries, onReveal: model.reveal, languageCode: model.languageCode)
                        }
                    }
                    if !summary.locations.isEmpty {
                        Panel {
                            VStack(alignment: .leading, spacing: Theme.Space.xs) {
                                SectionLabel(loc(model.languageCode, "Locations", "Orte", "Ubicaciones", "Emplacements"))
                                ForEach(summary.locations.keys.sorted(), id: \.self) { key in
                                    HStack(spacing: Theme.Space.s) {
                                        Text(key.capitalized)
                                            .font(Theme.Font.body)
                                        Spacer()
                                        Text(summary.locations[key] ?? "")
                                            .font(Theme.Font.caption)
                                            .foregroundStyle(.secondary)
                                            .textSelection(.enabled)
                                    }
                                    .padding(.vertical, 3)
                                }
                            }
                        }
                    }
                } else {
                    emptyState
                }
            }
            .padding(.horizontal, Theme.Space.xl)
            .padding(.vertical, Theme.Space.l)
            .frame(maxWidth: 940, alignment: .topLeading)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .background(Theme.Palette.surface)
        .task { await model.refreshStorage(jobID: model.selectedStorageJobID) }
    }

    private var header: some View {
        HStack(alignment: .center, spacing: Theme.Space.s) {
            Picker("", selection: scopeBinding) {
                Text(loc(model.languageCode, "All runs", "Alle Laeufe", "Todos", "Tous")).tag(Optional<String>.none)
                ForEach(model.orderedLibraryEntries) { entry in
                    Text(entry.title).tag(Optional(entry.job_id))
                }
            }
            .pickerStyle(.menu)
            .labelsHidden()
            .frame(maxWidth: 320)

            Spacer()

            Menu {
                Button(loc(model.languageCode,
                           model.selectedStorageJobID == nil ? "Clean unpinned" : "Clean this run",
                           model.selectedStorageJobID == nil ? "Ungepinntes aufraeumen" : "Diesen Lauf aufraeumen",
                           model.selectedStorageJobID == nil ? "Limpiar no fijados" : "Limpiar este",
                           model.selectedStorageJobID == nil ? "Nettoyer non epingles" : "Nettoyer celui-ci")) {
                    model.cleanupArtifacts(jobID: model.selectedStorageJobID)
                }
            } label: {
                Label(loc(model.languageCode, "Clean up", "Aufraeumen", "Limpiar", "Nettoyer"),
                      systemImage: "trash")
            }
            .menuStyle(.button)
            .fixedSize()
            .disabled(model.storageSummary?.entries.isEmpty ?? true)
        }
    }

    private var scopeBinding: Binding<String?> {
        Binding(
            get: { model.selectedStorageJobID },
            set: { model.selectStorageScope($0) }
        )
    }

    private var emptyState: some View {
        VStack(spacing: Theme.Space.m) {
            Image(systemName: "externaldrive")
                .font(.system(size: 36, weight: .light))
                .foregroundStyle(.tertiary)
            Text(loc(model.languageCode, "No artifacts yet", "Noch kein Speicher", "Sin archivos", "Pas de stockage"))
                .font(Theme.Font.title)
            Text(loc(model.languageCode,
                     "Artifacts appear here after an analysis writes audio, excerpts, or stems to disk.",
                     "Artefakte erscheinen hier, wenn eine Analyse Audio oder Stems speichert.",
                     "Los artefactos aparecen tras un analisis que guarde audio o stems.",
                     "Les artefacts apparaissent apres une analyse sauvegardant audio ou stems."))
                .font(Theme.Font.body)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(maxWidth: 420)
        .frame(maxWidth: .infinity)
        .padding(.vertical, 60)
    }
}

struct StorageHeroPanel: View {
    let summary: StorageSummaryPayload
    let languageCode: String

    var body: some View {
        Panel(padding: Theme.Space.l, radius: Theme.Radius.panel) {
            HStack(alignment: .center, spacing: Theme.Space.xl) {
                VStack(alignment: .leading, spacing: 6) {
                    SectionLabel(loc(languageCode, "On disk", "Auf dem Mac", "En disco", "Sur le disque"))
                    Text(formatBytes(summary.total_size_bytes))
                        .font(.system(size: 36, weight: .semibold, design: .rounded))
                        .monospacedDigit()
                    HStack(spacing: Theme.Space.xs) {
                        Pill("\(summary.entries.count) " + loc(languageCode, "items", "Eintraege", "elementos", "elements"), icon: "doc.on.doc")
                        Pill(summary.auto_clean
                             ? loc(languageCode, "Auto-clean", "Auto-Clean", "Auto-limpiar", "Auto-nettoyage")
                             : loc(languageCode, "Retained", "Behalten", "Retenido", "Conserve"),
                             icon: summary.auto_clean ? "sparkles" : "lock",
                             tint: summary.auto_clean ? Theme.Palette.accent : Theme.Palette.textSecondary)
                    }
                }
                Spacer()
                categoryBar
                    .frame(maxWidth: 360)
            }
        }
    }

    private var categoryBar: some View {
        let total = max(1, summary.total_size_bytes)
        return VStack(alignment: .leading, spacing: 6) {
            SectionLabel(loc(languageCode, "Split", "Verteilung", "Reparto", "Repartition"))
            HStack(spacing: 2) {
                ForEach(Array(summary.categories.enumerated()), id: \.element.category) { index, category in
                    Rectangle()
                        .fill(colorFor(index: index))
                        .frame(height: 10)
                        .frame(width: nil)
                        .layoutPriority(Double(category.size_bytes) / Double(total))
                }
            }
            .clipShape(Capsule())

            VStack(alignment: .leading, spacing: 3) {
                ForEach(Array(summary.categories.prefix(4).enumerated()), id: \.element.category) { index, category in
                    HStack(spacing: 6) {
                        Circle()
                            .fill(colorFor(index: index))
                            .frame(width: 7, height: 7)
                        Text(category.category.replacingOccurrences(of: "_", with: " ").capitalized)
                            .font(Theme.Font.caption)
                        Spacer()
                        Text(formatBytes(category.size_bytes))
                            .font(Theme.Font.caption)
                            .foregroundStyle(.secondary)
                            .monospacedDigit()
                    }
                }
            }
        }
    }

    private func colorFor(index: Int) -> Color {
        let palette: [Color] = [.accentColor, .purple, .pink, .orange, .teal, .green, .indigo]
        return palette[index % palette.count]
    }
}

struct StorageCategoriesList: View {
    let categories: [ArtifactCategorySummaryPayload]

    var body: some View {
        VStack(spacing: 0) {
            ForEach(Array(categories.enumerated()), id: \.element.category) { index, category in
                HStack {
                    Text(category.category.replacingOccurrences(of: "_", with: " ").capitalized)
                        .font(Theme.Font.body)
                    Spacer()
                    Text("\(category.count)")
                        .font(Theme.Font.caption)
                        .foregroundStyle(.secondary)
                        .frame(width: 48, alignment: .trailing)
                    Text(formatBytes(category.size_bytes))
                        .font(Theme.Font.caption)
                        .foregroundStyle(.secondary)
                        .frame(width: 92, alignment: .trailing)
                        .monospacedDigit()
                }
                .padding(.vertical, 6)
                if index < categories.count - 1 {
                    Divider().opacity(0.4)
                }
            }
        }
    }
}

struct StorageArtifactsList: View {
    let entries: [ArtifactEntryPayload]
    let onReveal: (String) -> Void
    let languageCode: String

    var body: some View {
        VStack(spacing: 0) {
            ForEach(Array(entries.enumerated()), id: \.element.id) { index, entry in
                HStack(alignment: .center, spacing: Theme.Space.xs) {
                    Image(systemName: entry.temporary ? "folder" : "externaldrive")
                        .font(.system(size: 12))
                        .foregroundStyle(entry.temporary ? AnyShapeStyle(.tint) : AnyShapeStyle(.secondary))
                        .frame(width: 18)

                    VStack(alignment: .leading, spacing: 2) {
                        HStack(spacing: 5) {
                            Text(entry.label)
                                .font(Theme.Font.rowTitle)
                                .lineLimit(1)
                            if entry.pinned {
                                Image(systemName: "pin.fill")
                                    .font(.system(size: 9, weight: .semibold))
                                    .foregroundStyle(.orange)
                            }
                        }
                        Text(entry.path)
                            .font(Theme.Font.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                            .textSelection(.enabled)
                    }
                    Spacer(minLength: 4)
                    Text(formatBytes(entry.size_bytes))
                        .font(Theme.Font.caption.weight(.medium))
                        .foregroundStyle(.secondary)
                        .monospacedDigit()
                    Button {
                        onReveal(entry.path)
                    } label: {
                        Image(systemName: "arrow.up.forward.app")
                            .font(.system(size: 11))
                    }
                    .buttonStyle(.borderless)
                    .help(loc(languageCode, "Reveal in Finder", "Im Finder zeigen", "Mostrar en Finder", "Afficher dans le Finder"))
                }
                .padding(.vertical, 7)
                if index < entries.count - 1 {
                    Divider().opacity(0.4)
                }
            }
        }
    }
}

// MARK: - Menu bar quick capture

struct MenuBarQuickCaptureView: View {
    @ObservedObject var model: AppModel
    @Environment(\.openWindow) private var openWindow
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        VStack(alignment: .leading, spacing: Theme.Space.s) {
            HStack(spacing: Theme.Space.xs) {
                Image(systemName: "waveform")
                    .foregroundStyle(.tint)
                    .font(.system(size: 13, weight: .semibold, design: .rounded))
                Text("Music Fetch")
                    .font(Theme.Font.title)
                Spacer()
                statusPill
            }

            if !model.quickCaptureSummary.isEmpty {
                Text(model.quickCaptureSummary)
                    .font(Theme.Font.body)
                    .fixedSize(horizontal: false, vertical: true)
                    .lineLimit(3)
            }

            Divider()

            HStack(spacing: Theme.Space.xs) {
                captureButton(
                    title: micTitle,
                    icon: "mic.fill",
                    tint: Theme.Palette.micTint,
                    active: model.captureState == .recordingMic
                ) {
                    model.toggleMicrophoneRecording()
                }
                captureButton(
                    title: systemTitle,
                    icon: "speaker.wave.2.fill",
                    tint: Theme.Palette.systemTint,
                    active: model.captureState == .recordingSystem
                ) {
                    model.toggleSystemAudioRecording()
                }
            }

            Divider()

            Button {
                openWindow(id: "main")
                model.showMainWindow()
            } label: {
                Label(loc(model.languageCode, "Open Music Fetch", "Music Fetch oeffnen", "Abrir Music Fetch", "Ouvrir Music Fetch"),
                      systemImage: "macwindow")
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            .buttonStyle(.borderless)

            Button {
                openSettings()
            } label: {
                Label(loc(model.languageCode, "Settings", "Einstellungen", "Ajustes", "Reglages"),
                      systemImage: "gearshape")
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            .buttonStyle(.borderless)

            Button(role: .destructive) {
                NSApp.terminate(nil)
            } label: {
                Label(loc(model.languageCode, "Quit", "Beenden", "Salir", "Quitter"),
                      systemImage: "power")
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .foregroundStyle(.secondary)
            }
            .buttonStyle(.borderless)
        }
        .padding(Theme.Space.s)
        .frame(width: 280)
    }

    private var statusPill: some View {
        HStack(spacing: 4) {
            StatusDot(color: statusColor, pulsing: model.captureState.isBusy)
            Text(model.statusTitle)
                .font(Theme.Font.caption)
                .foregroundStyle(.secondary)
                .lineLimit(1)
        }
    }

    private func captureButton(title: String, icon: String, tint: Color, active: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Label(title, systemImage: icon)
                .frame(maxWidth: .infinity)
        }
        .buttonStyle(.bordered)
        .tint(tint)
        .controlSize(.regular)
        .foregroundStyle(active ? AnyShapeStyle(tint) : AnyShapeStyle(.primary))
    }

    private var micTitle: String {
        (model.captureState == .recordingMic || model.captureState == .stoppingMic)
            ? loc(model.languageCode, "Stop mic", "Mikro stoppen", "Detener mic", "Arreter micro")
            : loc(model.languageCode, "Mic", "Mikro", "Mic", "Micro")
    }

    private var systemTitle: String {
        (model.captureState == .recordingSystem || model.captureState == .stoppingSystem)
            ? loc(model.languageCode, "Stop system", "System stoppen", "Detener sistema", "Arreter")
            : loc(model.languageCode, "System", "System", "Sistema", "Systeme")
    }

    private var statusColor: Color {
        switch model.captureState {
        case .recordingMic: return Theme.Palette.micTint
        case .recordingSystem: return Theme.Palette.systemTint
        case .startingMic, .stoppingMic, .startingSystem, .stoppingSystem: return Theme.Palette.warningTint
        case .idle:
            if model.libraryEntries.contains(where: { ["queued", "running"].contains($0.status) }) {
                return Theme.Palette.accent
            }
            return Theme.Palette.successTint
        }
    }
}
