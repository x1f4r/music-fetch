import AppKit
import SwiftUI

// MARK: - Shared primitives

struct Card<Content: View>: View {
    let padding: CGFloat
    let spacing: CGFloat
    let cornerRadius: CGFloat
    @ViewBuilder let content: Content

    init(padding: CGFloat = 16, spacing: CGFloat = 12, cornerRadius: CGFloat = 12, @ViewBuilder content: () -> Content) {
        self.padding = padding
        self.spacing = spacing
        self.cornerRadius = cornerRadius
        self.content = content()
    }

    var body: some View {
        content
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: .topLeading)
            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: cornerRadius, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
                    .strokeBorder(Color.primary.opacity(0.08), lineWidth: 1)
            )
    }
}

struct SectionHeading: View {
    let title: String
    let subtitle: String?

    init(_ title: String, subtitle: String? = nil) {
        self.title = title
        self.subtitle = subtitle
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title)
                .font(.headline)
            if let subtitle {
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

struct MetricChip: View {
    let icon: String
    let label: String
    var tint: Color = .secondary

    var body: some View {
        HStack(spacing: 4) {
            Image(systemName: icon)
                .font(.caption2)
            Text(label)
                .font(.caption.weight(.medium))
        }
        .foregroundStyle(tint)
    }
}

// MARK: - ContentView

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
            HStack(spacing: 8) {
                Image(systemName: "waveform.circle.fill")
                    .font(.system(size: 22, weight: .semibold))
                    .foregroundStyle(.tint)
                VStack(alignment: .leading, spacing: 0) {
                    Text("Music Fetch")
                        .font(.headline)
                    Text("v0.3")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }
            .padding(.horizontal, 14)
            .padding(.top, 14)
            .padding(.bottom, 10)

            List(selection: workspaceBinding) {
                Section(loc(model.languageCode, "Workspaces", "Bereiche", "Espacios", "Espaces")) {
                    ForEach(WorkspaceSection.allCases) { section in
                        NavigationLink(value: section) {
                            Label(section.title(model.languageCode), systemImage: section.icon)
                        }
                        .badge(sidebarBadge(for: section))
                    }
                }

                if !model.recentAnalyses.isEmpty {
                    Section(loc(model.languageCode, "Recent", "Zuletzt", "Recientes", "Recents")) {
                        ForEach(Array(model.recentAnalyses.prefix(6))) { analysis in
                            Button {
                                model.restoreRecent(analysis)
                            } label: {
                                SidebarRecentRow(analysis: analysis)
                            }
                            .buttonStyle(.plain)
                            .listRowInsets(EdgeInsets(top: 4, leading: 8, bottom: 4, trailing: 8))
                        }
                    }
                }
            }
            .listStyle(.sidebar)
            .scrollContentBackground(.hidden)

            Divider()

            SidebarFooter(model: model)
        }
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

    private func sidebarBadge(for section: WorkspaceSection) -> Int {
        switch section {
        case .analyze: return 0
        case .library: return model.libraryEntries.count
        case .storage: return 0
        }
    }
}

struct SidebarRecentRow: View {
    let analysis: RecentAnalysis

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: statusSymbol)
                .font(.caption)
                .foregroundStyle(statusColor)
                .frame(width: 14)
            VStack(alignment: .leading, spacing: 1) {
                Text(analysis.title)
                    .font(.callout)
                    .lineLimit(1)
                Text(relative)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer(minLength: 0)
        }
        .contentShape(Rectangle())
    }

    private var statusSymbol: String {
        switch analysis.status {
        case "succeeded": return "checkmark.circle.fill"
        case "failed": return "exclamationmark.triangle.fill"
        case "canceled": return "xmark.circle"
        case "running", "queued": return "circle.dotted"
        case "partial_failed": return "exclamationmark.circle.fill"
        default: return "music.note"
        }
    }

    private var statusColor: Color {
        switch analysis.status {
        case "succeeded": return .green
        case "failed": return .orange
        case "partial_failed": return .yellow
        case "canceled": return .secondary
        case "running", "queued": return .blue
        default: return .secondary
        }
    }

    private var relative: String {
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .abbreviated
        return formatter.localizedString(for: analysis.createdAt, relativeTo: Date())
    }
}

struct SidebarFooter: View {
    @ObservedObject var model: AppModel

    var body: some View {
        HStack(spacing: 8) {
            Circle()
                .fill(statusColor)
                .frame(width: 8, height: 8)
            Text(model.statusTitle)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(1)
            Spacer(minLength: 6)
            SettingsLink {
                Image(systemName: "gearshape")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundStyle(.secondary)
                    .frame(width: 22, height: 22)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help(loc(model.languageCode, "Settings", "Einstellungen", "Ajustes", "Reglages"))
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 10)
    }

    private var statusColor: Color {
        switch model.captureState {
        case .recordingMic: return .red
        case .recordingSystem: return .orange
        case .startingMic, .stoppingMic, .startingSystem, .stoppingSystem: return .yellow
        case .idle:
            switch model.viewState {
            case .idle: return .secondary
            case .analyzing: return .blue
            case .recordingMic: return .red
            case .recordingSystem: return .orange
            case .showingResults: return .green
            case .error: return .orange
            }
        }
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
            ToolbarItemGroup(placement: .primaryAction) {
                Button {
                    model.startNewAnalysis()
                } label: {
                    Label(loc(model.languageCode, "New", "Neu", "Nuevo", "Nouveau"), systemImage: "square.and.pencil")
                }
                .help(loc(model.languageCode, "New analysis", "Neue Analyse", "Nuevo analisis", "Nouvelle analyse"))
                .keyboardShortcut("n", modifiers: [.command])

                if model.selectedWorkspace == .analyze {
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
            VStack(alignment: .leading, spacing: 18) {
                InputCard(model: model)
                ResultsSectionView(model: model)
            }
            .padding(.horizontal, 22)
            .padding(.vertical, 20)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .background(Color(NSColor.windowBackgroundColor))
    }
}

struct InputCard: View {
    @ObservedObject var model: AppModel
    @AppStorage(preferredInputDefaultsKey) private var preferredInputRaw = PreferredInput.link.rawValue
    @FocusState private var focused: Bool

    var body: some View {
        Card(padding: 20, spacing: 14, cornerRadius: 14) {
            VStack(alignment: .leading, spacing: 16) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(loc(model.languageCode, "Find music", "Musik finden", "Buscar musica", "Trouver la musique"))
                        .font(.system(size: 20, weight: .semibold))
                    Text(subtitle)
                        .font(.callout)
                        .foregroundStyle(.secondary)
                }

                Group {
                    switch preferredInput {
                    case .link:
                        linkRow
                    case .file:
                        fileRow
                    case .microphone:
                        captureRow(icon: "mic.fill",
                                   hint: loc(model.languageCode,
                                             "Capture audio from the selected microphone.",
                                             "Audio vom gewaehlten Mikrofon aufnehmen.",
                                             "Captura audio del microfono seleccionado.",
                                             "Capturez l'audio du micro selectionne."),
                                   tint: .red)
                    case .system:
                        captureRow(icon: "waveform",
                                   hint: loc(model.languageCode,
                                             "Record currently playing system audio (requires permission).",
                                             "Laufendes Systemaudio aufnehmen (benoetigt Berechtigung).",
                                             "Graba el audio del sistema (requiere permiso).",
                                             "Enregistre l'audio systeme (permission requise)."),
                                   tint: .orange)
                    }
                }

                Picker("", selection: preferredBinding) {
                    ForEach(PreferredInput.allCases) { input in
                        Text(input.title(model.languageCode)).tag(input)
                    }
                }
                .pickerStyle(.segmented)
                .labelsHidden()

                if case let .analyzing(phase) = model.viewState {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small)
                        Text(phase)
                            .font(.callout)
                            .foregroundStyle(.secondary)
                        Spacer()
                    }
                } else if case let .error(message) = model.viewState {
                    HStack(spacing: 8) {
                        Image(systemName: "exclamationmark.triangle.fill")
                            .foregroundStyle(.orange)
                        Text(message)
                            .font(.callout)
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                        Spacer()
                    }
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

    // MARK: rows

    private var linkRow: some View {
        HStack(spacing: 10) {
            TextField(
                loc(model.languageCode, "Paste a link", "Link einfuegen", "Pega un enlace", "Collez un lien"),
                text: $model.inputValue
            )
            .textFieldStyle(.roundedBorder)
            .controlSize(.large)
            .focused($focused)
            .submitLabel(.go)
            .onSubmit { runPrimary() }

            Button(primaryLabel) { runPrimary() }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)
                .keyboardShortcut(.return, modifiers: [.command])
                .disabled(!canRunLinkOrFile)
        }
    }

    private var fileRow: some View {
        HStack(spacing: 10) {
            TextField(
                loc(model.languageCode, "File path", "Dateipfad", "Ruta de archivo", "Chemin du fichier"),
                text: $model.inputValue
            )
            .textFieldStyle(.roundedBorder)
            .controlSize(.large)
            .focused($focused)
            .submitLabel(.go)
            .onSubmit { runPrimary() }

            Button {
                model.chooseFile()
            } label: {
                Image(systemName: "folder")
            }
            .buttonStyle(.bordered)
            .controlSize(.large)
            .help(loc(model.languageCode, "Choose file", "Datei waehlen", "Elegir archivo", "Choisir un fichier"))

            Button(primaryLabel) { runPrimary() }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)
                .keyboardShortcut(.return, modifiers: [.command])
                .disabled(!canRunLinkOrFile)
        }
    }

    private func captureRow(icon: String, hint: String, tint: Color) -> some View {
        HStack(alignment: .center, spacing: 12) {
            Image(systemName: icon)
                .font(.system(size: 28))
                .foregroundStyle(tint)
                .frame(width: 40, height: 40)
                .background(tint.opacity(0.12), in: Circle())

            Text(hint)
                .font(.callout)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)

            Spacer(minLength: 8)

            Button(primaryLabel) { runPrimary() }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)
                .tint(tint)
                .keyboardShortcut(.return, modifiers: [.command])
        }
        .padding(.vertical, 4)
    }

    // MARK: computed

    private var subtitle: String {
        switch preferredInput {
        case .link:
            return loc(model.languageCode, "Paste a URL from YouTube, TikTok, Vimeo, or other supported sites.",
                       "Fuege eine URL von YouTube, TikTok, Vimeo oder anderen unterstuetzten Seiten ein.",
                       "Pega una URL de YouTube, TikTok, Vimeo u otros sitios soportados.",
                       "Collez une URL depuis YouTube, TikTok, Vimeo ou un site pris en charge.")
        case .file:
            return loc(model.languageCode, "Pick an audio or video file from your Mac.",
                       "Waehle eine Audio- oder Videodatei auf deinem Mac.",
                       "Elige un archivo de audio o video de tu Mac.",
                       "Choisissez un fichier audio ou video sur votre Mac.")
        case .microphone:
            return loc(model.languageCode, "Use your microphone to identify music playing nearby.",
                       "Nutze dein Mikrofon, um Musik in der Umgebung zu erkennen.",
                       "Usa el microfono para identificar la musica del entorno.",
                       "Utilisez le micro pour identifier la musique autour de vous.")
        case .system:
            return loc(model.languageCode, "Capture whatever is playing through your speakers.",
                       "Nimm auf, was gerade ueber deine Lautsprecher laeuft.",
                       "Captura lo que suena por los altavoces.",
                       "Capturez ce qui joue sur vos haut-parleurs.")
        }
    }

    private var canRunLinkOrFile: Bool {
        !model.isBusy && !model.inputValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private var preferredInput: PreferredInput {
        PreferredInput(rawValue: preferredInputRaw) ?? .link
    }

    private var preferredBinding: Binding<PreferredInput> {
        Binding(
            get: { preferredInput },
            set: { newValue in
                preferredInputRaw = newValue.rawValue
                if newValue == .link || newValue == .file {
                    focused = true
                }
            }
        )
    }

    private var primaryLabel: String {
        switch preferredInput {
        case .link:
            return loc(model.languageCode, "Analyze", "Analysieren", "Analizar", "Analyser")
        case .file:
            return loc(model.languageCode, "Analyze", "Analysieren", "Analizar", "Analyser")
        case .microphone:
            return (model.captureState == .recordingMic || model.captureState == .stoppingMic)
                ? loc(model.languageCode, "Stop", "Stopp", "Detener", "Arreter")
                : loc(model.languageCode, "Start recording", "Aufnahme starten", "Iniciar grabacion", "Demarrer l'enregistrement")
        case .system:
            return (model.captureState == .recordingSystem || model.captureState == .stoppingSystem)
                ? loc(model.languageCode, "Stop", "Stopp", "Detener", "Arreter")
                : loc(model.languageCode, "Start capture", "Aufnahme starten", "Iniciar captura", "Demarrer la capture")
        }
    }

    private func runPrimary() {
        switch preferredInput {
        case .link:
            model.analyze()
        case .file:
            let trimmed = model.inputValue.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty {
                model.chooseFile()
            } else {
                model.analyze()
            }
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
    @State private var pinnedOnly: Bool = false

    var body: some View {
        ScrollView {
            Group {
                if filteredEntries.isEmpty {
                    ContentUnavailableView(
                        loc(model.languageCode, "No runs yet", "Noch keine Laeufe", "Sin analisis", "Aucune analyse"),
                        systemImage: "books.vertical",
                        description: Text(loc(model.languageCode,
                                              "Analyze a link or file and it will appear here.",
                                              "Starte eine Analyse, dann erscheint sie hier.",
                                              "Analiza un enlace o archivo y aparecera aqui.",
                                              "Lancez une analyse, elle apparaitra ici."))
                    )
                    .frame(maxWidth: .infinity)
                    .padding(.top, 40)
                } else {
                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 280, maximum: 360), spacing: 14)], spacing: 14) {
                        ForEach(filteredEntries) { entry in
                            LibraryRunCard(
                                entry: entry,
                                languageCode: model.languageCode,
                                selected: model.selectedLibraryJobID == entry.job_id
                            ) {
                                open(entry)
                            }
                            .contextMenu {
                                Button(entry.pinned
                                    ? loc(model.languageCode, "Unpin", "Losen", "Desfijar", "Detacher")
                                    : loc(model.languageCode, "Pin", "Pinnen", "Fijar", "Epingler")) {
                                    model.setPinned(jobID: entry.job_id, pinned: !entry.pinned)
                                }
                                Button(loc(model.languageCode, "Show in Storage", "In Speicher zeigen", "Mostrar en almacenamiento", "Afficher dans le stockage")) {
                                    model.showStorage(jobID: entry.job_id)
                                }
                                Divider()
                                Button(loc(model.languageCode, "Open", "Oeffnen", "Abrir", "Ouvrir")) {
                                    open(entry)
                                }
                            }
                        }
                    }
                    .padding(.horizontal, 22)
                    .padding(.vertical, 20)
                }
            }
        }
        .background(Color(NSColor.windowBackgroundColor))
        .searchable(text: $searchText, prompt: loc(model.languageCode, "Search runs", "Laeufe suchen", "Buscar analisis", "Rechercher"))
        .toolbar {
            ToolbarItem(placement: .secondaryAction) {
                Toggle(isOn: $pinnedOnly) {
                    Label(loc(model.languageCode, "Pinned only", "Nur gepinnt", "Solo fijados", "Epingles"), systemImage: "pin.fill")
                }
                .toggleStyle(.button)
            }
        }
        .task {
            await model.refreshLibrary()
        }
        .refreshable {
            await model.refreshLibrary()
        }
    }

    private var filteredEntries: [LibraryEntryPayload] {
        model.orderedLibraryEntries.filter { entry in
            if pinnedOnly && !entry.pinned { return false }
            if searchText.isEmpty { return true }
            return entry.title.localizedCaseInsensitiveContains(searchText)
                || entry.input_value.localizedCaseInsensitiveContains(searchText)
        }
    }

    private func open(_ entry: LibraryEntryPayload) {
        model.route.libraryJobID = entry.job_id
        model.loadLibraryJob(entry.job_id)
        model.activateWorkspace(.analyze)
    }
}

struct LibraryRunCard: View {
    let entry: LibraryEntryPayload
    let languageCode: String
    let selected: Bool
    let onOpen: () -> Void

    var body: some View {
        Button(action: onOpen) {
            VStack(alignment: .leading, spacing: 12) {
                HStack(alignment: .top, spacing: 8) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(entry.title)
                            .font(.headline)
                            .lineLimit(2)
                            .multilineTextAlignment(.leading)
                            .fixedSize(horizontal: false, vertical: true)
                        Text(entry.input_value)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    Spacer(minLength: 4)
                    if entry.pinned {
                        Image(systemName: "pin.fill")
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }

                Divider().opacity(0.5)

                HStack(spacing: 12) {
                    MetricChip(icon: "waveform", label: "\(entry.segment_count)")
                    MetricChip(icon: "checkmark.seal", label: "\(entry.matched_count)")
                    MetricChip(icon: "internaldrive", label: formatBytes(entry.artifact_size_bytes))
                    Spacer()
                    statusBadge
                }
            }
            .padding(14)
            .frame(maxWidth: .infinity, minHeight: 112, alignment: .topLeading)
            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .strokeBorder(selected ? Color.accentColor.opacity(0.55) : Color.primary.opacity(0.08),
                                  lineWidth: selected ? 1.5 : 1)
            )
        }
        .buttonStyle(.plain)
    }

    private var statusBadge: some View {
        let descriptor: (String, Color, String) = {
            switch entry.status {
            case "succeeded":
                return ("checkmark.circle.fill", .green, loc(languageCode, "Done", "Fertig", "Listo", "Termine"))
            case "running", "queued":
                return ("circle.dotted", .blue, loc(languageCode, "Running", "Laeuft", "En curso", "En cours"))
            case "failed":
                return ("exclamationmark.triangle.fill", .orange, loc(languageCode, "Failed", "Fehler", "Fallo", "Echec"))
            case "partial_failed":
                return ("exclamationmark.circle.fill", .yellow, loc(languageCode, "Partial", "Teilweise", "Parcial", "Partiel"))
            case "canceled":
                return ("xmark.circle", .secondary, loc(languageCode, "Canceled", "Abgebrochen", "Cancelado", "Annule"))
            default:
                return ("clock", .secondary, entry.status.capitalized)
            }
        }()
        return HStack(spacing: 4) {
            Image(systemName: descriptor.0)
            Text(descriptor.2)
        }
        .font(.caption2.weight(.medium))
        .foregroundStyle(descriptor.1)
    }
}

// MARK: - Storage workspace

struct StorageView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                storageHeader

                if let summary = model.storageSummary {
                    StorageOverviewRow(summary: summary, languageCode: model.languageCode)

                    if !summary.categories.isEmpty {
                        Card(padding: 16, cornerRadius: 12) {
                            VStack(alignment: .leading, spacing: 12) {
                                SectionHeading(loc(model.languageCode, "Categories", "Kategorien", "Categorias", "Categories"))
                                StorageCategoriesList(categories: summary.categories)
                            }
                        }
                    }

                    Card(padding: 16, cornerRadius: 12) {
                        VStack(alignment: .leading, spacing: 12) {
                            SectionHeading(loc(model.languageCode, "Artifacts", "Artefakte", "Artefactos", "Artefacts"),
                                           subtitle: "\(summary.entries.count) " + loc(model.languageCode, "items", "Eintraege", "elementos", "elements"))
                            StorageArtifactsList(entries: summary.entries, onReveal: model.reveal, languageCode: model.languageCode)
                        }
                    }

                    if !summary.locations.isEmpty {
                        Card(padding: 16, cornerRadius: 12) {
                            VStack(alignment: .leading, spacing: 8) {
                                SectionHeading(loc(model.languageCode, "Locations", "Orte", "Ubicaciones", "Emplacements"))
                                ForEach(summary.locations.keys.sorted(), id: \.self) { key in
                                    HStack {
                                        Text(key.capitalized)
                                            .font(.callout)
                                        Spacer()
                                        Text(summary.locations[key] ?? "")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                            .textSelection(.enabled)
                                    }
                                }
                            }
                        }
                    }
                } else {
                    ContentUnavailableView(
                        loc(model.languageCode, "No storage yet", "Noch kein Speicher", "Sin almacenamiento", "Pas de stockage"),
                        systemImage: "internaldrive",
                        description: Text(loc(model.languageCode,
                                              "Run an analysis to populate local artifacts.",
                                              "Starte eine Analyse, um lokale Artefakte zu erzeugen.",
                                              "Ejecuta un analisis para generar artefactos locales.",
                                              "Lancez une analyse pour generer des artefacts locaux."))
                    )
                    .frame(maxWidth: .infinity, minHeight: 280)
                }
            }
            .padding(.horizontal, 22)
            .padding(.vertical, 20)
        }
        .background(Color(NSColor.windowBackgroundColor))
        .task {
            await model.refreshStorage(jobID: model.selectedStorageJobID)
        }
    }

    private var storageHeader: some View {
        HStack(alignment: .center, spacing: 12) {
            Picker(loc(model.languageCode, "Scope", "Bereich", "Alcance", "Portee"), selection: scopeBinding) {
                Text(loc(model.languageCode, "All runs", "Alle Laeufe", "Todos", "Tous")).tag(Optional<String>.none)
                ForEach(model.orderedLibraryEntries) { entry in
                    Text(entry.title).tag(Optional(entry.job_id))
                }
            }
            .pickerStyle(.menu)
            .frame(maxWidth: 340)

            Spacer()

            Button {
                model.cleanupArtifacts(jobID: model.selectedStorageJobID)
            } label: {
                Label(cleanupLabel, systemImage: "trash")
            }
            .buttonStyle(.bordered)
            .controlSize(.regular)
            .disabled(model.storageSummary?.entries.isEmpty ?? true)
        }
    }

    private var cleanupLabel: String {
        model.selectedStorageJobID == nil
            ? loc(model.languageCode, "Clean all", "Alles aufraeumen", "Limpiar todo", "Tout nettoyer")
            : loc(model.languageCode, "Clean this run", "Diesen Lauf aufraeumen", "Limpiar este", "Nettoyer celui-ci")
    }

    private var scopeBinding: Binding<String?> {
        Binding(
            get: { model.selectedStorageJobID },
            set: { model.selectStorageScope($0) }
        )
    }
}

struct StorageOverviewRow: View {
    let summary: StorageSummaryPayload
    let languageCode: String

    var body: some View {
        HStack(spacing: 12) {
            OverviewTile(
                title: loc(languageCode, "Artifacts", "Artefakte", "Artefactos", "Artefacts"),
                value: "\(summary.entries.count)",
                icon: "doc.on.doc"
            )
            OverviewTile(
                title: loc(languageCode, "Total size", "Gesamt", "Tamano total", "Taille totale"),
                value: formatBytes(summary.total_size_bytes),
                icon: "internaldrive"
            )
            OverviewTile(
                title: loc(languageCode, "Policy", "Regel", "Politica", "Politique"),
                value: summary.auto_clean
                    ? loc(languageCode, "Auto-clean", "Auto-Clean", "Auto-limpiar", "Nettoyage auto")
                    : loc(languageCode, "Retained", "Behalten", "Retenido", "Conserve"),
                icon: summary.auto_clean ? "sparkles" : "lock"
            )
        }
    }
}

struct OverviewTile: View {
    let title: String
    let value: String
    let icon: String

    var body: some View {
        Card(padding: 14, cornerRadius: 12) {
            HStack(spacing: 12) {
                Image(systemName: icon)
                    .font(.system(size: 18, weight: .medium))
                    .foregroundStyle(.tint)
                    .frame(width: 34, height: 34)
                    .background(Color.accentColor.opacity(0.12), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(value)
                        .font(.headline)
                }
                Spacer()
            }
        }
    }
}

struct StorageCategoriesList: View {
    let categories: [ArtifactCategorySummaryPayload]

    var body: some View {
        VStack(spacing: 0) {
            ForEach(Array(categories.enumerated()), id: \.element.category) { index, category in
                HStack {
                    Text(category.category.replacingOccurrences(of: "_", with: " ").capitalized)
                        .font(.callout)
                    Spacer()
                    Text("\(category.count)")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .frame(width: 48, alignment: .trailing)
                    Text(formatBytes(category.size_bytes))
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .frame(width: 92, alignment: .trailing)
                        .monospacedDigit()
                }
                .padding(.vertical, 7)
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
                HStack(alignment: .center, spacing: 10) {
                    Image(systemName: entry.temporary ? "folder" : "externaldrive")
                        .font(.system(size: 14))
                        .foregroundStyle(entry.temporary ? AnyShapeStyle(.tint) : AnyShapeStyle(.secondary))
                        .frame(width: 18)

                    VStack(alignment: .leading, spacing: 2) {
                        HStack(spacing: 6) {
                            Text(entry.label)
                                .font(.callout.weight(.medium))
                                .lineLimit(1)
                            if entry.pinned {
                                Image(systemName: "pin.fill")
                                    .font(.caption2)
                                    .foregroundStyle(.orange)
                            }
                        }
                        Text(entry.path)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                            .textSelection(.enabled)
                    }
                    Spacer(minLength: 8)
                    Text(formatBytes(entry.size_bytes))
                        .font(.caption.weight(.medium))
                        .foregroundStyle(.secondary)
                        .monospacedDigit()
                    Button {
                        onReveal(entry.path)
                    } label: {
                        Image(systemName: "arrow.up.forward.app")
                    }
                    .buttonStyle(.borderless)
                    .help(loc(languageCode, "Reveal in Finder", "Im Finder zeigen", "Mostrar en Finder", "Afficher dans le Finder"))
                }
                .padding(.vertical, 8)
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
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 8) {
                Image(systemName: "waveform.circle.fill")
                    .foregroundStyle(.tint)
                    .font(.system(size: 16, weight: .semibold))
                Text("Music Fetch")
                    .font(.headline)
                Spacer()
                statusBadge
            }

            if !model.quickCaptureSummary.isEmpty {
                Text(model.quickCaptureSummary)
                    .font(.subheadline)
                    .foregroundStyle(.primary)
                    .fixedSize(horizontal: false, vertical: true)
                    .lineLimit(3)
            }

            Divider()

            HStack(spacing: 8) {
                Button {
                    model.toggleMicrophoneRecording()
                } label: {
                    Label(micLabel, systemImage: "mic.fill")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .tint(.red)

                Button {
                    model.toggleSystemAudioRecording()
                } label: {
                    Label(systemLabel, systemImage: "waveform")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .tint(.orange)
            }

            Divider()

            HStack(spacing: 8) {
                Button {
                    openWindow(id: "main")
                    model.showMainWindow()
                } label: {
                    Label(loc(model.languageCode, "Open window", "Fenster oeffnen", "Abrir ventana", "Ouvrir la fenetre"),
                          systemImage: "macwindow")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderless)

                Button {
                    openSettings()
                } label: {
                    Label(loc(model.languageCode, "Settings", "Einstellungen", "Ajustes", "Reglages"),
                          systemImage: "gearshape")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderless)
            }

            Button(role: .destructive) {
                NSApp.terminate(nil)
            } label: {
                Text(loc(model.languageCode, "Quit", "Beenden", "Salir", "Quitter"))
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderless)
            .foregroundStyle(.secondary)
        }
        .padding(14)
        .frame(width: 280)
    }

    private var micLabel: String {
        (model.captureState == .recordingMic || model.captureState == .stoppingMic)
            ? loc(model.languageCode, "Stop mic", "Mikro stoppen", "Detener mic", "Arreter micro")
            : loc(model.languageCode, "Mic", "Mikro", "Mic", "Micro")
    }

    private var systemLabel: String {
        (model.captureState == .recordingSystem || model.captureState == .stoppingSystem)
            ? loc(model.languageCode, "Stop system", "System stoppen", "Detener sistema", "Arreter")
            : loc(model.languageCode, "System", "System", "Sistema", "Systeme")
    }

    private var statusBadge: some View {
        HStack(spacing: 5) {
            Circle()
                .fill(statusColor)
                .frame(width: 6, height: 6)
            Text(model.statusTitle)
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
                .lineLimit(1)
        }
    }

    private var statusColor: Color {
        switch model.captureState {
        case .recordingMic: return .red
        case .recordingSystem: return .orange
        case .startingMic, .stoppingMic, .startingSystem, .stoppingSystem: return .yellow
        case .idle:
            switch model.viewState {
            case .idle: return .secondary
            case .analyzing: return .blue
            case .recordingMic: return .red
            case .recordingSystem: return .orange
            case .showingResults: return .green
            case .error: return .orange
            }
        }
    }
}
