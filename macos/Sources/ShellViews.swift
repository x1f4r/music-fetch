import SwiftUI

struct ContentView: View {
    @ObservedObject var model: AppModel
    @State private var columnVisibility: NavigationSplitViewVisibility = .all

    var body: some View {
        NavigationSplitView(columnVisibility: $columnVisibility) {
            StudioControlRail(model: model)
                .navigationSplitViewColumnWidth(min: 270, ideal: 300, max: 340)
        } detail: {
            StudioWorkspaceSurface(model: model)
        }
        .navigationSplitViewStyle(.balanced)
        .background(studioBackdrop)
        .sheet(isPresented: $model.isDiagnosticsPresented) {
            DiagnosticsSheetView(model: model)
        }
        .onReceive(NotificationCenter.default.publisher(for: .musicFetchAnalyze)) { _ in
            model.analyze()
        }
        .onReceive(NotificationCenter.default.publisher(for: .musicFetchShowDiagnostics)) { _ in
            model.showDiagnostics()
        }
        .task {
            model.bootstrap()
        }
    }

    private var studioBackdrop: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color(red: 0.06, green: 0.08, blue: 0.12),
                    Color(red: 0.10, green: 0.12, blue: 0.18),
                    Color(red: 0.05, green: 0.07, blue: 0.10),
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            Circle()
                .fill(Color.cyan.opacity(0.14))
                .frame(width: 460, height: 460)
                .blur(radius: 80)
                .offset(x: -240, y: -220)
            Circle()
                .fill(Color.orange.opacity(0.12))
                .frame(width: 360, height: 360)
                .blur(radius: 90)
                .offset(x: 280, y: 220)
        }
        .ignoresSafeArea()
    }
}

struct StudioControlRail: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            StudioPanel(padding: 18) {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Music Fetch")
                        .font(.system(size: 27, weight: .black, design: .rounded))
                        .foregroundStyle(.white)
                    Text(loc(model.languageCode, "Studio console for local-first music recognition.", "Studio-Konsole fuer lokale Musikerkennung.", "Consola de estudio para reconocimiento musical local.", "Console studio pour reconnaissance musicale locale."))
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.72))
                    HStack(spacing: 8) {
                        Image(systemName: "dot.radiowaves.left.and.right")
                        Text(model.statusTitle)
                            .lineLimit(1)
                    }
                    .font(.caption.weight(.semibold))
                    .padding(.horizontal, 10)
                    .padding(.vertical, 8)
                    .background(Color.white.opacity(0.08), in: Capsule())
                    .foregroundStyle(.white)
                }
            }

            VStack(alignment: .leading, spacing: 10) {
                ForEach(WorkspaceSection.allCases) { section in
                    Button {
                        model.activateWorkspace(section)
                    } label: {
                        HStack(spacing: 12) {
                            Image(systemName: section.icon)
                                .frame(width: 18)
                            VStack(alignment: .leading, spacing: 2) {
                                Text(section.title(model.languageCode))
                                    .font(.headline)
                                Text(section.subtitle(model.languageCode))
                                    .font(.caption)
                                    .lineLimit(1)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer()
                        }
                        .padding(.horizontal, 14)
                        .padding(.vertical, 12)
                    }
                    .buttonStyle(StudioNavigationButtonStyle(isSelected: model.selectedWorkspace == section))
                }
            }

            Group {
                switch model.selectedWorkspace {
                case .analyze:
                    StudioPanel {
                        VStack(alignment: .leading, spacing: 10) {
                            Text(loc(model.languageCode, "Quick Notes", "Kurznotizen", "Notas rapidas", "Notes rapides"))
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.secondary)
                            Text(loc(model.languageCode, "Use the command deck to switch between link, file, mic, and system capture without leaving the main workspace.", "Nutze das Command Deck, um zwischen Link, Datei, Mikro und Systemaufnahme zu wechseln, ohne den Workspace zu verlassen.", "Usa el panel superior para cambiar entre enlace, archivo, micro y sistema sin salir del espacio principal.", "Utilisez le panneau de commande pour changer entre lien, fichier, micro et systeme sans quitter l'espace principal."))
                                .font(.subheadline)
                                .foregroundStyle(.primary)
                        }
                    }
                case .library:
                    ContextLibraryRail(model: model)
                case .storage:
                    ContextStorageRail(model: model)
                }
            }

            Spacer(minLength: 0)

            VStack(spacing: 10) {
                Button {
                    model.startNewAnalysis()
                } label: {
                    Label(loc(model.languageCode, "New Analysis", "Neue Analyse", "Nuevo analisis", "Nouvelle analyse"), systemImage: "plus.circle.fill")
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .buttonStyle(.borderedProminent)

                Button {
                    model.showDiagnostics()
                } label: {
                    Label(loc(model.languageCode, "Diagnostics", "Diagnostik", "Diagnostico", "Diagnostic"), systemImage: "stethoscope")
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .buttonStyle(.bordered)

                SettingsLink {
                    Label(loc(model.languageCode, "Settings", "Einstellungen", "Ajustes", "Reglages"), systemImage: "gearshape")
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .buttonStyle(.bordered)
            }
        }
        .padding(18)
        .background(
            LinearGradient(
                colors: [
                    Color.black.opacity(0.22),
                    Color.black.opacity(0.12),
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
        )
    }
}

struct ContextLibraryRail: View {
    @ObservedObject var model: AppModel

    var body: some View {
        StudioPanel {
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    Text(loc(model.languageCode, "Library Runs", "Bibliothek-Laeufe", "Analisis guardados", "Analyses en bibliotheque"))
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
                        ForEach(model.orderedLibraryEntries) { entry in
                            Button {
                                model.route.libraryJobID = entry.job_id
                                model.loadLibraryJob(entry.job_id)
                            } label: {
                                LibraryContextRow(entry: entry, isSelected: model.selectedLibraryJobID == entry.job_id)
                            }
                            .buttonStyle(.plain)
                            .contextMenu {
                                Button(entry.pinned ? loc(model.languageCode, "Unpin", "Losen", "Desfijar", "Detacher") : loc(model.languageCode, "Pin", "Pinnen", "Fijar", "Epingler")) {
                                    model.setPinned(jobID: entry.job_id, pinned: !entry.pinned)
                                }
                                Button(loc(model.languageCode, "Open In Storage", "Im Speicher oeffnen", "Abrir en almacenamiento", "Ouvrir dans stockage")) {
                                    model.showStorage(jobID: entry.job_id)
                                }
                            }
                        }
                    }
                }
                .frame(minHeight: 220)
            }
        }
    }
}

struct ContextStorageRail: View {
    @ObservedObject var model: AppModel

    var body: some View {
        StudioPanel {
            VStack(alignment: .leading, spacing: 12) {
                Text(loc(model.languageCode, "Storage Scope", "Speicherbereich", "Alcance de almacenamiento", "Portee du stockage"))
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.secondary)

                Button {
                    model.selectStorageScope(nil)
                } label: {
                    StorageScopeRow(
                        title: loc(model.languageCode, "All Temp Files", "Alle Temp-Dateien", "Todos los temporales", "Tous les temporaires"),
                        subtitle: loc(model.languageCode, "Across every run", "Ueber alle Laeufe", "En todos los analisis", "Sur tous les runs"),
                        isSelected: model.selectedStorageJobID == nil
                    )
                }
                .buttonStyle(.plain)

                ScrollView {
                    LazyVStack(spacing: 8) {
                        ForEach(model.orderedLibraryEntries) { entry in
                            Button {
                                model.selectStorageScope(entry.job_id)
                            } label: {
                                StorageScopeRow(
                                    title: entry.title,
                                    subtitle: "\(formatBytes(entry.artifact_size_bytes)) • \(entry.pinned ? loc(model.languageCode, "Pinned", "Gepinnt", "Fijado", "Epingle") : loc(model.languageCode, "Temp", "Temp", "Temp", "Temp"))",
                                    isSelected: model.selectedStorageJobID == entry.job_id
                                )
                            }
                            .buttonStyle(.plain)
                        }
                    }
                }
                .frame(minHeight: 220)
            }
        }
    }
}

struct StudioNavigationButtonStyle: ButtonStyle {
    let isSelected: Bool

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .foregroundStyle(isSelected ? .white : .primary)
            .background(
                RoundedRectangle(cornerRadius: 20, style: .continuous)
                    .fill(
                        isSelected
                        ? LinearGradient(colors: [Color.cyan.opacity(configuration.isPressed ? 0.28 : 0.36), Color.blue.opacity(configuration.isPressed ? 0.20 : 0.28)], startPoint: .topLeading, endPoint: .bottomTrailing)
                        : LinearGradient(colors: [Color.white.opacity(configuration.isPressed ? 0.12 : 0.06), Color.white.opacity(0.03)], startPoint: .topLeading, endPoint: .bottomTrailing)
                    )
            )
            .overlay(
                RoundedRectangle(cornerRadius: 20, style: .continuous)
                    .strokeBorder(isSelected ? Color.white.opacity(0.16) : Color.white.opacity(0.06))
            )
    }
}

struct StudioWorkspaceSurface: View {
    @ObservedObject var model: AppModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                CommandDeckHeader(model: model)

                switch model.selectedWorkspace {
                case .analyze:
                    AnalyzeWorkspaceView(model: model)
                case .library:
                    LibraryWorkspaceView(model: model)
                case .storage:
                    StorageWorkspaceView(model: model)
                }
            }
            .padding(20)
        }
    }
}

struct CommandDeckHeader: View {
    @ObservedObject var model: AppModel
    @AppStorage(recordingTargetDefaultsKey) private var recordingTargetRaw = RecordingTarget.microphone.rawValue

    var body: some View {
        StudioPanel {
            VStack(alignment: .leading, spacing: 18) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text(model.selectedWorkspace.title(model.languageCode))
                            .font(.system(size: 32, weight: .black, design: .rounded))
                        Text(model.selectedWorkspace.subtitle(model.languageCode))
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    StatusPill(viewState: model.viewState, captureState: model.captureState, languageCode: model.languageCode)
                }

                HStack(spacing: 10) {
                    Button {
                        NotificationCenter.default.post(name: .musicFetchFocusInput, object: nil)
                    } label: {
                        Label(loc(model.languageCode, "Focus Input", "Eingabe fokussieren", "Enfocar entrada", "Focaliser l'entree"), systemImage: "cursorarrow.rays")
                    }
                    .buttonStyle(.bordered)

                    Button {
                        model.chooseFile()
                    } label: {
                        Label(loc(model.languageCode, "Choose File", "Datei waehlen", "Elegir archivo", "Choisir un fichier"), systemImage: "doc.badge.plus")
                    }
                    .buttonStyle(.bordered)

                    if recordingTarget == .microphone {
                        Button {
                            recordingTargetRaw = RecordingTarget.microphone.rawValue
                            model.toggleMicrophoneRecording()
                        } label: {
                            Label(microphoneLabel, systemImage: "mic.fill")
                        }
                        .buttonStyle(.borderedProminent)
                    } else {
                        Button {
                            recordingTargetRaw = RecordingTarget.microphone.rawValue
                            model.toggleMicrophoneRecording()
                        } label: {
                            Label(microphoneLabel, systemImage: "mic.fill")
                        }
                        .buttonStyle(.bordered)
                    }

                    if recordingTarget == .system {
                        Button {
                            recordingTargetRaw = RecordingTarget.system.rawValue
                            model.toggleSystemAudioRecording()
                        } label: {
                            Label(systemLabel, systemImage: "waveform")
                        }
                        .buttonStyle(.borderedProminent)
                    } else {
                        Button {
                            recordingTargetRaw = RecordingTarget.system.rawValue
                            model.toggleSystemAudioRecording()
                        } label: {
                            Label(systemLabel, systemImage: "waveform")
                        }
                        .buttonStyle(.bordered)
                    }

                    Button {
                        model.refreshCurrentWorkspace()
                    } label: {
                        Label(loc(model.languageCode, "Refresh", "Aktualisieren", "Actualizar", "Actualiser"), systemImage: "arrow.clockwise")
                    }
                    .buttonStyle(.bordered)

                    SettingsLink {
                        Label(loc(model.languageCode, "Settings", "Einstellungen", "Ajustes", "Reglages"), systemImage: "gearshape")
                    }
                    .buttonStyle(.bordered)

                    Spacer()
                }
            }
        }
    }

    private var recordingTarget: RecordingTarget {
        RecordingTarget(rawValue: recordingTargetRaw) ?? .microphone
    }

    private var microphoneLabel: String {
        if model.captureState == .recordingMic || model.captureState == .stoppingMic {
            return loc(model.languageCode, "Stop Mic", "Mikro stoppen", "Detener mic", "Arreter micro")
        }
        return loc(model.languageCode, "Mic", "Mikro", "Mic", "Micro")
    }

    private var systemLabel: String {
        if model.captureState == .recordingSystem || model.captureState == .stoppingSystem {
            return loc(model.languageCode, "Stop System", "System stoppen", "Detener sistema", "Arreter systeme")
        }
        return loc(model.languageCode, "System", "System", "Sistema", "Systeme")
    }
}

struct StatusPill: View {
    let viewState: AppViewState
    let captureState: CaptureState
    let languageCode: String

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: iconName)
            Text(message)
                .lineLimit(1)
        }
        .font(.caption.weight(.semibold))
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(backgroundColor.opacity(0.16), in: Capsule())
        .foregroundStyle(backgroundColor)
    }

    private var iconName: String {
        switch captureState {
        case .startingMic, .stoppingMic, .startingSystem, .stoppingSystem:
            return "clock"
        case .recordingMic:
            return "mic.fill"
        case .recordingSystem:
            return "waveform"
        case .idle:
            switch viewState {
            case .idle: return "sparkles"
            case .analyzing: return "waveform.and.magnifyingglass"
            case .recordingMic: return "mic.fill"
            case .recordingSystem: return "waveform"
            case .showingResults: return "checkmark.circle.fill"
            case .error: return "exclamationmark.triangle.fill"
            }
        }
    }

    private var backgroundColor: Color {
        switch captureState {
        case .recordingMic:
            return .red
        case .recordingSystem:
            return .orange
        case .startingMic, .stoppingMic, .startingSystem, .stoppingSystem:
            return .yellow
        case .idle:
            switch viewState {
            case .idle: return .secondary
            case .analyzing: return .cyan
            case .recordingMic: return .red
            case .recordingSystem: return .orange
            case .showingResults: return .green
            case .error: return .orange
            }
        }
    }

    private var message: String {
        switch captureState {
        case .startingMic:
            return loc(languageCode, "Starting mic", "Mikro startet", "Iniciando mic", "Demarrage micro")
        case .recordingMic:
            return loc(languageCode, "Mic recording", "Mikro laeuft", "Mic grabando", "Micro en cours")
        case .stoppingMic:
            return loc(languageCode, "Stopping mic", "Mikro stoppt", "Deteniendo mic", "Arret micro")
        case .startingSystem:
            return loc(languageCode, "Starting system audio", "Systemaudio startet", "Iniciando audio del sistema", "Demarrage audio systeme")
        case .recordingSystem:
            return loc(languageCode, "System audio recording", "Systemaudio laeuft", "Sistema grabando", "Audio systeme en cours")
        case .stoppingSystem:
            return loc(languageCode, "Stopping system audio", "Systemaudio stoppt", "Deteniendo audio del sistema", "Arret audio systeme")
        case .idle:
            switch viewState {
            case .idle:
                return loc(languageCode, "Ready", "Bereit", "Listo", "Pret")
            case let .analyzing(phase):
                return phase
            case .recordingMic:
                return loc(languageCode, "Mic recording", "Mikro laeuft", "Mic grabando", "Micro en cours")
            case .recordingSystem:
                return loc(languageCode, "System audio recording", "Systemaudio laeuft", "Sistema grabando", "Audio systeme en cours")
            case .showingResults:
                return loc(languageCode, "Results loaded", "Ergebnisse geladen", "Resultados listos", "Resultats charges")
            case let .error(message):
                return message
            }
        }
    }
}

struct AnalyzeWorkspaceView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            AnalyzeComposerView(model: model)
            ResultsSectionView(model: model)
        }
    }
}

struct AnalyzeComposerView: View {
    @ObservedObject var model: AppModel
    @AppStorage(preferredInputDefaultsKey) private var preferredInputRaw = PreferredInput.link.rawValue
    @FocusState private var isInputFocused: Bool

    var body: some View {
        StudioPanel {
            VStack(alignment: .leading, spacing: 20) {
                HStack(alignment: .bottom, spacing: 14) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text(loc(model.languageCode, "Find Music", "Musik finden", "Buscar musica", "Trouver la musique"))
                            .font(.system(size: 30, weight: .black, design: .rounded))
                        Text(loc(model.languageCode, "Paste a link, stage a file, or capture live audio from the command deck below.", "Fuege einen Link ein, waehl eine Datei oder nimm Live-Audio direkt hier auf.", "Pega un enlace, prepara un archivo o captura audio en vivo desde aqui.", "Collez un lien, preparez un fichier ou capturez de l'audio en direct depuis ce panneau."))
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    StatusBannerView(state: model.viewState, captureState: model.captureState)
                }

                HStack(alignment: .top, spacing: 14) {
                    TextField(
                        loc(model.languageCode, "Link or local file", "Link oder lokale Datei", "Enlace o archivo local", "Lien ou fichier local"),
                        text: $model.inputValue,
                        axis: .vertical
                    )
                    .textFieldStyle(.roundedBorder)
                    .font(.system(size: 16))
                    .focused($isInputFocused)

                    Button(primaryActionTitle) {
                        runPrimaryAction()
                    }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.large)
                    .disabled(model.isBusy)
                }

                HStack(spacing: 10) {
                    ForEach(PreferredInput.allCases) { input in
                        Button {
                            preferredInputRaw = input.rawValue
                            trigger(input)
                        } label: {
                            Label(input.title(model.languageCode), systemImage: input.systemImage)
                                .padding(.horizontal, 12)
                                .padding(.vertical, 9)
                                .frame(maxWidth: .infinity)
                        }
                        .buttonStyle(InputChipButtonStyle(isSelected: preferredInput == input))
                    }
                }

                if !model.recentAnalyses.isEmpty {
                    VStack(alignment: .leading, spacing: 12) {
                        Text(loc(model.languageCode, "Recent Runs", "Letzte Laeufe", "Analisis recientes", "Analyses recentes"))
                            .font(.headline)
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 12) {
                                ForEach(model.recentAnalyses) { analysis in
                                    Button {
                                        model.restoreRecent(analysis)
                                    } label: {
                                        RecentAnalysisCard(analysis: analysis, languageCode: model.languageCode)
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                        }
                    }
                }
            }
            .onReceive(NotificationCenter.default.publisher(for: .musicFetchFocusInput)) { _ in
                isInputFocused = true
            }
            .onAppear {
                if preferredInput == .link {
                    isInputFocused = true
                }
            }
        }
    }

    private var preferredInput: PreferredInput {
        PreferredInput(rawValue: preferredInputRaw) ?? .link
    }

    private var primaryActionTitle: String {
        switch preferredInput {
        case .link:
            return loc(model.languageCode, "Analyze Link", "Link analysieren", "Analizar enlace", "Analyser le lien")
        case .file:
            return loc(model.languageCode, "Analyze File", "Datei analysieren", "Analizar archivo", "Analyser le fichier")
        case .microphone:
            return model.captureState == .recordingMic || model.captureState == .stoppingMic
                ? loc(model.languageCode, "Stop Mic", "Mikro stoppen", "Detener mic", "Arreter micro")
                : loc(model.languageCode, "Start Mic", "Mikro starten", "Iniciar mic", "Demarrer micro")
        case .system:
            return model.captureState == .recordingSystem || model.captureState == .stoppingSystem
                ? loc(model.languageCode, "Stop System", "System stoppen", "Detener sistema", "Arreter systeme")
                : loc(model.languageCode, "Start System", "System starten", "Iniciar sistema", "Demarrer systeme")
        }
    }

    private func runPrimaryAction() {
        switch preferredInput {
        case .link:
            model.analyze()
        case .file:
            if FileManager.default.fileExists(atPath: model.inputValue) {
                model.analyze()
            } else {
                model.chooseFile()
            }
        case .microphone:
            model.toggleMicrophoneRecording()
        case .system:
            model.toggleSystemAudioRecording()
        }
    }

    private func trigger(_ input: PreferredInput) {
        switch input {
        case .link:
            isInputFocused = true
        case .file:
            model.chooseFile()
        case .microphone:
            model.toggleMicrophoneRecording()
        case .system:
            model.toggleSystemAudioRecording()
        }
    }
}

struct LibraryWorkspaceView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            StudioPanel {
                HStack(alignment: .center, spacing: 16) {
                    if let selected = model.orderedLibraryEntries.first(where: { $0.job_id == model.selectedLibraryJobID }) {
                        VStack(alignment: .leading, spacing: 6) {
                            Text(selected.title)
                                .font(.system(size: 28, weight: .black, design: .rounded))
                            Text(selected.input_value)
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                            HStack(spacing: 12) {
                                SummaryBadge(title: loc(model.languageCode, "Segments", "Segmente", "Segmentos", "Segments"), value: "\(selected.segment_count)")
                                SummaryBadge(title: loc(model.languageCode, "Artifacts", "Artefakte", "Artefactos", "Artefacts"), value: formatBytes(selected.artifact_size_bytes))
                            }
                        }
                        Spacer()
                        Button(selected.pinned ? loc(model.languageCode, "Unpin", "Losen", "Desfijar", "Detacher") : loc(model.languageCode, "Pin", "Pinnen", "Fijar", "Epingler")) {
                            model.setPinned(jobID: selected.job_id, pinned: !selected.pinned)
                        }
                        .buttonStyle(.bordered)
                        Button(loc(model.languageCode, "Open In Storage", "Im Speicher oeffnen", "Abrir en almacenamiento", "Ouvrir dans stockage")) {
                            model.showStorage(jobID: selected.job_id)
                        }
                        .buttonStyle(.borderedProminent)
                    } else {
                        VStack(alignment: .leading, spacing: 6) {
                            Text(loc(model.languageCode, "Library", "Bibliothek", "Biblioteca", "Bibliotheque"))
                                .font(.system(size: 28, weight: .black, design: .rounded))
                            Text(loc(model.languageCode, "Choose a run from the rail to reopen its timeline.", "Waehle links einen Lauf, um dessen Timeline erneut zu oeffnen.", "Elige un analisis en el lateral para reabrir su timeline.", "Choisissez une analyse dans le panneau pour reouvrir sa timeline."))
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                        }
                        Spacer()
                    }
                }
            }

            ResultsSectionView(model: model)
        }
    }
}

struct StorageWorkspaceView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            StudioPanel {
                HStack(alignment: .top, spacing: 18) {
                    VStack(alignment: .leading, spacing: 8) {
                        Text(model.selectedStorageJobID == nil ? loc(model.languageCode, "Storage Control", "Speicherkontrolle", "Control de almacenamiento", "Controle du stockage") : loc(model.languageCode, "Job Storage", "Job-Speicher", "Almacenamiento del analisis", "Stockage du job"))
                            .font(.system(size: 28, weight: .black, design: .rounded))
                        Text(model.storageSummary?.auto_clean == true ? loc(model.languageCode, "Auto-clean is enabled for temporary material.", "Auto-Clean ist fuer temporaere Daten aktiv.", "La limpieza automatica esta activa para material temporal.", "Le nettoyage automatique est actif pour les donnees temporaires.") : loc(model.languageCode, "Artifacts are currently retained until you clean them up.", "Artefakte bleiben erhalten, bis du sie aufraeumst.", "Los artefactos se conservan hasta que los limpies.", "Les artefacts sont conserves jusqu'au nettoyage."))
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                    Button(loc(model.languageCode, "Clean All", "Alles loeschen", "Limpiar todo", "Tout nettoyer")) {
                        model.cleanupArtifacts()
                    }
                    .buttonStyle(.bordered)

                    if let jobID = model.selectedStorageJobID {
                        Button(loc(model.languageCode, "Clean Job", "Job loeschen", "Limpiar analisis", "Nettoyer le job")) {
                            model.cleanupArtifacts(jobID: jobID)
                        }
                        .buttonStyle(.borderedProminent)
                    }
                }
            }

            HSplitView {
                StudioPanel {
                    VStack(alignment: .leading, spacing: 12) {
                        Text(loc(model.languageCode, "Scopes", "Bereiche", "Alcances", "Portees"))
                            .font(.headline)
                        Button {
                            model.selectStorageScope(nil)
                        } label: {
                            StorageScopeRow(
                                title: loc(model.languageCode, "All Temp Files", "Alle Temp-Dateien", "Todos los temporales", "Tous les temporaires"),
                                subtitle: loc(model.languageCode, "Across every run", "Ueber alle Laeufe", "En todos los analisis", "Sur tous les runs"),
                                isSelected: model.selectedStorageJobID == nil
                            )
                        }
                        .buttonStyle(.plain)

                        ScrollView {
                            LazyVStack(spacing: 8) {
                                ForEach(model.orderedLibraryEntries) { entry in
                                    Button {
                                        model.selectStorageScope(entry.job_id)
                                    } label: {
                                        StorageScopeRow(
                                            title: entry.title,
                                            subtitle: "\(formatBytes(entry.artifact_size_bytes)) • \(entry.segment_count) \(loc(model.languageCode, "segments", "Segmente", "segmentos", "segments"))",
                                            isSelected: model.selectedStorageJobID == entry.job_id
                                        )
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                        }
                    }
                }
                .frame(minWidth: 260, idealWidth: 300, maxWidth: 340)

                StudioPanel {
                    if let summary = model.storageSummary {
                        VStack(alignment: .leading, spacing: 18) {
                            StorageSummaryHeaderView(summary: summary, languageCode: model.languageCode)
                            StorageArtifactsListView(summary: summary, onReveal: model.reveal, languageCode: model.languageCode)
                        }
                    } else {
                        ContentUnavailableView(
                            loc(model.languageCode, "No Storage Yet", "Noch kein Speicher", "Sin almacenamiento", "Pas de stockage"),
                            systemImage: "internaldrive",
                            description: Text(loc(model.languageCode, "Run analysis or pick a job scope to inspect artifacts.", "Starte eine Analyse oder waehle einen Jobbereich, um Artefakte zu sehen.", "Ejecuta un analisis o elige un alcance para inspeccionar artefactos.", "Lancez une analyse ou choisissez une portee pour inspecter les artefacts."))
                        )
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                    }
                }
            }
            .frame(minHeight: 520)
        }
    }
}

struct RecentAnalysisCard: View {
    let analysis: RecentAnalysis
    let languageCode: String

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(analysis.title)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.primary)
                .lineLimit(2)
            Text(relativeDate)
                .font(.caption)
                .foregroundStyle(.secondary)
            HStack(spacing: 10) {
                SummaryBadge(title: loc(languageCode, "Status", "Status", "Estado", "Statut"), value: analysis.status.capitalized)
                SummaryBadge(title: loc(languageCode, "Segments", "Segmente", "Segmentos", "Segments"), value: "\(analysis.segmentCount)")
            }
        }
        .frame(width: 240, alignment: .leading)
        .padding(16)
        .background(Color.white.opacity(0.05), in: RoundedRectangle(cornerRadius: 20, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .strokeBorder(Color.white.opacity(0.08))
        )
    }

    private var relativeDate: String {
        RelativeDateTimeFormatter().localizedString(for: analysis.createdAt, relativeTo: Date())
    }
}

struct InputChipButtonStyle: ButtonStyle {
    let isSelected: Bool

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.subheadline.weight(.semibold))
            .foregroundStyle(isSelected ? Color.white : Color.primary)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(
                        isSelected
                        ? LinearGradient(colors: [Color.orange.opacity(configuration.isPressed ? 0.65 : 0.75), Color.red.opacity(configuration.isPressed ? 0.55 : 0.65)], startPoint: .topLeading, endPoint: .bottomTrailing)
                        : LinearGradient(colors: [Color.white.opacity(configuration.isPressed ? 0.12 : 0.06), Color.white.opacity(0.03)], startPoint: .topLeading, endPoint: .bottomTrailing)
                    )
            )
            .overlay(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .strokeBorder(Color.white.opacity(isSelected ? 0.14 : 0.08))
            )
    }
}

struct LibraryContextRow: View {
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
                Text("\(entry.segment_count) • \(formatBytes(entry.artifact_size_bytes))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(isSelected ? Color.cyan.opacity(0.14) : Color.white.opacity(0.04))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .strokeBorder(isSelected ? Color.cyan.opacity(0.22) : Color.white.opacity(0.06))
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
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(isSelected ? Color.orange.opacity(0.14) : Color.white.opacity(0.04))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .strokeBorder(isSelected ? Color.orange.opacity(0.22) : Color.white.opacity(0.06))
        )
    }
}

struct SummaryBadge: View {
    let title: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title)
                .font(.caption2)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.caption.weight(.semibold))
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(Color.white.opacity(0.05), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
    }
}

struct MenuBarQuickCaptureView: View {
    @ObservedObject var model: AppModel
    @Environment(\.openWindow) private var openWindow

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Music Fetch")
                .font(.headline)
            Text(model.statusTitle)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(model.quickCaptureSummary)
                .font(.subheadline)
                .foregroundStyle(.primary)
                .fixedSize(horizontal: false, vertical: true)

            HStack(spacing: 10) {
                Button(model.captureState == .recordingMic || model.captureState == .stoppingMic ? "Stop Mic" : "Start Mic") {
                    model.toggleMicrophoneRecording()
                }
                .buttonStyle(.borderedProminent)

                Button(model.captureState == .recordingSystem || model.captureState == .stoppingSystem ? "Stop System" : "Start System") {
                    model.toggleSystemAudioRecording()
                }
                .buttonStyle(.bordered)
            }

            Divider()

            Button("Show Main Window") {
                openWindow(id: "main")
                model.showMainWindow()
            }
            .buttonStyle(.bordered)

            Button("Diagnostics") {
                model.showDiagnostics()
            }
            .buttonStyle(.bordered)

            Button("Quit") {
                NSApp.terminate(nil)
            }
            .buttonStyle(.plain)
        }
        .padding(16)
        .frame(width: 300)
    }
}
