import SwiftUI

struct StatusBannerView: View {
    let state: AppViewState
    let captureState: CaptureState
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()

    var body: some View {
        HStack(spacing: 10) {
            switch captureState {
            case .startingMic:
                ProgressView()
                    .controlSize(.small)
                Text(loc(languageCode, "Starting mic...", "Mikro startet...", "Iniciando mic...", "Demarrage micro..."))
                    .foregroundStyle(.secondary)
            case .recordingMic:
                Image(systemName: "mic.fill")
                    .foregroundStyle(.red)
                Text(loc(languageCode, "Mic is recording. Tap again to stop.", "Mikro laeuft. Erneut tippen zum Stoppen.", "El microfono graba. Pulsa otra vez para parar.", "Le micro enregistre. Touchez encore pour arreter."))
                    .foregroundStyle(.secondary)
            case .stoppingMic:
                ProgressView()
                    .controlSize(.small)
                Text(loc(languageCode, "Finishing mic clip...", "Mikroclip wird beendet...", "Finalizando clip de mic...", "Finalisation du clip micro..."))
                    .foregroundStyle(.secondary)
            case .startingSystem:
                ProgressView()
                    .controlSize(.small)
                Text(loc(languageCode, "Starting system audio...", "Systemaudio startet...", "Iniciando audio del sistema...", "Demarrage audio systeme..."))
                    .foregroundStyle(.secondary)
            case .recordingSystem:
                Image(systemName: "waveform")
                    .foregroundStyle(.orange)
                Text(loc(languageCode, "System audio is recording. Tap again to stop.", "Systemaudio laeuft. Erneut tippen zum Stoppen.", "El audio del sistema graba. Pulsa otra vez para parar.", "L'audio systeme enregistre. Touchez encore pour arreter."))
                    .foregroundStyle(.secondary)
            case .stoppingSystem:
                ProgressView()
                    .controlSize(.small)
                Text(loc(languageCode, "Finishing system clip...", "Systemclip wird beendet...", "Finalizando clip del sistema...", "Finalisation du clip systeme..."))
                    .foregroundStyle(.secondary)
            case .idle:
                switch state {
                case .idle:
                    Image(systemName: "sparkles")
                        .foregroundStyle(.secondary)
                    Text(loc(languageCode, "Paste a link or choose a file.", "Link einfuegen oder Datei waehlen.", "Pega un enlace o elige un archivo.", "Collez un lien ou choisissez un fichier."))
                        .foregroundStyle(.secondary)
                case let .analyzing(phase):
                    ProgressView()
                        .controlSize(.small)
                    Text(phase)
                        .foregroundStyle(.secondary)
                case .recordingMic, .recordingSystem:
                    EmptyView()
                case .showingResults:
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundStyle(.green)
                    Text(loc(languageCode, "Analysis done.", "Analyse fertig.", "Analisis listo.", "Analyse terminee."))
                        .foregroundStyle(.secondary)
                case let .error(message):
                    Image(systemName: "exclamationmark.triangle.fill")
                        .foregroundStyle(.orange)
                    Text(message)
                        .foregroundStyle(.secondary)
                }
            }
            Spacer()
        }
        .font(.subheadline)
    }
}

struct ResultsSectionView: View {
    @ObservedObject var model: AppModel
    @AppStorage(debugDetailsDefaultsKey) private var debugDetails = false
    @State private var showOnlySongs = true

    var body: some View {
        StudioPanel {
            VStack(alignment: .leading, spacing: 18) {
                HStack(alignment: .center) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(loc(model.languageCode, "Results", "Ergebnisse", "Resultados", "Resultats"))
                            .font(.headline)
                        if let result = model.result {
                            Text("\(result.segments.count) \(loc(model.languageCode, "segments", "Segmente", "segmentos", "segments"))")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }
                    Spacer()
                    if let result = model.result {
                        HStack(spacing: 10) {
                            ResultMetricChip(title: loc(model.languageCode, "Job", "Job", "Job", "Job"), value: result.job.status.capitalized)
                            ResultMetricChip(title: loc(model.languageCode, "Items", "Elemente", "Elementos", "Elements"), value: "\(result.items.count)")
                        }
                    }
                }

                if case let .analyzing(phase) = model.viewState {
                    LoadingResultsView(phase: phase)
                } else if let result = model.result {
                    let viewModels = result.segments.map(model.makeSegmentViewModel)
                    let hasSongs = viewModels.contains(where: { $0.payload.kind == "matched_track" })
                    let filteredModels = filteredSegments(from: viewModels, showOnlySongs: showOnlySongs && hasSongs)

                    ResultsToolbarView(
                        jobStatus: result.job.status,
                        totalCount: viewModels.count,
                        songCount: viewModels.filter { $0.payload.kind == "matched_track" }.count,
                        unresolvedCount: viewModels.filter { $0.payload.kind == "music_unresolved" }.count,
                        showOnlySongs: Binding(
                            get: { showOnlySongs && hasSongs },
                            set: { showOnlySongs = $0 }
                        ),
                        hasSongs: hasSongs,
                        onRetry: { model.retryUnresolvedSegments() },
                        onCancel: { model.cancelActiveJob() },
                        onExport: { format in model.exportCurrentResults(format: format) },
                        languageCode: model.languageCode
                    )

                    ResultsTimelineView(segments: filteredModels, selectedSegmentID: model.selectedSegmentID) { segmentID in
                        model.selectSegment(segmentID)
                    }
                    .frame(height: 42)

                    CompactResultsView(
                        segments: filteredModels,
                        selectedSegmentID: Binding(
                            get: { model.selectedSegmentID },
                            set: { if let value = $0 { model.selectSegment(value) } }
                        ),
                        onCopy: { text in model.copy(text) },
                        onCorrect: { segment, title, artist, album in
                            model.correctSegment(segment, title: title, artist: artist, album: album)
                        },
                        languageCode: model.languageCode
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

                    if debugDetails, let events = result.events, !events.isEmpty {
                        DebugEventsPanel(events: events, languageCode: model.languageCode)
                    }
                } else {
                    EmptyStateView(languageCode: model.languageCode)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    private func filteredSegments(from segments: [SegmentViewModel], showOnlySongs: Bool) -> [SegmentViewModel] {
        if showOnlySongs {
            return segments.filter { $0.payload.kind == "matched_track" }
        }
        return segments
    }
}

struct ResultMetricChip: View {
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
        .background(Color.white.opacity(0.06), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
    }
}

struct ResultsToolbarView: View {
    let jobStatus: String
    let totalCount: Int
    let songCount: Int
    let unresolvedCount: Int
    @Binding var showOnlySongs: Bool
    let hasSongs: Bool
    let onRetry: () -> Void
    let onCancel: () -> Void
    let onExport: (String) -> Void
    let languageCode: String

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

            if unresolvedCount > 0 {
                Button(loc(languageCode, "Retry unresolved", "Unklare erneut pruefen", "Reintentar sin resolver", "Reessayer les non resolus")) {
                    onRetry()
                }
                .buttonStyle(.bordered)
            }

            Menu(loc(languageCode, "Export", "Export", "Exportar", "Exporter")) {
                Button("JSON") { onExport("json") }
                Button("CSV") { onExport("csv") }
                Button(loc(languageCode, "Chapters", "Kapitel", "Capitulos", "Chapitres")) { onExport("chapters") }
            }
            .menuStyle(.borderlessButton)

            if jobStatus == "queued" || jobStatus == "running" {
                Button(loc(languageCode, "Cancel", "Abbrechen", "Cancelar", "Annuler")) {
                    onCancel()
                }
                .buttonStyle(.borderedProminent)
                .tint(.orange)
            }

            Text(
                hasSongs && showOnlySongs
                ? "\(songCount) \(loc(languageCode, "songs", "Songs", "canciones", "titres"))"
                : "\(totalCount) \(loc(languageCode, "items", "Abschnitte", "bloques", "blocs"))"
            )
            .font(.caption)
            .foregroundStyle(.secondary)
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
            Text(loc(languageCode, "Long mixes are segmented first, then matched.", "Lange Mixe werden erst segmentiert, dann zugeordnet.", "Las mezclas largas se segmentan primero y luego se identifican.", "Les longs mixes sont d'abord segmentes puis identifies."))
                .foregroundStyle(.secondary)
            RoundedRectangle(cornerRadius: 10)
                .fill(Color.white.opacity(0.06))
                .frame(height: 14)
            ForEach(0..<3, id: \.self) { _ in
                RoundedRectangle(cornerRadius: 18)
                    .fill(Color.white.opacity(0.04))
                    .frame(height: 92)
            }
        }
    }
}

struct EmptyStateView: View {
    let languageCode: String

    var body: some View {
        ContentUnavailableView(
            loc(languageCode, "No analysis", "Noch keine Analyse", "Sin analisis", "Pas d'analyse"),
            systemImage: "waveform.and.magnifyingglass",
            description: Text(loc(languageCode, "Run analysis to see the timeline.", "Starte eine Analyse, um die Timeline zu sehen.", "Ejecuta un analisis para ver la timeline.", "Lancez une analyse pour voir la timeline."))
        )
        .frame(maxWidth: .infinity, minHeight: 260)
    }
}

struct ResultsTimelineView: View {
    let segments: [SegmentViewModel]
    let selectedSegmentID: String?
    let onSelect: (String) -> Void
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()
    private let trackInset: CGFloat = 4

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(loc(languageCode, "Timeline", "Timeline", "Timeline", "Timeline"))
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)
            GeometryReader { geometry in
                let totalDuration = max(1, segments.map(\.endMs).max() ?? 1)
                let trackWidth = max(0, geometry.size.width - (trackInset * 2))
                ZStack(alignment: .leading) {
                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                        .fill(Color.white.opacity(0.06))
                    ForEach(segments) { segment in
                        let segmentWidth = clampedWidth(for: segment, totalDuration: totalDuration, trackWidth: trackWidth)
                        let segmentOffset = clampedOffset(for: segment, totalDuration: totalDuration, trackWidth: trackWidth, segmentWidth: segmentWidth)
                        Button {
                            onSelect(segment.id)
                        } label: {
                            RoundedRectangle(cornerRadius: 10, style: .continuous)
                                .fill(segment.timelineColor)
                                .overlay(
                                    RoundedRectangle(cornerRadius: 10, style: .continuous)
                                        .strokeBorder(selectedSegmentID == segment.id ? Color.white.opacity(0.42) : .clear, lineWidth: 2)
                                )
                        }
                        .buttonStyle(.plain)
                        .frame(width: segmentWidth, height: selectedSegmentID == segment.id ? 28 : 20)
                        .offset(x: trackInset + segmentOffset)
                        .help(segment.title)
                    }
                }
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
            }
            .frame(height: 30)
        }
    }

    private func clampedWidth(for segment: SegmentViewModel, totalDuration: Int, trackWidth: CGFloat) -> CGFloat {
        guard trackWidth > 0 else { return 0 }
        let fraction = CGFloat(segment.endMs - segment.startMs) / CGFloat(totalDuration)
        return min(trackWidth, max(6, trackWidth * fraction))
    }

    private func clampedOffset(for segment: SegmentViewModel, totalDuration: Int, trackWidth: CGFloat, segmentWidth: CGFloat) -> CGFloat {
        guard trackWidth > 0 else { return 0 }
        let rawOffset = CGFloat(segment.startMs) / CGFloat(totalDuration) * trackWidth
        return min(max(0, rawOffset), max(0, trackWidth - segmentWidth))
    }
}

struct CompactResultsView: View {
    let segments: [SegmentViewModel]
    @Binding var selectedSegmentID: String?
    let onCopy: (String) -> Void
    let onCorrect: (SegmentPayload, String, String?, String?) -> Void
    let languageCode: String

    var body: some View {
        HSplitView {
            SegmentListPane(segments: segments, selectedSegmentID: $selectedSegmentID)
                .frame(minWidth: 260, idealWidth: 300, maxWidth: 340)

            SegmentInspectorPane(
                viewModel: segments.first(where: { $0.id == selectedSegmentID }) ?? segments.first,
                onCopy: onCopy,
                onCorrect: onCorrect,
                languageCode: languageCode
            )
            .frame(minWidth: 360, maxWidth: .infinity)
        }
        .frame(minHeight: 380, idealHeight: 460)
        .background(Color.white.opacity(0.03), in: RoundedRectangle(cornerRadius: 20, style: .continuous))
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
                                .fill(selectedSegmentID == segment.id ? Color.cyan.opacity(0.12) : Color.white.opacity(0.04))
                        )
                        .overlay(
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .strokeBorder(selectedSegmentID == segment.id ? Color.cyan.opacity(0.26) : Color.white.opacity(0.06))
                        )
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(12)
        }
    }

    private func timeRange(_ segment: SegmentViewModel) -> String {
        "\(formatTime(segment.startMs))-\(formatTime(segment.endMs))"
    }
}

struct SegmentInspectorPane: View {
    let viewModel: SegmentViewModel?
    let onCopy: (String) -> Void
    let onCorrect: (SegmentPayload, String, String?, String?) -> Void
    let languageCode: String
    @State private var correctionTitle = ""
    @State private var correctionArtist = ""
    @State private var correctionAlbum = ""
    @State private var showCorrectionSheet = false

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
                        InfoPill(systemImage: "clock", text: "\(formatTime(viewModel.startMs)) - \(formatTime(viewModel.endMs))")
                        InfoPill(systemImage: "sparkles", text: viewModel.detailLabel)
                        if let quality = viewModel.qualityLabel {
                            InfoPill(systemImage: "checkmark.seal", text: quality)
                        }
                        if let uncertainty = viewModel.payload.uncertainty {
                            InfoPill(systemImage: "gauge.with.dots.needle.33percent", text: "U \(String(format: "%.2f", uncertainty))")
                        }
                    }

                    if let hint = viewModel.metadataHint, !hint.isEmpty {
                        Text(cleanHint(hint))
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }

                    Divider()

                    VStack(alignment: .leading, spacing: 12) {
                        Text(loc(languageCode, "Actions", "Aktionen", "Acciones", "Actions"))
                            .font(.headline)

                        HStack(spacing: 10) {
                            Button(loc(languageCode, "Copy", "Kopieren", "Copiar", "Copier")) {
                                onCopy(viewModel.title)
                            }
                            .buttonStyle(.bordered)

                            Button(loc(languageCode, "Correct", "Korrigieren", "Corregir", "Corriger")) {
                                correctionTitle = viewModel.payload.track?.title ?? ""
                                correctionArtist = viewModel.payload.track?.artist ?? ""
                                correctionAlbum = viewModel.payload.track?.album ?? ""
                                showCorrectionSheet = true
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
                                Menu(loc(languageCode, "More", "Mehr", "Mas", "Plus")) {
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

                    if let explanation = viewModel.payload.explanation, !explanation.isEmpty {
                        VStack(alignment: .leading, spacing: 10) {
                            Text(loc(languageCode, "Why this result", "Warum dieses Ergebnis", "Por que este resultado", "Pourquoi ce resultat"))
                                .font(.headline)
                            ForEach(explanation, id: \.self) { line in
                                Text(line)
                                    .font(.subheadline)
                                    .foregroundStyle(.secondary)
                                    .fixedSize(horizontal: false, vertical: true)
                            }
                        }
                    }

                    if !viewModel.payload.alternates.isEmpty {
                        VStack(alignment: .leading, spacing: 10) {
                            Text(loc(languageCode, "Alternates", "Weitere Kandidaten", "Alternativas", "Alternatives"))
                                .font(.headline)
                            ForEach(viewModel.payload.alternates, id: \.self) { alternate in
                                Text(alternate.artist.map { "\($0) - \(alternate.title)" } ?? alternate.title)
                                    .font(.subheadline)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }

                    Spacer(minLength: 0)
                }
                .padding(24)
                .sheet(isPresented: $showCorrectionSheet) {
                    VStack(alignment: .leading, spacing: 16) {
                        Text(loc(languageCode, "Manual correction", "Manuelle Korrektur", "Correccion manual", "Correction manuelle"))
                            .font(.title3.weight(.semibold))
                        TextField(loc(languageCode, "Title", "Titel", "Titulo", "Titre"), text: $correctionTitle)
                            .textFieldStyle(.roundedBorder)
                        TextField(loc(languageCode, "Artist", "Artist", "Artista", "Artiste"), text: $correctionArtist)
                            .textFieldStyle(.roundedBorder)
                        TextField(loc(languageCode, "Album", "Album", "Album", "Album"), text: $correctionAlbum)
                            .textFieldStyle(.roundedBorder)
                        HStack {
                            Spacer()
                            Button(loc(languageCode, "Cancel", "Abbrechen", "Cancelar", "Annuler")) {
                                showCorrectionSheet = false
                            }
                            Button(loc(languageCode, "Save", "Speichern", "Guardar", "Enregistrer")) {
                                onCorrect(
                                    viewModel.payload,
                                    correctionTitle,
                                    correctionArtist.isEmpty ? nil : correctionArtist,
                                    correctionAlbum.isEmpty ? nil : correctionAlbum
                                )
                                showCorrectionSheet = false
                            }
                            .buttonStyle(.borderedProminent)
                            .disabled(correctionTitle.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        }
                    }
                    .padding(24)
                    .frame(width: 360)
                }
            } else {
                ContentUnavailableView(
                    loc(languageCode, "No segment", "Kein Abschnitt gewaehlt", "Sin segmento", "Aucun segment"),
                    systemImage: "line.3.horizontal.decrease.circle",
                    description: Text(loc(languageCode, "Pick a segment to inspect it.", "Waehle links einen Abschnitt.", "Elige un segmento.", "Choisissez un segment."))
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
    }

    private func cleanHint(_ hint: String) -> String {
        hint.replacingOccurrences(of: "tracklist:", with: "")
            .replacingOccurrences(of: "chapter:", with: "")
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
            .background(Color.white.opacity(0.06), in: Capsule())
            .foregroundStyle(.secondary)
    }
}

struct DebugEventsPanel: View {
    let events: [JobEventPayload]
    let languageCode: String

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(loc(languageCode, "Debug Event Stream", "Debug-Event-Stream", "Flujo de eventos debug", "Flux d'evenements debug"))
                .font(.headline)
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 8) {
                    ForEach(events) { event in
                        VStack(alignment: .leading, spacing: 4) {
                            HStack {
                                Text(event.level.uppercased())
                                    .font(.caption2.weight(.bold))
                                    .foregroundStyle(color(for: event.level))
                                Spacer()
                                Text(event.created_at)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                            Text(event.message)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .textSelection(.enabled)
                        }
                        .padding(12)
                        .background(Color.white.opacity(0.04), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                }
            }
            .frame(minHeight: 120, maxHeight: 220)
        }
    }

    private func color(for level: String) -> Color {
        switch level.lowercased() {
        case "error":
            return .red
        case "warning":
            return .orange
        default:
            return .cyan
        }
    }
}

struct StorageSummaryHeaderView: View {
    let summary: StorageSummaryPayload
    let languageCode: String

    var body: some View {
        HStack(spacing: 14) {
            SummaryChip(title: loc(languageCode, "Artifacts", "Artefakte", "Artefactos", "Artefacts"), value: "\(summary.entries.count)")
            SummaryChip(title: loc(languageCode, "Size", "Groesse", "Tamano", "Taille"), value: formatBytes(summary.total_size_bytes))
            SummaryChip(title: loc(languageCode, "Policy", "Regel", "Politica", "Politique"), value: summary.auto_clean ? "Auto-clean" : "Retained")
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
        .background(Color.white.opacity(0.06), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
    }
}

struct StorageArtifactsListView: View {
    let summary: StorageSummaryPayload
    let onReveal: (String) -> Void
    let languageCode: String

    var body: some View {
        ScrollView {
            LazyVStack(alignment: .leading, spacing: 12) {
                if !summary.categories.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text(loc(languageCode, "Categories", "Kategorien", "Categorias", "Categories"))
                            .font(.headline)
                        ForEach(summary.categories, id: \.category) { category in
                            HStack {
                                Text(category.category.replacingOccurrences(of: "_", with: " ").capitalized)
                                Spacer()
                                Text("\(category.count)")
                                    .foregroundStyle(.secondary)
                                Text(formatBytes(category.size_bytes))
                                    .foregroundStyle(.secondary)
                            }
                            .font(.subheadline)
                        }
                    }
                    .padding(18)
                    .background(Color.white.opacity(0.04), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
                }

                VStack(alignment: .leading, spacing: 8) {
                    Text(loc(languageCode, "Files And Folders", "Dateien und Ordner", "Archivos y carpetas", "Fichiers et dossiers"))
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
                                        Text(loc(languageCode, "Pinned", "Gepinnt", "Fijado", "Epingle"))
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
                                Text(formatBytes(entry.size_bytes))
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer()
                            Button(loc(languageCode, "Reveal", "Anzeigen", "Mostrar", "Afficher")) {
                                onReveal(entry.path)
                            }
                            .buttonStyle(.bordered)
                        }
                        .padding(14)
                        .background(Color.white.opacity(0.03), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                }
                .padding(18)
                .background(Color.white.opacity(0.04), in: RoundedRectangle(cornerRadius: 18, style: .continuous))

                VStack(alignment: .leading, spacing: 8) {
                    Text(loc(languageCode, "Locations", "Orte", "Ubicaciones", "Emplacements"))
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
                .background(Color.white.opacity(0.04), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
            }
        }
    }
}
