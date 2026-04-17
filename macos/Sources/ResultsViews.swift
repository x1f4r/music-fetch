import AppKit
import SwiftUI

// MARK: - Results section (used inside Analyze workspace)

struct ResultsSectionView: View {
    @ObservedObject var model: AppModel
    @AppStorage(debugDetailsDefaultsKey) private var debugDetails = false
    @State private var showOnlySongs = true

    var body: some View {
        Card(padding: 18, cornerRadius: 14) {
            VStack(alignment: .leading, spacing: 14) {
                header

                if case let .analyzing(phase) = model.viewState, model.result == nil {
                    LoadingResultsView(phase: phase, languageCode: model.languageCode)
                } else if let result = model.result {
                    let models = result.segments.map(model.makeSegmentViewModel)
                    let hasSongs = models.contains(where: { $0.payload.kind == "matched_track" })
                    let filtered = filtered(from: models, onlySongs: showOnlySongs && hasSongs)

                    ResultsToolbar(
                        jobStatus: result.job.status,
                        total: models.count,
                        songs: models.filter { $0.payload.kind == "matched_track" }.count,
                        unresolved: models.filter { $0.payload.kind == "music_unresolved" }.count,
                        showOnlySongs: Binding(
                            get: { showOnlySongs && hasSongs },
                            set: { showOnlySongs = $0 }
                        ),
                        hasSongs: hasSongs,
                        onRetry: { model.retryUnresolvedSegments() },
                        onCancel: { model.cancelActiveJob() },
                        onExport: { fmt in model.exportCurrentResults(format: fmt) },
                        languageCode: model.languageCode
                    )

                    if !filtered.isEmpty {
                        ResultsTimelineView(
                            segments: filtered,
                            selectedID: model.selectedSegmentID,
                            onSelect: { id in model.selectSegment(id) }
                        )
                        .frame(height: 30)
                    }

                    SegmentList(
                        segments: filtered,
                        selectedID: segmentSelectionBinding
                    )
                    .frame(minHeight: 260)
                    .onAppear {
                        if model.selectedSegmentID == nil {
                            model.selectedSegmentID = filtered.first?.id
                        }
                    }
                    .onChange(of: filtered.map(\.id)) { _, ids in
                        guard !ids.isEmpty else {
                            model.selectedSegmentID = nil
                            return
                        }
                        if let current = model.selectedSegmentID, ids.contains(current) { return }
                        model.selectedSegmentID = ids.first
                    }

                    if debugDetails, let events = result.events, !events.isEmpty {
                        DebugEventsPanel(events: events, languageCode: model.languageCode)
                    }
                } else {
                    EmptyResultsView(languageCode: model.languageCode)
                }
            }
        }
    }

    private var header: some View {
        HStack(alignment: .center) {
            SectionHeading(
                loc(model.languageCode, "Results", "Ergebnisse", "Resultados", "Resultats"),
                subtitle: model.result.map { result in
                    "\(result.segments.count) " + loc(model.languageCode, "segments", "Segmente", "segmentos", "segments")
                }
            )
            Spacer()
        }
    }

    private var segmentSelectionBinding: Binding<String?> {
        Binding(
            get: { model.selectedSegmentID },
            set: { newValue in
                if let newValue { model.selectSegment(newValue) }
            }
        )
    }

    private func filtered(from models: [SegmentViewModel], onlySongs: Bool) -> [SegmentViewModel] {
        onlySongs ? models.filter { $0.payload.kind == "matched_track" } : models
    }
}

// MARK: - Toolbar

struct ResultsToolbar: View {
    let jobStatus: String
    let total: Int
    let songs: Int
    let unresolved: Int
    @Binding var showOnlySongs: Bool
    let hasSongs: Bool
    let onRetry: () -> Void
    let onCancel: () -> Void
    let onExport: (String) -> Void
    let languageCode: String

    var body: some View {
        HStack(spacing: 10) {
            if hasSongs {
                Picker("", selection: $showOnlySongs) {
                    Text("\(loc(languageCode, "Songs", "Songs", "Canciones", "Titres")) (\(songs))").tag(true)
                    Text("\(loc(languageCode, "All", "Alle", "Todo", "Tout")) (\(total))").tag(false)
                }
                .pickerStyle(.segmented)
                .labelsHidden()
                .frame(width: 220)
            }

            Spacer()

            if unresolved > 0 {
                Button {
                    onRetry()
                } label: {
                    Label(loc(languageCode, "Retry unresolved", "Unklare erneut pruefen", "Reintentar", "Reessayer"),
                          systemImage: "arrow.clockwise")
                }
                .buttonStyle(.bordered)
            }

            Menu {
                Button("JSON") { onExport("json") }
                Button("CSV") { onExport("csv") }
                Button(loc(languageCode, "Chapters", "Kapitel", "Capitulos", "Chapitres")) { onExport("chapters") }
            } label: {
                Label(loc(languageCode, "Export", "Export", "Exportar", "Exporter"), systemImage: "square.and.arrow.up")
            }
            .menuStyle(.borderlessButton)
            .fixedSize()

            if jobStatus == "queued" || jobStatus == "running" {
                Button {
                    onCancel()
                } label: {
                    Label(loc(languageCode, "Cancel", "Abbrechen", "Cancelar", "Annuler"), systemImage: "xmark.circle")
                }
                .buttonStyle(.bordered)
                .tint(.orange)
            }
        }
    }
}

// MARK: - Timeline

struct ResultsTimelineView: View {
    let segments: [SegmentViewModel]
    let selectedID: String?
    let onSelect: (String) -> Void

    var body: some View {
        GeometryReader { geo in
            let totalDuration = max(1, segments.map(\.endMs).max() ?? 1)
            let width = geo.size.width
            ZStack(alignment: .leading) {
                RoundedRectangle(cornerRadius: 6, style: .continuous)
                    .fill(Color.primary.opacity(0.06))

                ForEach(segments) { seg in
                    let segWidth = max(3, CGFloat(seg.endMs - seg.startMs) / CGFloat(totalDuration) * width)
                    let segX = CGFloat(seg.startMs) / CGFloat(totalDuration) * width
                    let isSelected = selectedID == seg.id

                    Button {
                        onSelect(seg.id)
                    } label: {
                        RoundedRectangle(cornerRadius: 4, style: .continuous)
                            .fill(seg.timelineColor.opacity(isSelected ? 1.0 : 0.75))
                            .overlay(
                                RoundedRectangle(cornerRadius: 4, style: .continuous)
                                    .strokeBorder(isSelected ? Color.primary.opacity(0.55) : Color.clear, lineWidth: 1.5)
                            )
                    }
                    .buttonStyle(.plain)
                    .frame(width: segWidth, height: isSelected ? 26 : 20)
                    .offset(x: segX, y: isSelected ? 2 : 5)
                    .help(seg.title)
                }
            }
            .clipShape(RoundedRectangle(cornerRadius: 6, style: .continuous))
        }
    }
}

// MARK: - Segment list

struct SegmentList: View {
    let segments: [SegmentViewModel]
    @Binding var selectedID: String?

    var body: some View {
        List(selection: $selectedID) {
            ForEach(segments) { seg in
                SegmentRow(segment: seg, isSelected: selectedID == seg.id)
                    .tag(Optional(seg.id))
                    .listRowSeparator(.hidden)
                    .listRowInsets(EdgeInsets(top: 2, leading: 2, bottom: 2, trailing: 2))
                    .listRowBackground(Color.clear)
            }
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
        .environment(\.defaultMinListRowHeight, 52)
    }
}

struct SegmentRow: View {
    let segment: SegmentViewModel
    let isSelected: Bool

    var body: some View {
        HStack(alignment: .center, spacing: 12) {
            RoundedRectangle(cornerRadius: 2, style: .continuous)
                .fill(segment.timelineColor)
                .frame(width: 4, height: 36)

            VStack(alignment: .leading, spacing: 2) {
                Text(segment.title)
                    .font(.callout.weight(.medium))
                    .foregroundStyle(.primary)
                    .lineLimit(1)
                HStack(spacing: 6) {
                    Text("\(formatTime(segment.startMs)) – \(formatTime(segment.endMs))")
                        .monospacedDigit()
                    Text("·")
                    Text(segment.detailLabel)
                    if segment.repeatGroupID != nil {
                        Text("·")
                        Image(systemName: "repeat")
                            .font(.caption2)
                    }
                }
                .font(.caption)
                .foregroundStyle(.secondary)
            }

            Spacer(minLength: 8)

            if let quality = segment.qualityLabel {
                Text(quality)
                    .font(.caption2.weight(.medium))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 7)
                    .padding(.vertical, 3)
                    .background(Color.primary.opacity(0.08), in: Capsule())
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .fill(isSelected ? Color.accentColor.opacity(0.16) : Color.clear)
        )
        .contentShape(Rectangle())
    }
}

// MARK: - Loading & empty states

struct LoadingResultsView: View {
    let phase: String
    let languageCode: String

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 10) {
                ProgressView().controlSize(.small)
                Text(phase)
                    .font(.callout.weight(.medium))
                    .foregroundStyle(.primary)
                Spacer()
            }
            Text(loc(languageCode, "Long mixes are segmented first, then matched to songs.",
                     "Lange Mixe werden erst segmentiert und dann Songs zugeordnet.",
                     "Las mezclas largas se segmentan primero y luego se identifican.",
                     "Les longs mixes sont d'abord segmentes puis identifies."))
                .font(.caption)
                .foregroundStyle(.secondary)
            ForEach(0..<3, id: \.self) { _ in
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(Color.primary.opacity(0.06))
                    .frame(height: 44)
            }
        }
        .padding(.vertical, 4)
    }
}

struct EmptyResultsView: View {
    let languageCode: String

    var body: some View {
        ContentUnavailableView(
            loc(languageCode, "No analysis yet", "Noch keine Analyse", "Sin analisis", "Aucune analyse"),
            systemImage: "waveform.and.magnifyingglass",
            description: Text(loc(languageCode,
                                  "Paste a link above and press Analyze to see the timeline.",
                                  "Fuege oben einen Link ein und starte die Analyse.",
                                  "Pega un enlace y pulsa Analizar para ver la timeline.",
                                  "Collez un lien et appuyez sur Analyser pour voir la timeline."))
        )
        .frame(maxWidth: .infinity, minHeight: 220)
    }
}

// MARK: - Debug events

struct DebugEventsPanel: View {
    let events: [JobEventPayload]
    let languageCode: String

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(loc(languageCode, "Debug events", "Debug-Events", "Eventos debug", "Evenements debug"))
                .font(.subheadline.weight(.medium))
                .foregroundStyle(.secondary)
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 4) {
                    ForEach(events) { event in
                        HStack(alignment: .firstTextBaseline, spacing: 8) {
                            Text(event.level.uppercased())
                                .font(.caption2.weight(.bold))
                                .foregroundStyle(color(for: event.level))
                                .frame(width: 54, alignment: .leading)
                            Text(event.message)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .textSelection(.enabled)
                        }
                        .padding(.vertical, 2)
                    }
                }
            }
            .frame(minHeight: 100, maxHeight: 200)
            .padding(10)
            .background(Color.primary.opacity(0.04), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
        }
    }

    private func color(for level: String) -> Color {
        switch level.lowercased() {
        case "error": return .red
        case "warning": return .orange
        default: return .blue
        }
    }
}

// MARK: - Inspector (segment details)

struct InspectorView: View {
    @ObservedObject var model: AppModel
    @State private var correctionSheetPresented = false
    @State private var correctionTitle = ""
    @State private var correctionArtist = ""
    @State private var correctionAlbum = ""
    @State private var correctionPayload: SegmentPayload?

    var body: some View {
        Group {
            if let segment = currentSegment {
                ScrollView {
                    VStack(alignment: .leading, spacing: 18) {
                        headerSection(segment)
                        metadataSection(segment)
                        actionsSection(segment)
                        if let explanation = segment.payload.explanation, !explanation.isEmpty {
                            explanationSection(explanation)
                        }
                        if !segment.payload.alternates.isEmpty {
                            alternatesSection(segment.payload.alternates)
                        }
                    }
                    .padding(18)
                    .frame(maxWidth: .infinity, alignment: .topLeading)
                }
            } else {
                ContentUnavailableView(
                    loc(model.languageCode, "No segment", "Kein Abschnitt", "Sin segmento", "Aucun segment"),
                    systemImage: "cursorarrow.click",
                    description: Text(loc(model.languageCode,
                                          "Select a segment to see details.",
                                          "Waehle ein Segment, um Details zu sehen.",
                                          "Elige un segmento para ver detalles.",
                                          "Choisissez un segment pour voir les details."))
                )
            }
        }
        .background(Color(NSColor.windowBackgroundColor))
        .sheet(isPresented: $correctionSheetPresented) {
            correctionSheet
        }
    }

    private var currentSegment: SegmentViewModel? {
        guard let result = model.result else { return nil }
        let models = result.segments.map(model.makeSegmentViewModel)
        if let id = model.selectedSegmentID,
           let match = models.first(where: { $0.id == id }) {
            return match
        }
        return models.first
    }

    // MARK: sections

    private func headerSection(_ segment: SegmentViewModel) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .top, spacing: 10) {
                RoundedRectangle(cornerRadius: 4, style: .continuous)
                    .fill(segment.timelineColor)
                    .frame(width: 6, height: 46)
                VStack(alignment: .leading, spacing: 3) {
                    Text(segment.title)
                        .font(.title3.weight(.semibold))
                        .fixedSize(horizontal: false, vertical: true)
                    Text(segment.subtitle)
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer(minLength: 0)
            }
        }
    }

    private func metadataSection(_ segment: SegmentViewModel) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 6) {
                InspectorPill(icon: "clock", text: "\(formatTime(segment.startMs)) – \(formatTime(segment.endMs))")
                InspectorPill(icon: "sparkles", text: segment.detailLabel)
            }
            HStack(spacing: 6) {
                if let quality = segment.qualityLabel {
                    InspectorPill(icon: "checkmark.seal", text: quality)
                }
                if let uncertainty = segment.payload.uncertainty {
                    InspectorPill(icon: "gauge.with.dots.needle.33percent",
                                  text: String(format: "U %.2f", uncertainty))
                }
            }

            if let hint = segment.metadataHint, !hint.isEmpty {
                Text(cleanHint(hint))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }

    private func actionsSection(_ segment: SegmentViewModel) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(loc(model.languageCode, "Actions", "Aktionen", "Acciones", "Actions"))
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)

            HStack(spacing: 8) {
                Button {
                    model.copy(segment.title)
                } label: {
                    Label(loc(model.languageCode, "Copy", "Kopieren", "Copiar", "Copier"), systemImage: "doc.on.doc")
                }
                .buttonStyle(.bordered)
                .controlSize(.small)

                if segment.isInteractive {
                    Button {
                        correctionPayload = segment.payload
                        correctionTitle = segment.payload.track?.title ?? ""
                        correctionArtist = segment.payload.track?.artist ?? ""
                        correctionAlbum = segment.payload.track?.album ?? ""
                        correctionSheetPresented = true
                    } label: {
                        Label(loc(model.languageCode, "Correct", "Korrigieren", "Corregir", "Corriger"), systemImage: "pencil")
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                }
                Spacer()
            }

            if !segment.primaryLinks.isEmpty || !segment.overflowLinks.isEmpty {
                VStack(alignment: .leading, spacing: 6) {
                    Text(loc(model.languageCode, "Listen", "Anhoeren", "Escuchar", "Ecouter"))
                        .font(.caption.weight(.medium))
                        .foregroundStyle(.secondary)
                        .textCase(.uppercase)
                    FlowLinks(primary: segment.primaryLinks, overflow: segment.overflowLinks, languageCode: model.languageCode)
                }
                .padding(.top, 4)
            }
        }
    }

    private func explanationSection(_ lines: [String]) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(loc(model.languageCode, "Why this result", "Warum dieses Ergebnis", "Por que este resultado", "Pourquoi ce resultat"))
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
            VStack(alignment: .leading, spacing: 4) {
                ForEach(lines, id: \.self) { line in
                    HStack(alignment: .top, spacing: 6) {
                        Text("•")
                            .foregroundStyle(.tertiary)
                        Text(line)
                            .font(.callout)
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
            }
        }
    }

    private func alternatesSection(_ tracks: [TrackMatchPayload]) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(loc(model.languageCode, "Alternates", "Weitere Kandidaten", "Alternativas", "Alternatives"))
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
            VStack(alignment: .leading, spacing: 4) {
                ForEach(tracks, id: \.self) { track in
                    Text(track.artist.map { "\($0) · \(track.title)" } ?? track.title)
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }
        }
    }

    private var correctionSheet: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text(loc(model.languageCode, "Manual correction", "Manuelle Korrektur", "Correccion manual", "Correction manuelle"))
                .font(.title3.weight(.semibold))
            Form {
                TextField(loc(model.languageCode, "Title", "Titel", "Titulo", "Titre"), text: $correctionTitle)
                TextField(loc(model.languageCode, "Artist", "Artist", "Artista", "Artiste"), text: $correctionArtist)
                TextField(loc(model.languageCode, "Album", "Album", "Album", "Album"), text: $correctionAlbum)
            }
            .formStyle(.grouped)

            HStack {
                Spacer()
                Button(loc(model.languageCode, "Cancel", "Abbrechen", "Cancelar", "Annuler")) {
                    correctionSheetPresented = false
                }
                .keyboardShortcut(.cancelAction)
                Button(loc(model.languageCode, "Save", "Speichern", "Guardar", "Enregistrer")) {
                    if let payload = correctionPayload {
                        model.correctSegment(
                            payload,
                            title: correctionTitle,
                            artist: correctionArtist.isEmpty ? nil : correctionArtist,
                            album: correctionAlbum.isEmpty ? nil : correctionAlbum
                        )
                    }
                    correctionSheetPresented = false
                }
                .buttonStyle(.borderedProminent)
                .keyboardShortcut(.defaultAction)
                .disabled(correctionTitle.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }
        .padding(20)
        .frame(width: 380)
    }

    private func cleanHint(_ hint: String) -> String {
        hint.replacingOccurrences(of: "tracklist:", with: "")
            .replacingOccurrences(of: "chapter:", with: "")
    }
}

struct InspectorPill: View {
    let icon: String
    let text: String

    var body: some View {
        HStack(spacing: 4) {
            Image(systemName: icon)
                .font(.caption2)
            Text(text)
                .font(.caption.weight(.medium))
        }
        .foregroundStyle(.secondary)
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .background(Color.primary.opacity(0.06), in: Capsule())
    }
}

struct FlowLinks: View {
    let primary: [PlatformLink]
    let overflow: [PlatformLink]
    let languageCode: String

    var body: some View {
        HStack(spacing: 6) {
            ForEach(primary) { link in
                Link(destination: link.url) {
                    Label(link.label, systemImage: link.icon)
                        .font(.caption.weight(.medium))
                        .padding(.horizontal, 9)
                        .padding(.vertical, 5)
                        .foregroundStyle(Color.white)
                        .background(link.color.gradient, in: Capsule())
                }
                .buttonStyle(.plain)
            }

            if !overflow.isEmpty {
                Menu {
                    ForEach(overflow) { link in
                        Link(destination: link.url) {
                            Label(link.label, systemImage: link.icon)
                        }
                    }
                } label: {
                    Label(loc(languageCode, "More", "Mehr", "Mas", "Plus"), systemImage: "ellipsis.circle")
                        .labelStyle(.iconOnly)
                }
                .menuStyle(.borderlessButton)
                .fixedSize()
            }
        }
    }
}
