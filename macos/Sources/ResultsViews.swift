import AppKit
import SwiftUI

// MARK: - Results section (used inside Analyze workspace)

struct ResultsSectionView: View {
    @ObservedObject var model: AppModel
    @AppStorage(debugDetailsDefaultsKey) private var debugDetails = false
    @State private var showOnlySongs = true
    @State private var timelineZoom: Double = 1.0

    var body: some View {
        Panel(padding: Theme.Space.m, radius: Theme.Radius.panel) {
            VStack(alignment: .leading, spacing: Theme.Space.s) {
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
                        HStack(spacing: Theme.Space.xs) {
                            ResultsTimelineView(
                                segments: filtered,
                                selectedID: model.selectedSegmentID,
                                onSelect: { id in model.selectSegment(id) },
                                zoom: $timelineZoom
                            )
                            .frame(height: 36)

                            TimelineZoomControl(zoom: $timelineZoom, languageCode: model.languageCode)
                        }
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
        HStack(alignment: .firstTextBaseline, spacing: Theme.Space.xs) {
            SectionLabel(loc(model.languageCode, "Results", "Ergebnisse", "Resultados", "Resultats"))
            if let result = model.result {
                Text("\(result.segments.count) " + loc(model.languageCode, "segments", "Segmente", "segmentos", "segments"))
                    .font(Theme.Font.caption)
                    .foregroundStyle(Theme.Palette.textTertiary)
                    .monospacedDigit()
            }
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
                Button(role: .destructive) {
                    onCancel()
                } label: {
                    Label(loc(languageCode, "Cancel", "Abbrechen", "Cancelar", "Annuler"), systemImage: "xmark.circle")
                }
                .buttonStyle(.bordered)
                .tint(.red)
            }
        }
    }
}

// MARK: - Timeline

struct ResultsTimelineView: View {
    let segments: [SegmentViewModel]
    let selectedID: String?
    let onSelect: (String) -> Void
    @Binding var zoom: Double

    private let minSegmentWidth: CGFloat = 3

    var body: some View {
        GeometryReader { geo in
            let totalDuration = max(1, segments.map(\.endMs).max() ?? 1)
            let containerWidth = max(1, geo.size.width)
            let contentWidth = containerWidth * CGFloat(zoom)
            let height = geo.size.height

            ZStack {
                RoundedRectangle(cornerRadius: Theme.Radius.row, style: .continuous)
                    .fill(Theme.Palette.divider)

                ScrollViewReader { proxy in
                    ScrollView(.horizontal, showsIndicators: false) {
                        ZStack(alignment: .leading) {
                            Color.clear
                                .frame(width: contentWidth, height: height)

                            ForEach(segments) { seg in
                                let rawWidth = CGFloat(seg.endMs - seg.startMs) / CGFloat(totalDuration) * contentWidth
                                let segWidth = max(minSegmentWidth, rawWidth)
                                let segX = CGFloat(seg.startMs) / CGFloat(totalDuration) * contentWidth
                                let isSelected = selectedID == seg.id

                                Button {
                                    onSelect(seg.id)
                                } label: {
                                    RoundedRectangle(cornerRadius: 4, style: .continuous)
                                        .fill(seg.timelineColor.opacity(isSelected ? 1.0 : 0.75))
                                        .overlay(
                                            RoundedRectangle(cornerRadius: 4, style: .continuous)
                                                .strokeBorder(isSelected ? Color.primary.opacity(0.6) : Color.clear, lineWidth: Theme.Stroke.emphasized)
                                        )
                                }
                                .buttonStyle(.plain)
                                .frame(width: segWidth, height: isSelected ? height - 8 : height - 14)
                                .position(x: segX + segWidth / 2, y: height / 2)
                                .help(seg.title)
                                .id(seg.id)
                            }
                        }
                        .frame(width: contentWidth, height: height, alignment: .leading)
                    }
                    .onChange(of: selectedID) { _, newID in
                        guard let newID else { return }
                        withAnimation(Theme.Motion.scroll) {
                            proxy.scrollTo(newID, anchor: .center)
                        }
                    }
                }
            }
            .clipShape(RoundedRectangle(cornerRadius: Theme.Radius.row, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: Theme.Radius.row, style: .continuous)
                    .strokeBorder(Theme.Palette.hairline, lineWidth: Theme.Stroke.hairline)
            )
        }
    }
}

struct TimelineZoomControl: View {
    @Binding var zoom: Double
    var languageCode: String = "en"

    private let minZoom: Double = 1.0
    private let maxZoom: Double = 40.0
    private let step: Double = 1.6

    var body: some View {
        HStack(spacing: 0) {
            Button {
                zoom = max(minZoom, zoom / step)
            } label: {
                Image(systemName: "minus")
                    .font(.system(size: 10, weight: .semibold))
                    .frame(width: 24, height: 24)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .foregroundStyle(zoom <= minZoom ? AnyShapeStyle(.tertiary) : AnyShapeStyle(.secondary))
            .disabled(zoom <= minZoom)

            Button {
                zoom = minZoom
            } label: {
                Text("\(Int(zoom * 100))%")
                    .font(.system(size: 10, weight: .medium).monospacedDigit())
                    .foregroundStyle(.secondary)
                    .frame(width: 40, height: 24)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .help(loc(languageCode, "Reset zoom", "Zoom zuruecksetzen", "Restablecer zoom", "Reinitialiser le zoom"))

            Button {
                zoom = min(maxZoom, zoom * step)
            } label: {
                Image(systemName: "plus")
                    .font(.system(size: 10, weight: .semibold))
                    .frame(width: 24, height: 24)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .foregroundStyle(zoom >= maxZoom ? AnyShapeStyle(.tertiary) : AnyShapeStyle(.secondary))
            .disabled(zoom >= maxZoom)
        }
        .frame(height: 24)
        .background(Theme.Palette.divider, in: Capsule())
        .overlay(Capsule().strokeBorder(Theme.Palette.hairline, lineWidth: Theme.Stroke.hairline))
    }
}

// MARK: - Segment list

struct SegmentList: View {
    let segments: [SegmentViewModel]
    @Binding var selectedID: String?

    var body: some View {
        ScrollViewReader { proxy in
            List(segments, selection: $selectedID) { seg in
                SegmentRow(segment: seg, isSelected: selectedID == seg.id)
                    .listRowInsets(EdgeInsets(top: 2, leading: 2, bottom: 2, trailing: 2))
                    .listRowSeparator(.hidden)
                    .listRowBackground(Color.clear)
                    .tag(seg.id)
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .scrollIndicators(.automatic)
            .onChange(of: selectedID) { _, newID in
                guard let newID else { return }
                withAnimation(Theme.Motion.quick) {
                    proxy.scrollTo(newID, anchor: .center)
                }
            }
        }
    }
}

struct SegmentRow: View {
    let segment: SegmentViewModel
    let isSelected: Bool

    var body: some View {
        HStack(alignment: .center, spacing: Theme.Space.s) {
            RoundedRectangle(cornerRadius: 2, style: .continuous)
                .fill(segment.timelineColor)
                .frame(width: 4, height: 36)

            VStack(alignment: .leading, spacing: 2) {
                Text(segment.title)
                    .font(Theme.Font.rowTitle)
                    .foregroundStyle(Theme.Palette.textPrimary)
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
                .font(Theme.Font.rowSubtitle)
                .foregroundStyle(Theme.Palette.textSecondary)
            }

            Spacer(minLength: 8)

            if let quality = segment.qualityLabel {
                Text(quality)
                    .font(.caption2.weight(.medium))
                    .foregroundStyle(Theme.Palette.textSecondary)
                    .padding(.horizontal, 7)
                    .padding(.vertical, 3)
                    .background(Theme.Palette.hairline, in: Capsule())
            }
        }
        .padding(.horizontal, Theme.Space.s)
        .padding(.vertical, 8)
        .contentShape(RoundedRectangle(cornerRadius: Theme.Radius.row, style: .continuous))
        .rowHoverable(selected: isSelected)
    }
}

// MARK: - Loading & empty states

struct LoadingResultsView: View {
    let phase: String
    let languageCode: String

    var body: some View {
        VStack(alignment: .leading, spacing: Theme.Space.s) {
            HStack(spacing: Theme.Space.xs) {
                ProgressView().controlSize(.small)
                Text(phase)
                    .font(Theme.Font.rowTitle)
                    .foregroundStyle(Theme.Palette.textPrimary)
                Spacer()
            }
            Text(loc(languageCode, "Long mixes are segmented first, then matched to songs.",
                     "Lange Mixe werden erst segmentiert und dann Songs zugeordnet.",
                     "Las mezclas largas se segmentan primero y luego se identifican.",
                     "Les longs mixes sont d'abord segmentes puis identifies."))
                .font(Theme.Font.caption)
                .foregroundStyle(Theme.Palette.textSecondary)
            ForEach(0..<3, id: \.self) { _ in
                RoundedRectangle(cornerRadius: Theme.Radius.row, style: .continuous)
                    .fill(Theme.Palette.divider)
                    .frame(height: 44)
            }
        }
        .padding(.vertical, Theme.Space.xxs)
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
        VStack(alignment: .leading, spacing: Theme.Space.xs) {
            SectionLabel(loc(languageCode, "Debug events", "Debug-Events", "Eventos debug", "Evenements debug"))
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 4) {
                    ForEach(events) { event in
                        HStack(alignment: .firstTextBaseline, spacing: 8) {
                            Text(event.level.uppercased())
                                .font(.caption2.weight(.bold))
                                .foregroundStyle(color(for: event.level))
                                .frame(width: 54, alignment: .leading)
                            Text(event.message)
                                .font(Theme.Font.caption)
                                .foregroundStyle(Theme.Palette.textSecondary)
                                .textSelection(.enabled)
                        }
                        .padding(.vertical, 2)
                    }
                }
            }
            .frame(minHeight: 100, maxHeight: 200)
            .padding(Theme.Space.xs)
            .background(Theme.Palette.divider, in: RoundedRectangle(cornerRadius: Theme.Radius.row, style: .continuous))
        }
    }

    private func color(for level: String) -> Color {
        switch level.lowercased() {
        case "error": return Theme.Palette.dangerTint
        case "warning": return Theme.Palette.warningTint
        default: return Theme.Palette.accent
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
                    VStack(alignment: .leading, spacing: Theme.Space.l) {
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
                    .padding(Theme.Space.l)
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
        .background(.regularMaterial)
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
        VStack(alignment: .leading, spacing: Theme.Space.xs) {
            HStack(alignment: .top, spacing: Theme.Space.xs) {
                RoundedRectangle(cornerRadius: 4, style: .continuous)
                    .fill(segment.timelineColor)
                    .frame(width: 6, height: 46)
                VStack(alignment: .leading, spacing: 3) {
                    Text(segment.title)
                        .font(Theme.Font.title)
                        .fixedSize(horizontal: false, vertical: true)
                    Text(segment.subtitle)
                        .font(Theme.Font.body)
                        .foregroundStyle(Theme.Palette.textSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer(minLength: 0)
            }
        }
    }

    private func metadataSection(_ segment: SegmentViewModel) -> some View {
        VStack(alignment: .leading, spacing: Theme.Space.xs) {
            HStack(spacing: 6) {
                Pill("\(formatTime(segment.startMs)) – \(formatTime(segment.endMs))", icon: "clock")
                Pill(segment.detailLabel, icon: "sparkles")
            }
            HStack(spacing: 6) {
                if let quality = segment.qualityLabel {
                    Pill(quality, icon: "checkmark.seal", tint: Theme.Palette.successTint)
                }
                if let uncertainty = segment.payload.uncertainty {
                    Pill(String(format: "U %.2f", uncertainty), icon: "gauge.with.dots.needle.33percent")
                }
            }

            if let hint = segment.metadataHint, !hint.isEmpty {
                Text(cleanHint(hint))
                    .font(Theme.Font.caption)
                    .foregroundStyle(Theme.Palette.textSecondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }

    private func actionsSection(_ segment: SegmentViewModel) -> some View {
        VStack(alignment: .leading, spacing: Theme.Space.xs) {
            SectionLabel(loc(model.languageCode, "Actions", "Aktionen", "Acciones", "Actions"))

            HStack(spacing: Theme.Space.xs) {
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
                    SectionLabel(loc(model.languageCode, "Listen", "Anhoeren", "Escuchar", "Ecouter"))
                    FlowLinks(primary: segment.primaryLinks, overflow: segment.overflowLinks, languageCode: model.languageCode)
                }
                .padding(.top, Theme.Space.xxs)
            }
        }
    }

    private func explanationSection(_ lines: [String]) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            SectionLabel(loc(model.languageCode, "Why this result", "Warum dieses Ergebnis", "Por que este resultado", "Pourquoi ce resultat"))
            VStack(alignment: .leading, spacing: 4) {
                ForEach(lines, id: \.self) { line in
                    HStack(alignment: .top, spacing: 6) {
                        Text("•")
                            .foregroundStyle(.tertiary)
                        Text(line)
                            .font(Theme.Font.body)
                            .foregroundStyle(Theme.Palette.textSecondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
            }
        }
    }

    private func alternatesSection(_ tracks: [TrackMatchPayload]) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            SectionLabel(loc(model.languageCode, "Alternates", "Weitere Kandidaten", "Alternativas", "Alternatives"))
            VStack(alignment: .leading, spacing: 4) {
                ForEach(tracks, id: \.self) { track in
                    Text(track.artist.map { "\($0) · \(track.title)" } ?? track.title)
                        .font(Theme.Font.body)
                        .foregroundStyle(Theme.Palette.textSecondary)
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

struct FlowLinks: View {
    let primary: [PlatformLink]
    let overflow: [PlatformLink]
    let languageCode: String

    var body: some View {
        HStack(spacing: 6) {
            ForEach(primary) { link in
                Link(destination: link.url) {
                    FlowLinkContent(icon: link.icon, iconColor: link.color, text: link.label)
                }
                .buttonStyle(FlowLinkButtonStyle())
            }

            if !overflow.isEmpty {
                Menu {
                    ForEach(overflow) { link in
                        Link(destination: link.url) {
                            Label(link.label, systemImage: link.icon)
                        }
                    }
                } label: {
                    FlowLinkContent(icon: "ellipsis", iconColor: .secondary, text: nil)
                }
                .menuStyle(.button)
                .buttonStyle(FlowLinkButtonStyle())
                .menuIndicator(.hidden)
                .fixedSize()
            }
        }
    }
}

private struct FlowLinkContent: View {
    let icon: String
    let iconColor: Color
    let text: String?

    var body: some View {
        HStack(spacing: 5) {
            Image(systemName: icon)
                .font(.system(size: 11, weight: .semibold))
                .foregroundStyle(iconColor)
                .frame(width: 12)
            if let text {
                Text(text)
                    .font(.system(size: 12, weight: .medium))
                    .foregroundStyle(.primary)
                    .lineLimit(1)
                    .fixedSize()
            }
        }
        .padding(.horizontal, text == nil ? 8 : 10)
        .frame(height: 24)
    }
}

private struct FlowLinkButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .background(
                Capsule(style: .continuous)
                    .fill(Color.primary.opacity(configuration.isPressed ? 0.12 : 0.06))
            )
            .overlay(
                Capsule(style: .continuous)
                    .strokeBorder(Theme.Palette.hairline, lineWidth: Theme.Stroke.hairline)
            )
            .contentShape(Capsule(style: .continuous))
            .animation(Theme.Motion.snap, value: configuration.isPressed)
    }
}
