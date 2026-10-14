import MusicFetchCore
import SwiftUI

/// Library (§6.8): sessions grouped by day, filter and search in the toolbar.
struct LibraryView: View {
    @Environment(AppModel.self) private var app
    @Environment(\.undoManager) private var undoManager

    var body: some View {
        @Bindable var library = app.library
        Group {
            if library.isLoaded, library.visibleEntries.isEmpty {
                if library.search.isEmpty, library.filter == .all {
                    ContentUnavailableView {
                        Label(Copy.libraryEmptyTitle, systemImage: "waveform")
                    } description: {
                        Text(Copy.libraryEmptyMessage)
                    }
                } else if !library.search.isEmpty {
                    ContentUnavailableView.search(text: library.search)
                } else {
                    ContentUnavailableView(Copy.libraryEmptyFiltered, systemImage: "square.stack")
                }
            } else {
                List(selection: $library.selection) {
                    ForEach(library.sections) { section in
                        Section {
                            ForEach(section.entries, id: \.id) { entry in
                                LibraryRow(entry: entry)
                                    .tag(entry.id)
                                    .listRowSeparator(.hidden)
                            }
                        } header: {
                            Text(section.title)
                                .font(.headline)
                                .foregroundStyle(.secondary)
                        }
                    }
                }
                .listStyle(.inset)
                .scrollContentBackground(.hidden)
                .contextMenu(forSelectionType: String.self) { ids in
                    contextMenu(Array(ids))
                } primaryAction: { ids in
                    if let id = ids.first { app.libraryPath.append(id) }
                }
                .onDeleteCommand {
                    app.requestDelete(Array(library.selection), undoManager: undoManager)
                }
                .onKeyPress(.return) {
                    guard library.selection.count == 1, let id = library.selection.first else { return .ignored }
                    app.libraryPath.append(id)
                    return .handled
                }
                .onChange(of: library.selection) { _, selection in
                    // The inspector shows the selected row's details.
                    guard selection.count == 1, let id = selection.first else {
                        app.inspectedLibrarySession = nil
                        return
                    }
                    Task {
                        let model = await app.session(id)
                        if library.selection == [id] { app.inspectedLibrarySession = model }
                    }
                }
            }
        }
        .task { await library.refresh() }
    }

    @ViewBuilder
    private func contextMenu(_ ids: [String]) -> some View {
        if ids.count == 1, let id = ids.first {
            Button(Copy.open) { app.libraryPath.append(id) }
            if let track = app.library.entry(id: id)?.track {
                OpenInMenu(track: track)
            }
            Divider()
            let pinned = app.library.entry(id: id)?.pinned ?? false
            Button(pinned ? Copy.unpin : Copy.pin) { app.library.togglePin(id) }
            Menu(Copy.exportMenu) {
                ForEach(ExportFormat.menuOrder, id: \.self) { format in
                    Button(Copy.exportTitle(format)) {
                        Task {
                            if let model = await app.session(id) { app.export(model, format: format) }
                        }
                    }
                }
            }
        } else if !ids.isEmpty {
            Button(Copy.pin) { app.library.setPinned(ids, pinned: true) }
        }
        if !ids.isEmpty {
            Divider()
            Button(Copy.delete, role: .destructive) { app.requestDelete(ids, undoManager: undoManager) }
        }
    }
}

/// A library row: artwork, title, kind symbol + subtitle, time, pin, spinner.
struct LibraryRow: View {
    @Environment(AppModel.self) private var app
    var entry: LibraryEntry

    @State private var candidateCount: Int?

    private var isMix: Bool { entry.trackCount >= 2 }

    var body: some View {
        HStack(spacing: 12) {
            ArtworkView(
                url: entry.artworkUrl ?? entry.track?.artworkUrl, size: 40, cornerRadius: 6,
                hatched: entry.artworkUrl == nil && entry.track == nil && entry.status != .running)
            VStack(alignment: .leading, spacing: 2) {
                Text(entry.displayTitle)
                    .lineLimit(1)
                HStack(spacing: 5) {
                    KindSymbol(kind: entry.kind, isMix: isMix)
                        .foregroundStyle(.tertiary)
                    Text(subtitle)
                        .lineLimit(1)
                }
                .font(.callout)
                .foregroundStyle(.secondary)
            }
            Spacer(minLength: 12)
            if entry.status == .running {
                ProgressView().controlSize(.small)
            }
            Text(Format.time(Format.date(milliseconds: entry.createdAtMs)))
                .font(.callout)
                .monospacedDigit()
                .foregroundStyle(.secondary)
            if entry.pinned {
                Image(systemName: "pin")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .accessibilityLabel(Copy.pinned)
            }
        }
        .frame(minHeight: 48)
        .task(id: entry.id) {
            guard entry.kind == .hum || entry.kind == .lyrics, entry.track == nil else { return }
            candidateCount = await app.session(entry.id)?.candidates.count
        }
        .accessibilityElement(children: .combine)
    }

    private var subtitle: String {
        let artist = entry.track?.artist ?? entry.subtitle
        let host = Format.host(entry.sourceUrl)
        switch entry.status {
        case .failed: return Copy.didNotFinish
        case .canceled where entry.trackCount == 0: return Copy.stoppedPlain
        case .noMatch: return Copy.noMatch(.notFound)
        default: break
        }
        switch entry.kind {
        case .hum, .lyrics:
            if entry.track != nil { return artist ?? "" }
            if let candidateCount, candidateCount > 0 { return Copy.candidatesCount(artist ?? "", candidateCount) }
            return artist ?? ""
        case .link, .file:
            if entry.status == .running {
                return entry.trackCount > 0 ? Copy.tracksSoFar(Int(entry.trackCount)) : (host ?? Copy.analyzing)
            }
            if isMix {
                return [artist, Copy.trackCount(Int(entry.trackCount)), host].compactMap { $0 }.joined(separator: " · ")
            }
            return [artist, host].compactMap { $0 }.joined(separator: " · ")
        case .listen, .systemAudio:
            return artist ?? ""
        }
    }
}

/// The kind symbol in front of library subtitles and pinned sidebar rows.
struct KindSymbol: View {
    var kind: SessionKind
    var isMix = false

    var body: some View {
        if kind == .hum {
            Image("ContourMark")
        } else {
            Image(systemName: Self.symbol(kind, isMix: isMix))
        }
    }

    static func symbol(_ kind: SessionKind, isMix: Bool) -> String {
        if isMix { return "rectangle.split.3x1" }
        switch kind {
        case .listen: return "mic"
        case .systemAudio: return "macbook"
        case .hum: return "waveform"
        case .link: return "link"
        case .file: return "doc"
        case .lyrics: return "text.alignleft"
        }
    }
}

extension ExportFormat {
    /// Order in export menus.
    static let menuOrder: [ExportFormat] = [.text, .csv, .cue, .json]
}
