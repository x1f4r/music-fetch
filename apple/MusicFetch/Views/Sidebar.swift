import MusicFetchCore
import SwiftUI

/// Listen, Library and pinned sessions (§3.1).
struct Sidebar: View {
    @Environment(AppModel.self) private var app
    @AppStorage(PreferenceKey.pinnedExpanded) private var pinnedExpanded = true

    /// Pinned rows in the sidebar before "Show All" takes over.
    static let pinnedLimit = 12

    var body: some View {
        List(selection: Binding(get: { app.route }, set: { app.selectRoute($0) })) {
            NavigationLink(value: AppModel.Route.listen) {
                Label(Copy.listenTitle, image: "contour.mark")
            }
            .onDrop(of: [.fileURL, .url, .plainText], isTargeted: nil) { app.handleDrop($0) }
            // Clicking Listen again leaves a result for the stage.
            .simultaneousGesture(TapGesture().onEnded { app.selectRoute(.listen) })
            NavigationLink(value: AppModel.Route.library) {
                Label(Copy.libraryTitle, systemImage: "square.stack")
            }
            .badge(app.library.runningCount)
            let pinned = app.library.visiblePinned
            if !pinned.isEmpty {
                Section(Copy.pinnedSection, isExpanded: $pinnedExpanded) {
                    ForEach(pinned.prefix(Self.pinnedLimit), id: \.id) { entry in
                        NavigationLink(value: AppModel.Route.session(entry.id)) {
                            Label {
                                Text(entry.displayTitle)
                            } icon: {
                                KindSymbol(kind: entry.kind, isMix: entry.trackCount >= 2)
                            }
                        }
                        .contextMenu { PinnedRowMenu(entry: entry) }
                    }
                    if pinned.count > Self.pinnedLimit {
                        Button(Copy.showAll) { app.showPinnedInLibrary() }
                            .buttonStyle(.plain)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
    }
}

/// The context menu of a pinned session in the sidebar.
private struct PinnedRowMenu: View {
    @Environment(AppModel.self) private var app
    @Environment(\.undoManager) private var undoManager
    var entry: LibraryEntry

    var body: some View {
        if let track = entry.track {
            OpenInMenu(track: track)
        }
        ExportSubmenu(id: entry.id)
        Button(Copy.showInLibrary) { app.showInLibrary(entry.id) }
        Divider()
        Button(Copy.unpin) { app.library.setPinned([entry.id], pinned: false, undoManager: undoManager) }
        Button(Copy.delete, role: .destructive) { app.requestDelete([entry.id], undoManager: undoManager) }
    }
}
