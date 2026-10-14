import MusicFetchCore
import SwiftUI

/// Listen, Library and pinned sessions (§3.1).
struct Sidebar: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        @Bindable var app = app
        List(selection: $app.route) {
            NavigationLink(value: AppModel.Route.listen) {
                Label(Copy.listenTitle, image: "ContourMark")
            }
            .onDrop(of: [.fileURL, .url, .plainText], isTargeted: nil) { app.handleDrop($0) }
            NavigationLink(value: AppModel.Route.library) {
                Label(Copy.libraryTitle, systemImage: "square.stack")
            }
            let pinned = app.library.visiblePinned
            if !pinned.isEmpty {
                Section(Copy.pinnedSection) {
                    ForEach(pinned, id: \.id) { entry in
                        NavigationLink(value: AppModel.Route.session(entry.id)) {
                            Label {
                                Text(entry.displayTitle)
                            } icon: {
                                KindSymbol(kind: entry.kind, isMix: entry.trackCount >= 2)
                            }
                        }
                        .contextMenu {
                            Button(Copy.unpin) { app.library.setPinned([entry.id], pinned: false) }
                        }
                    }
                }
            }
        }
    }
}
