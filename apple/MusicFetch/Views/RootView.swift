import MusicFetchCore
import SwiftUI

/// The main window: sidebar, detail column, inspector and toolbar (§3.1).
struct RootView: View {
    @Environment(AppModel.self) private var app
    @Environment(\.undoManager) private var undoManager
    @Environment(\.openSettings) private var openSettings
    @Environment(\.openWindow) private var openWindow
    @Namespace private var glass
    @FocusState private var searchFocused: Bool

    var body: some View {
        @Bindable var app = app
        NavigationSplitView {
            Sidebar()
                .navigationSplitViewColumnWidth(min: 180, ideal: 220, max: 280)
        } detail: {
            detail
        }
        .frame(minWidth: 900, minHeight: 600)
        .sheet(item: $app.lyricSearch) { model in
            LyricSearchSheet(model: model)
        }
        .sheet(isPresented: $app.showProviderPanel) {
            ProviderPanel(suggested: app.suggestedProvider, startsWithCustom: app.providerPanelStartsWithCustom)
        }
        .alert(Copy.melodyPromptTitle, isPresented: $app.showMelodyPrompt) {
            Button(Copy.melodyDownload) {
                app.settings.downloadMelody()
                app.showMelodyPrompt = false
            }
            .keyboardShortcut(.defaultAction)
            Button(Copy.notNow, role: .cancel) { app.declineMelodyLibrary() }
        } message: {
            Text(Copy.melodyPromptMessage(app.settings.melodyDownloadSize))
        }
        .alert(
            Copy.microphonePromptTitle,
            isPresented: Binding(get: { app.microphonePrePromptSource != nil }, set: { if !$0 { app.microphonePrePromptSource = nil } })
        ) {
            Button(Copy.continueAction) {
                let source = app.microphonePrePromptSource ?? .microphone
                app.microphonePrePromptSource = nil
                Task { await app.listen.requestMicrophone(then: source) }
            }
            .keyboardShortcut(.defaultAction)
            Button(Copy.notNow, role: .cancel) { app.microphonePrePromptSource = nil }
        } message: {
            Text(Copy.microphonePromptMessage)
        }
        .alert(
            Copy.deleteConfirmTitle(deleteTitle),
            isPresented: Binding(get: { app.deleteConfirmation != nil }, set: { if !$0 { app.deleteConfirmation = nil } })
        ) {
            Button(Copy.delete, role: .destructive) { app.confirmDelete() }
            Button(Copy.cancel, role: .cancel) { app.deleteConfirmation = nil }
        } message: {
            Text(Copy.deleteConfirmMessage)
        }
        .alert(
            app.exportError ?? "",
            isPresented: Binding(get: { app.exportError != nil }, set: { if !$0 { app.exportError = nil } })
        ) {
            Button(Copy.ok) { app.exportError = nil }
        }
        .onOpenURL { app.handleURL($0) }
        .task {
            app.openSettingsAction = { openSettings() }
            app.openMainWindowAction = { openWindow(id: "main") }
            await app.start()
        }
    }

    private var deleteTitle: String {
        guard let ids = app.deleteConfirmation else { return "" }
        if ids.count == 1, let id = ids.first {
            return app.library.entry(id: id).map { $0.track?.title ?? $0.title } ?? ""
        }
        return Copy.sessionsCount(ids.count)
    }

    @ViewBuilder
    private var detail: some View {
        switch app.route {
        case .listen, nil:
            ListenPane(glass: glass)
                .appInspector()
        case .library:
            @Bindable var app = app
            NavigationStack(path: $app.libraryPath) {
                LibraryView()
                    .navigationTitle(Copy.libraryTitle)
                    .navigationDestination(for: String.self) { id in
                        SessionScreen(id: id)
                            .appInspector()
                    }
                    .appInspector()
                    .toolbar { libraryToolbar }
                    .searchable(text: $app.library.search, placement: .toolbar, prompt: Copy.search)
                    .searchFocused($searchFocused)
                    .onChange(of: app.searchFocusRequest) { _, _ in searchFocused = true }
            }
        case .session(let id):
            NavigationStack {
                SessionScreen(id: id)
                    .appInspector()
            }
        }
    }

    @ToolbarContentBuilder
    private var libraryToolbar: some ToolbarContent {
        ToolbarItem(placement: .principal) {
            @Bindable var library = app.library
            Picker(Copy.filter, selection: $library.filter) {
                ForEach(LibraryModel.Filter.allCases) { filter in
                    Text(filter.title).tag(filter)
                }
            }
            .pickerStyle(.segmented)
            .labelsHidden()
            .fixedSize()
        }
        ToolbarItem(placement: .automatic) { ListenToolbarButton() }
        ToolbarItem(placement: .automatic) { InspectorToggle() }
    }
}

/// The Listen pane: the stage, or the session that replaced it, in one glass container
/// so the button can morph into "Open in".
struct ListenPane: View {
    @Environment(AppModel.self) private var app
    var glass: Namespace.ID

    var body: some View {
        let listen = app.listen!
        // Spacing 0: neighbors must not blend; the container only carries the button-to-result morph.
        GlassContainer(spacing: 0) {
            Group {
                if let shown = listen.shown {
                    SessionContentView(model: shown, glass: glass)
                        .transition(.opacity)
                } else {
                    ListenStage(listen: listen, glass: glass)
                        .transition(.opacity)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .animation(app.motion(.hero), value: listen.shown?.id)
        .onDrop(of: [.fileURL, .url, .plainText], isTargeted: Binding(get: { listen.isDropTargeted }, set: { listen.isDropTargeted = $0 })) {
            app.handleDrop($0)
        }
        .onExitCommand {
            if listen.stage == .listening || listen.stage == .matching {
                listen.escape()
            } else if listen.shown != nil {
                listen.showStage()
            }
        }
        .navigationTitle(Copy.listenTitle)
        .toolbar {
            ToolbarItem(placement: .principal) { LinkField() }
            if let shown = listen.shown, shown.presentation == .timeline || shown.presentation == .collection {
                ToolbarItem(placement: .automatic) { ExportMenu(model: shown) }
            }
            ToolbarItem(placement: .automatic) { ListenToolbarButton() }
                .hidden(app.isOnListenStage)
            ToolbarItem(placement: .automatic) { InspectorToggle() }
        }
    }
}

/// A pushed or pinned session.
struct SessionScreen: View {
    @Environment(AppModel.self) private var app
    var id: String
    @State private var model: SessionModel?

    var body: some View {
        Group {
            if let model {
                SessionContentView(model: model)
            } else {
                ProgressView().frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .task(id: id) { model = await app.session(id) }
        .navigationTitle(model?.title ?? "")
        .toolbar(removing: .title)
        .toolbar {
            // Without a title nothing pushes the actions to the trailing edge (macOS 26).
            if #available(macOS 26, *) {
                ToolbarSpacer(.flexible)
            }
            if let model, model.presentation == .timeline || model.presentation == .collection || model.presentation == .result {
                ToolbarItem(placement: .automatic) { ExportMenu(model: model) }
            }
            ToolbarItem(placement: .automatic) { ListenToolbarButton() }
            ToolbarItem(placement: .automatic) { InspectorToggle() }
        }
    }
}

/// The toolbar link field (§5.5).
struct LinkField: View {
    @Environment(AppModel.self) private var app
    @FocusState private var focused: Bool

    var body: some View {
        @Bindable var app = app
        HStack(spacing: 6) {
            Image(systemName: "link")
                .foregroundStyle(.secondary)
                .accessibilityHidden(true)
            TextField(Copy.pasteLink, text: $app.linkText)
                .textFieldStyle(.plain)
                .focused($focused)
                .onSubmit { app.submitLinkField() }
                .accessibilityLabel(Copy.pasteLink)
        }
        .padding(.horizontal, 12)
        .frame(width: 300)
        .onChange(of: app.linkFieldFocusRequest) { _, _ in focused = true }
    }
}

/// [Listen ▸] in the toolbar (prominent; the SDK has no `.glassProminent` yet, and
/// `.borderedProminent` is the native style before macOS 26 too).
struct ListenToolbarButton: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        Button {
            app.listenNow()
        } label: {
            Label(Copy.listenAction, image: "ContourMark")
                .labelStyle(.titleAndIcon)
        }
        .buttonStyle(.borderedProminent)
        .help(Copy.listenHelp)
    }
}

struct InspectorToggle: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        Button {
            withAnimation(AppModel.spring(.large)) { app.setInspector(!app.showInspector) }
        } label: {
            Label(Copy.inspector, systemImage: "sidebar.trailing")
        }
        .help(app.showInspector ? Copy.hideInspector : Copy.showInspector)
    }
}

/// Export ▾: Copy Tracklist and the four file formats.
struct ExportMenu: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel

    var body: some View {
        Menu {
            Button(Copy.copyTracklist) { app.copyTracklist(model) }
            Divider()
            ForEach(ExportFormat.menuOrder, id: \.self) { format in
                Button(Copy.exportTitle(format)) { app.export(model, format: format) }
            }
        } label: {
            Label(Copy.exportMenu, systemImage: "square.and.arrow.up")
        }
        .help(Copy.exportMenu)
        .disabled(model.isRunning && model.listedSegments.isEmpty)
    }
}

extension View {
    /// The inspector column (§3.1), on each screen of the detail column so it stays
    /// visible when sessions are pushed.
    func appInspector() -> some View {
        modifier(AppInspector())
    }
}

private struct AppInspector: ViewModifier {
    @Environment(AppModel.self) private var app

    func body(content: Content) -> some View {
        content.inspector(isPresented: Binding(get: { app.showInspector }, set: { app.setInspector($0) })) {
            InspectorView()
                .inspectorColumnWidth(min: 260, ideal: 300, max: 360)
        }
    }
}
