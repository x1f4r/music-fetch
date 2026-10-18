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
    @State private var columns = NavigationSplitViewVisibility.automatic

    var body: some View {
        @Bindable var app = app
        NavigationSplitView(columnVisibility: $columns) {
            Sidebar()
                .navigationSplitViewColumnWidth(min: 180, ideal: 220, max: 280)
        } detail: {
            detail
        }
        // One inspector for every screen. Its toolbar item makes SwiftUI add the
        // inspector's tracking separator, so no item of the detail column ever sits on
        // the inspector's divider.
        .inspector(isPresented: Binding(get: { app.isInspectorPresented }, set: { app.presentInspector($0) })) {
            InspectorView()
                .inspectorColumnWidth(min: 260, ideal: 300, max: 360)
                .toolbar {
                    ToolbarItem { InspectorToggle() }
                }
        }
        .frame(minWidth: 900, minHeight: 600)
        .onGeometryChange(for: CGFloat.self) { $0.size.width } action: { width in
            app.windowWidthChanged(width, sidebarShown: columns != .detailOnly)
        }
        .onChange(of: columns) { _, columns in
            app.windowWidthChanged(nil, sidebarShown: columns != .detailOnly)
        }
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
        .overlay(alignment: .topLeading) {
            if app.demoReady { DemoReadyMarker() }
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
        case .library:
            @Bindable var app = app
            NavigationStack(path: $app.libraryPath) {
                LibraryView()
                    .navigationTitle(Copy.libraryTitle)
                    .navigationDestination(for: String.self) { id in
                        SessionScreen(id: id)
                    }
                    .toolbar { libraryToolbar }
                    .searchable(text: $app.library.search, placement: .toolbar, prompt: Copy.searchLibrary)
                    .searchFocused($searchFocused)
                    .onChange(of: app.searchFocusRequest) { _, _ in searchFocused = true }
            }
        case .session(let id):
            NavigationStack {
                SessionScreen(id: id)
            }
        }
    }

    @ToolbarContentBuilder
    private var libraryToolbar: some ToolbarContent {
        ToolbarItem { PinToolbarToggle(ids: Array(app.library.selection)) }
        ToolbarItem { ListenToolbarButton() }
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
            } else {
                // A result, a no-match or an error: back to the idle stage.
                withAnimation(app.motion(.hero)) { listen.showStage() }
            }
        }
        // The sidebar already says "Listen"; the title stays for the Window menu.
        .navigationTitle(Copy.listenTitle)
        .toolbar(removing: .title)
        .toolbar {
            if listen.shown != nil {
                ToolbarItem(placement: .navigation) { BackToStageButton() }
            }
            ToolbarItem(placement: .navigation) { LinkField() }
            TrailingActionsSpacer()
            if let shown = listen.shown {
                SessionToolbarItems(model: shown)
            }
            ToolbarItem { ListenToolbarButton() }
                .hidden(app.isOnListenStage)
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
            TrailingActionsSpacer()
            if let model {
                SessionToolbarItems(model: model)
            }
            ToolbarItem { ListenToolbarButton() }
        }
    }
}

/// Without a title nothing pushes the actions to the trailing edge: a flexible space
/// on macOS 26, an empty centered item before (the toolbar keeps flexible space around
/// it).
struct TrailingActionsSpacer: ToolbarContent {
    var body: some ToolbarContent {
        if #available(macOS 26, *) {
            ToolbarSpacer(.flexible)
        } else {
            ToolbarItem(placement: .principal) {
                Color.clear
                    .frame(width: 1, height: 1)
                    .accessibilityHidden(true)
            }
        }
    }
}

/// Export and Pin for the session on screen, next to Listen (§3.1).
struct SessionToolbarItems: ToolbarContent {
    var model: SessionModel

    var body: some ToolbarContent {
        ToolbarItemGroup {
            if model.hasExport {
                ExportMenu(model: model)
            }
            PinToolbarToggle(ids: [model.id])
        }
    }
}

/// "‹" on a result shown on the Listen pane: back to the stage (⌘[, Esc).
struct BackToStageButton: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        Button {
            withAnimation(app.motion(.hero)) { app.listen.showStage() }
        } label: {
            Label(Copy.backToListen, systemImage: "chevron.backward")
        }
        .labelStyle(.iconOnly)
        .keyboardShortcut("[", modifiers: .command)
        .help(Copy.backToListen)
        .accessibilityIdentifier("mf.backToStage")
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
        .modifier(LinkFieldChrome())
        .onChange(of: app.linkFieldFocusRequest) { _, _ in focused = true }
    }
}

/// On macOS 26 the toolbar draws the field's capsule; before, it gets a rounded border
/// like a search field.
private struct LinkFieldChrome: ViewModifier {
    func body(content: Content) -> some View {
        if Compat.usesMacOS26Features {
            content
                .padding(.horizontal, 12)
                .frame(width: 240)
        } else {
            content
                .padding(.horizontal, 7)
                .frame(width: 240, height: 22)
                .background(.background, in: RoundedRectangle(cornerRadius: 6, style: .continuous))
                .overlay {
                    RoundedRectangle(cornerRadius: 6, style: .continuous)
                        .strokeBorder(.separator, lineWidth: 1)
                }
        }
    }
}

/// [Listen ▾] in the toolbar: the button listens again with the current method, the
/// chevron offers every other way to identify (§3.1). Shows Stop while capturing.
struct ListenToolbarButton: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        let listen = app.listen!
        Group {
            if Compat.usesMacOS26Features {
                // A prominent button makes the toolbar tint the whole glass capsule,
                // chevron included; the hairline marks the split like "Open in".
                HStack(spacing: 0) {
                    Button { app.listenNow() } label: { label(listen) }
                        .buttonStyle(.borderedProminent)
                    Rectangle()
                        .fill(.white.opacity(0.35))
                        .frame(width: 1, height: 16)
                        .accessibilityHidden(true)
                    methodsMenu
                }
            } else {
                // Toolbars before macOS 26 draw every button plain, prominent or not.
                LegacyListenSplit(label: label(listen), action: { app.listenNow() })
            }
        }
        .labelStyle(.titleAndIcon)
        .disabled(app.engine == nil || listen.stage == .matching)
        .help(listen.isCapturing ? Copy.stop : Copy.listenHelp(listen.source))
        .accessibilityElement(children: .contain)
        .accessibilityLabel(listen.isCapturing ? Copy.stop : Copy.listenAction)
        .accessibilityValue(listen.buttonValue)
        .accessibilityIdentifier("mf.listen")
    }

    @ViewBuilder
    private func label(_ listen: ListenModel) -> some View {
        switch listen.stage {
        case .listening:
            Label(Copy.stop, systemImage: "stop.fill")
        case .matching:
            Label(Copy.matching, systemImage: "ellipsis")
        default:
            Label { Text(Copy.listenAction) } icon: { listen.source.icon }
        }
    }

    private var methodsMenu: some View {
        Menu {
            ListenMethodItems()
        } label: {
            Label(Copy.listenMethods, systemImage: "chevron.down")
                .labelStyle(.iconOnly)
                .font(.footnote.weight(.semibold))
        }
        .menuIndicator(.hidden)
        .fixedSize()
        .help(Copy.listenMethods)
    }
}

/// The Listen split button before macOS 26: an accent capsule with the label and a
/// chevron for the menu, the size of the toolbar's other controls.
private struct LegacyListenSplit<Label: View>: View {
    var label: Label
    var action: () -> Void

    var body: some View {
        HStack(spacing: 0) {
            Button(action: action) {
                label
                    .padding(.leading, 10)
                    .padding(.trailing, 8)
                    .frame(height: 24)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            Rectangle()
                .fill(.white.opacity(0.35))
                .frame(width: 1, height: 14)
                .accessibilityHidden(true)
            Menu {
                ListenMethodItems()
            } label: {
                Image(systemName: "chevron.down")
                    .font(.caption.weight(.semibold))
                    .frame(width: 24, height: 24)
                    .contentShape(Rectangle())
            }
            .menuStyle(.button)
            .buttonStyle(.plain)
            .menuIndicator(.hidden)
            .fixedSize()
            .help(Copy.listenMethods)
            .accessibilityLabel(Copy.listenMethods)
        }
        .foregroundStyle(.white)
        .prominentGlass(tint: .accentColor, in: Capsule())
        .fixedSize()
    }
}

/// Every way to identify, for the Listen button's menu.
struct ListenMethodItems: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        let busy = app.listen.isBusy
        ForEach(ListenSource.allCases) { source in
            Button {
                app.listenNow(source)
            } label: {
                Label { Text(source.menuTitle) } icon: { source.icon }
            }
            .disabled(busy)
        }
        Divider()
        Button(Copy.identifyLinkMenu) { app.focusLinkField() }
        Button(Copy.openFileMenu) { app.openFile() }
        Button(Copy.searchLyricsMenu) { app.presentLyricSearch() }
    }
}

struct InspectorToggle: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        Button {
            withAnimation(AppModel.spring(.large)) { app.toggleInspector() }
        } label: {
            Label(Copy.inspector, systemImage: "sidebar.trailing")
        }
        .help(app.isInspectorPresented ? Copy.hideInspector : Copy.showInspector)
        .accessibilityIdentifier("mf.inspectorToggle")
    }
}

/// Pin or unpin sessions (§3.3): Unpin when all of them are pinned.
struct PinToolbarToggle: View {
    @Environment(AppModel.self) private var app
    @Environment(\.undoManager) private var undoManager
    var ids: [String]

    var body: some View {
        let unpin = !ids.isEmpty && app.library.allPinned(ids)
        Button {
            app.library.togglePinned(ids, undoManager: undoManager)
        } label: {
            Label(unpin ? Copy.unpin : Copy.pin, systemImage: unpin ? "pin.slash" : "pin")
        }
        .disabled(ids.isEmpty)
        .help(unpin ? Copy.unpin : Copy.pin)
        .accessibilityIdentifier("mf.pin")
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

extension ListenSource {
    /// The source's symbol: `mic`, `macbook` or the contour mark.
    var icon: Image {
        symbol.map { Image(systemName: $0) } ?? Image("contour.mark")
    }
}

/// An invisible element the screenshot tests wait for (demo states only).
private struct DemoReadyMarker: View {
    var body: some View {
        Text(verbatim: "MFDemoReady")
            .font(.system(size: 1))
            .foregroundStyle(.clear)
            .frame(width: 1, height: 1)
            .allowsHitTesting(false)
            .accessibilityIdentifier("MFDemoReady")
    }
}
