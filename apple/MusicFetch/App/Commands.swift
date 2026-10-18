import AppKit
import MusicFetchCore
import SwiftUI

/// Menu bar commands (§3.1 "Keyboard"). Every toolbar action is here too.
struct MusicFetchCommands: Commands {
    var app: AppModel
    @Environment(\.openWindow) private var openWindow

    private var session: SessionModel? { app.currentSession }

    private var hasTracklist: Bool {
        guard let session else { return false }
        return session.presentation == .timeline || session.presentation == .collection
    }

    var body: some Commands {
        SidebarCommands()

        CommandGroup(replacing: .newItem) {
            Button(Copy.identifyLinkMenu) {
                showMain()
                app.focusLinkField()
            }
            .keyboardShortcut("k")
            Button(Copy.openFileMenu) {
                showMain()
                app.openFile()
            }
            .keyboardShortcut("o")
            Button(Copy.searchLyricsMenu) {
                showMain()
                app.presentLyricSearch()
            }
        }
        CommandGroup(after: .saveItem) {
            Button(Copy.exportEllipsis) {
                if let session { app.exportWithPanel(session) }
            }
            .keyboardShortcut("e")
            .disabled(session == nil || session?.isRunning == true && session?.listedSegments.isEmpty == true)
        }

        CommandGroup(replacing: .pasteboard) {
            Button(Copy.cut) { Self.sendToText(#selector(NSText.cut(_:))) }
                .keyboardShortcut("x")
            Button(Copy.copy) {
                if Self.isEditingText {
                    Self.sendToText(#selector(NSText.copy(_:)))
                } else if let track = app.selectedTrack {
                    app.copy(track)
                }
            }
            .keyboardShortcut("c")
            Button(Copy.paste) {
                if Self.isEditingText {
                    Self.sendToText(#selector(NSText.paste(_:)))
                } else {
                    showMain()
                    app.pasteFromPasteboard()
                }
            }
            .keyboardShortcut("v")
            Button(Copy.delete) { Self.sendToText(#selector(NSText.delete(_:))) }
                .keyboardShortcut(.delete, modifiers: [])
            Button(Copy.selectAll) { Self.sendToText(#selector(NSText.selectAll(_:))) }
                .keyboardShortcut("a")
            Divider()
            Button(Copy.copyTracklist) {
                if let session { app.copyTracklist(session) }
            }
            .keyboardShortcut("c", modifiers: [.command, .shift])
            .disabled(!hasTracklist)
        }
        CommandGroup(after: .textEditing) {
            Button(Copy.searchLibrary) {
                showMain()
                app.route = .library
                app.searchFocusRequest += 1
            }
            .keyboardShortcut("f")
        }

        CommandGroup(before: .sidebar) {
            Button(Copy.listenTitle) {
                showMain()
                app.showListen()
            }
            .keyboardShortcut("1")
            Button(Copy.libraryTitle) {
                showMain()
                app.showLibrary()
            }
            .keyboardShortcut("2")
            Button(app.isInspectorPresented ? Copy.hideInspector : Copy.showInspector) {
                withAnimation(AppModel.spring(.large)) { app.toggleInspector() }
            }
            .keyboardShortcut("i", modifiers: [.command, .option])
            Divider()
            Button(Copy.zoomIn) { zoom(by: TimelineStrip.zoomStep) }
                .keyboardShortcut("+")
                .disabled(!hasTimeline)
            Button(Copy.zoomOut) { zoom(by: 1 / TimelineStrip.zoomStep) }
                .keyboardShortcut("-")
                .disabled(!hasTimeline)
            Button(Copy.actualSize) { session?.zoom = 1 }
                .keyboardShortcut("0")
                .disabled(!hasTimeline)
            Divider()
        }

        CommandMenu(Copy.listenMenu) {
            Button(app.listen.isCapturing ? Copy.stop : Copy.listenAction) {
                showMain()
                app.listenNow()
            }
            .keyboardShortcut("l")
            .disabled(app.engine == nil || app.listen.stage == .matching)
            Button(Copy.stop) { app.listen.escape() }
                .keyboardShortcut(.escape, modifiers: [])
                .disabled(!(app.listen.stage == .listening || app.listen.stage == .matching))
            Divider()
            // Listen right away with one method, whichever is selected.
            ForEach(ListenSource.allCases) { source in
                Button(source.menuTitle) {
                    showMain()
                    app.listenNow(source)
                }
                .keyboardShortcut(source.shortcut)
                .disabled(app.engine == nil || app.listen.isBusy)
            }
            Divider()
            // The method the Listen button and ⌘L use (the stage's source toggles).
            Picker(Copy.source, selection: Binding(get: { app.listen.source }, set: { app.listen.select($0) })) {
                ForEach(ListenSource.allCases) { source in
                    Text(source.title).tag(source)
                }
            }
            .pickerStyle(.inline)
            .disabled(app.listen.isBusy)
        }

        CommandMenu(Copy.sessionMenu) {
            Button(Copy.openIn(app.providerPreference.name)) {
                if let track = app.selectedTrack { LinkOpener.openPreferred(track, preference: app.providerPreference) }
            }
            .keyboardShortcut(.return, modifiers: .command)
            .disabled(app.selectedTrack == nil)
            Button(Copy.playPreview) {
                if let track = app.selectedTrack, let preview = track.previewUrl, let url = URL(string: preview) {
                    app.preview.toggle(id: track.identityKey, url: url)
                }
            }
            .disabled(app.selectedTrack?.previewUrl == nil)
            Divider()
            let targets = app.targetSessionIDs
            Button(!targets.isEmpty && app.library.allPinned(targets) ? Copy.unpin : Copy.pin) {
                app.library.togglePinned(targets, undoManager: NSApp.keyWindow?.undoManager)
            }
            .keyboardShortcut("p", modifiers: [.command, .shift])
            .disabled(targets.isEmpty)
        }
    }

    private var hasTimeline: Bool {
        session?.presentation == .timeline
    }

    private func zoom(by factor: Double) {
        guard let session else { return }
        let value = min(max(session.zoom * factor, TimelineStrip.zoomRange.lowerBound), TimelineStrip.zoomRange.upperBound)
        withAnimation(app.motion(.large)) { session.zoom = value }
    }

    private func showMain() {
        if NSApp.windows.first(where: { $0.identifier?.rawValue == "main" && $0.isVisible }) == nil {
            openWindow(id: "main")
        }
    }

    /// True while a text field or text view is being edited in the key window.
    static var isEditingText: Bool {
        NSApp.keyWindow?.firstResponder is NSText
    }

    static func sendToText(_ action: Selector) {
        NSApp.sendAction(action, to: nil, from: nil)
    }
}

private extension ListenSource {
    /// ⌘⇧H hums; the other methods have no shortcut of their own (⌘L listens with the
    /// selected one).
    var shortcut: KeyboardShortcut? {
        self == .hum ? KeyboardShortcut("h", modifiers: [.command, .shift]) : nil
    }
}
