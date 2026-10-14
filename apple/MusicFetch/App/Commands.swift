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
                app.route = .listen
                app.linkFieldFocusRequest += 1
            }
            .keyboardShortcut("k")
            Button(Copy.openFileMenu) {
                showMain()
                app.openFile()
            }
            .keyboardShortcut("o")
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
            Button(app.showInspector ? Copy.hideInspector : Copy.showInspector) {
                withAnimation(AppModel.spring(.large)) { app.setInspector(!app.showInspector) }
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
            Button(Copy.hum) {
                showMain()
                app.listenNow(.hum)
            }
            .keyboardShortcut("h", modifiers: [.command, .shift])
            .disabled(app.engine == nil || app.listen.isBusy)
            Button(Copy.searchLyricsMenu) {
                showMain()
                app.presentLyricSearch()
            }
            .disabled(app.engine == nil)
            Button(Copy.stop) { app.listen.escape() }
                .keyboardShortcut(.escape, modifiers: [])
                .disabled(!(app.listen.stage == .listening || app.listen.stage == .matching))
            Divider()
            Picker(Copy.source, selection: Binding(get: { app.listen.source }, set: { app.listen.source = $0 })) {
                ForEach(ListenSource.allCases) { source in
                    Text(source.title).tag(source)
                }
            }
            .pickerStyle(.inline)
            .disabled(app.listen.isBusy)
        }

        CommandMenu(Copy.sessionMenu) {
            Button(Copy.openInDefault) {
                if let track = app.selectedTrack { LinkOpener.openPreferred(track, defaultService: app.defaultService) }
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
            Button(app.isTargetPinned ? Copy.unpin : Copy.pin) {
                if let id = app.targetSessionID { app.library.togglePin(id) }
            }
            .keyboardShortcut("p", modifiers: [.command, .shift])
            .disabled(app.targetSessionID == nil)
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
