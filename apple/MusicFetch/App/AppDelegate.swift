import AppKit
import MusicFetchCore

/// App lifecycle hooks SwiftUI does not cover: the dock menu, the Services menu
/// ("Identify with Music Fetch"), staying alive for the menu bar extra, and
/// committing pending deletes at quit.
final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.servicesProvider = self
        NSUpdateDynamicServices()
        // The menu bar extra needs the engine even when no window opens.
        Task { await AppModel.shared?.start() }
        // Esc stops listening wherever focus is (a menu key equivalent alone is not
        // reliable for Escape).
        // Space plays the selected preview unless text is being edited.
        keyMonitor = NSEvent.addLocalMonitorForEvents(matching: .keyDown) { event in
            guard event.modifierFlags.intersection(.deviceIndependentFlagsMask).isEmpty, let app = AppModel.shared,
                event.window?.attachedSheet == nil
            else { return event }
            switch event.keyCode {
            case 53 where app.listen.stage == .listening || app.listen.stage == .matching:
                app.listen.escape()
                return nil
            case 49 where !(event.window?.firstResponder is NSText) && event.window?.identifier?.rawValue.hasPrefix("main") == true:
                return app.playSelection() ? nil : event
            default:
                return event
            }
        }
    }

    private var keyMonitor: Any?

    /// Keep running after the last window closes only while the menu bar extra shows.
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        !UserDefaults.standard.bool(forKey: PreferenceKey.showMenuBarExtra)
    }

    func applicationWillTerminate(_ notification: Notification) {
        AppModel.shared?.prepareForTermination()
    }

    func applicationDockMenu(_ sender: NSApplication) -> NSMenu? {
        guard let app = AppModel.shared else { return nil }
        let menu = NSMenu()
        menu.addItem(ClosureMenuItem.make(title: Copy.listenAction) { app.listenNow(.microphone) })
        menu.addItem(ClosureMenuItem.make(title: Copy.hum) { app.listenNow(.hum) })
        let recent = app.library.recentResults
        if !recent.isEmpty {
            menu.addItem(.separator())
            for entry in recent {
                let title = entry.track.map { Format.artistTitle($0) } ?? entry.title
                menu.addItem(ClosureMenuItem.make(title: title) {
                    NSApp.activate()
                    app.openMainWindowAction?()
                    app.showOnListen(entry.id)
                })
            }
        }
        return menu
    }

    /// Services menu: text containing a link, a URL or a file.
    @objc func identifyWithMusicFetch(_ pasteboard: NSPasteboard, userData: String?, error: AutoreleasingUnsafeMutablePointer<NSString?>) {
        var input: String?
        if let url = pasteboard.readObjects(forClasses: [NSURL.self])?.first as? URL {
            input = url.isFileURL ? url.path : url.absoluteString
        } else if let text = pasteboard.string(forType: .string) {
            input = AppModel.extractInput(from: text)
        }
        guard let input, let app = AppModel.shared else {
            error.pointee = Copy.serviceNoLink as NSString
            return
        }
        NSApp.activate()
        app.openMainWindowAction?()
        app.identify(input)
    }
}

/// Builds menu items that run a closure (the target lives as the represented object).
enum ClosureMenuItem {
    static func make(title: String, handler: @escaping () -> Void) -> NSMenuItem {
        let target = MenuAction(handler: handler)
        let item = NSMenuItem(title: title, action: #selector(MenuAction.run), keyEquivalent: "")
        item.target = target
        item.representedObject = target
        return item
    }
}

final class MenuAction: NSObject {
    private let handler: () -> Void

    init(handler: @escaping () -> Void) {
        self.handler = handler
    }

    @objc func run() {
        handler()
    }
}

/// Target for the export panel's format popup.
final class PopupObserver: NSObject {
    private let onChange: (Int) -> Void

    init(onChange: @escaping (Int) -> Void) {
        self.onChange = onChange
    }

    @objc func changed(_ sender: NSPopUpButton) {
        onChange(sender.indexOfSelectedItem)
    }
}
