import AppKit
import SwiftUI

final class MusicFetchAppDelegate: NSObject, NSApplicationDelegate {
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        false
    }
}

@main
struct MusicFetchMacApp: App {
    @NSApplicationDelegateAdaptor(MusicFetchAppDelegate.self) private var appDelegate
    @StateObject private var model = AppModel()

    var body: some Scene {
        WindowGroup(id: "main") {
            ContentView(model: model)
        }
        .defaultSize(width: 1520, height: 980)
        .commands {
            CommandGroup(after: .newItem) {
                Button("Run Analysis") {
                    NotificationCenter.default.post(name: .musicFetchAnalyze, object: nil)
                }
                .keyboardShortcut(.return, modifiers: [.command])

                Button("Focus Input") {
                    NotificationCenter.default.post(name: .musicFetchFocusInput, object: nil)
                }
                .keyboardShortcut("l", modifiers: [.command])

                Button("Diagnostics") {
                    NotificationCenter.default.post(name: .musicFetchShowDiagnostics, object: nil)
                }
                .keyboardShortcut("d", modifiers: [.command, .shift])
            }
        }

        MenuBarExtra {
            MenuBarQuickCaptureView(model: model)
        } label: {
            Image(systemName: "music.note.magnifyingglass")
                .accessibilityLabel("Music Fetch")
        }
        .menuBarExtraStyle(.window)

        Settings {
            SettingsView(model: model)
        }
    }
}
