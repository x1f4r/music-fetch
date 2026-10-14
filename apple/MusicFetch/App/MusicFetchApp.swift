import SwiftUI

@main
struct MusicFetchApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var delegate
    @State private var app: AppModel
    @AppStorage(PreferenceKey.showMenuBarExtra) private var showMenuBarExtra = true

    init() {
        Preferences.registerDefaults()
        _app = State(initialValue: AppModel())
    }

    var body: some Scene {
        Window(Copy.appName, id: "main") {
            RootView()
                .environment(app)
        }
        .defaultSize(width: 1180, height: 760)
        .commands { MusicFetchCommands(app: app) }

        Settings {
            SettingsView()
                .environment(app)
        }

        MenuBarExtra(isInserted: $showMenuBarExtra) {
            MenuBarContent()
                .environment(app)
        } label: {
            Image("ContourMark")
                .accessibilityLabel(Copy.appName)
        }
        .menuBarExtraStyle(.window)
    }
}
