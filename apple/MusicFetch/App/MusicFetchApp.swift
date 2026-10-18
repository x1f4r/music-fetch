import SwiftUI

@main
struct MusicFetchApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var delegate
    @State private var app: AppModel
    @AppStorage(PreferenceKey.showMenuBarExtra) private var showMenuBarExtra = true

    init() {
        Preferences.registerDefaults()
        if RenderHarness.isActive {
            // The offscreen harness: no Dock icon, never active.
            NSApplication.shared.setActivationPolicy(.prohibited)
        } else {
            ProviderPreference.migrate()
        }
        _app = State(initialValue: AppModel())
    }

    var body: some Scene {
        Window(Copy.appName, id: "main") {
            RootView()
                .environment(app)
        }
        .defaultSize(width: 1180, height: 760)
        .defaultLaunchBehavior(RenderHarness.isActive ? .suppressed : .automatic)
        .restorationBehavior(RenderHarness.isActive ? .disabled : .automatic)
        .commands { MusicFetchCommands(app: app) }

        Settings {
            SettingsView()
                .environment(app)
        }

        MenuBarExtra(isInserted: RenderHarness.isActive ? .constant(false) : $showMenuBarExtra) {
            MenuBarContent()
                .environment(app)
        } label: {
            Image("contour.mark")
                .accessibilityLabel(Copy.appName)
        }
        .menuBarExtraStyle(.window)
    }
}
