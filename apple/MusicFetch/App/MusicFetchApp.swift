import SwiftUI

@main
struct MusicFetchApp: App {
    var body: some Scene {
        Window("Music Fetch", id: "main") {
            ContentView()
        }
        .windowStyle(.hiddenTitleBar)
        .windowResizability(.contentSize)
    }
}
