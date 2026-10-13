import MusicFetchCore
import SwiftUI

/// Placeholder window: proves the engine is linked and the bundled runtime runs.
struct ContentView: View {
    @State private var ytDlp = "…"

    var body: some View {
        VStack(spacing: 6) {
            Text("Music Fetch")
                .font(.largeTitle.weight(.semibold))
            Group {
                Text("Core \(coreVersion())")
                Text("yt-dlp \(ytDlp)")
            }
            .font(.callout.monospacedDigit())
            .foregroundStyle(.secondary)
            .textSelection(.enabled)
        }
        .fixedSize()
        .padding(.horizontal, 64)
        .padding(.vertical, 40)
        .task { ytDlp = await Self.ytDlpVersion() }
    }

    private static func ytDlpVersion() async -> String {
        do {
            return try await MediaRuntime.locate().runYtDlpVersion()
        } catch {
            return "unavailable: \(error.localizedDescription)"
        }
    }
}
