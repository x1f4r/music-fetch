import MusicFetchCore
import SwiftUI

/// The menu bar extra window (§6.10).
struct MenuBarContent: View {
    @Environment(AppModel.self) private var app
    @Environment(\.openWindow) private var openWindow
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        let listen = app.listen!
        VStack(spacing: 0) {
            GlassContainer(spacing: 24) {
                ListenButton(listen: listen, diameter: 72)
                    .frame(height: 96)
            }
            Text(menuStateLine(listen))
                .font(.body)
                .foregroundStyle(listen.isStateLineActive ? .primary : .secondary)
                .multilineTextAlignment(.center)
                .lineLimit(2)
                .frame(minHeight: 20)
                .padding(.top, 4)
                .contentTransition(.opacity)
                .animation(.easeInOut(duration: 0.15), value: listen.stateLine)
            SourceToggles(listen: listen, compact: true)
                .frame(height: 28)
                .padding(.top, 12)
            if !listen.recoveryActions.isEmpty {
                RecoveryRow(listen: listen, compact: true)
                    .frame(height: 28)
                    .padding(.top, 8)
            }

            // While humming, the answers so far take the last result's place.
            if listen.showsLiveAnswers {
                Divider().padding(.top, 16)
                LiveAnswersView(listen: listen)
                    .frame(height: LiveAnswersView.height)
                    .padding(.vertical, 8)
            } else if let recent = app.library.recent {
                Divider().padding(.top, 16)
                MenuBarResultRow(entry: recent) { showMainWindow(then: { app.showOnListen(recent.id) }) }
                    .padding(.vertical, 10)
            }

            Divider().padding(.top, app.library.recent == nil && !listen.showsLiveAnswers ? 16 : 0)
            HStack {
                Button(Copy.openMusicFetch) { showMainWindow() }
                    .buttonStyle(.plain)
                Spacer()
                Button {
                    NSApp.activate()
                    openSettings()
                } label: {
                    Image(systemName: "gearshape")
                }
                .buttonStyle(.plain)
                .foregroundStyle(.secondary)
                .help(Copy.settingsTitle)
                .accessibilityLabel(Copy.settingsTitle)
            }
            .padding(.top, 8)
        }
        .padding(.top, 20)
        .padding(.horizontal, 16)
        .padding(.bottom, 10)
        .frame(width: 320)
        .onChange(of: listen.shown?.id) { _, _ in
            // Candidates and mixes need the main window; a match updates the row in place.
            guard let shown = listen.shown, shown.presentation != .result else { return }
            showMainWindow()
        }
    }

    /// While matching after a match, the extra stays idle: no result hero here.
    private func menuStateLine(_ listen: ListenModel) -> String {
        listen.stateLine
    }

    private func showMainWindow(then action: (() -> Void)? = nil) {
        NSApp.activate()
        openWindow(id: "main")
        action?()
    }
}

/// Artwork, title and artist of the last result with "Open in ‹service›".
struct MenuBarResultRow: View {
    var entry: LibraryEntry
    var open: () -> Void

    var body: some View {
        HStack(spacing: 12) {
            Button(action: open) {
                HStack(spacing: 12) {
                    ArtworkView(url: entry.artworkUrl ?? entry.track?.artworkUrl, size: 40, cornerRadius: 6)
                    VStack(alignment: .leading, spacing: 2) {
                        Text(entry.displayTitle)
                            .font(.headline)
                            .lineLimit(1)
                        Text(entry.track?.artist ?? entry.subtitle ?? "")
                            .font(.callout)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    Spacer(minLength: 8)
                }
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            if let track = entry.track {
                OpenInButton(track: track, style: .bordered)
            }
        }
    }
}
