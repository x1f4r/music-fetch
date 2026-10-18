import MusicFetchCore
import SwiftUI

/// The Listen stage (§6.1). Every slot keeps its frame in every state; positions are
/// relative to the pane center `c`.
struct ListenStage: View {
    @Environment(AppModel.self) private var app
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    var listen: ListenModel
    var glass: Namespace.ID

    private var showsTrace: Bool {
        (listen.stage == .listening || listen.stage == .matching) && listen.impression
    }

    private var stateLineHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize ? 48 : 24
    }

    var body: some View {
        GeometryReader { geometry in
            let size = geometry.size
            let c = size.height / 2
            let x = size.width / 2
            ZStack {
                if showsTrace {
                    PitchTraceView(listen: listen)
                        .frame(width: min(560, size.width - 64), height: 96)
                        .transition(
                            reduceMotion
                                ? .opacity
                                : .scale(scale: 0.12, anchor: .bottom).combined(with: .opacity))
                        .position(x: x, y: c - 120 - 48)
                }

                ListenButton(listen: listen, glassNamespace: glass)
                    .position(x: x, y: c - 44)

                StateLine(listen: listen)
                    .frame(width: min(size.width - 48, 600), height: stateLineHeight)
                    .position(x: x, y: c + 30 + stateLineHeight / 2)

                SourceToggles(listen: listen)
                    .frame(height: 28)
                    .position(x: x, y: c + 30 + stateLineHeight + 20 + 14)

                RecoveryRow(listen: listen)
                    .frame(height: 28)
                    .position(x: x, y: c + 30 + stateLineHeight + 20 + 28 + 12 + 14)

                if let recent = app.library.recent, size.height > 420 {
                    RecentRow(entry: recent)
                        .frame(width: min(520, size.width - 48), height: 56)
                        .position(x: x, y: size.height - 24 - 28)
                }
            }
            .animation(app.motion(.hero), value: showsTrace)
        }
    }
}

/// The state line (§5.3): one `.title3` line with a fixed height; crossfades.
struct StateLine: View {
    var listen: ListenModel

    var body: some View {
        Group {
            if case .listening = listen.stage, case .rateLimited = listen.notice {
                TimelineView(.periodic(from: .now, by: 1)) { _ in content }
            } else {
                content
            }
        }
    }

    private var content: some View {
        HStack(spacing: 8) {
            if listen.showsSpinner {
                ProgressView().controlSize(.small)
            }
            Text(listen.stateLine)
                .contentTransition(.opacity)
        }
        .font(.title3)
        .foregroundStyle(listen.isStateLineActive ? .primary : .secondary)
        .multilineTextAlignment(.center)
        .lineLimit(2)
        .minimumScaleFactor(0.8)
        .animation(.easeInOut(duration: 0.15), value: listen.stateLine)
        // Read through announcements only (§8), so the line itself isn't a live region.
        .accessibilityAddTraits(.isStaticText)
    }
}

/// The row under the source toggles: up to three ways forward after a no-match, an
/// error or a prompt (§5.4). Empty, but still in its slot, otherwise.
struct RecoveryRow: View {
    var listen: ListenModel
    var compact = false

    var body: some View {
        let actions = listen.recoveryActions
        GlassContainer(spacing: 12) {
            HStack(spacing: compact ? 8 : 12) {
                // Actions, not choices: accent text, so they don't read as more toggles.
                ForEach(actions, id: \.self) { action in
                    Button(Copy.title(for: action)) { listen.perform(action) }
                        .buttonStyle(.borderless)
                        .foregroundStyle(.tint)
                        .fontWeight(.medium)
                        .controlSize(compact ? .small : .large)
                        .padding(.horizontal, compact ? 4 : 8)
                        .frame(minHeight: compact ? 24 : 28)
                        .contentShape(Rectangle())
                        .accessibilityIdentifier("mf.action.\(action)")
                }
            }
        }
        .animation(.easeInOut(duration: 0.15), value: actions)
    }
}

/// Microphone · This Mac · Hum (§5.2): always on the stage, so the method can change
/// after any result. Choosing one never starts listening. Disabled at 45 % while a
/// session runs.
struct SourceToggles: View {
    var listen: ListenModel
    var compact = false

    var body: some View {
        GlassContainer(spacing: 12) {
            HStack(spacing: compact ? 8 : 12) {
                ForEach(ListenSource.allCases) { source in
                    let selected = listen.source == source
                    Toggle(isOn: Binding(get: { selected }, set: { if $0 { listen.select(source) } })) {
                        Label { Text(source.title) } icon: { source.icon }
                    }
                    .toggleStyle(.button)
                    .glassButtonStyle()
                    .tint(selected ? .accentColor : nil)
                    .controlSize(compact ? .small : .large)
                    .accessibilityAddTraits(selected ? .isSelected : [])
                    .accessibilityIdentifier("mf.source.\(source.rawValue)")
                }
            }
            .frame(height: compact ? 24 : 28)
        }
        .disabled(listen.isBusy)
        .opacity(listen.isBusy ? 0.45 : 1)
    }
}

/// The last result under the stage (§6.1 "Recent row"): highlights on hover, opens on
/// click, and has the session's context menu.
struct RecentRow: View {
    @Environment(AppModel.self) private var app
    @Environment(\.undoManager) private var undoManager
    var entry: LibraryEntry
    @State private var hovering = false

    var body: some View {
        HStack(spacing: 12) {
            Button {
                app.showOnListen(entry.id)
            } label: {
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
                    Spacer(minLength: 12)
                    Text(Format.time(Format.date(milliseconds: entry.createdAtMs)))
                        .font(.callout)
                        .monospacedDigit()
                        .foregroundStyle(.secondary)
                }
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .accessibilityLabel(Copy.recentAccessibility(entry))
            if let track = entry.track {
                OpenInButton(track: track, style: .bordered)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 6)
        .background {
            RoundedRectangle(cornerRadius: 10, style: .continuous)
                .fill(.fill.quaternary)
                .opacity(hovering ? 1 : 0)
        }
        .onHover { hovering = $0 }
        .animation(.easeOut(duration: 0.12), value: hovering)
        .contextMenu {
            Button(Copy.open) { app.showOnListen(entry.id) }
            if let track = entry.track {
                OpenInMenu(track: track)
                Button(Copy.copy) { app.copy(track) }
            }
            Divider()
            let pinned = app.library.isPinned(entry.id)
            Button(pinned ? Copy.unpin : Copy.pin) {
                app.library.setPinned([entry.id], pinned: !pinned, undoManager: undoManager)
            }
        }
    }
}
