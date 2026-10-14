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

                ActionRow(listen: listen)
                    .frame(height: 28)
                    .position(x: x, y: c + 30 + stateLineHeight + 20 + 14)

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

/// The slot under the state line: the source toggles or up to three actions (§5.4).
struct ActionRow: View {
    @Environment(AppModel.self) private var app
    var listen: ListenModel
    var compact = false

    var body: some View {
        Group {
            if let actions = listen.actions {
                GlassEffectContainer(spacing: 12) {
                    HStack(spacing: 12) {
                        ForEach(actions, id: \.self) { action in
                            Button(Copy.title(for: action)) { listen.perform(action) }
                                .buttonStyle(.glass)
                                .controlSize(compact ? .small : .large)
                        }
                    }
                }
                .transition(.opacity)
            } else {
                SourceToggles(listen: listen, compact: compact)
                    .transition(.opacity)
            }
        }
        .animation(.easeInOut(duration: 0.15), value: listen.actions)
    }
}

/// Microphone · This Mac · Hum (§5.2). Disabled at 45 % while a session runs.
struct SourceToggles: View {
    var listen: ListenModel
    var compact = false

    var body: some View {
        GlassEffectContainer(spacing: 12) {
            HStack(spacing: compact ? 8 : 12) {
                ForEach(ListenSource.allCases) { source in
                    let selected = listen.source == source
                    Toggle(isOn: Binding(get: { selected }, set: { if $0 { listen.source = source } })) {
                        Label {
                            Text(source.title)
                        } icon: {
                            if let symbol = source.symbol {
                                Image(systemName: symbol)
                            } else {
                                Image("ContourMark")
                            }
                        }
                    }
                    .toggleStyle(.button)
                    .buttonStyle(.glass)
                    .tint(selected ? .accentColor : nil)
                    .controlSize(compact ? .small : .large)
                    .accessibilityAddTraits(selected ? .isSelected : [])
                }
            }
        }
        .disabled(listen.isBusy)
        .opacity(listen.isBusy ? 0.45 : 1)
    }
}

/// The last result under the stage (§6.1 "Recent row").
struct RecentRow: View {
    @Environment(AppModel.self) private var app
    var entry: LibraryEntry

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
    }
}
