import MusicFetchCore
import SwiftUI

/// The matched track (§6.3): hero wash, artwork, title, artist, meta, context and the
/// "Open in" actions.
struct ResultView: View {
    @Environment(AppModel.self) private var app
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    var model: SessionModel
    var glass: Namespace.ID?

    @State private var revealed = false

    var body: some View {
        if let track = model.track {
            ZStack {
                HeroWash(url: track.artworkUrl, revealed: revealed)
                ScrollView {
                    content(track)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 32)
                        .containerRelativeFrame(.vertical, alignment: .center) { length, _ in length }
                }
                .scrollBounceBehavior(.basedOnSize)
            }
            .onAppear {
                if reduceMotion {
                    withAnimation(.easeInOut(duration: 0.15)) { revealed = true }
                } else {
                    revealed = true
                }
            }
            .onDisappear {
                if app.preview.currentID == track.identityKey { app.preview.stop() }
            }
        }
    }

    private func content(_ track: Track) -> some View {
        VStack(spacing: 0) {
            artwork(track)
                .opacity(revealed ? 1 : 0)
                .scaleEffect(revealed || reduceMotion ? 1 : 0.92)
                .animation(app.motion(.hero).delay(0.05), value: revealed)

            Text(track.title)
                .font(.largeTitle.bold())
                .dynamicTypeSize(...DynamicTypeSize.accessibility2)
                .multilineTextAlignment(.center)
                .lineLimit(2)
                .frame(maxWidth: 520)
                .padding(.top, 24)
                .modifier(Rise(revealed: revealed, delay: 0.09))
            Text(track.artist)
                .font(.title3)
                .multilineTextAlignment(.center)
                .padding(.top, 4)
                .modifier(Rise(revealed: revealed, delay: 0.13))
            if let meta = Format.meta(track) {
                Text(meta)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .padding(.top, 4)
                    .modifier(Rise(revealed: revealed, delay: 0.17))
            }
            if let context = contextLine {
                Text(context)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .frame(maxWidth: 520)
                    .padding(.top, 12)
                    .modifier(Rise(revealed: revealed, delay: 0.17))
            }
            GlassContainer(spacing: 8) {
                HStack(spacing: 12) {
                    OpenInButton(track: track, glassNamespace: glass)
                    ShareLink(item: shareText(track)) {
                        Image(systemName: "square.and.arrow.up").frame(width: 16, height: 16)
                    }
                    .glassButtonStyle()
                    .buttonBorderShape(.circle)
                    .controlSize(.large)
                    .help(Copy.share)
                    .accessibilityLabel(Copy.share)
                    CopyButton(track: track)
                }
            }
            .padding(.top, 22)
        }
        .padding(.horizontal, 32)
    }

    private func artwork(_ track: Track) -> some View {
        let playing = app.preview.isPlaying(track.identityKey)
        return Button {
            togglePreview(track)
        } label: {
            ArtworkView(url: track.artworkUrl, size: 240, cornerRadius: 12)
                .overlay {
                    if app.preview.currentID == track.identityKey {
                        PreviewProgressBorder(progress: app.preview.progress, cornerRadius: 12)
                    }
                }
                .shadow(color: .black.opacity(0.22), radius: 14, y: 10)
        }
        .buttonStyle(.plain)
        .disabled(track.previewUrl == nil)
        .accessibilityLabel(playing ? Copy.pausePreview : Copy.playPreview)
        .accessibilityHidden(track.previewUrl == nil)
    }

    /// "At 12:34 in ‹source›" for links, "From ‹file›" for files.
    private var contextLine: String? {
        switch model.kind {
        case .link:
            guard let title = model.source?.title, !title.isEmpty else { return nil }
            let cue = Format.cue(milliseconds: model.resultSegment?.startMs ?? 0)
            return Copy.contextLink(at: cue, in: title)
        case .file:
            guard let name = model.source?.title ?? model.entry?.title else { return nil }
            return Copy.contextFile(name)
        default:
            return nil
        }
    }

    private func shareText(_ track: Track) -> String {
        let link = TrackLinks(track: track, preference: app.providerPreference).primary?.url
        return [Format.artistTitle(track), link].compactMap { $0 }.joined(separator: "\n")
    }

    private func togglePreview(_ track: Track) {
        guard let preview = track.previewUrl, let url = URL(string: preview) else { return }
        app.preview.toggle(id: track.identityKey, url: url)
    }
}

/// Title, artist and meta rise 8 pt with opacity, staggered.
private struct Rise: ViewModifier {
    @Environment(AppModel.self) private var app
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    var revealed: Bool
    var delay: Double

    func body(content: Content) -> some View {
        content
            .opacity(revealed ? 1 : 0)
            .offset(y: revealed || reduceMotion ? 0 : 8)
            .animation(app.motion(.hero).delay(reduceMotion ? 0 : delay), value: revealed)
    }
}

/// Copy "Artist – Title", with a short "Copied" confirmation.
struct CopyButton: View {
    @Environment(AppModel.self) private var app
    var track: Track
    @State private var copied = false

    var body: some View {
        Button {
            app.copy(track)
            copied = true
            AccessibilityNotification.Announcement(Copy.copied).post()
            Task {
                try? await Task.sleep(for: .seconds(1.5))
                copied = false
            }
        } label: {
            Image(systemName: copied ? "checkmark" : "doc.on.doc")
                .contentTransition(.symbolEffect(.replace))
                .frame(width: 16, height: 16)
        }
        .glassButtonStyle()
        .buttonBorderShape(.circle)
        .controlSize(.large)
        .help(copied ? Copy.copied : Copy.copy)
        .accessibilityLabel(Copy.copy)
    }
}

/// A 2 pt accent ring around the artwork while its preview plays.
struct PreviewProgressBorder: View {
    var progress: Double
    var cornerRadius: CGFloat

    var body: some View {
        RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
            .trim(from: 0, to: max(min(progress, 1), 0))
            .stroke(Color.accentColor, style: StrokeStyle(lineWidth: 2, lineCap: .round))
            .animation(.linear(duration: 0.1), value: progress)
    }
}

/// The blurred artwork behind the result, fading into the window background.
struct HeroWash: View {
    @Environment(\.colorScheme) private var colorScheme
    var url: String?
    var revealed: Bool

    @State private var image: NSImage?

    var body: some View {
        GeometryReader { geometry in
            ZStack {
                if let palette = DemoArtwork.palette(for: url) {
                    Rectangle().fill(palette)
                } else if let image = image ?? url.flatMap(ArtworkLoader.shared.cached) {
                    Image(nsImage: image)
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                        .frame(width: geometry.size.width, height: geometry.size.height)
                        .clipped()
                }
            }
            .blur(radius: 50, opaque: true)
            .saturation(1.3)
            .opacity(revealed ? (colorScheme == .dark ? 0.5 : 0.42) : 0)
            .animation(.easeOut(duration: 0.3), value: revealed)
            .mask {
                LinearGradient(
                    stops: [.init(color: .black, location: 0.2), .init(color: .clear, location: 1)],
                    startPoint: .top, endPoint: .bottom)
            }
        }
        .extendsBackgroundUnderSidebars()
        .ignoresSafeArea()
        .allowsHitTesting(false)
        .accessibilityHidden(true)
        .task(id: url) {
            guard let url, DemoArtwork.palette(for: url) == nil, ArtworkLoader.shared.cached(url) == nil else { return }
            image = await ArtworkLoader.shared.image(for: url)
        }
    }
}
