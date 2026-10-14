import MusicFetchCore
import SwiftUI

/// Shows a session with the screen its presentation calls for.
struct SessionContentView: View {
    var model: SessionModel
    var glass: Namespace.ID?

    var body: some View {
        switch model.presentation {
        case .result:
            ResultView(model: model, glass: glass)
        case .candidates:
            CandidatesView(model: model)
        case .timeline, .collection, .running:
            MixView(model: model)
        case .empty:
            EmptySessionView(model: model)
        }
    }
}

/// A finished session without an answer.
struct EmptySessionView: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel

    var body: some View {
        VStack(spacing: 16) {
            ArtworkView(url: model.artworkURL, size: 64, cornerRadius: 8, hatched: model.artworkURL == nil)
            Text(model.title)
                .font(.headline)
                .lineLimit(2)
                .multilineTextAlignment(.center)
            Text(reason)
                .font(.title3)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
            HStack(spacing: 12) {
                Button(Copy.tryAgain) { retry() }
                    .glassButtonStyle()
                    .controlSize(.large)
                if model.kind == .listen || model.kind == .systemAudio || model.kind == .hum {
                    Button(Copy.humIt) { app.listenNow(.hum) }
                        .glassButtonStyle()
                        .controlSize(.large)
                }
                Button(Copy.typeLyric) { app.presentLyricSearch() }
                    .glassButtonStyle()
                    .controlSize(.large)
            }
        }
        .padding(32)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var reason: String {
        if let failure = model.failure { return Copy.failure(failure.code, message: failure.message) }
        if model.status == .canceled { return Copy.stoppedPlain }
        return Copy.noMatch(model.noMatchReason ?? .notFound)
    }

    private func retry() {
        switch model.kind {
        case .link:
            if let url = model.source?.url ?? model.entry?.sourceUrl { app.identify(url, mode: model.mode) }
        case .hum:
            app.listenNow(.hum)
        case .systemAudio:
            app.listenNow(.thisMac)
        case .lyrics:
            app.presentLyricSearch()
        case .file, .listen:
            app.listenNow(.microphone)
        }
    }
}
