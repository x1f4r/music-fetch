import MusicFetchCore
import SwiftUI

/// Ranked hum or lyric candidates (§6.4).
struct CandidatesView: View {
    @Environment(AppModel.self) private var app
    @Bindable var model: SessionModel

    var body: some View {
        VStack(spacing: 0) {
            if model.kind != .lyrics, !model.humContour.isEmpty || model.humDurationSeconds > 0 {
                humRow
            }
            List(selection: $model.selectedCandidateID) {
                ForEach(Array(model.candidates.enumerated()), id: \.element.track.identityKey) { index, candidate in
                    CandidateRow(
                        model: model, candidate: candidate,
                        isSelected: (model.selectedCandidateID ?? model.candidates.first?.track.identityKey) == candidate.track.identityKey
                    )
                    .tag(candidate.track.identityKey)
                    .listRowInsets(EdgeInsets(top: 4, leading: 16, bottom: 4, trailing: 16))
                    .listRowSeparator(.hidden)
                    .accessibilityElement(children: .contain)
                    .accessibilityLabel(Copy.candidateAccessibility(candidate, rank: index + 1))
                }
                HStack(spacing: 10) {
                    Text(Copy.notHere)
                        .font(.callout)
                        .foregroundStyle(.secondary)
                    Button {
                        app.presentLyricSearch()
                    } label: {
                        Label(Copy.typeLyric, systemImage: "text.alignleft")
                    }
                    .buttonStyle(.bordered)
                }
                .padding(.top, 18)
                .listRowSeparator(.hidden)
                .selectionDisabled()
            }
            .listStyle(.inset)
            .scrollContentBackground(.hidden)
            .animation(AppModel.spring(.chrome), value: model.candidates.map(\.track.identityKey))
        }
        .onAppear {
            if model.selectedCandidateID == nil { model.selectedCandidateID = model.candidates.first?.track.identityKey }
        }
    }

    private var humRow: some View {
        HStack(spacing: 16) {
            HumStrip(contour: model.humContour)
                .frame(height: 56)
            Text(Copy.yourHum(model.humDurationSeconds))
                .font(.callout)
                .foregroundStyle(.secondary)
                .monospacedDigit()
                .fixedSize()
            Button {
                app.listenNow(.hum)
            } label: {
                Label(Copy.humAgain, image: "ContourMark")
            }
            .buttonStyle(.bordered)
            .fixedSize()
        }
        .frame(height: 72)
        .padding(.horizontal, 28)
        .padding(.vertical, 8)
    }

    private func togglePreview(_ track: Track) {
        guard let preview = track.previewUrl, let url = URL(string: preview) else { return }
        app.preview.toggle(id: track.identityKey, url: url)
    }
}

/// One candidate: artwork, title and artist, match capsule, preview, "This is it".
struct CandidateRow: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel
    var candidate: Candidate
    var isSelected: Bool

    var body: some View {
        let track = candidate.track
        let playing = app.preview.isPlaying(track.identityKey)
        HStack(spacing: 12) {
            ArtworkView(url: track.artworkUrl, size: 48, cornerRadius: 8)
            VStack(alignment: .leading, spacing: 2) {
                Text(track.title)
                    .font(.headline)
                    .lineLimit(1)
                Text(track.artist)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer(minLength: 12)
            CandidateTag(tier: candidate.tier)
            if let preview = track.previewUrl, let url = URL(string: preview) {
                Button {
                    app.preview.toggle(id: track.identityKey, url: url)
                } label: {
                    Image(systemName: playing ? "pause.fill" : "play.fill")
                        .frame(width: 14, height: 14)
                }
                .buttonStyle(.bordered)
                .buttonBorderShape(.circle)
                .help(playing ? Copy.pausePreview : Copy.playPreview)
                .accessibilityLabel(playing ? Copy.pausePreview : Copy.playPreview)
            }
            if isSelected {
                Button(Copy.thisIsIt) { app.confirm(candidate, in: model) }
                    .buttonStyle(.borderedProminent)
                    .keyboardShortcut(.defaultAction)
            } else {
                Button(Copy.thisIsIt) { app.confirm(candidate, in: model) }
                    .buttonStyle(.bordered)
            }
        }
        .frame(minHeight: 60)
        .contextMenu {
            OpenInMenu(track: track)
            Button(Copy.copy) { app.copy(track) }
        }
    }
}
