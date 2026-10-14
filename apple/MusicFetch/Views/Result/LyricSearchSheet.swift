import MusicFetchCore
import SwiftUI

/// Typed lyric search (§6.7): a 480 × 360 sheet.
struct LyricSearchSheet: View {
    @Bindable var model: LyricSearchModel
    @FocusState private var fieldFocused: Bool

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 8) {
                Image(systemName: "text.alignleft")
                    .foregroundStyle(.secondary)
                TextField(Copy.lyricPrompt, text: $model.text)
                    .textFieldStyle(.plain)
                    .focused($fieldFocused)
                    .onSubmit { model.search() }
                if model.isSearching {
                    ProgressView().controlSize(.small)
                }
            }
            .padding(.horizontal, 12)
            .frame(height: 32)
            .background(Capsule().fill(.fill.tertiary))
            .padding(16)

            Group {
                if let error = model.error {
                    message(error)
                } else if model.isEmpty {
                    VStack(spacing: 12) {
                        Text(Copy.lyricsEmpty).foregroundStyle(.secondary)
                        Button(Copy.tryDifferentWords) {
                            model.reset()
                            fieldFocused = true
                        }
                        .buttonStyle(.bordered)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    List(selection: $model.selection) {
                        ForEach(model.results, id: \.track.identityKey) { candidate in
                            LyricResultRow(candidate: candidate)
                                .tag(candidate.track.identityKey)
                        }
                    }
                    .listStyle(.inset)
                    .contextMenu(forSelectionType: String.self) { _ in
                    } primaryAction: { ids in
                        if let id = ids.first, let candidate = model.results.first(where: { $0.track.identityKey == id }) {
                            model.choose(candidate)
                        }
                    }
                }
            }
            .frame(maxHeight: .infinity)

            HStack {
                Spacer()
                Button(Copy.cancel, role: .cancel) { model.cancel() }
                    .keyboardShortcut(.cancelAction)
                Button(Copy.choose) {
                    if let id = model.selection, let candidate = model.results.first(where: { $0.track.identityKey == id }) {
                        model.choose(candidate)
                    } else {
                        model.search()
                    }
                }
                .keyboardShortcut(.defaultAction)
                .disabled(model.text.trimmingCharacters(in: .whitespaces).isEmpty)
            }
            .padding(16)
        }
        .frame(width: 480, height: 360)
        .onAppear { fieldFocused = true }
    }

    private func message(_ text: String) -> some View {
        Text(text)
            .foregroundStyle(.secondary)
            .multilineTextAlignment(.center)
            .padding()
            .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

struct LyricResultRow: View {
    var candidate: Candidate

    var body: some View {
        HStack(spacing: 12) {
            ArtworkView(url: candidate.track.artworkUrl, size: 40, cornerRadius: 6)
            VStack(alignment: .leading, spacing: 2) {
                Text(candidate.track.title).font(.headline).lineLimit(1)
                Text(candidate.track.artist).font(.callout).foregroundStyle(.secondary).lineLimit(1)
            }
            Spacer(minLength: 8)
            Text(Copy.lyrics)
                .font(.subheadline.weight(.medium))
                .padding(.horizontal, 8)
                .frame(minHeight: 18)
                .background(Capsule().fill(.fill.quaternary))
        }
        .padding(.vertical, 2)
        .accessibilityElement(children: .combine)
    }
}
