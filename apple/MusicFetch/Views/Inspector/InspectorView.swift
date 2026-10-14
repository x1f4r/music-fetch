import MusicFetchCore
import SwiftUI

/// The inspector column (§3.1): details for the matched track, the selected
/// candidate or the selected mix segment. Empty otherwise.
struct InspectorView: View {
    @Environment(AppModel.self) private var app

    var body: some View {
        ScrollView {
            Group {
                if let model = inspected {
                    switch model.presentation {
                    case .result:
                        if let track = model.track { TrackInspector(model: model, track: track) }
                    case .candidates:
                        if let candidate = model.selectedCandidate { CandidateInspector(model: model, candidate: candidate) }
                    case .timeline, .collection:
                        if let segment = model.selectedSegment { SegmentInspector(model: model, segment: segment) }
                    default:
                        EmptyView()
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(20)
        }
        .scrollBounceBehavior(.basedOnSize)
    }

    /// The session in the detail column, or the selected (not opened) library row.
    private var inspected: SessionModel? {
        if app.route == .library, app.libraryPath.isEmpty {
            guard app.library.selection.count == 1, let id = app.library.selection.first else { return nil }
            return app.cachedSession(id)
        }
        return app.currentSession
    }
}

/// A label–value row.
struct InfoRow: View {
    var label: String
    var value: String
    var monospaced = false

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 12) {
            Text(label)
                .foregroundStyle(.secondary)
            Spacer(minLength: 8)
            Text(value)
                .multilineTextAlignment(.trailing)
                .monospacedDigit()
                .fontDesign(monospaced ? .monospaced : .default)
                .textSelection(.enabled)
        }
        .font(.body)
        .accessibilityElement(children: .combine)
    }
}

/// Section heading in the inspector (`.headline`).
struct InspectorHeading: View {
    var text: String

    var body: some View {
        Text(text)
            .font(.headline)
            .accessibilityAddTraits(.isHeader)
    }
}

/// Matched track: Album, Released, Length, ISRC · Heard · Lyrics.
struct TrackInspector: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel
    var track: Track

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            VStack(spacing: 10) {
                if let album = track.album, !album.isEmpty { InfoRow(label: Copy.album, value: album) }
                if let year = track.releaseYear { InfoRow(label: Copy.released, value: String(year)) }
                if let duration = track.durationMs { InfoRow(label: Copy.length, value: Format.cue(milliseconds: duration)) }
                if let isrc = track.isrc, !isrc.isEmpty { InfoRow(label: Copy.isrc, value: isrc) }
            }
            VStack(alignment: .leading, spacing: 10) {
                InspectorHeading(text: Copy.heard)
                InfoRow(label: Copy.source, value: Copy.sourceName(model))
                InfoRow(label: Copy.when, value: Format.when(model.createdAt))
                if let matchedBy = Copy.matchedBy(model) {
                    InfoRow(label: Copy.matchedByLabel, value: matchedBy)
                }
            }
            if let lyrics = model.lyrics, !lyrics.displayText.isEmpty {
                VStack(alignment: .leading, spacing: 10) {
                    InspectorHeading(text: Copy.lyrics)
                    Text(lyrics.displayText)
                        .font(.body)
                        .foregroundStyle(.secondary)
                        .lineSpacing(4)
                        .textSelection(.enabled)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        }
        .task(id: track.identityKey) {
            guard !model.lyricsLoaded, let engine = app.engine else { return }
            let lyrics = await engine.fetchLyrics(track)
            model.setLyrics(lyrics)
        }
    }
}

/// Hum candidate: the overlay, then Album, Released, Matched by, Where.
struct CandidateInspector: View {
    var model: SessionModel
    var candidate: Candidate

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            if let alignment = candidate.alignment {
                VStack(alignment: .leading, spacing: 12) {
                    InspectorHeading(text: Copy.overlayTitle(candidate.track.title))
                    ContourOverlay(alignment: alignment, title: candidate.track.title)
                }
            }
            VStack(spacing: 10) {
                if let album = candidate.track.album, !album.isEmpty { InfoRow(label: Copy.album, value: album) }
                if let year = candidate.track.releaseYear { InfoRow(label: Copy.released, value: String(year)) }
                InfoRow(label: Copy.matchedByLabel, value: Copy.matchedBy(candidate.sources))
                if let place = Copy.wherePhrase(candidate.sources) {
                    InfoRow(label: Copy.whereLabel, value: place)
                }
            }
        }
    }
}

/// Mix segment: artwork, title, artists, cue range · Match, Evidence, Album · actions.
struct SegmentInspector: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel
    var segment: Segment

    var body: some View {
        VStack(alignment: .leading, spacing: 20) {
            HStack(alignment: .top, spacing: 12) {
                ArtworkView(url: segment.track?.artworkUrl, size: 64, cornerRadius: 8, hatched: segment.track == nil)
                VStack(alignment: .leading, spacing: 2) {
                    Text(segment.track?.title ?? Copy.unknownTrack)
                        .font(.headline)
                        .foregroundStyle(segment.track == nil ? .secondary : .primary)
                    if let artist = segment.track?.artist {
                        Text(artist).font(.callout).foregroundStyle(.secondary)
                    }
                    if segment.endMs > segment.startMs {
                        Text(Format.cueRange(start: segment.startMs, end: segment.endMs))
                            .font(.callout)
                            .monospacedDigit()
                            .foregroundStyle(.secondary)
                    }
                }
            }
            VStack(spacing: 10) {
                InfoRow(label: Copy.match, value: Copy.tier(segment.tier))
                if !segment.evidence.isEmpty {
                    InfoRow(label: Copy.evidenceLabel, value: Copy.evidence(segment.evidence, confidence: segment.confidence))
                }
                if let album = segment.track?.album, !album.isEmpty { InfoRow(label: Copy.album, value: album) }
            }
            VStack(spacing: 8) {
                if let track = segment.track {
                    OpenInButton(track: track, fillsWidth: true)
                }
                if let sourceURL = segment.sourceUrl, let site = model.source?.site ?? Format.host(model.source?.url) {
                    Button {
                        LinkOpener.openSource(sourceURL)
                    } label: {
                        Label(Copy.openSource(site: site, cue: Format.cue(milliseconds: segment.startMs)),
                              systemImage: "arrow.up.right.square")
                            .frame(maxWidth: .infinity)
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.large)
                }
                if segment.tier == .possible, let track = segment.track {
                    HStack {
                        Button(Copy.confirm) { app.correct(segmentID: segment.id, in: model.id, with: track) }
                        NotThisMenu(model: model, segment: segment)
                    }
                    .controlSize(.large)
                }
                if segment.track == nil {
                    Button {
                        app.humSegment(segment, in: model)
                    } label: {
                        Label(Copy.humIt, image: "ContourMark").frame(maxWidth: .infinity)
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.large)
                }
            }
        }
    }
}

/// "Not this": pick an alternate, or hum the part.
struct NotThisMenu: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel
    var segment: Segment
    var iconOnly = false

    var body: some View {
        Menu {
            ForEach(segment.alternates, id: \.identityKey) { alternate in
                Button(Copy.useAlternate(alternate)) { app.correct(segmentID: segment.id, in: model.id, with: alternate) }
            }
            if !segment.alternates.isEmpty { Divider() }
            Button(Copy.humPart) { app.humSegment(segment, in: model) }
            Button(Copy.typeLyric) { app.presentLyricSearch() }
        } label: {
            if iconOnly {
                Image(systemName: "xmark")
            } else {
                Text(Copy.notThis)
            }
        }
        .menuIndicator(.hidden)
        .fixedSize()
        .help(Copy.notThis)
        .accessibilityLabel(Copy.notThis)
    }
}

extension Lyrics {
    /// Synced lines joined, or the plain text.
    nonisolated var displayText: String {
        if !synced.isEmpty { return synced.map(\.text).joined(separator: "\n") }
        return plain.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
