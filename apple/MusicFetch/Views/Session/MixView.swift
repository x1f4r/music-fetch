import MusicFetchCore
import SwiftUI

/// Mix session (§6.5) and collections (§6.6): header, optional timeline strip pinned
/// above the tracklist.
struct MixView: View {
    @Environment(AppModel.self) private var app
    @Bindable var model: SessionModel

    private var showsTimeline: Bool { model.presentation != .collection }

    var body: some View {
        ScrollViewReader { proxy in
            List(selection: $model.selectedSegmentID) {
                ForEach(model.listedSegments, id: \.id) { segment in
                    SegmentRow(model: model, segment: segment, showsIndex: !showsTimeline)
                        .tag(segment.id)
                        .id(segment.id)
                        .listRowInsets(EdgeInsets(top: 3, leading: 20, bottom: 3, trailing: 20))
                        .listRowSeparator(.hidden)
                        .transition(.offset(y: 6).combined(with: .opacity))
                }
                if model.listedSegments.isEmpty, !model.isRunning {
                    Text(Copy.noMatch(.notFound))
                        .foregroundStyle(.secondary)
                        .selectionDisabled()
                }
            }
            .listStyle(.inset)
            .scrollContentBackground(.hidden)
            .animation(AppModel.spring(.chrome), value: model.listedSegments.map(\.id))
            .pinnedTopBar {
                VStack(spacing: 0) {
                    MixHeader(model: model)
                    if showsTimeline {
                        TimelineStrip(model: model)
                            .padding(.horizontal, 20)
                            .padding(.bottom, 10)
                    }
                }
            }
            .onChange(of: model.selectedSegmentID) { _, id in
                guard let id else { return }
                withAnimation(AppModel.spring(.chrome)) { proxy.scrollTo(id) }
            }
            .onAppear { selectFirstWhenFinished() }
            .onChange(of: model.isRunning) { _, _ in selectFirstWhenFinished() }
            .onKeyPress(.return) {
                guard let track = model.selectedSegment?.track else { return .ignored }
                LinkOpener.openPreferred(track, preference: app.providerPreference)
                return .handled
            }
        }
    }

    /// A finished session shows its first track in the inspector.
    private func selectFirstWhenFinished() {
        if model.selectedSegmentID == nil, !model.isRunning {
            model.selectedSegmentID = model.listedSegments.first?.id
        }
    }

}

/// Thumbnail, title, subtitle and status (progress + Stop, or the identified count).
struct MixHeader: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel

    private var isCollection: Bool { model.presentation == .collection }

    var body: some View {
        HStack(spacing: 14) {
            if isCollection {
                ArtworkView(url: model.artworkURL, size: 56, cornerRadius: 6)
            } else {
                ArtworkView(url: model.artworkURL, size: 100, cornerRadius: 6, height: 56)
            }
            VStack(alignment: .leading, spacing: 2) {
                Text(model.title)
                    .font(.headline)
                    .lineLimit(1)
                if let subtitle = isCollection ? model.collectionSubtitle : model.mediaSubtitle {
                    Text(subtitle)
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }
            .accessibilityElement(children: .combine)
            .accessibilityAddTraits(.isHeader)
            Spacer(minLength: 12)
            if model.isRunning {
                HStack(spacing: 10) {
                    if model.phase == .downloading, model.progress == nil, let fraction = model.fraction {
                        ProgressView(value: fraction)
                            .progressViewStyle(.circular)
                            .controlSize(.small)
                    } else {
                        ProgressView().controlSize(.small)
                    }
                    Text(model.statusText)
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .monospacedDigit()
                        .contentTransition(.numericText())
                    Button(Copy.stop) { model.cancel() }
                        .buttonStyle(.bordered)
                        .accessibilityLabel(Copy.stop)
                }
                .fixedSize()
            } else if !isCollection {
                Text(model.statusText)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
                    .fixedSize()
            }
        }
        .padding(.top, 6)
        .padding(.horizontal, 20)
        .padding(.bottom, 14)
    }
}

/// A tracklist row (§5.9): cue (or item number), artwork, title and artists, trailing
/// tag and actions.
struct SegmentRow: View {
    @Environment(AppModel.self) private var app
    var model: SessionModel
    var segment: Segment
    var showsIndex: Bool

    var body: some View {
        HStack(spacing: 12) {
            Text(showsIndex ? "\(segment.itemIndex + 1)" : Format.cue(milliseconds: segment.startMs))
                .font(.callout)
                .monospacedDigit()
                .foregroundStyle(.secondary)
                .frame(width: 44, alignment: showsIndex ? .trailing : .leading)
            ArtworkView(url: segment.track?.artworkUrl, size: 40, cornerRadius: 6, hatched: segment.track == nil)
            VStack(alignment: .leading, spacing: 2) {
                if let track = segment.track {
                    Text(track.title).font(.body).lineLimit(1)
                    Text(track.artist).font(.callout).foregroundStyle(.secondary).lineLimit(1)
                } else {
                    Text(Copy.unknownTrack).font(.body).foregroundStyle(.secondary)
                }
            }
            Spacer(minLength: 12)
            trailing
        }
        .frame(minHeight: 44)
        .contentShape(Rectangle())
        .contextMenu { contextMenu }
        .accessibilityElement(children: .contain)
        .accessibilityLabel(Copy.segmentAccessibility(segment, showsIndex: showsIndex))
    }

    @ViewBuilder
    private var trailing: some View {
        if segment.track == nil {
            Button {
                app.humSegment(segment, in: model)
            } label: {
                // Without it, list rows before macOS 26 tint the mark with the accent color.
                Label(Copy.humIt, image: "contour.mark")
                    .foregroundStyle(.primary)
            }
            .buttonStyle(.bordered)
        } else if segment.tier == .possible, let track = segment.track {
            HStack(spacing: 8) {
                PossibleTag()
                Button {
                    app.correct(segmentID: segment.id, in: model.id, with: track)
                } label: {
                    Image(systemName: "checkmark").frame(width: 14, height: 14)
                }
                .buttonStyle(.bordered)
                .buttonBorderShape(.circle)
                .help(Copy.confirm)
                .accessibilityLabel(Copy.confirm)
                NotThisMenu(model: model, segment: segment, iconOnly: true)
                    .buttonStyle(.bordered)
                    .buttonBorderShape(.circle)
            }
        }
    }

    @ViewBuilder
    private var contextMenu: some View {
        if let track = segment.track {
            OpenInMenu(track: track)
            if segment.tier == .possible {
                Button(Copy.confirm) { app.correct(segmentID: segment.id, in: model.id, with: track) }
            }
            Button(Copy.notThisMenu) { app.reject(segmentID: segment.id, in: model.id) }
            ForEach(segment.alternates, id: \.identityKey) { alternate in
                Button(Copy.useAlternate(alternate)) { app.correct(segmentID: segment.id, in: model.id, with: alternate) }
            }
        }
        Button(Copy.humPart) { app.humSegment(segment, in: model) }
        if let track = segment.track {
            Button(Copy.copy) { app.copy(track) }
        }
        if let url = segment.sourceUrl, let site = model.source?.site ?? Format.host(model.source?.url) {
            Divider()
            Button(Copy.playOnSite(site, cue: Format.cue(milliseconds: segment.startMs))) { LinkOpener.openSource(url) }
        }
    }
}
