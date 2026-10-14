import Accessibility
import MusicFetchCore
import SwiftUI

/// The timeline strip (§5.12): a 16 pt ruler over a 28 pt strip with one bar per
/// segment, drawn by tier with fill style (never color alone), an analysis veil and
/// zoom (⌘+ / ⌘− / pinch).
struct TimelineStrip: View {
    @Environment(\.colorSchemeContrast) private var contrast
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Bindable var model: SessionModel

    @State private var position = ScrollPosition(edge: .leading)
    @State private var pinchBase: Double?

    static let zoomRange: ClosedRange<Double> = 1...40
    static let zoomStep = 1.6

    private var duration: UInt64 {
        max(model.durationMs ?? 0, model.segments.map(\.endMs).max() ?? 0, 1)
    }

    var body: some View {
        GeometryReader { geometry in
            let width = geometry.size.width * model.zoom
            ScrollView(.horizontal) {
                VStack(spacing: 0) {
                    ruler(width: width)
                        .frame(width: width, height: 16)
                    strip(width: width)
                        .frame(width: width, height: 28)
                }
            }
            .scrollPosition($position)
            .scrollIndicators(model.zoom > 1 ? .automatic : .hidden)
            .scrollDisabled(model.zoom <= 1)
            .onChange(of: model.selectedSegmentID) { _, _ in reveal(width: width, visible: geometry.size.width) }
            .onChange(of: model.zoom) { _, _ in reveal(width: width, visible: geometry.size.width) }
            .gesture(
                MagnifyGesture()
                    .onChanged { value in
                        let base = pinchBase ?? model.zoom
                        pinchBase = base
                        model.zoom = min(max(base * value.magnification, Self.zoomRange.lowerBound), Self.zoomRange.upperBound)
                    }
                    .onEnded { _ in pinchBase = nil }
            )
        }
        .frame(height: 44)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(Copy.timeline)
        .accessibilityChartDescriptor(StripChart(segments: model.listedSegments, duration: duration))
    }

    /// Keeps the selected segment in view when zoomed.
    private func reveal(width: CGFloat, visible: CGFloat) {
        guard model.zoom > 1, let segment = model.selectedSegment else { return }
        let x = CGFloat(Double(segment.startMs) / Double(duration)) * width
        let target = max(min(x - visible / 3, width - visible), 0)
        withAnimation(reduceMotion ? .easeInOut(duration: 0.15) : AppModel.spring(.large)) {
            position.scrollTo(x: target)
        }
    }

    // MARK: Ruler

    private func ruler(width: CGFloat) -> some View {
        Canvas { context, size in
            for step in 0...4 {
                let fraction = Double(step) / 4
                let label = Text(Format.cue(milliseconds: UInt64(Double(duration) * fraction)))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
                let resolved = context.resolve(label)
                let textSize = resolved.measure(in: size)
                var x = size.width * fraction
                if step == 0 { x = 0 } else if step == 4 { x = size.width - textSize.width } else { x -= textSize.width / 2 }
                context.draw(resolved, at: CGPoint(x: x, y: 0), anchor: .topLeading)
            }
        }
        .accessibilityHidden(true)
    }

    // MARK: Strip

    private func strip(width: CGFloat) -> some View {
        Canvas { context, size in
            draw(in: &context, size: size)
        }
        .contentShape(Rectangle())
        .gesture(
            SpatialTapGesture()
                .onEnded { value in
                    select(at: value.location.x, width: width)
                }
        )
    }

    private func select(at x: CGFloat, width: CGFloat) {
        let time = UInt64(max(Double(x / width), 0) * Double(duration))
        let hit = model.listedSegments.first { $0.startMs <= time && time < max($0.endMs, $0.startMs + 1) }
        if let hit { model.selectedSegmentID = hit.id }
    }

    private func x(_ ms: UInt64, _ size: CGSize) -> CGFloat {
        CGFloat(Double(ms) / Double(duration)) * size.width
    }

    private func draw(in context: inout GraphicsContext, size: CGSize) {
        let track = Path(roundedRect: CGRect(origin: .zero, size: size), cornerRadius: 4, style: .continuous)
        context.fill(track, with: .style(.fill.quaternary))

        let segments = model.segments
        let likelyOpacity = contrast == .increased ? 0.75 : 0.6
        for (index, segment) in segments.enumerated() {
            let start = x(segment.startMs, size)
            let end = max(x(segment.endMs, size), start + 3)
            // Overlaps with the neighbors are drawn as stacked half-height bars.
            let previousEnd = index > 0 ? segments[index - 1].endMs : 0
            let nextStart = index + 1 < segments.count ? segments[index + 1].startMs : UInt64.max
            var rects: [CGRect] = []
            if segment.kind == .speech {
                rects = [CGRect(x: start, y: (size.height - 6) / 2, width: end - start, height: 6)]
            } else {
                let overlapLeft = previousEnd > segment.startMs ? min(x(previousEnd, size), end) : start
                let overlapRight = nextStart < segment.endMs ? max(x(nextStart, size), start) : end
                if overlapLeft > start {
                    rects.append(CGRect(x: start, y: size.height / 2, width: overlapLeft - start, height: size.height / 2))
                }
                if overlapRight > overlapLeft {
                    rects.append(CGRect(x: overlapLeft, y: 0, width: overlapRight - overlapLeft, height: size.height))
                }
                if end > overlapRight {
                    rects.append(CGRect(x: overlapRight, y: 0, width: end - overlapRight, height: size.height / 2))
                }
            }
            for rect in rects {
                let bar = Path(roundedRect: rect.insetBy(dx: 0.5, dy: 0), cornerRadius: 3, style: .continuous)
                switch (segment.kind, segment.tier) {
                case (.silence, _):
                    continue
                case (.speech, _):
                    context.fill(bar, with: .style(.tertiary))
                case (_, .confirmed):
                    context.fill(bar, with: .color(.accentColor))
                case (_, .likely):
                    context.fill(bar, with: .color(.accentColor.opacity(likelyOpacity)))
                case (_, .possible):
                    context.stroke(
                        Path(roundedRect: rect.insetBy(dx: 1.25, dy: 0.75), cornerRadius: 3, style: .continuous),
                        with: .color(.accentColor), lineWidth: 1.5)
                case (_, .unknown):
                    var hatch = context
                    hatch.clip(to: bar)
                    var lines = Path()
                    var lineX = rect.minX - rect.height
                    while lineX < rect.maxX {
                        lines.move(to: CGPoint(x: lineX, y: rect.maxY))
                        lines.addLine(to: CGPoint(x: lineX + rect.height, y: rect.minY))
                        lineX += 6
                    }
                    hatch.stroke(lines, with: .color(.primary.opacity(0.22)), lineWidth: 1.5)
                }
            }
        }

        // Veil over what has not been analyzed yet, with the frontier line.
        if model.isRunning, let progress = model.progress, progress.totalMs > 0 {
            let frontier = x(progress.analyzedMs, size)
            if frontier < size.width {
                context.fill(
                    Path(CGRect(x: frontier, y: 0, width: size.width - frontier, height: size.height)),
                    with: .color(Color(nsColor: .windowBackgroundColor).opacity(0.75)))
                context.fill(Path(CGRect(x: frontier - 0.75, y: -2, width: 1.5, height: size.height + 4)), with: .style(.primary))
            }
        }

        // Selection: label outline with an inner window-colored gap.
        if let selected = model.selectedSegment {
            let start = x(selected.startMs, size)
            let end = max(x(selected.endMs, size), start + 3)
            let rect = CGRect(x: start, y: 0, width: end - start, height: size.height)
            context.stroke(
                Path(roundedRect: rect.insetBy(dx: 0.75, dy: 0.75), cornerRadius: 4, style: .continuous),
                with: .color(Color(nsColor: .windowBackgroundColor)), lineWidth: 4.5)
            context.stroke(
                Path(roundedRect: rect.insetBy(dx: -0.75, dy: -0.75), cornerRadius: 5, style: .continuous),
                with: .style(.primary), lineWidth: 1.5)
        }
    }
}

/// VoiceOver chart: one series "Tracks" with cue and tier per segment.
struct StripChart: AXChartDescriptorRepresentable {
    var segments: [Segment]
    var duration: UInt64

    func makeChartDescriptor() -> AXChartDescriptor {
        let xAxis = AXNumericDataAxisDescriptor(
            title: Copy.timeAxis, range: 0...Double(max(duration, 1)) / 1000, gridlinePositions: []
        ) { value in Format.cue(milliseconds: UInt64(max(value, 0) * 1000)) }
        let yAxis = AXNumericDataAxisDescriptor(title: Copy.match, range: 0...1, gridlinePositions: []) { value in
            "\(Int((value * 100).rounded())) %"
        }
        let points = segments.map { segment in
            let title = segment.track.map { "\($0.title), \($0.artist)" } ?? Copy.unknownTrack
            return AXDataPoint(
                x: Double(segment.startMs) / 1000, y: Double(segment.confidence),
                label: "\(title), \(Copy.tier(segment.tier))")
        }
        let series = AXDataSeriesDescriptor(name: Copy.tracksSeries, isContinuous: false, dataPoints: points)
        return AXChartDescriptor(
            title: Copy.timeline, summary: nil, xAxis: xAxis, yAxis: yAxis, additionalAxes: [], series: [series])
    }
}
