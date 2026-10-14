import MusicFetchCore
import SwiftUI

/// The live pitch trace band (§5.6): the last 8 s, newest at the right edge, ±12
/// semitones around the running median, gaps where unvoiced.
struct PitchTraceView: View {
    var listen: ListenModel

    /// Points per semitone.
    private let semitone: CGFloat = 4

    var body: some View {
        TimelineView(.animation(minimumInterval: nil, paused: listen.stage != .listening)) { context in
            Canvas { canvas, size in
                draw(in: &canvas, size: size, now: context.date)
            }
        }
        .accessibilityElement()
        .accessibilityLabel(Copy.yourPitch)
        .accessibilityValue(Copy.secondsHummed(listen.voicedSeconds))
    }

    private func draw(in context: inout GraphicsContext, size: CGSize, now: Date) {
        let signal = listen.signal
        guard let median = signal.median, !signal.frames.isEmpty else { return }
        let head = signal.headTime(at: now)
        let window = LiveSignal.window * 1000
        func point(_ frame: PitchFrame) -> CGPoint {
            let x = size.width - CGFloat((head - Double(frame.tMs)) / window) * size.width
            let semitones = CGFloat(min(max(frame.midi - median, -12), 12))
            return CGPoint(x: x, y: size.height / 2 - semitones * semitone)
        }
        let style = StrokeStyle(lineWidth: 4, lineCap: .round, lineJoin: .round)
        var previous: PitchFrame?
        for frame in signal.frames {
            defer { previous = frame }
            guard frame.midi.isFinite, let last = previous, last.midi.isFinite, frame.tMs > last.tMs, frame.tMs - last.tMs <= 120
            else { continue }
            var segment = Path()
            segment.move(to: point(last))
            segment.addLine(to: point(frame))
            context.opacity = 0.6 + 0.4 * Double(min(max(frame.confidence, 0), 1))
            context.stroke(segment, with: .color(.accentColor), style: style)
        }
        if let newest = signal.frames.last, newest.midi.isFinite {
            context.opacity = 1
            let center = point(newest)
            context.fill(Path(ellipseIn: CGRect(x: center.x - 4, y: center.y - 4, width: 8, height: 8)), with: .color(.accentColor))
        }
    }
}

/// A static contour (semitones or MIDI, NaN = gap) scaled to ±12 semitones around a
/// median.
enum ContourPath {
    static func path(_ values: [Float], in rect: CGRect, median: Float, frames: Int? = nil) -> Path {
        var path = Path()
        let count = max(frames ?? values.count, 2)
        let semitone = rect.height / 24
        var drawing = false
        for (index, value) in values.enumerated() {
            guard value.isFinite else {
                drawing = false
                continue
            }
            let x = rect.minX + CGFloat(index) / CGFloat(count - 1) * rect.width
            let y = rect.midY - CGFloat(min(max(value - median, -12), 12)) * semitone
            if drawing {
                path.addLine(to: CGPoint(x: x, y: y))
            } else {
                path.move(to: CGPoint(x: x, y: y))
                drawing = true
            }
        }
        return path
    }

    static func median(_ values: [Float]) -> Float? {
        LiveSignal.median(values.filter(\.isFinite))
    }
}

/// The frozen hum at the top of the candidates view.
struct HumStrip: View {
    var contour: [Float]

    var body: some View {
        Canvas { context, size in
            guard let median = ContourPath.median(contour) else { return }
            let rect = CGRect(origin: .zero, size: size).insetBy(dx: 2, dy: 2)
            context.stroke(
                ContourPath.path(contour, in: rect, median: median), with: .color(.accentColor),
                style: StrokeStyle(lineWidth: 2.5, lineCap: .round, lineJoin: .round))
        }
        .accessibilityHidden(true)
    }
}

/// "Your hum vs. the song" (§5.7): reference under the hum, shared scale.
struct ContourOverlay: View {
    var alignment: MelodyAlignment
    var title: String

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Canvas { context, size in
                guard let median = ContourPath.median(alignment.reference) ?? ContourPath.median(alignment.query) else { return }
                let rect = CGRect(origin: .zero, size: size).insetBy(dx: 2, dy: 2)
                let frames = max(alignment.reference.count, alignment.query.count)
                context.stroke(
                    ContourPath.path(alignment.reference, in: rect, median: median, frames: frames),
                    with: .style(.tertiary), style: StrokeStyle(lineWidth: 4, lineCap: .round, lineJoin: .round))
                context.stroke(
                    ContourPath.path(alignment.query, in: rect, median: median, frames: frames),
                    with: .color(.accentColor), style: StrokeStyle(lineWidth: 2.5, lineCap: .round, lineJoin: .round))
            }
            .frame(height: 120)
            HStack(spacing: 16) {
                legend(Copy.legendYou, style: AnyShapeStyle(Color.accentColor))
                legend(title, style: AnyShapeStyle(.tertiary))
            }
            .font(.callout)
            .foregroundStyle(.secondary)
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(Copy.overlayAccessibility(title: title, alignment: alignment))
    }

    private func legend(_ text: String, style: AnyShapeStyle) -> some View {
        HStack(spacing: 6) {
            Capsule().fill(style).frame(width: 14, height: 3)
            Text(text).lineLimit(1)
        }
    }
}
