import SwiftUI

/// The contour mark (DESIGN.md §2.1): one round-capped stroke in a 48 × 20 unit box.
struct ContourMark: Shape {
    nonisolated static let box = CGSize(width: 48, height: 20)
    /// Stroke width in units of the 48-unit box.
    static let strokeUnits: CGFloat = 3

    func path(in rect: CGRect) -> Path {
        var path = Path()
        path.move(to: CGPoint(x: 3, y: 11))
        path.addCurve(to: CGPoint(x: 12, y: 5), control1: CGPoint(x: 7, y: 11), control2: CGPoint(x: 8, y: 5))
        path.addCurve(to: CGPoint(x: 21, y: 15), control1: CGPoint(x: 16, y: 5), control2: CGPoint(x: 17, y: 15))
        path.addCurve(to: CGPoint(x: 31, y: 3), control1: CGPoint(x: 26, y: 15), control2: CGPoint(x: 26, y: 3))
        path.addCurve(to: CGPoint(x: 40, y: 13), control1: CGPoint(x: 36, y: 3), control2: CGPoint(x: 36, y: 13))
        path.addCurve(to: CGPoint(x: 45, y: 9), control1: CGPoint(x: 43, y: 13), control2: CGPoint(x: 44, y: 9))
        let scale = min(rect.width / Self.box.width, rect.height / Self.box.height)
        let offset = CGPoint(
            x: rect.minX + (rect.width - Self.box.width * scale) / 2,
            y: rect.minY + (rect.height - Self.box.height * scale) / 2)
        return path.applying(CGAffineTransform(translationX: offset.x, y: offset.y).scaledBy(x: scale, y: scale))
    }
}

/// The mark drawn as a stroke whose width scales with the frame.
struct ContourMarkGlyph: View {
    var size: CGSize
    /// Stroke width in points at `size` (DESIGN.md §2.1 table).
    var lineWidth: CGFloat

    var body: some View {
        ContourMark()
            .stroke(style: StrokeStyle(lineWidth: lineWidth, lineCap: .round, lineJoin: .round))
            .frame(width: size.width, height: size.height)
    }
}

/// The live line (§4.4): the mark's stroke as a sum of three sines whose amplitude is
/// the input level.
enum LiveLine {
    static let amplitudes: [Double] = [0.6, 0.3, 0.15]
    static let frequencies: [Double] = [1.0, 2.3, 3.7]
    static let samples = 65

    /// The path in `rect` at time `t` (seconds) for `level` 0…1.
    static func path(in rect: CGRect, level: Double, time t: Double) -> Path {
        var path = Path()
        let half = rect.height / 2
        for index in 0..<samples {
            let u = Double(index) / Double(samples - 1)
            var sum = 0.0
            for k in 0..<3 {
                let phase = frequencies[k] * t
                sum += amplitudes[k] * sin(2 * .pi * frequencies[k] * (2 * u) + phase)
            }
            let envelope = sin(.pi * u)
            let y = rect.midY + envelope * level * half * sum
            let point = CGPoint(x: rect.minX + u * rect.width, y: y)
            if index == 0 { path.move(to: point) } else { path.addLine(to: point) }
        }
        return path
    }
}
