import SwiftUI

/// The listen button (§5.1) with its halo and ring, at 112 pt on the stage and at
/// 64 % in the menu bar extra.
struct ListenButton: View {
    @Environment(AppModel.self) private var app
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Environment(\.accessibilityReduceTransparency) private var reduceTransparency

    var listen: ListenModel
    var diameter: CGFloat = 112
    var glassNamespace: Namespace.ID?

    private var scale: CGFloat { diameter / 112 }

    private var isAnimating: Bool {
        switch listen.stage {
        case .listening, .matching: true
        case .link: listen.linkSession?.phase != .downloading
        default: false
        }
    }

    var body: some View {
        TimelineView(.animation(minimumInterval: nil, paused: !isAnimating)) { context in
            let time = context.date.timeIntervalSinceReferenceDate
            let level = listen.stage == .listening ? listen.signal.level : 0
            ZStack {
                if listen.stage == .listening, !reduceTransparency {
                    Halo(level: level, reduceMotion: reduceMotion)
                        .frame(width: 176 * scale, height: 176 * scale)
                        .allowsHitTesting(false)
                }
                ring(time: time)
                button(level: level, time: time)
            }
            .frame(width: 176 * scale, height: 176 * scale)
        }
    }

    // MARK: Button

    private func button(level: Double, time: Double) -> some View {
        let targeted = listen.isDropTargeted
        let tint = Color.accentColor.opacity(targeted ? 1 : 0.85 + 0.15 * level)
        return Button(action: listen.toggle) {
            glyph(level: level, time: time)
                .frame(width: diameter, height: diameter)
                .contentShape(Circle())
        }
        .buttonStyle(PressScaleStyle())
        .glassEffect(.regular.tint(tint).interactive(), in: .circle)
        .modifier(GlassID(namespace: glassNamespace))
        .scaleEffect(targeted ? 1.04 : 1)
        .animation(app.motion(.press), value: targeted)
        .disabled(listen.stage == .matching || app.engine == nil)
        .accessibilityLabel(Copy.listenAction)
        .accessibilityValue(listen.buttonValue)
        .accessibilityHint(Copy.listenHint)
    }

    @ViewBuilder
    private func glyph(level: Double, time: Double) -> some View {
        switch listen.stage {
        case .listening:
            let amplitude = listen.impression ? 0.5 : 1
            liveLine(level: level * amplitude, time: time)
                .transition(.opacity)
        case .matching:
            liveLine(level: listen.signal.level * (listen.impression ? 0.5 : 1), time: listen.signal.frozenTime ?? time)
                .opacity(0.7)
                .transition(.opacity)
        case .link:
            Image(systemName: "link")
                .font(.system(size: 34 * scale, weight: .semibold))
                .foregroundStyle(.white)
                .transition(.opacity)
        default:
            ContourMarkGlyph(size: CGSize(width: 56 * scale, height: 24 * scale), lineWidth: 3.5 * scale)
                .foregroundStyle(.white)
                .transition(.opacity)
        }
    }

    private func liveLine(level: Double, time: Double) -> some View {
        Canvas { context, size in
            let rect = CGRect(origin: .zero, size: size).insetBy(dx: 2 * scale, dy: 2 * scale)
            let path = LiveLine.path(in: rect, level: level, time: time)
            context.stroke(path, with: .color(.white), style: StrokeStyle(lineWidth: 3 * scale, lineCap: .round, lineJoin: .round))
        }
        .frame(width: 64 * scale, height: 40 * scale)
    }

    // MARK: Ring

    @ViewBuilder
    private func ring(time: Double) -> some View {
        let radius = 62 * scale
        switch listen.stage {
        case .listening where listen.impression:
            ProgressRing(progress: listen.ringProgress, lineWidth: 1.5)
                .frame(width: radius * 2, height: radius * 2)
                .animation(.linear(duration: 0.1), value: listen.ringProgress)
                .transition(.opacity.animation(.easeOut(duration: 0.15)))
        case .link:
            switch listen.linkSession?.phase {
            case .downloading:
                ProgressRing(progress: listen.linkSession?.fraction ?? 0, lineWidth: 1.5)
                    .frame(width: radius * 2, height: radius * 2)
                    .animation(.linear(duration: 0.2), value: listen.linkSession?.fraction)
            case .resolving, nil:
                EmptyView()
            default:
                SpinningArc(time: reduceMotion ? 0 : time, lineWidth: 1.5)
                    .frame(width: radius * 2, height: radius * 2)
            }
        default:
            EmptyView()
        }
    }
}

/// Scales the label to 0.96 while pressed (fast spring).
struct PressScaleStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.96 : 1)
            .animation(AppModel.spring(.press), value: configuration.isPressed)
    }
}

/// `glassEffectID("primary")` when a namespace is available.
struct GlassID: ViewModifier {
    var namespace: Namespace.ID?

    func body(content: Content) -> some View {
        if let namespace {
            content.glassEffectID("primary", in: namespace)
        } else {
            content
        }
    }
}

/// A breathing radial glow behind the button.
struct Halo: View {
    var level: Double
    var reduceMotion: Bool

    var body: some View {
        Canvas { context, size in
            let scale = reduceMotion ? 1 : 1 + 0.12 * level
            let diameter = min(size.width, size.height) * scale
            let rect = CGRect(x: (size.width - diameter) / 2, y: (size.height - diameter) / 2, width: diameter, height: diameter)
            context.opacity = 0.15 + 0.45 * level
            context.addFilter(.blur(radius: 14))
            context.fill(
                Path(ellipseIn: rect),
                with: .radialGradient(
                    Gradient(colors: [.accentColor, .accentColor.opacity(0)]), center: CGPoint(x: size.width / 2, y: size.height / 2),
                    startRadius: 0, endRadius: diameter / 2))
        }
    }
}

/// A thin ring trimmed clockwise from 12 o'clock.
struct ProgressRing: View {
    var progress: Double
    var lineWidth: CGFloat

    var body: some View {
        ZStack {
            Circle().stroke(.quaternary, lineWidth: lineWidth)
            Circle()
                .trim(from: 0, to: max(min(progress, 1), 0))
                .stroke(Color.accentColor, style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
                .rotationEffect(.degrees(-90))
        }
    }
}

/// Indeterminate ring: a rotating 90° arc.
struct SpinningArc: View {
    var time: Double
    var lineWidth: CGFloat

    var body: some View {
        ZStack {
            Circle().stroke(.quaternary, lineWidth: lineWidth)
            Circle()
                .trim(from: 0, to: 0.25)
                .stroke(Color.accentColor, style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
                .rotationEffect(.degrees(time.truncatingRemainder(dividingBy: 1.2) / 1.2 * 360 - 90))
        }
    }
}
