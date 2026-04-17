import AppKit
import SwiftUI

enum Theme {
    // MARK: - Spacing (4-point rhythm)

    enum Space {
        static let xxs: CGFloat = 4
        static let xs: CGFloat = 8
        static let s: CGFloat = 12
        static let m: CGFloat = 16
        static let l: CGFloat = 20
        static let xl: CGFloat = 28
        static let xxl: CGFloat = 40
    }

    // MARK: - Radii

    enum Radius {
        static let pill: CGFloat = 999
        static let row: CGFloat = 8
        static let card: CGFloat = 12
        static let panel: CGFloat = 16
    }

    // MARK: - Typography

    enum Font {
        static var display: SwiftUI.Font {
            .system(size: 22, weight: .semibold, design: .rounded)
        }

        static var largeTitle: SwiftUI.Font {
            .system(size: 28, weight: .semibold, design: .rounded)
        }

        static var title: SwiftUI.Font {
            .system(size: 17, weight: .semibold, design: .rounded)
        }

        static var sectionHeader: SwiftUI.Font {
            .system(size: 11, weight: .semibold).smallCaps()
        }

        static var rowTitle: SwiftUI.Font {
            .system(size: 13, weight: .medium)
        }

        static var rowSubtitle: SwiftUI.Font {
            .system(size: 11, weight: .regular)
        }

        static var body: SwiftUI.Font {
            .system(size: 13, weight: .regular)
        }

        static var caption: SwiftUI.Font {
            .system(size: 11, weight: .regular)
        }

        static var mono: SwiftUI.Font {
            .system(size: 12, weight: .medium, design: .monospaced)
        }
    }

    // MARK: - Colors (semantic)

    enum Palette {
        static var surface: Color { Color(NSColor.windowBackgroundColor) }
        static var surfaceRaised: Color { Color(NSColor.controlBackgroundColor) }
        static var surfaceSunken: Color { Color(NSColor.underPageBackgroundColor) }

        static var hairline: Color { Color.primary.opacity(0.08) }
        static var hairlineStrong: Color { Color.primary.opacity(0.14) }
        static var divider: Color { Color.primary.opacity(0.06) }

        static var accent: Color { Color.accentColor }
        static var accentSoft: Color { Color.accentColor.opacity(0.12) }

        static var micTint: Color { .red }
        static var systemTint: Color { .orange }
        static var successTint: Color { .green }
        static var warningTint: Color { .yellow }
        static var dangerTint: Color { .orange }

        static var textPrimary: Color { .primary }
        static var textSecondary: Color { .secondary }
        static var textTertiary: Color { Color.primary.opacity(0.45) }
    }

    // MARK: - Surface modifiers

    struct PanelBackground: ViewModifier {
        var radius: CGFloat = Radius.card
        var elevated: Bool = true

        func body(content: Content) -> some View {
            content
                .background(
                    RoundedRectangle(cornerRadius: radius, style: .continuous)
                        .fill(elevated ? Palette.surfaceRaised : Palette.surface)
                )
                .overlay(
                    RoundedRectangle(cornerRadius: radius, style: .continuous)
                        .strokeBorder(Palette.hairline, lineWidth: 0.5)
                )
        }
    }

    struct RowHoverable: ViewModifier {
        let selected: Bool
        @State private var hovering = false

        func body(content: Content) -> some View {
            content
                .background(
                    RoundedRectangle(cornerRadius: Radius.row, style: .continuous)
                        .fill(background)
                )
                .overlay(
                    RoundedRectangle(cornerRadius: Radius.row, style: .continuous)
                        .strokeBorder(selected ? Palette.accent.opacity(0.35) : Color.clear, lineWidth: 1)
                )
                .onHover { hovering = $0 }
        }

        private var background: Color {
            if selected { return Palette.accentSoft }
            if hovering { return Color.primary.opacity(0.05) }
            return Color.clear
        }
    }
}

extension View {
    func panelBackground(radius: CGFloat = Theme.Radius.card, elevated: Bool = true) -> some View {
        modifier(Theme.PanelBackground(radius: radius, elevated: elevated))
    }

    func rowHoverable(selected: Bool) -> some View {
        modifier(Theme.RowHoverable(selected: selected))
    }
}

// MARK: - Reusable primitives

struct Panel<Content: View>: View {
    var padding: CGFloat = Theme.Space.m
    var radius: CGFloat = Theme.Radius.card
    var elevated: Bool = true
    @ViewBuilder var content: Content

    var body: some View {
        content
            .padding(padding)
            .frame(maxWidth: .infinity, alignment: .topLeading)
            .panelBackground(radius: radius, elevated: elevated)
    }
}

struct SectionLabel: View {
    let title: String

    init(_ title: String) {
        self.title = title
    }

    var body: some View {
        Text(title)
            .font(Theme.Font.sectionHeader)
            .tracking(0.4)
            .foregroundStyle(Theme.Palette.textSecondary)
    }
}

struct Pill: View {
    let icon: String?
    let text: String
    var tint: Color = Theme.Palette.textSecondary

    init(_ text: String, icon: String? = nil, tint: Color = Theme.Palette.textSecondary) {
        self.text = text
        self.icon = icon
        self.tint = tint
    }

    var body: some View {
        HStack(spacing: 4) {
            if let icon {
                Image(systemName: icon)
                    .font(.system(size: 10, weight: .semibold))
            }
            Text(text)
                .font(.system(size: 11, weight: .medium))
                .monospacedDigit()
        }
        .foregroundStyle(tint)
        .padding(.horizontal, 7)
        .padding(.vertical, 3)
        .background(tint.opacity(0.1), in: Capsule(style: .continuous))
    }
}

struct StatusDot: View {
    let color: Color
    var pulsing: Bool = false

    @State private var phase: CGFloat = 0

    var body: some View {
        ZStack {
            if pulsing {
                Circle()
                    .stroke(color.opacity(0.4), lineWidth: 2)
                    .frame(width: 14, height: 14)
                    .scaleEffect(1 + phase * 0.6)
                    .opacity(Double(1 - phase))
            }
            Circle()
                .fill(color)
                .frame(width: 7, height: 7)
        }
        .onAppear {
            guard pulsing else { return }
            withAnimation(.easeOut(duration: 1.4).repeatForever(autoreverses: false)) {
                phase = 1
            }
        }
    }
}

struct LiveProgressBar: View {
    let progress: Double?
    let tint: Color

    @State private var shimmer: CGFloat = -0.4

    var body: some View {
        GeometryReader { geo in
            ZStack(alignment: .leading) {
                Capsule()
                    .fill(tint.opacity(0.14))
                if let progress {
                    Capsule()
                        .fill(tint)
                        .frame(width: geo.size.width * clamp(progress))
                        .animation(.easeOut(duration: 0.4), value: progress)
                } else {
                    Capsule()
                        .fill(
                            LinearGradient(
                                colors: [tint.opacity(0.0), tint.opacity(0.85), tint.opacity(0.0)],
                                startPoint: .leading,
                                endPoint: .trailing
                            )
                        )
                        .frame(width: geo.size.width * 0.45)
                        .offset(x: geo.size.width * shimmer)
                        .onAppear {
                            withAnimation(.linear(duration: 1.4).repeatForever(autoreverses: false)) {
                                shimmer = 1.1
                            }
                        }
                }
            }
        }
        .frame(height: 4)
        .clipShape(Capsule())
    }

    private func clamp(_ value: Double) -> Double {
        max(0, min(1, value))
    }
}
