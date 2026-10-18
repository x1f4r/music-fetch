import SwiftUI

// Liquid Glass on macOS 26; native materials, fills and bordered button styles before.

extension View {
    /// Accent-tinted interactive glass in `shape` (the listen button, the "Open in"
    /// button), joined to the glass morph through `namespace`. Before macOS 26: an accent
    /// fill with a soft highlight.
    @ViewBuilder
    func prominentGlass<S: InsettableShape>(tint: Color, in shape: S, id namespace: Namespace.ID? = nil) -> some View {
        if #available(macOS 26, *), !Compat.forcesLegacy {
            glassEffect(.regular.tint(tint).interactive(), in: shape)
                .modifier(GlassID(namespace: namespace))
        } else {
            modifier(ProminentFill(tint: tint, shape: shape))
        }
    }

    /// `.buttonStyle(.glass)`, or `.bordered` before macOS 26.
    @ViewBuilder
    func glassButtonStyle() -> some View {
        if #available(macOS 26, *), !Compat.forcesLegacy {
            buttonStyle(.glass)
        } else {
            buttonStyle(.bordered)
        }
    }

    /// Mirrors the view under the sidebar and inspector on macOS 26; nothing before.
    @ViewBuilder
    func extendsBackgroundUnderSidebars() -> some View {
        if #available(macOS 26, *), !Compat.forcesLegacy {
            backgroundExtensionEffect()
        } else {
            self
        }
    }

    /// A bar pinned above scrolling content: `safeAreaBar` with a hard scroll edge on
    /// macOS 26; before, the bar on the toolbar material with a divider.
    @ViewBuilder
    func pinnedTopBar<Bar: View>(@ViewBuilder _ bar: () -> Bar) -> some View {
        if #available(macOS 26, *), !Compat.forcesLegacy {
            safeAreaBar(edge: .top, spacing: 0, content: bar)
                .scrollEdgeEffectStyle(.hard, for: .top)
        } else {
            safeAreaInset(edge: .top, spacing: 0) {
                VStack(spacing: 0) {
                    bar()
                    Divider()
                }
                .background(.bar)
            }
        }
    }
}

/// `GlassEffectContainer` on macOS 26 (neighbouring glass blends and morphs); the plain
/// content before.
struct GlassContainer<Content: View>: View {
    var spacing: CGFloat?
    @ViewBuilder var content: Content

    var body: some View {
        if #available(macOS 26, *), !Compat.forcesLegacy {
            GlassEffectContainer(spacing: spacing) { content }
        } else {
            content
        }
    }
}

/// `glassEffectID("primary")` when a namespace is available (macOS 26 only).
struct GlassID: ViewModifier {
    var namespace: Namespace.ID?

    func body(content: Content) -> some View {
        if #available(macOS 26, *), let namespace {
            content.glassEffectID("primary", in: namespace)
        } else {
            content
        }
    }
}

/// The stand-in for tinted interactive glass before macOS 26: the tint fills the shape,
/// lit from above, with a hairline edge, a soft shadow and a hover highlight.
private struct ProminentFill<S: InsettableShape>: ViewModifier {
    var tint: Color
    var shape: S

    @Environment(\.isEnabled) private var isEnabled
    @Environment(\.colorSchemeContrast) private var contrast
    @State private var hovering = false

    func body(content: Content) -> some View {
        content
            .background {
                shape.fill(tint)
                    .overlay {
                        shape.fill(
                            LinearGradient(
                                colors: [.white.opacity(0.2), .white.opacity(0)], startPoint: .top, endPoint: .center))
                    }
                    .overlay {
                        shape.strokeBorder(contrast == .increased ? Color.primary : Color.black.opacity(0.1), lineWidth: 0.5)
                    }
                    .brightness(hovering && isEnabled ? 0.05 : 0)
                    .shadow(color: .black.opacity(0.16), radius: 2, y: 1)
                    .animation(.easeOut(duration: 0.12), value: hovering)
            }
            .onHover { hovering = $0 }
    }
}
