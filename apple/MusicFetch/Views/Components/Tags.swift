import MusicFetchCore
import SwiftUI

/// Match capsule for hum candidates (§4.1, §5.8): Strong filled accent-soft, Possible
/// filled quaternary, Long shot outlined.
struct CandidateTag: View {
    var tier: CandidateTier
    @Environment(\.colorSchemeContrast) private var contrast

    var body: some View {
        let shape = Capsule()
        Text(Copy.tier(tier))
            .font(.subheadline.weight(.medium))
            .lineLimit(1)
            .padding(.horizontal, 8)
            .frame(minHeight: 18)
            .foregroundStyle(foreground)
            .background {
                switch tier {
                case .strong: shape.fill(Color.accentColor.opacity(0.16))
                case .possible: shape.fill(.fill.quaternary)
                case .longShot: shape.strokeBorder(.secondary, lineWidth: 1)
                }
            }
            .overlay {
                if contrast == .increased, tier != .longShot {
                    shape.strokeBorder(.primary, lineWidth: 1)
                }
            }
            .fixedSize()
    }

    private var foreground: AnyShapeStyle {
        switch tier {
        case .strong: AnyShapeStyle(Color.accentColor)
        case .possible: AnyShapeStyle(.primary)
        case .longShot: AnyShapeStyle(.secondary)
        }
    }
}

/// The outlined "Possible" tag on uncertain mix rows.
struct PossibleTag: View {
    var body: some View {
        Text(Copy.tier(Tier.possible))
            .font(.subheadline.weight(.medium))
            .lineLimit(1)
            .padding(.horizontal, 8)
            .frame(minHeight: 18)
            .foregroundStyle(.secondary)
            .background(Capsule().strokeBorder(.secondary, lineWidth: 1))
            .fixedSize()
    }
}

/// A small icon-only glass (bordered before macOS 26) button with a 28 pt hit target.
struct IconButton: View {
    var symbol: String
    var label: String
    var prominent = false
    var action: () -> Void

    var body: some View {
        Button(action: action) {
            Image(systemName: symbol)
                .frame(width: 16, height: 16)
        }
        .glassButtonStyle()
        .buttonBorderShape(.circle)
        .controlSize(.large)
        .help(label)
        .accessibilityLabel(label)
    }
}
