import MusicFetchCore
import SwiftUI

/// A provider's monogram (or a system symbol) on a color that hints at the service. Not
/// the service's logo.
struct ProviderBadge: View {
    enum Kind: Hashable {
        case service(Service)
        case custom
        case more
    }

    var kind: Kind
    var size: CGFloat = 28

    var body: some View {
        let style = Self.style(kind)
        Circle()
            .fill(style.background.gradient)
            .overlay {
                Group {
                    if let symbol = style.symbol {
                        Image(systemName: symbol)
                            .font(.system(size: size * 0.42, weight: .semibold))
                    } else {
                        Text(verbatim: style.letters)
                            .font(.system(size: size * (style.letters.count > 1 ? 0.36 : 0.48), weight: .bold, design: .rounded))
                    }
                }
                .foregroundStyle(style.foreground)
            }
            .frame(width: size, height: size)
            .accessibilityHidden(true)
    }

    struct Style {
        var letters = ""
        var symbol: String?
        var background: Color
        var foreground: Color = .white
    }

    static func style(_ kind: Kind) -> Style {
        switch kind {
        case .custom: return Style(symbol: "link", background: .gray)
        case .more: return Style(symbol: "ellipsis", background: .gray)
        case .service(let service): return style(service)
        }
    }

    static func style(_ service: Service) -> Style {
        switch service {
        case .spotify: Style(letters: "S", background: .green)
        case .appleMusic: Style(symbol: "music.note", background: .pink)
        case .youTubeMusic: Style(symbol: "play.fill", background: .red)
        case .youTube: Style(symbol: "play.rectangle.fill", background: .red)
        case .deezer: Style(letters: "D", background: .purple)
        case .tidal: Style(letters: "T", background: .primary, foreground: Color(nsColor: .windowBackgroundColor))
        case .amazonMusic: Style(letters: "AM", background: .cyan)
        case .soundCloud: Style(symbol: "cloud.fill", background: .orange)
        case .qobuz: Style(letters: "Q", background: .blue)
        case .pandora: Style(letters: "P", background: .indigo)
        case .audiomack: Style(letters: "A", background: .orange)
        case .anghami: Style(letters: "A", background: .purple)
        case .jioSaavn: Style(letters: "J", background: .teal)
        case .boomplay: Style(letters: "B", background: .blue)
        case .kkbox: Style(letters: "K", background: .cyan)
        case .yandexMusic: Style(letters: "Y", background: .yellow, foreground: .black)
        case .netEaseMusic: Style(letters: "N", background: .red)
        case .bandcamp: Style(letters: "B", background: .teal)
        case .shazam: Style(symbol: "shazam.logo.fill", background: .blue)
        case .lastFm: Style(letters: "L", background: .red)
        case .genius: Style(letters: "G", background: .yellow, foreground: .black)
        case .mixcloud: Style(letters: "M", background: .indigo)
        case .vimeo: Style(letters: "V", background: .cyan)
        case .tikTok: Style(letters: "T", background: .pink)
        case .instagram: Style(letters: "I", background: .pink)
        case .facebook: Style(letters: "F", background: .blue)
        case .x: Style(letters: "X", background: .primary, foreground: Color(nsColor: .windowBackgroundColor))
        case .reddit: Style(letters: "R", background: .orange)
        case .twitch: Style(letters: "T", background: .purple)
        case .web: Style(symbol: "globe", background: .gray)
        }
    }
}
