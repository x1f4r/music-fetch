import MusicFetchCore
import SwiftUI

/// "Open in ‹service› ▾" (§5.11): the primary part opens the default service, the
/// menu lists every available link, then "Search the web".
struct OpenInButton: View {
    enum Style {
        /// Prominent and large: the result hero and the inspector.
        case prominent
        /// Bordered: recent rows, menu bar extra.
        case bordered
    }

    var track: Track
    var style: Style = .prominent
    var fillsWidth = false
    var glassNamespace: Namespace.ID?

    @AppStorage(PreferenceKey.defaultService) private var defaultServiceKey = ServiceKey.spotify.rawValue

    private var defaultService: Service {
        (ServiceKey(rawValue: defaultServiceKey) ?? .spotify).service
    }

    private var primary: ServiceLink? {
        LinkOpener.preferredLink(in: track.links, defaultService: defaultService)
    }

    private var title: String {
        primary.map { Copy.openIn($0.service) } ?? Copy.searchWeb
    }

    var body: some View {
        switch style {
        case .prominent:
            // A split button on accent-tinted glass (the same material as the listen
            // button, so the two can morph). Menus ignore button styles on macOS, so the
            // two halves are drawn on one glass capsule.
            HStack(spacing: 0) {
                Button(action: openPrimary) {
                    Text(title)
                        .lineLimit(1)
                        .padding(.leading, 16)
                        .padding(.trailing, 12)
                        .frame(maxWidth: fillsWidth ? .infinity : nil, minHeight: 32)
                        .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityLabel(title)
                Rectangle()
                    .fill(.white.opacity(0.35))
                    .frame(width: 1, height: 16)
                    .accessibilityHidden(true)
                Menu {
                    menuItems
                } label: {
                    Image(systemName: "chevron.down")
                        .font(.caption.weight(.semibold))
                        .frame(width: 34, height: 32)
                        .contentShape(Rectangle())
                }
                .menuStyle(.button)
                .buttonStyle(.plain)
                .menuIndicator(.hidden)
                .fixedSize()
                .accessibilityLabel(Copy.openInMenu)
            }
            .foregroundStyle(.white)
            .glassEffect(.regular.tint(.accentColor).interactive(), in: .capsule)
            .modifier(GlassID(namespace: glassNamespace))
            .fixedSize(horizontal: !fillsWidth, vertical: true)
        case .bordered:
            Button(title, action: openPrimary)
                .buttonStyle(.bordered)
                .lineLimit(1)
                .fixedSize()
        }
    }

    @ViewBuilder
    private var menuItems: some View {
        ForEach(LinkOpener.menuLinks(track.links), id: \.url) { link in
            Button(Copy.menuTitle(for: link)) { LinkOpener.open(link) }
        }
        if !LinkOpener.menuLinks(track.links).isEmpty { Divider() }
        Button(Copy.searchWeb) {
            if let url = LinkOpener.webSearchURL(for: track) { NSWorkspace.shared.open(url) }
        }
    }

    private func openPrimary() {
        LinkOpener.openPreferred(track, defaultService: defaultService)
    }
}

/// The "Open in ‹service›" submenu for context menus.
struct OpenInMenu: View {
    var track: Track

    var body: some View {
        Menu(Copy.openInMenu) {
            ForEach(LinkOpener.menuLinks(track.links), id: \.url) { link in
                Button(Copy.menuTitle(for: link)) { LinkOpener.open(link) }
            }
            Divider()
            Button(Copy.searchWeb) {
                if let url = LinkOpener.webSearchURL(for: track) { NSWorkspace.shared.open(url) }
            }
        }
    }
}
