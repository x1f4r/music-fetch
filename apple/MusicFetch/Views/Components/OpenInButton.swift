import MusicFetchCore
import SwiftUI

/// "Open in ‹provider› ▾" (§5.11, docs/APPS.md "Opening links"): the big part opens the
/// user's provider; the chevron opens the track once in any other provider without
/// changing the setting.
struct OpenInButton: View {
    enum Style {
        /// Prominent and large: the result hero and the inspector.
        case prominent
        /// Bordered: recent rows, menu bar extra.
        case bordered
    }

    @Environment(AppModel.self) private var app

    var track: Track
    var style: Style = .prominent
    var fillsWidth = false
    var glassNamespace: Namespace.ID?

    var body: some View {
        let preference = app.providerPreference
        let links = TrackLinks(track: track, preference: preference)
        let title = links.primary.map { Copy.openIn(Copy.name(of: $0.provider, custom: preference.custom)) } ?? Copy.searchWeb
        switch style {
        case .prominent:
            // A split button on accent-tinted glass (the same material as the listen
            // button, so the two can morph). Menus ignore button styles on macOS, so the
            // two halves are drawn on one capsule.
            HStack(spacing: 0) {
                Button {
                    open(links.primary)
                } label: {
                    Text(title)
                        .lineLimit(1)
                        .padding(.leading, 16)
                        .padding(.trailing, links.others.isEmpty ? 16 : 12)
                        .frame(maxWidth: fillsWidth ? .infinity : nil, minHeight: 32)
                        .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityLabel(title)
                if !links.others.isEmpty {
                    Rectangle()
                        .fill(.white.opacity(0.35))
                        .frame(width: 1, height: 16)
                        .accessibilityHidden(true)
                    Menu {
                        ProviderMenuItems(menu: links.menu, custom: preference.custom)
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
                    .help(Copy.openInMenu)
                    .accessibilityLabel(Copy.openInMenu)
                }
            }
            .foregroundStyle(.white)
            .prominentGlass(tint: .accentColor, in: Capsule(), id: glassNamespace)
            .fixedSize(horizontal: !fillsWidth, vertical: true)
        case .bordered:
            Button(title) { open(links.primary) }
                .buttonStyle(.bordered)
                .lineLimit(1)
                .fixedSize()
        }
    }

    private func open(_ link: ProviderLink?) {
        if let link {
            LinkOpener.open(link)
        } else if let url = LinkOpener.webSearchURL(for: track) {
            NSWorkspace.shared.open(url)
        }
    }
}

/// The provider sections of an "Open in" menu, separated by dividers: exact links,
/// searches (the less common providers in "More…"), the custom provider.
struct ProviderMenuItems: View {
    var menu: ProviderMenu
    var custom: CustomProvider

    var body: some View {
        items(menu.exact)
        if !menu.exact.isEmpty, !menu.searches.isEmpty || !menu.more.isEmpty { Divider() }
        items(menu.searches)
        if !menu.more.isEmpty {
            Menu(Copy.moreProviders) { items(menu.more) }
        }
        if !menu.custom.isEmpty, menu.custom.count < menu.all.count { Divider() }
        items(menu.custom)
    }

    private func items(_ links: [ProviderLink]) -> some View {
        ForEach(links, id: \.self) { link in
            Button(Copy.menuTitle(for: link, custom: custom)) { LinkOpener.open(link) }
        }
    }
}

/// The "Open in" submenu for context menus: the user's provider first, then the rest.
struct OpenInMenu: View {
    @Environment(AppModel.self) private var app
    var track: Track

    var body: some View {
        let preference = app.providerPreference
        let links = TrackLinks(track: track, preference: preference)
        Menu(Copy.openInMenu) {
            if let primary = links.primary {
                Button(Copy.menuTitle(for: primary, custom: preference.custom)) { LinkOpener.open(primary) }
                if !links.others.isEmpty { Divider() }
            }
            ProviderMenuItems(menu: links.menu, custom: preference.custom)
            if links.primary == nil {
                Button(Copy.searchWeb) {
                    if let url = LinkOpener.webSearchURL(for: track) { NSWorkspace.shared.open(url) }
                }
            }
        }
    }
}
