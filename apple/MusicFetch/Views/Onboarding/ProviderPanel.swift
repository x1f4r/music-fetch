import MusicFetchCore
import SwiftUI

/// "Where do you listen?" (docs/APPS.md, "Opening links"): a sheet at launch until the
/// user chooses a provider or skips. Nothing is stored before that, so quitting with the
/// sheet open asks again next time.
struct ProviderPanel: View {
    @Environment(AppModel.self) private var app

    private enum Tile: Hashable {
        case service(Service)
        case more
    }

    @State private var tile: Tile
    /// The provider picked under "More…".
    @State private var more: Provider = .service(Providers.more[0])
    @State private var customName = ""
    @State private var customTemplate = ""
    @FocusState private var nameFocused: Bool

    init(suggested: Service, startsWithCustom: Bool = false) {
        _tile = State(initialValue: Providers.common.contains(suggested) && !startsWithCustom ? .service(suggested) : .more)
        if startsWithCustom {
            _more = State(initialValue: .custom)
        } else if !Providers.common.contains(suggested) {
            _more = State(initialValue: .service(suggested))
        }
    }

    private static let columns = Array(repeating: GridItem(.fixed(112), spacing: 10), count: 4)

    private var custom: CustomProvider { CustomProvider(name: customName, template: customTemplate) }

    private var choice: Provider {
        switch tile {
        case .service(let service): .service(service)
        case .more: more
        }
    }

    private var canContinue: Bool { choice != .custom || custom.isValid }

    var body: some View {
        VStack(spacing: 0) {
            VStack(spacing: 6) {
                Text(Copy.providerPanelTitle)
                    .font(.title2.weight(.semibold))
                    .accessibilityAddTraits(.isHeader)
                Text(Copy.providerPanelMessage)
                    .font(.callout)
                    .foregroundStyle(.secondary)
            }
            .multilineTextAlignment(.center)

            LazyVGrid(columns: Self.columns, spacing: 10) {
                ForEach(Providers.common, id: \.self) { service in
                    ProviderTile(title: Copy.name(of: service), badge: .service(service), isSelected: tile == .service(service)) {
                        tile = .service(service)
                    }
                }
                ProviderTile(title: Copy.moreProviders, badge: .more, isSelected: tile == .more) {
                    tile = .more
                }
            }
            .padding(.top, 22)

            if tile == .more {
                moreSection
                    .padding(.top, 18)
                    .transition(.opacity)
            }

            HStack(spacing: 12) {
                Spacer()
                Button(Copy.skip) { app.skipProviderChoice() }
                    .keyboardShortcut(.cancelAction)
                Button(Copy.continueAction) {
                    app.chooseProvider(choice, custom: choice == .custom ? custom : nil)
                }
                .keyboardShortcut(.defaultAction)
                .disabled(!canContinue)
            }
            .controlSize(.large)
            .padding(.top, 24)
        }
        .padding(.top, 28)
        .padding(.horizontal, 28)
        .padding(.bottom, 20)
        .frame(width: 534)
        .animation(app.motion(.chrome), value: tile)
        .animation(app.motion(.chrome), value: more)
        .interactiveDismissDisabled()
        .onAppear {
            if tile == .more, more == .custom { nameFocused = true }
        }
    }

    /// The full list and the custom provider's name and web address.
    private var moreSection: some View {
        Grid(alignment: .leadingFirstTextBaseline, horizontalSpacing: 10, verticalSpacing: 10) {
            GridRow {
                Text(Copy.providerService)
                    .gridColumnAlignment(.trailing)
                Picker(Copy.providerService, selection: $more) {
                    ForEach(Providers.more, id: \.self) { service in
                        Text(Copy.name(of: service)).tag(Provider.service(service))
                    }
                    Divider()
                    Text(Copy.customProviderEllipsis).tag(Provider.custom)
                }
                .labelsHidden()
                .fixedSize()
                .onChange(of: more) { _, value in
                    if value == .custom { nameFocused = true }
                }
            }
            if more == .custom {
                GridRow {
                    Text(Copy.providerName)
                    TextField(Copy.providerName, text: $customName, prompt: Text(Copy.providerNamePrompt))
                        .labelsHidden()
                        .focused($nameFocused)
                }
                GridRow {
                    Text(Copy.providerAddress)
                    VStack(alignment: .leading, spacing: 6) {
                        TextField(Copy.providerAddress, text: $customTemplate, prompt: Text(verbatim: Copy.providerAddressPrompt))
                            .labelsHidden()
                        CustomTemplateHint(template: customTemplate)
                    }
                }
            }
        }
        .autocorrectionDisabled()
        .frame(maxWidth: 420)
    }
}

/// A selectable provider tile: badge and name.
private struct ProviderTile: View {
    var title: String
    var badge: ProviderBadge.Kind
    var isSelected: Bool
    var action: () -> Void

    @State private var hovering = false
    @Environment(\.colorSchemeContrast) private var contrast

    var body: some View {
        let shape = RoundedRectangle(cornerRadius: 10, style: .continuous)
        Button(action: action) {
            VStack(spacing: 8) {
                ProviderBadge(kind: badge, size: 36)
                Text(title)
                    .font(.callout)
                    .lineLimit(1)
                    .minimumScaleFactor(0.85)
            }
            .frame(width: 112, height: 84)
            .background {
                if isSelected {
                    shape.fill(Color.accentColor.opacity(0.14))
                } else if hovering {
                    shape.fill(.fill.tertiary)
                } else {
                    shape.fill(.fill.quaternary)
                }
            }
            .overlay {
                if isSelected {
                    shape.strokeBorder(Color.accentColor, lineWidth: 2)
                } else if contrast == .increased {
                    shape.strokeBorder(.primary, lineWidth: 1)
                }
            }
            .contentShape(shape)
        }
        .buttonStyle(.plain)
        .onHover { hovering = $0 }
        .accessibilityLabel(title)
        .accessibilityAddTraits(isSelected ? .isSelected : [])
    }
}

/// What the placeholders do, or why the web address can't be used yet.
struct CustomTemplateHint: View {
    var template: String

    var body: some View {
        let invalid = !template.trimmingCharacters(in: .whitespaces).isEmpty && !CustomProvider.isValidTemplate(template)
        Text(invalid ? Copy.providerAddressInvalid : Copy.providerAddressHelp)
            .font(.caption)
            .foregroundStyle(invalid ? AnyShapeStyle(.red) : AnyShapeStyle(.secondary))
            .fixedSize(horizontal: false, vertical: true)
    }
}
