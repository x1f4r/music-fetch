import MusicFetchCore
import SwiftUI

/// Settings (§6.9): five panes, grouped forms, the last pane restored.
struct SettingsView: View {
    @Environment(AppModel.self) private var app
    @AppStorage(PreferenceKey.settingsPane) private var storedPane = SettingsPane.general.rawValue

    /// The stored pane; demo states keep theirs in memory.
    private var pane: Binding<String> {
        Binding {
            app.demoSettingsPane ?? storedPane
        } set: { value in
            if app.isDemo { app.demoSettingsPane = value } else { storedPane = value }
        }
    }

    var body: some View {
        TabView(selection: pane) {
            Tab(Copy.settingsGeneral, systemImage: "gearshape", value: SettingsPane.general.rawValue) {
                GeneralPane()
            }
            Tab(Copy.settingsAudio, systemImage: "mic", value: SettingsPane.audio.rawValue) {
                AudioPane()
            }
            Tab(Copy.settingsServices, systemImage: "key", value: SettingsPane.services.rawValue) {
                ServicesPane()
            }
            Tab(Copy.settingsMelody, image: "contour.mark", value: SettingsPane.melody.rawValue) {
                MelodyPane()
            }
            Tab(Copy.settingsStorage, systemImage: "internaldrive", value: SettingsPane.storage.rawValue) {
                StoragePane()
            }
            Tab(Copy.settingsAbout, systemImage: "info.circle", value: SettingsPane.about.rawValue) {
                AboutPane()
            }
        }
        .frame(width: 620)
    }
}

enum SettingsPane: String {
    case general, audio, services, melody, storage, about
}

// MARK: - General

struct GeneralPane: View {
    @Environment(AppModel.self) private var app
    @AppStorage(PreferenceKey.showMenuBarExtra) private var showMenuBar = true
    @AppStorage(PreferenceKey.notifyInBackground) private var notify = true

    var body: some View {
        let preference = app.providerPreference
        let selected = preference.key.flatMap(Provider.init(storageKey:)) ?? Providers.fallback
        Form {
            Section {
                Picker(Copy.openSongsIn, selection: Binding(get: { selected }, set: { app.setPreferredProvider($0) })) {
                    ForEach(Providers.common, id: \.self) { service in
                        Text(Copy.name(of: service)).tag(Provider.service(service))
                    }
                    Divider()
                    ForEach(Providers.more, id: \.self) { service in
                        Text(Copy.name(of: service)).tag(Provider.service(service))
                    }
                    Divider()
                    Text(Copy.customProviderEllipsis).tag(Provider.custom)
                }
                if selected == .custom {
                    TextField(
                        Copy.providerName,
                        text: Binding(
                            get: { preference.custom.name },
                            set: { app.setCustomProvider(CustomProvider(name: $0, template: app.providerPreference.custom.template)) }),
                        prompt: Text(Copy.providerNamePrompt))
                    TextField(
                        Copy.providerAddress,
                        text: Binding(
                            get: { preference.custom.template },
                            set: { app.setCustomProvider(CustomProvider(name: app.providerPreference.custom.name, template: $0)) }),
                        prompt: Text(verbatim: Copy.providerAddressPrompt))
                }
            } footer: {
                if selected == .custom {
                    CustomTemplateHint(template: preference.custom.template)
                }
            }
            .autocorrectionDisabled()
            Section {
                generalToggles
            }
        }
        .formStyle(.grouped)
        .frame(minHeight: 260)
    }

    @ViewBuilder
    private var generalToggles: some View {
        Toggle(Copy.showInMenuBar, isOn: $showMenuBar)
        Toggle(Copy.launchAtLogin, isOn: Binding(get: { app.settings.launchAtLogin }, set: { app.settings.setLaunchAtLogin($0) }))
        if let error = app.settings.loginItemError {
            Text(error).font(.callout).foregroundStyle(.secondary)
        }
        Toggle(Copy.notifyInBackground, isOn: $notify)
            .onChange(of: notify) { _, enabled in
                if enabled { Task { _ = await app.notifier.requestAuthorization() } }
            }
    }
}

// MARK: - Audio

/// Microphone priority and the output "This Mac" listens to.
struct AudioPane: View {
    @State private var devices = AudioDeviceList()
    @AppStorage(AudioDevices.captureOutputKey) private var captureOutput = ""

    var body: some View {
        Form {
            Section {
                List {
                    ForEach(Array(devices.orderedInputs.enumerated()), id: \.element.uid) { index, device in
                        MicrophoneRow(
                            device: device,
                            rank: index + 1,
                            connected: devices.connectedUIDs.contains(device.uid),
                            active: device.uid == devices.activeInputUID,
                            isFirst: index == 0,
                            isLast: index == devices.orderedInputs.count - 1,
                            list: devices
                        )
                    }
                    .onMove { devices.move(from: $0, to: $1) }
                }
                .frame(minHeight: 132)
            } header: {
                Text(Copy.microphoneOrder)
            } footer: {
                Text(Copy.microphoneOrderFooter)
                    .foregroundStyle(.secondary)
            }

            Section {
                Picker(Copy.listenTo, selection: $captureOutput) {
                    Text(Copy.systemOutput).tag("")
                    ForEach(devices.outputs) { device in
                        Text(device.name).tag(device.uid)
                    }
                    if !captureOutput.isEmpty, !devices.outputs.contains(where: { $0.uid == captureOutput }) {
                        Text(Copy.notConnected).tag(captureOutput)
                    }
                }
            } header: {
                Text(Copy.thisMac)
            } footer: {
                Text(Copy.outputFooter)
                    .foregroundStyle(.secondary)
            }
        }
        .formStyle(.grouped)
        .frame(minHeight: 360)
        .onAppear { devices.refresh() }
    }
}

private struct MicrophoneRow: View {
    let device: AudioDevice
    let rank: Int
    let connected: Bool
    let active: Bool
    let isFirst: Bool
    let isLast: Bool
    let list: AudioDeviceList

    var body: some View {
        HStack(spacing: 10) {
            Text("\(rank)")
                .font(.callout.monospacedDigit())
                .foregroundStyle(.secondary)
                .frame(width: 16, alignment: .trailing)
            VStack(alignment: .leading, spacing: 1) {
                Text(device.name)
                    .foregroundStyle(connected ? .primary : .secondary)
                if active {
                    Text(Copy.inUse).font(.caption).foregroundStyle(.tint)
                } else if !connected {
                    Text(Copy.notConnected).font(.caption).foregroundStyle(.secondary)
                }
            }
            Spacer()
            ControlGroup {
                Button { list.moveUp(device.uid) } label: { Image(systemName: "chevron.up") }
                    .disabled(isFirst)
                    .accessibilityLabel(Copy.moveUp(device.name))
                Button { list.moveDown(device.uid) } label: { Image(systemName: "chevron.down") }
                    .disabled(isLast)
                    .accessibilityLabel(Copy.moveDown(device.name))
            }
            .controlGroupStyle(.navigation)
            .fixedSize()
        }
        .padding(.vertical, 2)
        .contextMenu {
            if !connected {
                Button(Copy.forget(device.name)) { list.forget(device.uid) }
            }
        }
    }
}

// MARK: - Services

struct ServicesPane: View {
    @Environment(AppModel.self) private var app
    @State private var saveTask: Task<Void, Never>?

    var body: some View {
        @Bindable var settings = app.settings
        Form {
            Section {
                SecureField(Copy.apiToken, text: $settings.credentials.auddToken, prompt: Text(placeholder(.auddToken)))
            } header: {
                Text(Copy.audd)
            } footer: {
                footer(Copy.auddFooter)
            }
            Section {
                TextField(Copy.host, text: $settings.credentials.acrHost, prompt: Text(verbatim: "identify-eu-west-1.acrcloud.com"))
                TextField(Copy.accessKey, text: $settings.credentials.acrAccessKey, prompt: Text(placeholder(.acrAccessKey)))
                SecureField(Copy.accessSecret, text: $settings.credentials.acrAccessSecret, prompt: Text(placeholder(.acrAccessSecret)))
            } header: {
                Text(Copy.acrCloud)
            } footer: {
                footer(Copy.acrFooter)
            }
            Section {
                TextField(Copy.host, text: $settings.credentials.humHost, prompt: Text(verbatim: "identify-eu-west-1.acrcloud.com"))
                TextField(Copy.accessKey, text: $settings.credentials.humAccessKey, prompt: Text(placeholder(.humAccessKey)))
                SecureField(Copy.accessSecret, text: $settings.credentials.humAccessSecret, prompt: Text(placeholder(.humAccessSecret)))
            } header: {
                Text(Copy.acrHumming)
            } footer: {
                footer(Copy.acrHummingFooter)
            }
            if let error = settings.credentialError {
                Text(error).foregroundStyle(.secondary)
            }
        }
        .formStyle(.grouped)
        .autocorrectionDisabled()
        .frame(minHeight: 520)
        .onChange(of: settings.credentials) { _, _ in
            saveTask?.cancel()
            saveTask = Task {
                try? await Task.sleep(for: .milliseconds(700))
                guard !Task.isCancelled else { return }
                settings.saveCredentials()
            }
        }
        .onDisappear { settings.saveCredentials() }
    }

    private func placeholder(_ account: CredentialFields.Account) -> String {
        app.settings.isSet(account) ? "••••" : ""
    }

    private func footer(_ text: String) -> some View {
        Text(text).font(.callout).foregroundStyle(.secondary)
    }
}

// MARK: - Melody

struct MelodyPane: View {
    @Environment(AppModel.self) private var app
    @AppStorage(PreferenceKey.searchSungLyrics) private var searchSungLyrics = true
    @AppStorage(PreferenceKey.autoStopAfterPause) private var autoStop = true

    var body: some View {
        let settings = app.settings!
        Form {
            Section {
                switch settings.melodyState {
                case .installed, .unknown:
                    if settings.melody.installed {
                        LabeledContent(Copy.melodyLibrary) {
                            Text(Copy.melodyInstalled(songs: settings.melody.songCount, bytes: settings.libraryBytes))
                        }
                        encoderRow(settings)
                        if let updated = Format.indexDate(settings.melody.version) {
                            LabeledContent(Copy.updated, value: updated)
                        }
                        if settings.hasMelodyUpdate {
                            LabeledContent(Copy.updateAvailable(settings.melody.downloadBytes)) {
                                Button(Copy.update) { settings.downloadMelody() }
                                    .accessibilityLabel(Copy.update)
                            }
                        }
                        HStack {
                            Spacer()
                            Button(Copy.checkForUpdates) { settings.checkForUpdates() }
                                .disabled(settings.isCheckingMelody)
                            Button(Copy.remove, role: .destructive) { settings.removeMelody() }
                        }
                    } else {
                        notInstalledRow(settings)
                    }
                case .notInstalled:
                    notInstalledRow(settings)
                case .downloading(let fraction):
                    LabeledContent(settings.isDownloadingEncoderOnly ? Copy.melodyModel : Copy.melodyLibrary) {
                        HStack(spacing: 10) {
                            ProgressView(value: fraction)
                                .frame(width: 140)
                            Text(Copy.downloadProgress(fraction: fraction, total: settings.melodyDownloadSize))
                                .monospacedDigit()
                                .foregroundStyle(.secondary)
                            Button(Copy.cancel) { settings.cancelMelodyDownload() }
                                .accessibilityLabel(Copy.cancel)
                        }
                    }
                case .failed:
                    LabeledContent(Copy.melodyLibrary) {
                        HStack(spacing: 10) {
                            Text(Copy.melodyError).foregroundStyle(.secondary)
                            Button(Copy.tryAgainTitle) { settings.downloadMelody() }
                                .accessibilityLabel(Copy.tryAgainTitle)
                        }
                    }
                }
            } footer: {
                Text(Copy.melodyFooter).font(.callout).foregroundStyle(.secondary)
            }
            Section(Copy.whileHumming) {
                let speech = app.speech!
                Toggle(isOn: $searchSungLyrics) {
                    Text(Copy.alsoSearchLyrics)
                    Text(Copy.speechNote(speech.status, language: speech.language))
                }
                .disabled(speech.status == .unsupported)
                .onChange(of: searchSungLyrics) { _, on in
                    // Turning it back on is the moment to ask (macOS 15).
                    if on, speech.status == .needsPermission { Task { await speech.requestPermission() } }
                }
                if speech.status == .denied, searchSungLyrics {
                    HStack {
                        Spacer()
                        Button(Copy.openSystemSettings) { app.openPrivacySettings(.speechRecognition) }
                    }
                }
                Toggle(isOn: $autoStop) {
                    Text(Copy.autoStopAfterPause)
                    Text(Copy.autoStopFooter)
                }
            }
        }
        .formStyle(.grouped)
        .frame(minHeight: 480)
        .onAppear { settings.refreshMelody() }
        .task { await app.speech.refresh() }
    }

    /// The index's encoder file: its size once installed, or the offer to download it.
    @ViewBuilder
    private func encoderRow(_ settings: SettingsModel) -> some View {
        switch settings.encoder {
        case .none:
            EmptyView()
        case .installed(let bytes):
            LabeledContent(Copy.melodyModel) {
                Text(bytes.map(Format.bytes) ?? Copy.installed)
            }
        case .available(let bytes):
            LabeledContent {
                HStack(spacing: 10) {
                    Text(Copy.melodyNotInstalled(bytes)).foregroundStyle(.secondary)
                    Button(Copy.melodyDownload) { settings.downloadMelody() }
                        .accessibilityLabel(Copy.downloadMelodyModel)
                }
            } label: {
                Text(Copy.melodyModel)
                Text(Copy.melodyModelNote)
            }
        case .updateAvailable(let bytes):
            LabeledContent(Copy.melodyModel) {
                HStack(spacing: 10) {
                    Text(Copy.updateAvailable(bytes)).foregroundStyle(.secondary)
                    Button(Copy.update) { settings.downloadMelody() }
                        .accessibilityLabel(Copy.updateMelodyModel)
                }
            }
        }
    }

    private func notInstalledRow(_ settings: SettingsModel) -> some View {
        LabeledContent(Copy.melodyLibrary) {
            HStack(spacing: 10) {
                Text(Copy.melodyNotInstalled(settings.melodyDownloadSize)).foregroundStyle(.secondary)
                Button(Copy.melodyDownload) { settings.downloadMelody() }
                    .accessibilityLabel(Copy.melodyDownload)
            }
        }
    }
}

// MARK: - Storage

struct StoragePane: View {
    @Environment(AppModel.self) private var app
    @State private var confirmClear = false

    var body: some View {
        let settings = app.settings!
        Form {
            Section {
                LabeledContent(Copy.onDisk) {
                    Text(settings.storage == nil ? "—" : Format.bytes(settings.totalBytes)).monospacedDigit()
                }
                if let storage = settings.storage {
                    LabeledContent(Copy.cachedAudio, value: Format.bytes(storage.cacheBytes))
                    // The engine counts the encoder file with the index; Melody lists them apart.
                    if let model = settings.encoderBytes, model <= storage.melodyIndexBytes {
                        LabeledContent(Copy.melodyLibrary, value: Format.bytes(storage.melodyIndexBytes - model))
                        LabeledContent(Copy.melodyModel, value: Format.bytes(model))
                    } else {
                        LabeledContent(Copy.melodyLibrary, value: Format.bytes(storage.melodyIndexBytes))
                    }
                    LabeledContent(Copy.libraryTitle, value: Format.bytes(storage.databaseBytes))
                }
                HStack {
                    Spacer()
                    Button(Copy.clearCache) { confirmClear = true }
                        .disabled(settings.isClearingCache || (settings.storage?.cacheBytes ?? 0) == 0)
                }
            }
            Section {
                LabeledContent(Copy.location) {
                    HStack(spacing: 10) {
                        Text(Format.abbreviatedPath(settings.dataDirectory))
                            .lineLimit(1)
                            .truncationMode(.middle)
                            .foregroundStyle(.secondary)
                        Button(Copy.showInFinder) {
                            if let url = settings.dataDirectory { NSWorkspace.shared.activateFileViewerSelecting([url]) }
                        }
                        .accessibilityLabel(Copy.showInFinder)
                    }
                }
            }
        }
        .formStyle(.grouped)
        .frame(minHeight: 300)
        .onAppear { settings.refreshStorage() }
        .alert(Copy.clearCacheConfirm(Format.bytes(settings.storage?.cacheBytes ?? 0)), isPresented: $confirmClear) {
            Button(Copy.clearCache, role: .destructive) { settings.clearCache() }
            Button(Copy.cancel, role: .cancel) {}
        }
    }
}

// MARK: - About

struct AboutPane: View {
    @State private var showLicenses = false
    @State private var showAcknowledgements = false

    var body: some View {
        Form {
            Section {
                VStack(spacing: 8) {
                    Image(nsImage: NSApp.applicationIconImage)
                        .resizable()
                        .frame(width: 96, height: 96)
                        .accessibilityHidden(true)
                    Text(Copy.versionLine(version: Self.version, build: Self.build))
                        .font(.headline)
                    Text(Copy.aboutLine)
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .multilineTextAlignment(.center)
                    HStack(spacing: 12) {
                        Button(Copy.licenses) { showLicenses = true }
                        Button(Copy.acknowledgements) { showAcknowledgements = true }
                    }
                    .padding(.top, 8)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 8)
            }
        }
        .formStyle(.grouped)
        .frame(minHeight: 300)
        .sheet(isPresented: $showLicenses) {
            SheetPage(title: Copy.licenses) { LicensesView() }
        }
        .sheet(isPresented: $showAcknowledgements) {
            SheetPage(title: Copy.acknowledgements) { AcknowledgementsView() }
        }
    }

    static var version: String {
        Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String ?? "1.0"
    }

    static var build: String {
        Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String ?? "1"
    }
}

/// Third-party licenses: THIRD_PARTY_NOTICES.md and the bundled media runtime.
struct LicensesView: View {
    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                ForEach(Licenses.all) { license in
                    VStack(alignment: .leading, spacing: 8) {
                        Text(license.title).font(.headline)
                        if !license.summary.isEmpty {
                            Text(license.summary).font(.callout).foregroundStyle(.secondary)
                        }
                        if !license.text.isEmpty {
                            Text(license.text)
                                .font(.caption.monospaced())
                                .foregroundStyle(.secondary)
                                .textSelection(.enabled)
                        }
                    }
                }
            }
            .padding(20)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}

struct AcknowledgementsView: View {
    var body: some View {
        Form {
            ForEach(Licenses.acknowledgements, id: \.self) { line in
                Text(line)
            }
        }
        .formStyle(.grouped)
    }
}

/// A sheet with a title bar and Done.
struct SheetPage<Content: View>: View {
    @Environment(\.dismiss) private var dismiss
    var title: String
    @ViewBuilder var content: Content

    var body: some View {
        VStack(spacing: 0) {
            Text(title)
                .font(.headline)
                .padding(.vertical, 12)
            Divider()
            content
            Divider()
            HStack {
                Spacer()
                Button(Copy.done) { dismiss() }
                    .keyboardShortcut(.defaultAction)
            }
            .padding(12)
        }
        .frame(width: 560, height: 520)
    }
}
