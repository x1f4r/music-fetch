import MusicFetchCore
import SwiftUI

/// Settings (§6.9): five panes, grouped forms, the last pane restored.
struct SettingsView: View {
    @AppStorage(PreferenceKey.settingsPane) private var pane = SettingsPane.general.rawValue

    var body: some View {
        TabView(selection: $pane) {
            Tab(Copy.settingsGeneral, systemImage: "gearshape", value: SettingsPane.general.rawValue) {
                GeneralPane()
            }
            Tab(Copy.settingsServices, systemImage: "key", value: SettingsPane.services.rawValue) {
                ServicesPane()
            }
            Tab(Copy.settingsMelody, image: "ContourMark", value: SettingsPane.melody.rawValue) {
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
    case general, services, melody, storage, about
}

// MARK: - General

struct GeneralPane: View {
    @Environment(AppModel.self) private var app
    @AppStorage(PreferenceKey.defaultService) private var defaultService = ServiceKey.spotify.rawValue
    @AppStorage(PreferenceKey.showMenuBarExtra) private var showMenuBar = true
    @AppStorage(PreferenceKey.notifyInBackground) private var notify = true

    var body: some View {
        Form {
            Picker(Copy.openSongsIn, selection: $defaultService) {
                ForEach(LinkOpener.defaultChoices, id: \.self) { service in
                    Text(Copy.name(of: service)).tag(ServiceKey(service).rawValue)
                }
            }
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
        .formStyle(.grouped)
        .frame(minHeight: 260)
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

    var body: some View {
        let settings = app.settings!
        Form {
            Section {
                switch settings.melodyState {
                case .installed, .unknown:
                    if settings.melody.installed {
                        LabeledContent(Copy.melodyLibrary) {
                            Text(Copy.melodyInstalled(songs: settings.melody.songCount, bytes: settings.melody.sizeBytes))
                        }
                        if let updated = Format.indexDate(settings.melody.version) {
                            LabeledContent(Copy.updated, value: updated)
                        }
                        if settings.hasMelodyUpdate {
                            LabeledContent(Copy.updateAvailable(settings.melody.downloadBytes)) {
                                Button(Copy.update) { settings.downloadMelody() }
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
                    LabeledContent(Copy.melodyLibrary) {
                        HStack(spacing: 10) {
                            ProgressView(value: fraction)
                                .frame(width: 140)
                            Text(Copy.downloadProgress(fraction: fraction, total: settings.melodyDownloadSize))
                                .monospacedDigit()
                                .foregroundStyle(.secondary)
                            Button(Copy.cancel) { settings.cancelMelodyDownload() }
                        }
                    }
                case .failed:
                    LabeledContent(Copy.melodyLibrary) {
                        HStack(spacing: 10) {
                            Text(Copy.melodyError).foregroundStyle(.secondary)
                            Button(Copy.tryAgainTitle) { settings.downloadMelody() }
                        }
                    }
                }
            } footer: {
                Text(Copy.melodyFooter).font(.callout).foregroundStyle(.secondary)
            }
            Section(Copy.whileHumming) {
                Toggle(Copy.alsoSearchLyrics, isOn: $searchSungLyrics)
            }
        }
        .formStyle(.grouped)
        .frame(minHeight: 320)
        .onAppear { settings.refreshMelody() }
    }

    private func notInstalledRow(_ settings: SettingsModel) -> some View {
        LabeledContent(Copy.melodyLibrary) {
            HStack(spacing: 10) {
                Text(Copy.melodyNotInstalled(settings.melodyDownloadSize)).foregroundStyle(.secondary)
                Button(Copy.melodyDownload) { settings.downloadMelody() }
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
                    LabeledContent(Copy.melodyLibrary, value: Format.bytes(storage.melodyIndexBytes))
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
    var body: some View {
        NavigationStack {
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
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 8)
                }
                Section {
                    NavigationLink(Copy.licenses) { LicensesView() }
                    NavigationLink(Copy.acknowledgements) { AcknowledgementsView() }
                }
            }
            .formStyle(.grouped)
        }
        .frame(minHeight: 380)
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
        .navigationTitle(Copy.licenses)
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
        .navigationTitle(Copy.acknowledgements)
    }
}
