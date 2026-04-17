import ServiceManagement
import SwiftUI

enum LaunchAtLoginController {
    static func setEnabled(_ enabled: Bool) {
        do {
            if enabled {
                try SMAppService.mainApp.register()
            } else {
                try SMAppService.mainApp.unregister()
            }
        } catch {
            return
        }
    }
}

struct SettingsView: View {
    @ObservedObject var model: AppModel
    @AppStorage(settingsTabDefaultsKey) private var settingsTabRaw = SettingsTab.general.rawValue
    @AppStorage(languageDefaultsKey) private var languageCode = UILanguage.en.rawValue

    var body: some View {
        TabView(selection: selectedTab) {
            GeneralSettingsTab(model: model, languageCode: languageCode)
                .tabItem { Label(SettingsTab.general.title(languageCode), systemImage: SettingsTab.general.systemImage) }
                .tag(SettingsTab.general)

            RecognitionSettingsTab(languageCode: languageCode)
                .tabItem { Label(SettingsTab.recognition.title(languageCode), systemImage: SettingsTab.recognition.systemImage) }
                .tag(SettingsTab.recognition)

            ConnectionsSettingsTab(languageCode: languageCode)
                .tabItem { Label(SettingsTab.connections.title(languageCode), systemImage: SettingsTab.connections.systemImage) }
                .tag(SettingsTab.connections)

            DiagnosticsSettingsTab(model: model, languageCode: languageCode)
                .tabItem { Label(SettingsTab.diagnostics.title(languageCode), systemImage: SettingsTab.diagnostics.systemImage) }
                .tag(SettingsTab.diagnostics)
        }
        .frame(minWidth: 560, idealWidth: 640, minHeight: 420, idealHeight: 500)
        .onAppear {
            if model.providerChecks.isEmpty {
                model.refreshDoctor()
            }
        }
    }

    private var selectedTab: Binding<SettingsTab> {
        Binding(
            get: { SettingsTab(rawValue: settingsTabRaw) ?? .general },
            set: { settingsTabRaw = $0.rawValue }
        )
    }
}

// MARK: - General

struct GeneralSettingsTab: View {
    @ObservedObject var model: AppModel
    let languageCode: String

    @AppStorage(languageDefaultsKey) private var storedLanguage = UILanguage.en.rawValue
    @AppStorage(openExternalLinksDefaultsKey) private var openExternalLinks = false
    @AppStorage(launchAtLoginDefaultsKey) private var launchAtLogin = false

    var body: some View {
        Form {
            Section {
                Picker(loc(languageCode, "Language", "Sprache", "Idioma", "Langue"), selection: Binding(
                    get: { storedLanguage },
                    set: { value in
                        storedLanguage = value
                        model.setLanguage(value)
                    }
                )) {
                    ForEach(UILanguage.allCases, id: \.rawValue) { language in
                        Text(language.label).tag(language.rawValue)
                    }
                }
            }

            Section {
                Toggle(loc(languageCode,
                           "Open best external link automatically",
                           "Besten externen Link automatisch oeffnen",
                           "Abrir automaticamente el mejor enlace externo",
                           "Ouvrir automatiquement le meilleur lien externe"),
                       isOn: $openExternalLinks)
                Toggle(loc(languageCode,
                           "Launch at login",
                           "Beim Login starten",
                           "Iniciar al arrancar",
                           "Lancer a la connexion"),
                       isOn: $launchAtLogin)
                    .onChange(of: launchAtLogin) { _, value in
                        LaunchAtLoginController.setEnabled(value)
                    }
            }
        }
        .formStyle(.grouped)
    }
}

// MARK: - Recognition

struct RecognitionSettingsTab: View {
    let languageCode: String

    @AppStorage(analysisModeDefaultsKey) private var analysisModeRaw = AnalysisMode.auto.rawValue
    @AppStorage(recallProfileDefaultsKey) private var recallProfileRaw = RecallProfile.maxRecall.rawValue
    @AppStorage(metadataHintsDefaultsKey) private var metadataHints = true
    @AppStorage(repeatDetectionDefaultsKey) private var repeatDetection = true
    @AppStorage(preferSeparationDefaultsKey) private var preferSeparation = true

    var body: some View {
        Form {
            Section {
                Picker(loc(languageCode,
                           "Analysis mode",
                           "Analysemodus",
                           "Modo de analisis",
                           "Mode d'analyse"),
                       selection: analysisModeBinding) {
                    ForEach(AnalysisMode.allCases) { mode in
                        Text(mode.title(languageCode)).tag(mode)
                    }
                }

                Picker(loc(languageCode,
                           "Recall profile",
                           "Recall-Profil",
                           "Perfil de recall",
                           "Profil de rappel"),
                       selection: recallBinding) {
                    ForEach(RecallProfile.allCases) { profile in
                        Text(profile.title(languageCode)).tag(profile)
                    }
                }
            } footer: {
                Text(loc(languageCode,
                         "Max recall searches more windows; Fast first stops earlier for short clips.",
                         "Max Recall sucht mehr Fenster ab; Fast first stoppt frueh bei kurzen Clips.",
                         "Max recall explora mas ventanas; Fast first se detiene antes en clips cortos.",
                         "Max recall analyse plus de fenetres ; Fast first s'arrete plus tot pour les courts clips."))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Section {
                Toggle(loc(languageCode,
                           "Use metadata hints",
                           "Metadaten-Hinweise nutzen",
                           "Usar pistas de metadatos",
                           "Utiliser les indices de metadonnees"),
                       isOn: $metadataHints)
                Toggle(loc(languageCode,
                           "Detect repeats",
                           "Wiederholungen erkennen",
                           "Detectar repeticiones",
                           "Detecter les repetitions"),
                       isOn: $repeatDetection)
                Toggle(loc(languageCode,
                           "Prefer source separation",
                           "Source-Separation bevorzugen",
                           "Preferir separacion de fuentes",
                           "Preferer la separation de sources"),
                       isOn: $preferSeparation)
            } footer: {
                Text(loc(languageCode,
                         "Separation isolates vocals and helps identify music buried under speech or FX.",
                         "Separation loest die Stimme heraus und hilft, Musik unter Stimmen oder Effekten zu erkennen.",
                         "La separacion aisla la voz y ayuda a detectar musica bajo habla o efectos.",
                         "La separation isole la voix et aide a identifier la musique sous la parole."))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .formStyle(.grouped)
    }

    private var analysisModeBinding: Binding<AnalysisMode> {
        Binding(
            get: { AnalysisMode(rawValue: analysisModeRaw) ?? .auto },
            set: { analysisModeRaw = $0.rawValue }
        )
    }

    private var recallBinding: Binding<RecallProfile> {
        Binding(
            get: { RecallProfile(rawValue: recallProfileRaw) ?? .maxRecall },
            set: { recallProfileRaw = $0.rawValue }
        )
    }
}

// MARK: - Connections

struct ConnectionsSettingsTab: View {
    let languageCode: String

    var body: some View {
        Form {
            Section {
                LabeledContent(loc(languageCode, "Local catalog", "Lokaler Katalog", "Catalogo local", "Catalogue local"),
                               value: "CLI / API")
                LabeledContent("AudD / ACRCloud",
                               value: loc(languageCode, "Backend config", "Backend-Konfiguration", "Configuracion del backend", "Configuration du backend"))
            } footer: {
                Text(loc(languageCode,
                         "Advanced provider credentials live in the backend config and CLI environment.",
                         "Erweiterte Provider-Zugangsdaten bleiben in Backend-Konfiguration und CLI-Umgebung.",
                         "Las credenciales avanzadas siguen en la configuracion del backend y el entorno CLI.",
                         "Les identifiants avances restent dans la configuration backend et l'environnement CLI."))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .formStyle(.grouped)
    }
}

// MARK: - Diagnostics

struct DiagnosticsSettingsTab: View {
    @ObservedObject var model: AppModel
    let languageCode: String

    @AppStorage(debugDetailsDefaultsKey) private var debugDetails = false

    var body: some View {
        Form {
            Section {
                TextField(loc(languageCode,
                              "Backend command",
                              "Backend-Befehl",
                              "Comando backend",
                              "Commande backend"),
                          text: backendBinding)
                    .textFieldStyle(.roundedBorder)
                    .font(.system(size: 12, design: .monospaced))

                HStack(spacing: 8) {
                    Button {
                        model.refreshDoctor()
                    } label: {
                        Label(loc(languageCode, "Refresh checks", "Checks aktualisieren", "Actualizar", "Actualiser"),
                              systemImage: "arrow.clockwise")
                    }
                    .buttonStyle(.bordered)
                    Button {
                        model.installMissingCoreDependencies()
                    } label: {
                        Label(loc(languageCode, "Install tools", "Tools installieren", "Instalar herramientas", "Installer les outils"),
                              systemImage: "arrow.down.app")
                    }
                    .buttonStyle(.bordered)
                    Spacer()
                }
            } header: {
                Text(loc(languageCode, "Backend", "Backend", "Backend", "Backend"))
            }

            Section {
                if model.providerChecks.isEmpty {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small)
                        Text(loc(languageCode, "Running checks", "Checks laufen", "Ejecutando", "Verification"))
                            .foregroundStyle(.secondary)
                    }
                    .padding(.vertical, 4)
                } else {
                    ForEach(model.providerChecks) { check in
                        HStack(alignment: .top, spacing: 10) {
                            Image(systemName: check.ok ? "checkmark.circle.fill" : "exclamationmark.triangle.fill")
                                .foregroundStyle(check.ok ? .green : .orange)
                                .font(.callout)
                            VStack(alignment: .leading, spacing: 2) {
                                Text(check.name)
                                    .font(.callout.weight(.medium))
                                Text(check.detail)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .fixedSize(horizontal: false, vertical: true)
                            }
                            Spacer()
                        }
                        .padding(.vertical, 4)
                    }
                }
            } header: {
                Text(loc(languageCode, "Provider checks", "Provider-Checks", "Proveedores", "Fournisseurs"))
            }

            Section {
                Toggle(loc(languageCode,
                           "Show debug event stream",
                           "Debug-Events anzeigen",
                           "Mostrar eventos debug",
                           "Afficher les evenements debug"),
                       isOn: $debugDetails)

                if let resources = model.systemResources {
                    LabeledContent(loc(languageCode, "System",
                                       "System",
                                       "Sistema",
                                       "Systeme"),
                                   value: "\(resources.cpu_count) CPU · \(String(format: "%.1f GB", resources.ram_gb))")
                    LabeledContent(loc(languageCode, "Parallel jobs",
                                       "Parallele Jobs",
                                       "Trabajos paralelos",
                                       "Travaux paralleles"),
                                   value: "\(resources.active_jobs) / \(resources.max_workers)")
                }
            } header: {
                Text(loc(languageCode, "Debug", "Debug", "Depuracion", "Debogage"))
            }
        }
        .formStyle(.grouped)
    }

    private var backendBinding: Binding<String> {
        Binding(
            get: { model.backendCommand },
            set: { model.updateBackendCommand($0) }
        )
    }
}
