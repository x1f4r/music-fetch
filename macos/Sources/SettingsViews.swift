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
    @AppStorage(openExternalLinksDefaultsKey) private var openExternalLinks = false
    @AppStorage(languageDefaultsKey) private var languageCode = UILanguage.en.rawValue
    @AppStorage(launchAtLoginDefaultsKey) private var launchAtLogin = false
    @AppStorage(analysisModeDefaultsKey) private var analysisModeRaw = AnalysisMode.auto.rawValue
    @AppStorage(recallProfileDefaultsKey) private var recallProfileRaw = RecallProfile.maxRecall.rawValue
    @AppStorage(metadataHintsDefaultsKey) private var metadataHints = true
    @AppStorage(repeatDetectionDefaultsKey) private var repeatDetection = true
    @AppStorage(preferSeparationDefaultsKey) private var preferSeparation = true
    @AppStorage(preferredInputDefaultsKey) private var preferredInputRaw = PreferredInput.link.rawValue
    @AppStorage(recordingTargetDefaultsKey) private var recordingTargetRaw = RecordingTarget.microphone.rawValue
    @AppStorage(debugDetailsDefaultsKey) private var debugDetails = false
    @AppStorage(settingsTabDefaultsKey) private var settingsTabRaw = SettingsTab.general.rawValue

    var body: some View {
        TabView(selection: selectedTab) {
            generalTab
                .tabItem { Label(SettingsTab.general.title(languageCode), systemImage: SettingsTab.general.systemImage) }
                .tag(SettingsTab.general)

            inputTab
                .tabItem { Label(SettingsTab.input.title(languageCode), systemImage: SettingsTab.input.systemImage) }
                .tag(SettingsTab.input)

            recognitionTab
                .tabItem { Label(SettingsTab.recognition.title(languageCode), systemImage: SettingsTab.recognition.systemImage) }
                .tag(SettingsTab.recognition)

            connectionsTab
                .tabItem { Label(SettingsTab.connections.title(languageCode), systemImage: SettingsTab.connections.systemImage) }
                .tag(SettingsTab.connections)

            diagnosticsTab
                .tabItem { Label(SettingsTab.diagnostics.title(languageCode), systemImage: SettingsTab.diagnostics.systemImage) }
                .tag(SettingsTab.diagnostics)
        }
        .frame(minWidth: 860, minHeight: 620)
        .onAppear {
            if model.providerChecks.isEmpty {
                model.refreshDoctor()
            }
        }
        .onChange(of: launchAtLogin) { _, value in
            LaunchAtLoginController.setEnabled(value)
        }
    }

    private var selectedTab: Binding<SettingsTab> {
        Binding(
            get: { SettingsTab(rawValue: settingsTabRaw) ?? .general },
            set: { settingsTabRaw = $0.rawValue }
        )
    }

    private var analysisMode: Binding<AnalysisMode> {
        Binding(
            get: { AnalysisMode(rawValue: analysisModeRaw) ?? .auto },
            set: { analysisModeRaw = $0.rawValue }
        )
    }

    private var recallProfile: Binding<RecallProfile> {
        Binding(
            get: { RecallProfile(rawValue: recallProfileRaw) ?? .maxRecall },
            set: { recallProfileRaw = $0.rawValue }
        )
    }

    private var preferredInput: Binding<PreferredInput> {
        Binding(
            get: { PreferredInput(rawValue: preferredInputRaw) ?? .link },
            set: { preferredInputRaw = $0.rawValue }
        )
    }

    private var recordingTarget: Binding<RecordingTarget> {
        Binding(
            get: { RecordingTarget(rawValue: recordingTargetRaw) ?? .microphone },
            set: { recordingTargetRaw = $0.rawValue }
        )
    }

    private var generalTab: some View {
        Form {
            Picker(loc(languageCode, "Language", "Sprache", "Idioma", "Langue"), selection: Binding(
                get: { languageCode },
                set: {
                    languageCode = $0
                    model.setLanguage($0)
                }
            )) {
                ForEach(UILanguage.allCases, id: \.rawValue) { language in
                    Text(language.label).tag(language.rawValue)
                }
            }

            Picker(loc(languageCode, "Default Analysis", "Standardanalyse", "Analisis por defecto", "Analyse par defaut"), selection: analysisMode) {
                ForEach(AnalysisMode.allCases) { mode in
                    Text(mode.title(languageCode)).tag(mode)
                }
            }

            Toggle(loc(languageCode, "Open best external link automatically", "Besten externen Link automatisch oeffnen", "Abrir automaticamente el mejor enlace externo", "Ouvrir automatiquement le meilleur lien externe"), isOn: $openExternalLinks)
            Toggle(loc(languageCode, "Launch at login", "Beim Login starten", "Iniciar al arrancar", "Lancer a la connexion"), isOn: $launchAtLogin)
        }
        .padding(22)
    }

    private var inputTab: some View {
        Form {
            Picker(loc(languageCode, "Preferred Input", "Bevorzugte Eingabe", "Entrada preferida", "Entree preferee"), selection: preferredInput) {
                ForEach(PreferredInput.allCases) { input in
                    Text(input.title(languageCode)).tag(input)
                }
            }

            Picker(loc(languageCode, "Primary Recording Target", "Primaeres Aufnahmeziel", "Objetivo principal de grabacion", "Cible d'enregistrement principale"), selection: recordingTarget) {
                ForEach(RecordingTarget.allCases) { target in
                    Text(target.title(languageCode)).tag(target)
                }
            }

            Text(loc(languageCode, "The command deck and analyze composer highlight these selections in the main window.", "Command Deck und Analyse-Composer heben diese Auswahl im Hauptfenster hervor.", "El panel superior y el compositor principal destacan estas selecciones.", "Le panneau de commande et le composeur principal mettent ces choix en avant."))
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(22)
    }

    private var recognitionTab: some View {
        Form {
            Picker(loc(languageCode, "Recall Profile", "Recall-Profil", "Perfil de recall", "Profil de rappel"), selection: recallProfile) {
                ForEach(RecallProfile.allCases) { profile in
                    Text(profile.title(languageCode)).tag(profile)
                }
            }

            Toggle(loc(languageCode, "Use metadata hints", "Metadaten-Hinweise nutzen", "Usar pistas de metadatos", "Utiliser les indices de metadonnees"), isOn: $metadataHints)
            Toggle(loc(languageCode, "Detect repeats", "Wiederholungen erkennen", "Detectar repeticiones", "Detecter les repetitions"), isOn: $repeatDetection)
            Toggle(loc(languageCode, "Prefer source separation", "Source-Separation bevorzugen", "Preferir separacion de fuentes", "Preferer la separation de sources"), isOn: $preferSeparation)
        }
        .padding(22)
    }

    private var connectionsTab: some View {
        Form {
            LabeledContent(loc(languageCode, "Local catalog", "Lokaler Katalog", "Catalogo local", "Catalogue local"), value: "CLI / API")
            LabeledContent("AudD / ACRCloud", value: loc(languageCode, "Backend config", "Backend-Konfiguration", "Configuracion del backend", "Configuration du backend"))
            Text(loc(languageCode, "Advanced provider credentials still live in the backend config and CLI environment.", "Erweiterte Provider-Zugangsdaten bleiben in Backend-Konfiguration und CLI-Umgebung.", "Las credenciales avanzadas siguen en la configuracion del backend y el entorno CLI.", "Les identifiants avances restent dans la configuration backend et l'environnement CLI."))
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(22)
    }

    private var diagnosticsTab: some View {
        DiagnosticsPanelView(model: model, languageCode: languageCode, debugDetails: $debugDetails)
            .padding(22)
    }
}

struct DiagnosticsPanelView: View {
    @ObservedObject var model: AppModel
    let languageCode: String
    @Binding var debugDetails: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text(loc(languageCode, "Diagnostics", "Diagnostik", "Diagnostico", "Diagnostic"))
                        .font(.system(size: 28, weight: .black, design: .rounded))
                    Text(loc(languageCode, "Inspect backend reachability, dependencies, and verbose run state.", "Backend-Erreichbarkeit, Abhaengigkeiten und ausfuehrlichen Laufstatus pruefen.", "Inspecciona el backend, dependencias y estado detallado de ejecucion.", "Inspectez l'accessibilite du backend, les dependances et l'etat detaille des analyses."))
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Toggle(loc(languageCode, "Show debug details", "Debug-Details zeigen", "Mostrar detalles debug", "Afficher les details debug"), isOn: $debugDetails)
                    .toggleStyle(.switch)
            }

            HStack {
                Button(loc(languageCode, "Refresh Checks", "Checks aktualisieren", "Actualizar checks", "Actualiser les checks")) {
                    model.refreshDoctor()
                }
                .buttonStyle(.borderedProminent)

                Button(loc(languageCode, "Install Tools", "Tools installieren", "Instalar herramientas", "Installer les outils")) {
                    model.installMissingCoreDependencies()
                }
                .buttonStyle(.bordered)
            }

            TextField(loc(languageCode, "Backend command", "Backend-Befehl", "Comando backend", "Commande backend"), text: Binding(
                get: { model.backendCommand },
                set: { model.updateBackendCommand($0) }
            ))
            .textFieldStyle(.roundedBorder)

            StudioPanel(padding: 18) {
                VStack(alignment: .leading, spacing: 12) {
                    Text(loc(languageCode, "Provider Checks", "Provider-Checks", "Comprobaciones de proveedores", "Verifications des fournisseurs"))
                        .font(.headline)
                    List(model.providerChecks) { check in
                        HStack {
                            Image(systemName: check.ok ? "checkmark.circle.fill" : "exclamationmark.triangle.fill")
                                .foregroundStyle(check.ok ? .green : .orange)
                            VStack(alignment: .leading, spacing: 3) {
                                Text(check.name)
                                Text(check.detail)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                    .frame(minHeight: 260)
                }
            }

            if debugDetails, let result = model.result {
                StudioPanel(padding: 18) {
                    VStack(alignment: .leading, spacing: 10) {
                        Text(loc(languageCode, "Live Snapshot", "Live-Snapshot", "Snapshot en vivo", "Snapshot en direct"))
                            .font(.headline)
                        LabeledContent("Job ID", value: result.job.id)
                        LabeledContent(loc(languageCode, "Status", "Status", "Estado", "Statut"), value: result.job.status)
                        LabeledContent(loc(languageCode, "Segments", "Segmente", "Segmentos", "Segments"), value: "\(result.segments.count)")
                        if let lastEvent = result.events?.last {
                            LabeledContent(loc(languageCode, "Last Event", "Letztes Event", "Ultimo evento", "Dernier evenement"), value: lastEvent.message)
                        }
                    }
                }
            }

            Spacer(minLength: 0)
        }
    }
}

struct DiagnosticsSheetView: View {
    @ObservedObject var model: AppModel
    @AppStorage(languageDefaultsKey) private var languageCode = defaultUILanguageCode()
    @AppStorage(debugDetailsDefaultsKey) private var debugDetails = false

    var body: some View {
        NavigationStack {
            DiagnosticsPanelView(model: model, languageCode: languageCode, debugDetails: $debugDetails)
                .padding(24)
                .frame(minWidth: 840, minHeight: 620)
                .toolbar {
                    ToolbarItem(placement: .primaryAction) {
                        SettingsLink {
                            Label(loc(languageCode, "Open Settings", "Einstellungen oeffnen", "Abrir ajustes", "Ouvrir les reglages"), systemImage: "gearshape")
                        }
                    }
                }
        }
    }
}
