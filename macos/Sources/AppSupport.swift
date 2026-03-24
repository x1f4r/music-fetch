import Foundation
import SwiftUI

let backendCommandDefaultsKey = "musicFetch.backendCommand"
let recentAnalysesDefaultsKey = "musicFetch.recentAnalyses"
let openExternalLinksDefaultsKey = "musicFetch.openExternalLinks"
let languageDefaultsKey = "musicFetch.language"
let launchAtLoginDefaultsKey = "musicFetch.launchAtLogin"
let analysisModeDefaultsKey = "musicFetch.analysisMode"
let recallProfileDefaultsKey = "musicFetch.recallProfile"
let metadataHintsDefaultsKey = "musicFetch.metadataHints"
let repeatDetectionDefaultsKey = "musicFetch.repeatDetection"
let preferSeparationDefaultsKey = "musicFetch.preferSeparation"
let preferredInputDefaultsKey = "musicFetch.preferredInput"
let recordingTargetDefaultsKey = "musicFetch.recordingTarget"
let debugDetailsDefaultsKey = "musicFetch.debugDetails"
let settingsTabDefaultsKey = "musicFetch.settingsTab"

func defaultUILanguageCode() -> String {
    let preferred = Locale.preferredLanguages.first?.lowercased() ?? "en"
    if preferred.hasPrefix("de") { return "de" }
    if preferred.hasPrefix("es") { return "es" }
    if preferred.hasPrefix("fr") { return "fr" }
    return "en"
}

enum UILanguage: String, CaseIterable {
    case en
    case de
    case es
    case fr

    var label: String {
        switch self {
        case .en: return "English"
        case .de: return "Deutsch"
        case .es: return "Espanol"
        case .fr: return "Francais"
        }
    }
}

func loc(_ code: String, _ en: String, _ de: String, _ es: String, _ fr: String) -> String {
    switch UILanguage(rawValue: code) ?? .en {
    case .en: return en
    case .de: return de
    case .es: return es
    case .fr: return fr
    }
}

extension Notification.Name {
    static let musicFetchAnalyze = Notification.Name("musicFetch.analyze")
    static let musicFetchFocusInput = Notification.Name("musicFetch.focusInput")
    static let musicFetchShowDiagnostics = Notification.Name("musicFetch.showDiagnostics")
}

struct DoctorCheck: Codable, Identifiable {
    var id: String { name }
    let name: String
    let ok: Bool
    let detail: String
}

struct TrackMatchPayload: Codable, Hashable {
    let title: String
    let artist: String?
    let album: String?
    let isrc: String?
    let provider_ids: [String: String]
    let external_links: [String: String]
}

struct SegmentPayload: Codable, Identifiable, Hashable {
    let source_item_id: String
    let start_ms: Int
    let end_ms: Int
    let kind: String?
    let confidence: Double
    let providers: [String]
    let evidence_count: Int
    let track: TrackMatchPayload?
    let alternates: [TrackMatchPayload]
    let repeat_group_id: String?
    let probe_count: Int?
    let provider_attempts: Int?
    let metadata_hints: [String]?
    let uncertainty: Double?
    let explanation: [String]?

    var id: String { "\(source_item_id)-\(start_ms)-\(end_ms)-\(track?.title ?? kind ?? "segment")" }
}

struct ItemPayload: Codable, Identifiable {
    let id: String
    let input_value: String
    let status: String
    let error: String?
}

struct JobPayload: Codable, Hashable {
    let id: String
    let status: String
    let created_at: String
    let updated_at: String
    let error: String?
}

struct JobEventPayload: Codable, Hashable, Identifiable {
    let id: Int
    let job_id: String
    let level: String
    let message: String
    let created_at: String
}

struct AnalyzeResponse: Codable {
    let job: JobPayload
    let items: [ItemPayload]
    let segments: [SegmentPayload]
    let events: [JobEventPayload]?
}

struct LibraryEnvelope: Codable {
    let entries: [LibraryEntryPayload]
}

struct StorageEnvelope: Codable {
    let storage: StorageSummaryPayload
}

struct PinResponse: Codable {
    let job_id: String
    let pinned: Bool
}

struct CancelResponse: Codable {
    let job_id: String
    let status: String
}

struct RetryResponse: Codable {
    let job_id: String
    let retried_segments: Int
    let matched_segments: Int
    let remaining_unresolved_segments: Int
}

struct ExportResponse: Codable {
    let job_id: String
    let format: String
    let filename: String
    let content: String
}

struct SegmentCorrectionRequest: Codable {
    let source_item_id: String
    let start_ms: Int
    let end_ms: Int
    let title: String
    let artist: String?
    let album: String?
}

struct SegmentCorrectionResponse: Codable {
    let job_id: String
    let segment: SegmentPayload
}

struct RetrySegmentsRequest: Codable {
    let source_item_id: String?
    let options: BackendJobOptions?
}

struct InstallResponse: Codable {
    let installed: [String]
    let skipped: [String]
    let failed: [String]
    let checks: [DoctorCheck]
}

struct LibraryEntryPayload: Codable, Identifiable, Hashable {
    let job_id: String
    let title: String
    let input_value: String
    let status: String
    let created_at: String
    let updated_at: String
    let item_count: Int
    let segment_count: Int
    let matched_count: Int
    let pinned: Bool
    let artifact_size_bytes: Int

    var id: String { job_id }
}

struct ArtifactCategorySummaryPayload: Codable, Hashable {
    let category: String
    let count: Int
    let size_bytes: Int
}

struct ArtifactEntryPayload: Codable, Identifiable, Hashable {
    let id: String
    let category: String
    let label: String
    let path: String
    let size_bytes: Int
    let exists: Bool
    let temporary: Bool
    let job_id: String?
    let source_item_id: String?
    let pinned: Bool
}

struct StorageSummaryPayload: Codable, Hashable {
    let job_id: String?
    let auto_clean: Bool
    let total_size_bytes: Int
    let categories: [ArtifactCategorySummaryPayload]
    let entries: [ArtifactEntryPayload]
    let locations: [String: String]
}

enum WorkspaceSection: String, CaseIterable, Identifiable {
    case analyze
    case library
    case storage

    var id: String { rawValue }

    var icon: String {
        switch self {
        case .analyze:
            return "waveform.and.magnifyingglass"
        case .library:
            return "books.vertical.fill"
        case .storage:
            return "internaldrive.fill"
        }
    }

    func title(_ code: String) -> String {
        switch self {
        case .analyze:
            return loc(code, "Analyze", "Analysieren", "Analizar", "Analyser")
        case .library:
            return loc(code, "Library", "Bibliothek", "Biblioteca", "Bibliotheque")
        case .storage:
            return loc(code, "Storage", "Speicher", "Almacenamiento", "Stockage")
        }
    }

    func subtitle(_ code: String) -> String {
        switch self {
        case .analyze:
            return loc(code, "Capture, ingest, and inspect results.", "Aufnehmen, importieren und Ergebnisse pruefen.", "Captura, importa y revisa resultados.", "Capturez, importez et inspectez les resultats.")
        case .library:
            return loc(code, "Browse finished runs and reopen timelines.", "Abgeschlossene Laeufe durchsuchen und Timelines erneut oeffnen.", "Explora analisis terminados y reabre timelines.", "Parcourez les analyses terminees et reouvrez les timelines.")
        case .storage:
            return loc(code, "Control temporary artifacts and cleanup.", "Temporare Artefakte und Cleanup steuern.", "Controla artefactos temporales y limpieza.", "Controlez les artefacts temporaires et le nettoyage.")
        }
    }
}

enum PreferredInput: String, CaseIterable, Identifiable {
    case link
    case file
    case microphone
    case system

    var id: String { rawValue }

    var systemImage: String {
        switch self {
        case .link: return "link"
        case .file: return "doc.fill"
        case .microphone: return "mic.fill"
        case .system: return "waveform"
        }
    }

    func title(_ code: String) -> String {
        switch self {
        case .link:
            return loc(code, "Link", "Link", "Enlace", "Lien")
        case .file:
            return loc(code, "File", "Datei", "Archivo", "Fichier")
        case .microphone:
            return loc(code, "Mic", "Mikro", "Mic", "Micro")
        case .system:
            return loc(code, "System", "System", "Sistema", "Systeme")
        }
    }
}

enum RecordingTarget: String, CaseIterable, Identifiable {
    case microphone
    case system

    var id: String { rawValue }

    var systemImage: String {
        switch self {
        case .microphone: return "mic.fill"
        case .system: return "waveform"
        }
    }

    func title(_ code: String) -> String {
        switch self {
        case .microphone:
            return loc(code, "Microphone", "Mikrofon", "Microfono", "Micro")
        case .system:
            return loc(code, "System Audio", "Systemaudio", "Audio del sistema", "Audio systeme")
        }
    }
}

enum AnalysisMode: String, CaseIterable, Identifiable {
    case auto
    case singleTrack = "single_track"
    case playlistEntry = "playlist_entry"
    case longMix = "long_mix"

    var id: String { rawValue }

    func title(_ code: String) -> String {
        switch self {
        case .auto:
            return loc(code, "Auto", "Auto", "Auto", "Auto")
        case .singleTrack:
            return loc(code, "Single Track", "Einzeltitel", "Pista unica", "Titre unique")
        case .playlistEntry:
            return loc(code, "Playlist Entry", "Playlist-Eintrag", "Entrada de lista", "Entree de playlist")
        case .longMix:
            return loc(code, "Long Mix", "Langer Mix", "Mezcla larga", "Long mix")
        }
    }
}

enum RecallProfile: String, CaseIterable, Identifiable {
    case maxRecall = "max_recall"
    case balanced
    case fastFirst = "fast_first"

    var id: String { rawValue }

    func title(_ code: String) -> String {
        switch self {
        case .maxRecall:
            return loc(code, "Max Recall", "Max Recall", "Max Recall", "Max Recall")
        case .balanced:
            return loc(code, "Balanced", "Ausgewogen", "Balanceado", "Equilibre")
        case .fastFirst:
            return loc(code, "Fast First", "Schnell zuerst", "Rapido primero", "Rapide d'abord")
        }
    }
}

enum SettingsTab: String, CaseIterable, Identifiable {
    case general
    case input
    case recognition
    case connections
    case diagnostics

    var id: String { rawValue }

    var systemImage: String {
        switch self {
        case .general: return "gearshape"
        case .input: return "square.and.arrow.down"
        case .recognition: return "waveform.path.ecg"
        case .connections: return "link"
        case .diagnostics: return "stethoscope"
        }
    }

    func title(_ code: String) -> String {
        switch self {
        case .general:
            return loc(code, "General", "Allgemein", "General", "General")
        case .input:
            return loc(code, "Input", "Eingabe", "Entrada", "Entree")
        case .recognition:
            return loc(code, "Recognition", "Erkennung", "Reconocimiento", "Reconnaissance")
        case .connections:
            return loc(code, "Connections", "Verbindungen", "Conexiones", "Connexions")
        case .diagnostics:
            return loc(code, "Diagnostics", "Diagnostik", "Diagnostico", "Diagnostic")
        }
    }
}

struct ShellRoutingState: Equatable {
    var workspace: WorkspaceSection = .analyze
    var libraryJobID: String?
    var storageJobID: String?
}

struct PlatformLink: Identifiable, Hashable {
    let id: String
    let label: String
    let icon: String
    let color: Color
    let url: URL
}

struct RecentAnalysis: Codable, Identifiable, Hashable {
    let id: String
    let input: String
    let title: String
    let status: String
    let segmentCount: Int
    let createdAt: Date
}

enum AppViewState: Equatable {
    case idle
    case analyzing(String)
    case recordingMic
    case recordingSystem
    case showingResults
    case error(String)
}

enum CaptureState: Equatable {
    case idle
    case startingMic
    case recordingMic
    case stoppingMic
    case startingSystem
    case recordingSystem
    case stoppingSystem

    var isBusy: Bool {
        self != .idle
    }
}

struct SegmentViewModel: Identifiable, Hashable {
    let id: String
    let payload: SegmentPayload
    let title: String
    let subtitle: String
    let detailLabel: String
    let qualityLabel: String?
    let accentColor: Color
    let timelineColor: Color
    let statusBadge: String?
    let metadataHint: String?
    let primaryLinks: [PlatformLink]
    let overflowLinks: [PlatformLink]
    let isInteractive: Bool
    let repeatGroupID: String?

    var startMs: Int { payload.start_ms }
    var endMs: Int { payload.end_ms }
}

func formatBytes(_ size: Int) -> String {
    ByteCountFormatter.string(fromByteCount: Int64(size), countStyle: .file)
}

func formatTime(_ milliseconds: Int) -> String {
    let totalSeconds = milliseconds / 1000
    let hours = totalSeconds / 3600
    let minutes = (totalSeconds % 3600) / 60
    let seconds = totalSeconds % 60
    if hours > 0 {
        return String(format: "%02d:%02d:%02d", hours, minutes, seconds)
    }
    return String(format: "%02d:%02d", minutes, seconds)
}

struct StudioPanel<Content: View>: View {
    let padding: CGFloat
    @ViewBuilder let content: Content

    init(padding: CGFloat = 18, @ViewBuilder content: () -> Content) {
        self.padding = padding
        self.content = content()
    }

    var body: some View {
        content
            .padding(padding)
            .background(
                RoundedRectangle(cornerRadius: 22, style: .continuous)
                    .fill(
                        LinearGradient(
                            colors: [
                                Color.white.opacity(0.13),
                                Color.white.opacity(0.045),
                            ],
                            startPoint: .topLeading,
                            endPoint: .bottomTrailing
                        )
                    )
            )
            .overlay(
                RoundedRectangle(cornerRadius: 22, style: .continuous)
                    .strokeBorder(Color.white.opacity(0.1))
            )
    }
}
