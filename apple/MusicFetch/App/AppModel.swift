import AppKit
import Foundation
import MusicFetchCore
import Observation
import SwiftUI
import UniformTypeIdentifiers

/// Root state: the engine, the per-screen models, navigation and app-wide actions.
@Observable
final class AppModel {
    /// The single instance (App Intents, the Services menu and the dock menu reach it).
    @ObservationIgnored nonisolated(unsafe) static var shared: AppModel!

    /// Sidebar selection.
    enum Route: Hashable {
        case listen
        case library
        case session(String)
    }

    /// Spring roles from DESIGN.md §4.4.
    enum Motion {
        case hero, press, large, color, chrome
    }

    enum PrivacyPane {
        case microphone, systemAudio
    }

    var route: Route? = .listen
    /// Sessions pushed on the Library stack.
    var libraryPath: [String] = []
    var showInspector: Bool
    var linkText = ""
    /// Bumped to move focus into the toolbar link field (⌘K) or library search (⌘F).
    var linkFieldFocusRequest = 0
    var searchFocusRequest = 0

    // Sheets and alerts.
    var lyricSearch: LyricSearchModel?
    var showMelodyPrompt = false
    var microphonePrePromptSource: ListenSource?
    var deleteConfirmation: [String]?
    var exportError: String?

    private(set) var engine: (any MusicEngine)?
    private(set) var engineError: String?
    let isDemo: Bool

    @ObservationIgnored var listen: ListenModel!
    @ObservationIgnored var library: LibraryModel!
    @ObservationIgnored var settings: SettingsModel!
    let preview = PreviewPlayer()
    @ObservationIgnored let notifier = Notifier()

    /// Sessions the app holds a model for (running analyses, opened sessions).
    @ObservationIgnored private var sessions: [String: SessionModel] = [:]
    /// A mix segment waiting for a hum ("Hum it" on an ID row).
    @ObservationIgnored private var segmentHum: (sessionID: String, segmentID: String)?
    @ObservationIgnored private let host = EngineHost()
    @ObservationIgnored private var melodyPromptDeclined = false
    @ObservationIgnored let demo: DemoState?
    /// Scene actions captured from the SwiftUI environment (`openSettings`, `openWindow`).
    @ObservationIgnored var openSettingsAction: (() -> Void)?
    @ObservationIgnored var openMainWindowAction: (() -> Void)?

    init(demo: DemoState? = DemoState.fromLaunchArguments()) {
        self.demo = demo
        isDemo = demo != nil
        showInspector = UserDefaults.standard.object(forKey: PreferenceKey.showInspector) as? Bool ?? true
        listen = ListenModel(app: self)
        library = LibraryModel(app: self)
        settings = SettingsModel(app: self)
        notifier.onOpen = { [weak self] id, openInService in
            self?.openFromNotification(id, openInService: openInService)
        }
        AppModel.shared = self
    }

    /// Creates the engine and loads the first data. Called once at launch.
    func start() async {
        if let starting {
            await starting.value
            return
        }
        let task = Task { await startEngine() }
        starting = task
        await task.value
    }

    @ObservationIgnored private var starting: Task<Void, Never>?

    private func startEngine() async {
        if let demo {
            engine = FakeEngine(scenario: demo.scenario)
        } else {
            let credentials = settings.loadCredentials()
            do {
                engine = try await host.start(credentials: credentials)
            } catch {
                Log.engine.error("Engine failed to start: \(String(describing: error), privacy: .public)")
                engineError = Copy.engineUnavailable
                return
            }
        }
        await library.refresh()
        settings.refreshMelody()
        settings.refreshStorage()
        if let demo { await demo.apply(to: self) }
    }

    // MARK: - Navigation

    /// The Listen pane shows the stage (not a result): its big button is the Listen action.
    var isOnListenStage: Bool {
        route == .listen && listen.shown == nil
    }

    func showListen() {
        route = .listen
        listen.showStage()
    }

    func showLibrary() {
        route = .library
    }

    /// Starts listening from anywhere (toolbar Listen, ⌘L, menus, intents).
    func listenNow(_ source: ListenSource? = nil) {
        route = .listen
        if let source {
            listen.start(source)
        } else if listen.isCapturing {
            listen.stop()
        } else {
            listen.start(listen.source)
        }
    }

    /// Opens a session in the Library stack.
    func openInLibrary(_ id: String) {
        route = .library
        libraryPath = [id]
    }

    /// Shows a stored session on the Listen pane (recent row, notifications, dock menu).
    func showOnListen(_ id: String) {
        Task {
            guard let model = await session(id) else { return }
            route = .listen
            withAnimation(motion(.hero)) { listen.shown = model }
        }
    }

    /// The model for a session: cached, or loaded from the store.
    func session(_ id: String) async -> SessionModel? {
        if let cached = sessions[id] { return cached }
        guard let engine else { return nil }
        do {
            guard let detail = try await offMain({ try engine.session(id: id) }) else { return nil }
            if let cached = sessions[id] { return cached }
            let model = SessionModel(detail: detail)
            sessions[id] = model
            return model
        } catch {
            Log.app.error("Loading a session failed: \(String(describing: error), privacy: .public)")
            return nil
        }
    }

    func cachedSession(_ id: String) -> SessionModel? {
        sessions[id]
    }

    func register(_ model: SessionModel) {
        sessions[model.id] = model
    }

    /// The session whose content is in the detail column (menu commands act on it).
    var currentSession: SessionModel? {
        switch route {
        case .listen: listen.shown
        case .library: libraryPath.last.flatMap { sessions[$0] }
        case .session(let id): sessions[id]
        case nil: nil
        }
    }

    /// The track ⌘C, ⌘↩ and Space act on.
    var selectedTrack: Track? {
        guard let session = currentSession else { return nil }
        switch session.presentation {
        case .result: return session.track
        case .candidates: return session.selectedCandidate?.track
        case .timeline, .collection: return session.selectedSegment?.track
        default: return nil
        }
    }

    // MARK: - Links, files, paste and drop

    /// Identifies a link or file path typed, pasted, dropped or sent from elsewhere.
    func identify(_ input: String, mode: AnalyzeMode = .auto) {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        route = .listen
        linkText = ""
        listen.analyze(trimmed, mode: mode)
    }

    func submitLinkField() {
        guard let input = Self.extractInput(from: linkText) else {
            if !linkText.trimmingCharacters(in: .whitespaces).isEmpty { identify(linkText) }
            return
        }
        identify(input)
    }

    /// ⌘V outside text fields: a URL on the pasteboard starts an analysis at once;
    /// other text goes to the link field.
    func pasteFromPasteboard() {
        let pasteboard = NSPasteboard.general
        if let url = pasteboard.readObjects(forClasses: [NSURL.self])?.first as? URL {
            identify(url.isFileURL ? url.path : url.absoluteString)
            return
        }
        guard let text = pasteboard.string(forType: .string) else { return }
        if let input = Self.extractInput(from: text) {
            identify(input)
        } else {
            linkText = text
            linkFieldFocusRequest += 1
        }
    }

    /// The first web URL or absolute file path in `text`.
    nonisolated static func extractInput(from text: String) -> String? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.hasPrefix("/"), !trimmed.contains("\n") { return trimmed }
        if let url = URL(string: trimmed), url.isFileURL { return url.path }
        guard let detector = try? NSDataDetector(types: NSTextCheckingResult.CheckingType.link.rawValue) else { return nil }
        let range = NSRange(trimmed.startIndex..., in: trimmed)
        for match in detector.matches(in: trimmed, range: range) {
            guard let url = match.url, let scheme = url.scheme?.lowercased() else { continue }
            if scheme == "https" || scheme == "http" { return url.absoluteString }
        }
        return nil
    }

    /// Accepts dropped files, web links and text containing a link.
    func handleDrop(_ providers: [NSItemProvider]) -> Bool {
        guard let provider = providers.first else { return false }
        if provider.hasItemConformingToTypeIdentifier(UTType.fileURL.identifier) {
            _ = provider.loadObject(ofClass: URL.self) { url, _ in
                guard let url else { return }
                let path = url.path
                Task { @MainActor in AppModel.shared.identify(path) }
            }
            return true
        }
        if provider.canLoadObject(ofClass: URL.self) {
            _ = provider.loadObject(ofClass: URL.self) { url, _ in
                guard let url else { return }
                let text = url.absoluteString
                Task { @MainActor in AppModel.shared.identify(text) }
            }
            return true
        }
        if provider.canLoadObject(ofClass: String.self) {
            _ = provider.loadObject(ofClass: String.self) { text, _ in
                guard let text, let input = AppModel.extractInput(from: text) else { return }
                Task { @MainActor in AppModel.shared.identify(input) }
            }
            return true
        }
        return false
    }

    /// `musicfetch://analyze?url=<link>`.
    func handleURL(_ url: URL) {
        guard url.scheme == "musicfetch" else {
            if url.isFileURL { identify(url.path) }
            return
        }
        let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        switch url.host() {
        case "analyze", "identify":
            if let link = components?.queryItems?.first(where: { $0.name == "url" })?.value { identify(link) }
        case "listen":
            listenNow(.microphone)
        case "hum":
            listenNow(.hum)
        default:
            break
        }
    }

    func openFile() {
        let panel = NSOpenPanel()
        panel.allowedContentTypes = [.audio, .movie, .audiovisualContent]
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        guard panel.runModal() == .OK, let url = panel.url else { return }
        identify(url.path)
    }

    // MARK: - Session results

    /// A session ended: refresh the library, store details, notify in the background.
    func sessionFinished(_ model: SessionModel) {
        register(model)
        library.scheduleRefresh()
        settings.refreshStorage()
        Task {
            guard let engine, let detail = try? await offMain({ try engine.session(id: model.id) }) else { return }
            model.apply(detail: detail)
        }
        notifyIfInBackground(model)
    }

    /// Events of analyses that left the stage.
    func backgroundEvent(_ event: SessionEvent, model: SessionModel) {
        switch event {
        case .finished, .failed, .canceled: sessionFinished(model)
        default: break
        }
    }

    private func notifyIfInBackground(_ model: SessionModel) {
        guard !NSApp.isActive, UserDefaults.standard.bool(forKey: PreferenceKey.notifyInBackground), !isDemo else { return }
        let title: String
        let body: String
        var action: String?
        switch model.presentation {
        case .result:
            guard let track = model.track else { return }
            title = Copy.notificationTitle(track)
            body = Format.meta(track) ?? model.source?.title ?? ""
            action = LinkOpener.preferredLink(in: track.links, defaultService: defaultService).map { Copy.openIn($0.service) }
        case .candidates:
            guard let best = model.candidates.first else { return }
            title = Copy.notificationTitle(best.track)
            body = Copy.possibleMatches(model.candidates)
        case .timeline, .collection:
            title = model.title
            body = model.statusText
        default:
            return
        }
        Task {
            _ = await notifier.requestAuthorization()
            await notifier.post(sessionID: model.id, title: title, body: body, actionTitle: action)
        }
    }

    private func openFromNotification(_ id: String, openInService: Bool) {
        NSApp.activate()
        Task {
            guard let model = await session(id) else { return }
            if openInService, let track = model.track {
                LinkOpener.openPreferred(track, defaultService: defaultService)
            } else if model.presentation == .timeline || model.presentation == .collection {
                openInLibrary(id)
            } else {
                showOnListen(id)
            }
        }
    }

    func sessionsDeleted(_ ids: [String]) {
        for id in ids {
            sessions[id] = nil
            if listen.shown?.id == id { listen.shown = nil }
            if route == .session(id) { route = .library }
        }
        libraryPath.removeAll { ids.contains($0) }
    }

    /// "This is it" on a candidate.
    func confirm(_ candidate: Candidate, in model: SessionModel) {
        guard let engine else { return }
        do {
            try engine.confirmCandidate(sessionID: model.id, track: candidate.track)
        } catch {
            Log.app.error("Confirm failed: \(String(describing: error), privacy: .public)")
        }
        withAnimation(motion(.hero)) { model.confirm(candidate.track) }
        AccessibilityNotification.Announcement(Copy.found(candidate.track)).post()
        if let pending = segmentHum {
            segmentHum = nil
            correct(segmentID: pending.segmentID, in: pending.sessionID, with: candidate.track)
        }
        library.scheduleRefresh()
    }

    /// Confirms a mix segment ("✓") or replaces its track with an alternate.
    func correct(segmentID: String, in sessionID: String, with track: Track) {
        guard let engine else { return }
        do {
            let segment = try engine.correctSegment(sessionID: sessionID, segmentID: segmentID, track: track)
            sessions[sessionID]?.replace(segment)
        } catch {
            Log.app.error("Correction failed: \(String(describing: error), privacy: .public)")
        }
        library.scheduleRefresh()
    }

    /// "Hum it" on an unidentified segment: the confirmed hum fills the segment.
    func humSegment(_ segment: Segment, in model: SessionModel) {
        segmentHum = (model.id, segment.id)
        listenNow(.hum)
    }

    // MARK: - Export and copy

    func export(_ model: SessionModel, format: ExportFormat) {
        guard let engine else { return }
        let id = model.id
        let title = model.title
        Task {
            do {
                let text = try await offMain { try engine.exportSession(id: id, format: format) }
                let panel = NSSavePanel()
                panel.nameFieldStringValue = Self.exportFileName(title, format: format)
                panel.allowedContentTypes = [Self.contentType(format)]
                panel.canCreateDirectories = true
                guard panel.runModal() == .OK, let url = panel.url else { return }
                try text.write(to: url, atomically: true, encoding: .utf8)
            } catch {
                Log.app.error("Export failed: \(String(describing: error), privacy: .public)")
                exportError = Copy.exportFailed
            }
        }
    }

    /// ⌘E: a save panel with a format popup.
    func exportWithPanel(_ model: SessionModel) {
        guard let engine else { return }
        let panel = NSSavePanel()
        let formats = ExportFormat.menuOrder
        let popup = NSPopUpButton(frame: .zero, pullsDown: false)
        popup.addItems(withTitles: formats.map(Copy.formatName))
        let label = NSTextField(labelWithString: Copy.formatLabel)
        let accessory = NSStackView(views: [label, popup])
        accessory.edgeInsets = NSEdgeInsets(top: 10, left: 10, bottom: 10, right: 10)
        panel.accessoryView = accessory
        panel.canCreateDirectories = true
        panel.nameFieldStringValue = Self.exportFileName(model.title, format: .text)
        panel.allowedContentTypes = [Self.contentType(.text)]
        let observer = PopupObserver { index in
            let format = formats[index]
            panel.allowedContentTypes = [Self.contentType(format)]
            let base = (panel.nameFieldStringValue as NSString).deletingPathExtension
            panel.nameFieldStringValue = "\(base).\(Self.fileExtension(format))"
        }
        popup.target = observer
        popup.action = #selector(PopupObserver.changed(_:))
        let response = withExtendedLifetime(observer) { panel.runModal() }
        guard response == .OK, let url = panel.url else { return }
        let format = formats[max(popup.indexOfSelectedItem, 0)]
        let id = model.id
        Task {
            do {
                let text = try await offMain { try engine.exportSession(id: id, format: format) }
                try text.write(to: url, atomically: true, encoding: .utf8)
            } catch {
                Log.app.error("Export failed: \(String(describing: error), privacy: .public)")
                exportError = Copy.exportFailed
            }
        }
    }

    func copyTracklist(_ model: SessionModel) {
        guard let engine else { return }
        let id = model.id
        Task {
            guard let text = try? await offMain({ try engine.exportSession(id: id, format: .text) }) else { return }
            NSPasteboard.general.clearContents()
            NSPasteboard.general.setString(text, forType: .string)
        }
    }

    func copy(_ track: Track) {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(Format.artistTitle(track), forType: .string)
    }

    nonisolated static func exportFileName(_ title: String, format: ExportFormat) -> String {
        let cleaned = title.components(separatedBy: CharacterSet(charactersIn: "/:\\?%*|\"<>")).joined(separator: "-")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let base = cleaned.isEmpty ? "Tracklist" : String(cleaned.prefix(80))
        return "\(base).\(fileExtension(format))"
    }

    nonisolated static func fileExtension(_ format: ExportFormat) -> String {
        switch format {
        case .text: "txt"
        case .csv: "csv"
        case .json: "json"
        case .cue: "cue"
        }
    }

    nonisolated static func contentType(_ format: ExportFormat) -> UTType {
        switch format {
        case .text: .plainText
        case .csv: .commaSeparatedText
        case .json: .json
        case .cue: UTType(filenameExtension: "cue") ?? .plainText
        }
    }

    // MARK: - Delete

    /// Deletes now with undo, or asks first for mixes and pinned sessions.
    func requestDelete(_ ids: [String], undoManager: UndoManager?) {
        guard !ids.isEmpty else { return }
        if library.needsConfirmation(ids) {
            deleteConfirmation = ids
        } else {
            library.delete(ids, undoManager: undoManager)
        }
    }

    func confirmDelete() {
        guard let ids = deleteConfirmation else { return }
        deleteConfirmation = nil
        Task { await library.deleteNow(ids) }
    }

    // MARK: - Sheets and prompts

    func presentLyricSearch() {
        lyricSearch = LyricSearchModel(app: self)
    }

    func presentMicrophonePrePrompt(for source: ListenSource) {
        microphonePrePromptSource = source
    }

    /// `Notice.melodyIndexMissing`: offer the download once per launch.
    func promptForMelodyLibrary() {
        guard !melodyPromptDeclined, !settings.isMelodyInstalled else { return }
        if case .downloading = settings.melodyState { return }
        showMelodyPrompt = true
    }

    func declineMelodyLibrary() {
        melodyPromptDeclined = true
        showMelodyPrompt = false
    }

    func openPrivacySettings(_ pane: PrivacyPane) {
        let anchor = pane == .microphone ? "Privacy_Microphone" : "Privacy_AudioCapture"
        if let url = URL(string: "x-apple.systempreferences:com.apple.preference.security?\(anchor)") {
            NSWorkspace.shared.open(url)
        }
    }

    // MARK: - Preferences

    var defaultService: Service {
        let raw = UserDefaults.standard.string(forKey: PreferenceKey.defaultService) ?? ServiceKey.spotify.rawValue
        return (ServiceKey(rawValue: raw) ?? .spotify).service
    }

    func setInspector(_ shown: Bool) {
        showInspector = shown
        UserDefaults.standard.set(shown, forKey: PreferenceKey.showInspector)
    }

    var reduceMotion: Bool {
        NSWorkspace.shared.accessibilityDisplayShouldReduceMotion
    }

    /// The spring for `role`, or a 150 ms crossfade with Reduce Motion.
    func motion(_ role: Motion) -> Animation {
        if reduceMotion { return .easeInOut(duration: 0.15) }
        return Self.spring(role)
    }

    nonisolated static func spring(_ role: Motion) -> Animation {
        switch role {
        case .hero: .spring(response: 0.32, dampingFraction: 0.8)
        case .press: .spring(response: 0.22, dampingFraction: 0.6)
        case .large: .spring(response: 0.44, dampingFraction: 0.8)
        case .color: .spring(response: 0.16, dampingFraction: 1)
        case .chrome: .spring(response: 0.24, dampingFraction: 0.9)
        }
    }

    // MARK: - Termination

    func prepareForTermination() {
        library.commitPendingDeletesSynchronously()
        listen.cancel()
        preview.stop()
    }
}
