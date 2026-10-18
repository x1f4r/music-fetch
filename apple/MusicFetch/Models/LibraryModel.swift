import Foundation
import MusicFetchCore
import Observation

/// Library list: entries grouped by day, filter, search, pin and delete (§6.8).
@Observable
final class LibraryModel {
    enum Filter: String, CaseIterable, Identifiable {
        case all, heard, hummed, links, mixes, pinned

        var id: String { rawValue }

        var title: String {
            switch self {
            case .all: Copy.filterAll
            case .heard: Copy.filterHeard
            case .hummed: Copy.filterHummed
            case .links: Copy.filterLinks
            case .mixes: Copy.filterMixes
            case .pinned: Copy.filterPinned
            }
        }

        var kinds: [SessionKind] {
            switch self {
            case .all, .pinned: []
            case .heard: [.listen, .systemAudio]
            case .hummed: [.hum]
            case .links: [.link, .file, .lyrics]
            case .mixes: [.link, .file]
            }
        }

        func includes(_ entry: LibraryEntry) -> Bool {
            switch self {
            case .mixes: entry.trackCount >= 2
            case .pinned: entry.pinned
            default: kinds.isEmpty || kinds.contains(entry.kind)
            }
        }
    }

    struct Section: Identifiable, Equatable {
        let day: Date
        let title: String
        let entries: [LibraryEntry]
        var id: Date { day }
    }

    /// How many entries one refresh loads.
    static let pageSize: UInt32 = 1000

    var filter: Filter = .all {
        didSet { if filter != oldValue { scheduleRefresh() } }
    }
    var search = "" {
        didSet { if search != oldValue { scheduleRefresh(debounce: true) } }
    }
    private(set) var entries: [LibraryEntry] = []
    private(set) var pinned: [LibraryEntry] = []
    /// The newest session with a single answer (the recent row and the menu bar).
    private(set) var recent: LibraryEntry?
    private(set) var recentResults: [LibraryEntry] = []
    /// Links and files still being analyzed (the sidebar's Library badge).
    private(set) var runningCount = 0
    private(set) var isLoaded = false
    var selection: Set<String> = []
    /// Deleted but still undoable: hidden from every list.
    private(set) var pendingDeletes: Set<String> = []

    @ObservationIgnored private unowned let app: AppModel
    @ObservationIgnored private var refreshTask: Task<Void, Never>?
    @ObservationIgnored private var deleteTimers: [String: Task<Void, Never>] = [:]
    /// Pin changes still on their way to the store; refreshing before they land would
    /// briefly show the old state.
    @ObservationIgnored private var pinWrites = 0

    init(app: AppModel) {
        self.app = app
    }

    var visibleEntries: [LibraryEntry] {
        entries.filter { !pendingDeletes.contains($0.id) && filter.includes($0) }
    }

    var visiblePinned: [LibraryEntry] {
        pinned.filter { !pendingDeletes.contains($0.id) }
    }

    var sections: [Section] {
        Self.group(visibleEntries)
    }

    static func group(_ entries: [LibraryEntry], now: Date = .now, calendar: Calendar = .current) -> [Section] {
        var sections: [Section] = []
        var current: (day: Date, entries: [LibraryEntry])?
        for entry in entries {
            let day = calendar.startOfDay(for: Format.date(milliseconds: entry.createdAtMs))
            if current?.day == day {
                current?.entries.append(entry)
            } else {
                if let current {
                    sections.append(Section(day: current.day, title: Format.dayHeader(current.day, now: now, calendar: calendar),
                                            entries: current.entries))
                }
                current = (day, [entry])
            }
        }
        if let current {
            sections.append(Section(day: current.day, title: Format.dayHeader(current.day, now: now, calendar: calendar),
                                    entries: current.entries))
        }
        return sections
    }

    func entry(id: String) -> LibraryEntry? {
        entries.first { $0.id == id } ?? pinned.first { $0.id == id }
    }

    // MARK: - Loading

    func scheduleRefresh(debounce: Bool = false) {
        refreshTask?.cancel()
        refreshTask = Task { [weak self] in
            if debounce { try? await Task.sleep(for: .milliseconds(200)) }
            guard !Task.isCancelled else { return }
            await self?.refresh()
        }
    }

    func refresh() async {
        guard let engine = app.engine else { return }
        let text = search.trimmingCharacters(in: .whitespacesAndNewlines)
        let query = LibraryQuery(kinds: filter.kinds, search: text.isEmpty ? nil : text, pinnedOnly: filter == .pinned,
                                 limit: Self.pageSize, offset: 0)
        let pinnedQuery = LibraryQuery(kinds: [], search: nil, pinnedOnly: true, limit: 50, offset: 0)
        let recentQuery = LibraryQuery(kinds: [], search: nil, pinnedOnly: false, limit: 30, offset: 0)
        do {
            let (list, pins, latest) = try await offMain {
                (try engine.library(query: query), try engine.library(query: pinnedQuery), try engine.library(query: recentQuery))
            }
            // A pin change still being written refreshes again once it has landed.
            guard !Task.isCancelled, pinWrites == 0 else { return }
            entries = list
            pinned = pins
            let results = latest.filter { $0.track != nil && !pendingDeletes.contains($0.id) }
            recentResults = Array(results.prefix(3))
            recent = results.first
            runningCount = latest.filter { $0.status == .running && !pendingDeletes.contains($0.id) }.count
            isLoaded = true
            selection = selection.filter { id in list.contains { $0.id == id } }
        } catch {
            Log.app.error("Library refresh failed: \(String(describing: error), privacy: .public)")
        }
    }

    // MARK: - Pin and delete

    /// Whether a session is pinned, from the list or an opened session.
    func isPinned(_ id: String) -> Bool {
        entry(id: id)?.pinned ?? app.cachedSession(id)?.pinned ?? false
    }

    func allPinned(_ ids: [String]) -> Bool {
        !ids.isEmpty && ids.allSatisfy(isPinned)
    }

    /// Pins or unpins sessions at once: the lists change right away, the store follows
    /// off the main thread, and Undo restores the previous state (§3.3).
    func setPinned(_ ids: [String], pinned value: Bool, undoManager: UndoManager? = nil) {
        guard let engine = app.engine else { return }
        let changed = ids.filter { isPinned($0) != value }
        guard !changed.isEmpty else { return }
        let set = Set(changed)
        entries = entries.map { entry in
            guard set.contains(entry.id) else { return entry }
            var entry = entry
            entry.pinned = value
            return entry
        }
        if value {
            let added = entries.filter { set.contains($0.id) }
            pinned = (pinned.filter { !set.contains($0.id) } + added).sorted { $0.createdAtMs > $1.createdAtMs }
        } else {
            pinned.removeAll { set.contains($0.id) }
        }
        for id in changed { app.cachedSession(id)?.markPinned(value) }
        undoManager?.registerUndo(withTarget: self) { model in
            MainActor.assumeIsolated { model.setPinned(changed, pinned: !value, undoManager: undoManager) }
        }
        undoManager?.setActionName(value ? Copy.pin : Copy.unpin)
        pinWrites += 1
        Task {
            do {
                try await offMain { for id in changed { try engine.setPinned(id: id, pinned: value) } }
            } catch {
                Log.app.error("Pin failed: \(String(describing: error), privacy: .public)")
            }
            pinWrites -= 1
            if pinWrites == 0 { await refresh() }
        }
    }

    /// Pins all of `ids` when any is unpinned, otherwise unpins them (⌘⇧P, the toolbar,
    /// the inspector, a row's pin).
    func togglePinned(_ ids: [String], undoManager: UndoManager? = nil) {
        setPinned(ids, pinned: !allPinned(ids), undoManager: undoManager)
    }

    /// Mixes and pinned sessions need confirmation before deleting.
    func needsConfirmation(_ ids: [String]) -> Bool {
        ids.contains { id in
            guard let entry = entry(id: id) else { return false }
            return entry.pinned || entry.trackCount >= 2
        }
    }

    /// Hides the sessions now and deletes them for good after 10 s unless undone.
    func delete(_ ids: [String], undoManager: UndoManager?) {
        let ids = ids.filter { !pendingDeletes.contains($0) }
        guard !ids.isEmpty else { return }
        pendingDeletes.formUnion(ids)
        selection.subtract(ids)
        app.sessionsDeleted(ids)
        undoManager?.registerUndo(withTarget: self) { model in
            MainActor.assumeIsolated { model.undoDelete(ids) }
        }
        undoManager?.setActionName(Copy.delete)
        for id in ids {
            deleteTimers[id]?.cancel()
            deleteTimers[id] = Task { [weak self] in
                try? await Task.sleep(for: .seconds(10))
                guard !Task.isCancelled else { return }
                await self?.commitDeletes([id])
            }
        }
    }

    /// Deletes immediately (confirmed deletes, app termination).
    func deleteNow(_ ids: [String]) async {
        pendingDeletes.formUnion(ids)
        selection.subtract(ids)
        app.sessionsDeleted(ids)
        await commitDeletes(ids)
    }

    func undoDelete(_ ids: [String]) {
        for id in ids {
            deleteTimers[id]?.cancel()
            deleteTimers[id] = nil
        }
        pendingDeletes.subtract(ids)
    }

    /// Everything still waiting for its undo window to close.
    func commitPendingDeletesSynchronously() {
        guard let engine = app.engine, !pendingDeletes.isEmpty else { return }
        let ids = Array(pendingDeletes)
        deleteTimers.values.forEach { $0.cancel() }
        deleteTimers.removeAll()
        try? engine.deleteSessions(ids: ids)
    }

    private func commitDeletes(_ ids: [String]) async {
        guard let engine = app.engine else { return }
        for id in ids { deleteTimers[id] = nil }
        do {
            try await offMain { try engine.deleteSessions(ids: ids) }
        } catch {
            Log.app.error("Delete failed: \(String(describing: error), privacy: .public)")
        }
        await refresh()
        pendingDeletes.subtract(ids)
    }
}
