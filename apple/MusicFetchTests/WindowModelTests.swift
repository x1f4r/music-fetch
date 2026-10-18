import Foundation
import MusicFetchCore
import Testing

@testable import MusicFetch

/// Pinning many sessions, the selection pin targets, method switching on the stage
/// and the inspector folding away in narrow windows, on the fake engine.
@MainActor
struct WindowModelTests {
    private func makeApp() async -> AppModel {
        let app = AppModel(demo: DemoState(name: "tests"))
        await app.start()
        return app
    }

    /// Waits for the engine's pinned sessions to settle on `expected`.
    private func enginePins(_ app: AppModel, settleOn expected: Set<String>) async throws -> Set<String> {
        let query = LibraryQuery(kinds: [], search: nil, pinnedOnly: true, limit: 50, offset: 0)
        var pins: Set<String> = []
        for _ in 0..<50 {
            pins = Set(try app.engine!.library(query: query).map(\.id))
            if pins == expected { break }
            try await Task.sleep(for: .milliseconds(20))
        }
        return pins
    }

    @Test func pinsManySessionsAtOnceWithUndo() async throws {
        let app = await makeApp()
        let library = app.library!
        let ids = [DemoFixtures.resultID, DemoFixtures.humID]
        #expect(!library.isPinned(DemoFixtures.resultID))
        let undo = UndoManager()
        undo.groupsByEvent = false

        undo.beginUndoGrouping()
        library.setPinned(ids, pinned: true, undoManager: undo)
        undo.endUndoGrouping()
        // The lists change before the store does.
        #expect(library.allPinned(ids))
        #expect(Set(library.pinned.map(\.id)).isSuperset(of: ids))
        let pinnedAll: Set = [DemoFixtures.mixID, DemoFixtures.nightsID, DemoFixtures.resultID, DemoFixtures.humID]
        #expect(try await enginePins(app, settleOn: pinnedAll) == pinnedAll)

        undo.undo()
        #expect(!library.isPinned(DemoFixtures.resultID))
        #expect(!library.isPinned(DemoFixtures.humID))
        #expect(!library.pinned.contains { ids.contains($0.id) })
        let pinnedBefore: Set = [DemoFixtures.mixID, DemoFixtures.nightsID]
        #expect(try await enginePins(app, settleOn: pinnedBefore) == pinnedBefore)
    }

    @Test func toggleOnAMixedSelectionPinsAllThenUnpinsAll() async throws {
        let app = await makeApp()
        let library = app.library!
        let ids = [DemoFixtures.resultID, DemoFixtures.mixID]
        #expect(!library.allPinned(ids))
        library.togglePinned(ids)
        #expect(library.allPinned(ids))
        library.togglePinned(ids)
        #expect(!library.isPinned(DemoFixtures.resultID))
        #expect(!library.isPinned(DemoFixtures.mixID))
        #expect(!library.allPinned([]))
    }

    @Test func pinnedFilterListsOnlyPinnedSessions() async {
        let app = await makeApp()
        let library = app.library!
        library.filter = .pinned
        await library.refresh()
        #expect(Set(library.visibleEntries.map(\.id)) == [DemoFixtures.mixID, DemoFixtures.nightsID])
    }

    @Test func pinTargetsFollowTheSelectionOrTheSessionOnScreen() async {
        let app = await makeApp()
        app.route = .library
        app.library.selection = [DemoFixtures.humID, DemoFixtures.resultID]
        #expect(app.targetSessionIDs == [DemoFixtures.resultID, DemoFixtures.humID].sorted())
        app.library.selection = []
        #expect(app.targetSessionIDs.isEmpty)

        let model = await app.session(DemoFixtures.nightsID)
        app.route = .listen
        app.listen.shown = model
        #expect(app.targetSessionIDs == [DemoFixtures.nightsID])
    }

    @Test func sourceTogglesChooseWithoutListeningAndLeaveFinishedStates() async {
        let app = await makeApp()
        let listen = app.listen!
        listen.showForDemo(.noMatch(.notFound))
        #expect(listen.recoveryActions == [.tryAgain, .humIt, .typeLyric])
        listen.select(.hum)
        #expect(listen.source == .hum)
        #expect(listen.stage == .idle)
        #expect(listen.recoveryActions.isEmpty)

        listen.showForDemo(.failed(.network, nil))
        listen.select(.thisMac)
        #expect(listen.stage == .idle)
        #expect(listen.source == .thisMac)

        // While a session runs the toggles do nothing.
        listen.start(.microphone)
        #expect(listen.stage == .listening)
        listen.select(.hum)
        #expect(listen.source == .microphone)
        listen.cancel()
    }

    @Test func humItIsOfferedOnlyAfterSomethingElse() async {
        let app = await makeApp()
        let listen = app.listen!
        listen.start(.hum)
        listen.cancel()
        listen.showForDemo(.noMatch(.notFound))
        #expect(listen.recoveryActions == [.tryAgain, .typeLyric])
        listen.start(.microphone)
        listen.cancel()
        listen.showForDemo(.noMatch(.tooQuiet))
        #expect(listen.recoveryActions == [.tryAgain, .humIt])
    }

    @Test func tryAgainAnalyzesALinkAgainInsteadOfListening() async {
        let app = await makeApp()
        let listen = app.listen!
        listen.analyze("https://example.com/watch?v=demo")
        #expect(listen.stage == .link)
        listen.showForDemo(.noMatch(.notFound))
        listen.perform(.tryAgain)
        #expect(listen.stage == .link)
        #expect(!listen.isCapturing)
        listen.perform(.stop)
    }

    @Test func listenInTheSidebarLeavesAResultForTheStage() async {
        let app = await makeApp()
        let model = await app.session(DemoFixtures.resultID)
        app.route = .listen
        app.listen.shown = model
        #expect(!app.isOnListenStage)
        app.selectRoute(.listen)
        #expect(app.listen.shown == nil)
        #expect(app.isOnListenStage)

        app.listen.shown = model
        app.selectRoute(.library)
        #expect(app.route == .library)
        // Coming back through the sidebar shows the stage too.
        app.selectRoute(.listen)
        #expect(app.isOnListenStage)
    }

    @Test func inspectorFoldsAwayInNarrowWindowsWithoutChangingThePreference() async throws {
        let app = await makeApp()
        app.setInspector(true)
        app.windowWidthChanged(1180, sidebarShown: true)
        try await Task.sleep(for: .milliseconds(250))
        #expect(app.isInspectorPresented)

        app.windowWidthChanged(960, sidebarShown: true)
        try await Task.sleep(for: .milliseconds(250))
        #expect(!app.isInspectorPresented)
        #expect(app.showInspector)

        // Without the sidebar there is room again.
        app.windowWidthChanged(nil, sidebarShown: false)
        try await Task.sleep(for: .milliseconds(250))
        #expect(app.isInspectorPresented)

        // Opened by hand in a narrow window, it stays open.
        app.windowWidthChanged(960, sidebarShown: true)
        try await Task.sleep(for: .milliseconds(250))
        #expect(!app.isInspectorPresented)
        app.toggleInspector()
        #expect(app.isInspectorPresented)
        app.windowWidthChanged(950, sidebarShown: true)
        try await Task.sleep(for: .milliseconds(250))
        #expect(app.isInspectorPresented)

        app.windowWidthChanged(1300, sidebarShown: true)
        try await Task.sleep(for: .milliseconds(250))
        app.toggleInspector()
        #expect(!app.isInspectorPresented)
        #expect(!app.showInspector)
    }
}
