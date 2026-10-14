import Foundation
import MusicFetchCore
import Observation
import SwiftUI

/// The typed lyric search sheet (§6.7).
@Observable
final class LyricSearchModel: Identifiable {
    let id = UUID()
    var text = ""
    private(set) var session: SessionModel?
    private(set) var error: String?
    var selection: String?

    @ObservationIgnored private unowned let app: AppModel

    init(app: AppModel) {
        self.app = app
    }

    var isSearching: Bool { session?.isRunning ?? false }

    var results: [Candidate] { session?.candidates ?? [] }

    /// True when a search finished without results.
    var isEmpty: Bool {
        guard let session, !session.isRunning else { return false }
        return session.candidates.isEmpty && session.failure == nil
    }

    func search() {
        guard let engine = app.engine else { return }
        let query = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return }
        session?.cancel()
        let bridge = ObserverBridge()
        do {
            let handle = try engine.searchLyrics(text: query, observer: bridge)
            let model = SessionModel(id: handle.sessionID, kind: .lyrics, isRunning: true)
            model.onEvent = { [weak app] model, event in
                if case .failed = event { app?.sessionFinished(model) }
                if case .finished = event { app?.sessionFinished(model) }
            }
            model.attach(handle, events: bridge.events)
            app.register(model)
            session = model
            error = nil
            selection = nil
        } catch let failure as MfError {
            bridge.close()
            error = Copy.failure(failure.code, message: failure.message)
        } catch {
            bridge.close()
            self.error = Copy.failure(.internal, message: nil)
        }
    }

    /// Clears the results so the user can type different words.
    func reset() {
        session?.cancel()
        session = nil
        error = nil
        selection = nil
    }

    /// Picking a result shows it as the matched track.
    func choose(_ candidate: Candidate) {
        guard let session else { return }
        app.confirm(candidate, in: session)
        app.route = .listen
        withAnimation(app.motion(.hero)) { app.listen.shown = session }
        app.lyricSearch = nil
    }

    func cancel() {
        if session?.isRunning == true { session?.cancel() }
        app.lyricSearch = nil
    }
}
