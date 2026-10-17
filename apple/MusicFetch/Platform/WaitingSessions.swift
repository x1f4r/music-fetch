import Foundation
import MusicFetchCore
import Network

/// Identifies sessions saved while offline (status `Waiting`) whenever a network path
/// becomes available: at launch and after every reconnect (`Engine.retryWaiting`).
/// Results land in the library and, in the background, as notifications.
final class WaitingSessions {
    private unowned let app: AppModel
    private let monitor = NWPathMonitor()
    private let observer = RetryObserver()
    private var isOnline = false
    /// Retried sessions that have not finished yet.
    private var pending: Set<String> = []
    private var consumer: Task<Void, Never>?

    init(app: AppModel) {
        self.app = app
    }

    func start(engine: any MusicEngine) {
        guard consumer == nil else { return }
        let events = observer.events
        consumer = Task { [weak self] in
            for await event in events {
                self?.handle(event)
            }
        }
        monitor.pathUpdateHandler = { [weak self] path in
            let online = path.status == .satisfied
            Task { @MainActor in self?.update(online: online, engine: engine) }
        }
        monitor.start(queue: DispatchQueue(label: "local.musicfetch.network", qos: .utility))
    }

    private func update(online: Bool, engine: any MusicEngine) {
        let cameOnline = online && !isOnline
        isOnline = online
        guard cameOnline else { return }
        let observer = observer
        Task {
            do {
                let count = try await offMain { try engine.retryWaiting(observer: observer) }
                guard count > 0 else { return }
                Log.engine.info("Identifying \(count, privacy: .public) sessions saved while offline")
                app.library.scheduleRefresh()
            } catch {
                Log.engine.error("Retrying saved sessions failed: \(String(describing: error), privacy: .public)")
            }
        }
    }

    private func handle(_ event: SessionEvent) {
        switch event {
        case .started(let id, _):
            pending.insert(id)
            app.library.scheduleRefresh()
        case .finished, .failed, .canceled:
            resolveFinished()
        default:
            break
        }
    }

    /// Terminal events carry no session id, so the stored status tells which retried
    /// sessions are done.
    private func resolveFinished() {
        let ids = pending
        Task {
            guard let engine = app.engine else { return }
            for id in ids {
                guard let detail = try? await offMain({ try engine.session(id: id) }),
                    detail.entry.status != .running, detail.entry.status != .waiting
                else { continue }
                pending.remove(id)
                if let model = await app.session(id) {
                    model.apply(detail: detail)
                    app.sessionFinished(model)
                }
            }
            app.library.scheduleRefresh()
        }
    }
}

/// One observer for every retried session; never finishes.
nonisolated final class RetryObserver: SessionObserver {
    let events: AsyncStream<SessionEvent>
    private let continuation: AsyncStream<SessionEvent>.Continuation

    init() {
        (events, continuation) = AsyncStream.makeStream(of: SessionEvent.self, bufferingPolicy: .unbounded)
    }

    func onEvent(event: SessionEvent) {
        switch event {
        case .level, .pitch, .progress:
            break
        default:
            continuation.yield(event)
        }
    }
}
