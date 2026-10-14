import Foundation
import MusicFetchCore

/// Receives engine events on engine threads and hands them to an `AsyncStream` that a
/// model consumes on the main actor. Never blocks the engine.
nonisolated final class ObserverBridge: SessionObserver {
    let events: AsyncStream<SessionEvent>
    private let continuation: AsyncStream<SessionEvent>.Continuation

    init() {
        let (stream, continuation) = AsyncStream.makeStream(of: SessionEvent.self, bufferingPolicy: .unbounded)
        self.events = stream
        self.continuation = continuation
    }

    func onEvent(event: SessionEvent) {
        continuation.yield(event)
        if event.isTerminal {
            continuation.finish()
        }
    }

    /// Ends the stream early, e.g. when a session could not start.
    func close() {
        continuation.finish()
    }
}

extension SessionEvent {
    /// `Finished`, `Failed` or `Canceled`: nothing follows.
    nonisolated var isTerminal: Bool {
        switch self {
        case .finished, .failed, .canceled: true
        default: false
        }
    }
}
