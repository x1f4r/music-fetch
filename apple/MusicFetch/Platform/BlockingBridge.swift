import Dispatch
import Foundation
import Synchronization

/// Runs async work from a synchronous engine worker thread (platform-service calls are
/// blocking by contract) with a timeout and cooperative cancellation.
///
/// Never call this from the main thread or a Swift concurrency thread: it blocks the
/// calling thread while the work runs on the cooperative pool.
nonisolated enum BlockingBridge {
    enum Failure: Error, Equatable {
        case timedOut
        case canceled
    }

    /// Waits for `operation`, polling `isCanceled` every 50 ms. On cancellation or
    /// timeout the task is cancelled and the call returns without waiting for it.
    static func run<T: Sendable>(
        timeout: Duration,
        isCanceled: @Sendable () -> Bool,
        operation: @escaping @Sendable () async throws -> T
    ) throws -> T {
        let box = ResultBox<T>()
        let done = DispatchSemaphore(value: 0)
        let task = Task.detached {
            do {
                box.store(.success(try await operation()))
            } catch {
                box.store(.failure(error))
            }
            done.signal()
        }

        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: timeout)
        while done.wait(timeout: .now() + .milliseconds(50)) == .timedOut {
            if isCanceled() {
                task.cancel()
                throw Failure.canceled
            }
            if clock.now >= deadline {
                task.cancel()
                throw Failure.timedOut
            }
        }
        guard let result = box.take() else { throw Failure.canceled }
        return try result.get()
    }
}

/// A thread-safe slot for one result.
nonisolated private final class ResultBox<T: Sendable>: Sendable {
    private let slot = Mutex<Result<T, Error>?>(nil)

    func store(_ result: Result<T, Error>) {
        slot.withLock { $0 = result }
    }

    func take() -> Result<T, Error>? {
        slot.withLock { value in
            defer { value = nil }
            return value
        }
    }
}
