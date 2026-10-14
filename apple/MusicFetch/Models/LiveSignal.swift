import Foundation
import MusicFetchCore

/// The fast-changing part of a live session: input level and pitch frames.
///
/// Deliberately not observable: the listen button, halo and trace band redraw every
/// frame in `TimelineView(.animation)` and read the latest values while drawing, so
/// 30 level updates per second never invalidate SwiftUI views.
final class LiveSignal {
    /// Seconds of history kept for the trace band.
    static let window: TimeInterval = 8
    /// Frame spacing until the first batch tells (the core's tracker uses 16 ms).
    static let defaultFrameSeconds: TimeInterval = 0.016

    /// Level smoothed with a 100 ms exponential moving average, 0…1.
    private(set) var level: Double = 0
    private var levelTime: Date?

    /// Frames of the last `window` seconds, oldest first.
    private(set) var frames: [PitchFrame] = []
    /// The session's frames, up to 30 s (for the frozen hum row).
    private(set) var history: [PitchFrame] = []
    private static let historyLimit = 600
    /// Running median (MIDI) of the voiced frames in the window.
    private(set) var median: Float?
    /// Newest frame time and when it arrived, to scroll smoothly between batches.
    private(set) var headMilliseconds: UInt64 = 0
    private(set) var headArrival: Date?
    private(set) var voicedFrameCount = 0
    /// Spacing of the pitch frames, measured from the batches.
    private(set) var frameSeconds = LiveSignal.defaultFrameSeconds

    /// Freezes the drawn line (matching after capture).
    var isFrozen = false
    private var frozenAt: Date?

    var voicedSeconds: TimeInterval {
        Double(voicedFrameCount) * frameSeconds
    }

    func reset() {
        level = 0
        levelTime = nil
        frames.removeAll()
        history.removeAll()
        median = nil
        headMilliseconds = 0
        headArrival = nil
        voicedFrameCount = 0
        frameSeconds = Self.defaultFrameSeconds
        isFrozen = false
        frozenAt = nil
    }

    func push(level rms: Float, at now: Date = .now) {
        guard !isFrozen else { return }
        let value = Double(min(max(rms.isFinite ? rms : 0, 0), 1))
        if let levelTime {
            let dt = max(now.timeIntervalSince(levelTime), 0)
            let alpha = 1 - exp(-dt / 0.1)
            level += alpha * (value - level)
        } else {
            level = value
        }
        levelTime = now
    }

    /// Appends a batch of frames and drops what scrolled out of the window.
    func push(frames batch: [PitchFrame], at now: Date = .now) {
        guard !isFrozen, !batch.isEmpty else { return }
        if batch.count >= 2, let first = batch.first, let last = batch.last, last.tMs > first.tMs {
            frameSeconds = Double(last.tMs - first.tMs) / 1000 / Double(batch.count - 1)
        }
        frames.append(contentsOf: batch)
        voicedFrameCount += batch.filter { $0.midi.isFinite }.count
        history.append(contentsOf: batch)
        if history.count > Self.historyLimit { history.removeFirst(history.count - Self.historyLimit) }
        headMilliseconds = max(headMilliseconds, batch.map(\.tMs).max() ?? 0)
        headArrival = now
        // Keep half a second beyond the window so the left edge scrolls out smoothly.
        let keep = UInt64(Self.window * 1000) + 500
        let cutoff = headMilliseconds > keep ? headMilliseconds - keep : 0
        if let first = frames.firstIndex(where: { $0.tMs >= cutoff }), first > 0 {
            frames.removeFirst(first)
        }
        median = Self.median(frames.map(\.midi).filter(\.isFinite))
    }

    func freeze(at now: Date = .now) {
        guard !isFrozen else { return }
        isFrozen = true
        frozenAt = now
    }

    /// The moment the line froze, as a reference-date time for drawing.
    var frozenTime: Double? {
        frozenAt?.timeIntervalSinceReferenceDate
    }

    /// Session time (ms) at the right edge of the trace band at `date`: the newest
    /// frame, advanced by the time since it arrived (at most 200 ms, so a stalled
    /// stream doesn't scroll everything away).
    func headTime(at date: Date) -> Double {
        guard let headArrival else { return Double(headMilliseconds) }
        let reference = isFrozen ? (frozenAt ?? date) : date
        let ahead = min(max(reference.timeIntervalSince(headArrival), 0), 0.2)
        return Double(headMilliseconds) + ahead * 1000
    }

    static func median(_ values: [Float]) -> Float? {
        guard !values.isEmpty else { return nil }
        let sorted = values.sorted()
        let mid = sorted.count / 2
        return sorted.count % 2 == 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid]
    }
}
