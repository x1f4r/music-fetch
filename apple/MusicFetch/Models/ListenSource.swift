import Foundation
import MusicFetchCore

/// The three source toggles under the listen button, in display order.
nonisolated enum ListenSource: String, CaseIterable, Identifiable, Sendable {
    case microphone
    case thisMac
    case hum

    var id: String { rawValue }

    var capture: CaptureSource {
        self == .thisMac ? .systemAudio : .microphone
    }

    /// Microphone and This Mac detect humming on their own; Hum skips recording rules.
    var mode: ListenMode {
        self == .hum ? .hum : .auto
    }

    var title: String {
        switch self {
        case .microphone: Copy.microphone
        case .thisMac: Copy.thisMac
        case .hum: Copy.hum
        }
    }

    /// SF Symbol, or nil for the contour mark.
    var symbol: String? {
        switch self {
        case .microphone: "mic"
        case .thisMac: "macbook"
        case .hum: nil
        }
    }
}
