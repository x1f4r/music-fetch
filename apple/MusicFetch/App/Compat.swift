import Foundation

/// Which system features the app uses. The app runs on macOS 15 and later; macOS 26
/// adds Liquid Glass and SpeechAnalyzer, which have native fallbacks before that
/// (materials and bordered button styles, `SFSpeechRecognizer`).
nonisolated enum Compat {
    /// Debug builds launched with `-MFLegacyUI YES` take every pre-macOS 26 branch, so the
    /// fallbacks can be checked on a newer system. Always false in Release builds.
    static let forcesLegacy: Bool = {
        #if DEBUG
        UserDefaults.standard.bool(forKey: "MFLegacyUI")
        #else
        false
        #endif
    }()

    /// True when Liquid Glass and the other macOS 26 additions are in use.
    static var usesMacOS26Features: Bool {
        if #available(macOS 26, *) { return !forcesLegacy }
        return false
    }
}
