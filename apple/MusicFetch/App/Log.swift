import OSLog

/// Loggers. Never log credentials, file paths or link contents.
nonisolated enum Log {
    private static let subsystem = "local.musicfetch.app"
    static let app = Logger(subsystem: subsystem, category: "app")
    static let engine = Logger(subsystem: subsystem, category: "engine")
    static let capture = Logger(subsystem: subsystem, category: "capture")
    static let speech = Logger(subsystem: subsystem, category: "speech")
}
