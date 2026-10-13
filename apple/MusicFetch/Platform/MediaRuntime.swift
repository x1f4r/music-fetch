import Foundation
import MusicFetchCore

/// The media download runtime: bundled CPython, yt-dlp and QuickJS, plus an optional
/// system Deno.
///
/// `scripts/fetch-macos-runtime.sh` builds the bundled part and
/// `scripts/build-macos-app.sh` copies it to `Contents/Resources/runtime`:
///
///     runtime/python/bin/python3   python-build-standalone CPython
///     runtime/yt-dlp               yt-dlp zipimport binary
///     runtime/qjs                  quickjs-ng
///     runtime/cacert.pem           CA bundle for Python's OpenSSL
///     runtime/VERSIONS.json        versions recorded at build time
///
/// A yt-dlp update lives outside the signed bundle, in
/// `~/Library/Application Support/Music Fetch/yt-dlp/` as `yt-dlp` (zipimport binary)
/// plus `VERSION` (e.g. `2026.10.10`). It is used when its version is newer than the
/// bundled one; without a `VERSION` file it is ignored.
nonisolated struct MediaRuntime: Sendable {
    nonisolated enum LocateError: LocalizedError {
        case missing(String)

        var errorDescription: String? {
            switch self {
            case .missing(let item): "The app's media runtime is incomplete (\(item) is missing). Reinstall Music Fetch."
            }
        }
    }

    nonisolated struct RunError: LocalizedError {
        let status: Int32
        /// Combined stdout and stderr.
        let output: String

        /// The last line of output (for Python, the exception), or the exit status.
        var errorDescription: String? {
            let lastLine = output.split(whereSeparator: \.isNewline).last.map(String.init)
            return lastLine ?? "yt-dlp exited with status \(status)."
        }
    }

    /// `Contents/Resources/runtime`.
    let root: URL
    let python: URL
    /// The zipimport binary in use (bundled or updated).
    let ytDlp: URL
    /// Version of `ytDlp` as recorded at build or update time, if known.
    let ytDlpVersion: String?
    /// True when `ytDlp` is the update in Application Support.
    let usesUpdatedYtDlp: Bool
    let quickJS: URL
    let caBundle: URL
    /// A system Deno; yt-dlp prefers it over QuickJS for YouTube challenges.
    let deno: URL?
    /// Private TMPDIR for yt-dlp (it writes challenge scripts there).
    let temporaryDirectory: URL

    /// Locate the runtime in `bundle` and pick the yt-dlp and JS runtimes to use.
    static func locate(bundle: Bundle = .main) throws -> MediaRuntime {
        let fileManager = FileManager.default
        guard let resources = bundle.resourceURL else { throw LocateError.missing("Contents/Resources") }
        let root = resources.appending(path: "runtime", directoryHint: .isDirectory)

        let python = root.appending(path: "python/bin/python3")
        let bundledYtDlp = root.appending(path: "yt-dlp")
        let quickJS = root.appending(path: "qjs")
        let caBundle = root.appending(path: "cacert.pem")
        for (url, executable) in [(python, true), (quickJS, true), (bundledYtDlp, false), (caBundle, false)] {
            let present = executable ? fileManager.isExecutableFile(atPath: url.path) : fileManager.isReadableFile(atPath: url.path)
            if !present { throw LocateError.missing("runtime/\(url.path.dropFirst(root.path.count + 1))") }
        }

        let bundledVersion = Self.bundledYtDlpVersion(root: root)
        var ytDlp = bundledYtDlp
        var ytDlpVersion = bundledVersion
        var usesUpdate = false
        if let update = Self.updatedYtDlp(fileManager: fileManager),
           let bundledVersion, Self.isVersion(update.version, newerThan: bundledVersion)
        {
            ytDlp = update.url
            ytDlpVersion = update.version
            usesUpdate = true
        }

        let bundleID = bundle.bundleIdentifier ?? "local.musicfetch.app"
        let temporaryDirectory = fileManager.temporaryDirectory.appending(path: "\(bundleID)/yt-dlp", directoryHint: .isDirectory)

        return MediaRuntime(
            root: root,
            python: python,
            ytDlp: ytDlp,
            ytDlpVersion: ytDlpVersion,
            usesUpdatedYtDlp: usesUpdate,
            quickJS: quickJS,
            caBundle: caBundle,
            deno: Self.systemDeno(fileManager: fileManager),
            temporaryDirectory: temporaryDirectory
        )
    }

    /// Values for yt-dlp's `--js-runtimes`, best first. The caller passes
    /// `--no-js-runtimes` before them so yt-dlp's default (Deno on PATH only) is replaced.
    var jsRuntimes: [String] {
        var runtimes: [String] = []
        if let deno { runtimes.append("deno:\(deno.path)") }
        runtimes.append("quickjs:\(quickJS.path)")
        return runtimes
    }

    /// Environment for every yt-dlp run. Bytecode writing stays off so nothing is
    /// written into the signed bundle.
    var environment: [String: String] {
        [
            "SSL_CERT_FILE": caBundle.path,
            "TMPDIR": temporaryDirectory.path,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONUTF8": "1",
        ]
    }

    /// Configuration for the engine's built-in yt-dlp backend. Creates the private
    /// TMPDIR (mode 0700) if needed.
    func mediaToolsConfig() throws -> MediaToolsConfig {
        try prepareTemporaryDirectory()
        return MediaToolsConfig(
            pythonPath: python.path,
            ytDlpPath: ytDlp.path,
            jsRuntimes: jsRuntimes,
            env: environment
        )
    }

    /// Run `yt-dlp --version` with the runtime (no network), the same way the engine
    /// runs it. No `-I`: isolated mode would ignore `PYTHONDONTWRITEBYTECODE`.
    @concurrent
    func runYtDlpVersion() async throws -> String {
        try prepareTemporaryDirectory()
        let process = Process()
        process.executableURL = python
        process.arguments = [ytDlp.path, "--version"]
        process.environment = ProcessInfo.processInfo.environment.merging(environment) { _, runtime in runtime }
        process.standardInput = FileHandle.nullDevice
        let output = Pipe()
        process.standardOutput = output
        process.standardError = output

        return try await withCheckedThrowingContinuation { continuation in
            process.terminationHandler = { process in
                // A version string or a short traceback; both fit the pipe buffer, so
                // reading after exit cannot deadlock.
                let data = output.fileHandleForReading.readDataToEndOfFile()
                let text = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
                if process.terminationStatus == 0 {
                    continuation.resume(returning: text)
                } else {
                    continuation.resume(throwing: RunError(status: process.terminationStatus, output: text))
                }
            }
            do {
                try process.run()
            } catch {
                process.terminationHandler = nil
                continuation.resume(throwing: error)
            }
        }
    }

    private func prepareTemporaryDirectory() throws {
        try FileManager.default.createDirectory(
            at: temporaryDirectory,
            withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700]
        )
    }

    // MARK: - Discovery

    /// The part of `VERSIONS.json` the app reads.
    private nonisolated struct BundledVersions: Decodable {
        nonisolated struct Component: Decodable { let version: String }
        let ytDlp: Component
    }

    private static func bundledYtDlpVersion(root: URL) -> String? {
        guard let data = try? Data(contentsOf: root.appending(path: "VERSIONS.json")) else { return nil }
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        return try? decoder.decode(BundledVersions.self, from: data).ytDlp.version
    }

    private static func updatedYtDlp(fileManager: FileManager) -> (url: URL, version: String)? {
        guard let support = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else { return nil }
        let directory = support.appending(path: "Music Fetch/yt-dlp", directoryHint: .isDirectory)
        let binary = directory.appending(path: "yt-dlp")
        guard fileManager.isReadableFile(atPath: binary.path),
              let raw = try? String(contentsOf: directory.appending(path: "VERSION"), encoding: .utf8)
        else { return nil }
        let version = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return version.isEmpty ? nil : (binary, version)
    }

    private static func systemDeno(fileManager: FileManager) -> URL? {
        let candidates = [
            "/opt/homebrew/bin/deno",
            "/usr/local/bin/deno",
            fileManager.homeDirectoryForCurrentUser.appending(path: ".deno/bin/deno").path,
        ]
        return candidates.first(where: fileManager.isExecutableFile(atPath:)).map { URL(filePath: $0) }
    }

    /// Compares yt-dlp versions (`2026.08.19`, `2026.10.10.233141`) numerically. Anything
    /// that is not dot-separated numbers is never newer.
    static func isVersion(_ candidate: String, newerThan current: String) -> Bool {
        guard let lhs = versionParts(candidate), let rhs = versionParts(current) else { return false }
        for index in 0..<max(lhs.count, rhs.count) {
            let a = index < lhs.count ? lhs[index] : 0
            let b = index < rhs.count ? rhs[index] : 0
            if a != b { return a > b }
        }
        return false
    }

    private static func versionParts(_ version: String) -> [Int]? {
        let parts = version.split(separator: ".", omittingEmptySubsequences: false).map { Int($0) }
        return parts.contains(nil) ? nil : parts.compactMap { $0 }
    }
}
