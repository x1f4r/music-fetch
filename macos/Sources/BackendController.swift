import Foundation

struct BackendJobOptions: Codable {
    let prefer_separation: Bool
    let analysis_mode: String
    let recall_profile: String
    let enable_metadata_hints: Bool
    let enable_repeat_detection: Bool
    let max_windows: Int
    let max_segments: Int
    let max_probes_per_segment: Int
    let max_provider_calls: Int
}

struct BackendJobCreateRequest: Codable {
    let inputs: [String]
    let options: BackendJobOptions
}

struct BackendCreateJobResponse: Codable {
    let job_id: String
    let status: String
}

struct BackendHealthResponse: Codable {
    let ok: Bool
}

actor BackendController {
    private static let startupAttempts = 150
    private static let startupPollInterval = Duration.milliseconds(200)
    private var process: Process?
    private var baseURL: URL?
    private var token = UUID().uuidString
    private let session = URLSession(configuration: .default)

    func ensureServer(command: String) async throws -> URL {
        if let baseURL, await healthCheck(baseURL: baseURL) {
            return baseURL
        }
        if process != nil || baseURL != nil {
            stopCurrentProcess()
        }

        let port = Int.random(in: 18000 ... 26000)
        let baseURL = URL(string: "http://127.0.0.1:\(port)")!
        let process = Process()
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw NSError(domain: "MusicFetchMac", code: 1, userInfo: [NSLocalizedDescriptionKey: "Backend command is empty"])
        }
        let resolved = try resolveBackendCommand(trimmed)
        process.executableURL = resolved.executableURL
        process.arguments = resolved.arguments + ["serve", "--host", "127.0.0.1", "--port", "\(port)"]

        var environment = Self.baseEnvironment()
        environment["MUSIC_FETCH_API_TOKEN"] = token
        let commandDir: String? = {
            if trimmed.contains("/") || trimmed.hasPrefix("~") {
                let expanded = NSString(string: trimmed).expandingTildeInPath
                return URL(fileURLWithPath: expanded).deletingLastPathComponent().path
            }
            return nil
        }()
        let pathParts = [
            commandDir,
            "/opt/homebrew/bin",
            "/opt/homebrew/sbin",
            "/usr/local/bin",
            "/usr/local/sbin",
            environment["PATH"],
        ].compactMap { $0 }.filter { !$0.isEmpty }
        environment["PATH"] = pathParts.joined(separator: ":")
        process.environment = environment
        process.currentDirectoryURL = Self.defaultWorkingDirectory()
        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr
        do {
            try process.run()
        } catch {
            throw NSError(
                domain: "MusicFetchMac",
                code: 3,
                userInfo: [
                    NSLocalizedDescriptionKey: "Could not launch backend command '\(trimmed)'. Open Settings and set the backend path explicitly."
                ]
            )
        }

        self.process = process
        self.baseURL = baseURL
        process.terminationHandler = { [weak self] _ in
            Task {
                await self?.clearDeadProcess()
            }
        }
        for _ in 0 ..< Self.startupAttempts {
            if await healthCheck(baseURL: baseURL) {
                return baseURL
            }
            if !process.isRunning {
                break
            }
            try? await Task.sleep(for: Self.startupPollInterval)
        }
        let stderrMessage = Self.readPipe(stderr)
        let stdoutMessage = Self.readPipe(stdout)
        stopCurrentProcess()
        let detail = stderrMessage.isEmpty ? stdoutMessage : stderrMessage
        let message = detail.isEmpty
            ? "Backend server did not start in time"
            : "Backend server did not start in time: \(detail)"
        throw NSError(domain: "MusicFetchMac", code: 2, userInfo: [NSLocalizedDescriptionKey: message])
    }

    func getJSON<T: Decodable>(_ path: String, command: String, queryItems: [URLQueryItem] = [], type: T.Type) async throws -> T {
        let baseURL = try await ensureServer(command: command)
        var components = URLComponents(url: baseURL.appending(path: path), resolvingAgainstBaseURL: false)!
        components.queryItems = queryItems.isEmpty ? nil : queryItems
        var request = URLRequest(url: components.url!)
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await session.data(for: request)
        try validate(response: response, data: data)
        return try JSONDecoder().decode(T.self, from: data)
    }

    func postJSON<Body: Encodable, T: Decodable>(_ path: String, command: String, body: Body, type: T.Type) async throws -> T {
        let baseURL = try await ensureServer(command: command)
        var request = URLRequest(url: baseURL.appending(path: path))
        request.httpMethod = "POST"
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(body)
        let (data, response) = try await session.data(for: request)
        try validate(response: response, data: data)
        return try JSONDecoder().decode(T.self, from: data)
    }

    func putJSON<Body: Encodable, T: Decodable>(_ path: String, command: String, body: Body, type: T.Type) async throws -> T {
        let baseURL = try await ensureServer(command: command)
        var request = URLRequest(url: baseURL.appending(path: path))
        request.httpMethod = "PUT"
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(body)
        let (data, response) = try await session.data(for: request)
        try validate(response: response, data: data)
        return try JSONDecoder().decode(T.self, from: data)
    }

    func deleteJSON<T: Decodable>(_ path: String, command: String, queryItems: [URLQueryItem] = [], type: T.Type) async throws -> T {
        let baseURL = try await ensureServer(command: command)
        var components = URLComponents(url: baseURL.appending(path: path), resolvingAgainstBaseURL: false)!
        components.queryItems = queryItems.isEmpty ? nil : queryItems
        var request = URLRequest(url: components.url!)
        request.httpMethod = "DELETE"
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await session.data(for: request)
        try validate(response: response, data: data)
        return try JSONDecoder().decode(T.self, from: data)
    }

    func uploadFile<T: Decodable>(_ path: String, command: String, fileURL: URL, options: BackendJobOptions, type: T.Type) async throws -> T {
        let baseURL = try await ensureServer(command: command)
        let boundary = "Boundary-\(UUID().uuidString)"
        var request = URLRequest(url: baseURL.appending(path: path))
        request.httpMethod = "POST"
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")

        var body = Data()
        let optionsJSON = try String(data: JSONEncoder().encode(options), encoding: .utf8) ?? "{}"
        body.append("--\(boundary)\r\n".data(using: .utf8)!)
        body.append("Content-Disposition: form-data; name=\"options_json\"\r\n\r\n".data(using: .utf8)!)
        body.append("\(optionsJSON)\r\n".data(using: .utf8)!)
        body.append("--\(boundary)\r\n".data(using: .utf8)!)
        body.append("Content-Disposition: form-data; name=\"file\"; filename=\"\(fileURL.lastPathComponent)\"\r\n".data(using: .utf8)!)
        body.append("Content-Type: audio/wav\r\n\r\n".data(using: .utf8)!)
        body.append(try Data(contentsOf: fileURL))
        body.append("\r\n--\(boundary)--\r\n".data(using: .utf8)!)
        request.httpBody = body

        let (data, response) = try await session.data(for: request)
        try validate(response: response, data: data)
        return try JSONDecoder().decode(T.self, from: data)
    }

    func streamEvents(jobID: String, command: String, onEvent: @escaping @MainActor (String) -> Void) async {
        do {
            let baseURL = try await ensureServer(command: command)
            var request = URLRequest(url: baseURL.appending(path: "/v1/jobs/\(jobID)/events"))
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
            let (bytes, response) = try await session.bytes(for: request)
            try validate(response: response, data: Data())
            for try await line in bytes.lines {
                guard line.hasPrefix("data: ") else { continue }
                let value = String(line.dropFirst(6))
                await onEvent(value)
            }
        } catch {
            return
        }
    }

    func stop() {
        stopCurrentProcess()
    }

    private func healthCheck(baseURL: URL) async -> Bool {
        do {
            var request = URLRequest(url: baseURL.appending(path: "/health"))
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
            let (data, response) = try await session.data(for: request)
            try validate(response: response, data: data)
            let health = try JSONDecoder().decode(BackendHealthResponse.self, from: data)
            return health.ok
        } catch {
            return false
        }
    }

    private func validate(response: URLResponse, data: Data) throws {
        guard let http = response as? HTTPURLResponse else {
            return
        }
        guard (200 ..< 300).contains(http.statusCode) else {
            let message = String(data: data, encoding: .utf8) ?? HTTPURLResponse.localizedString(forStatusCode: http.statusCode)
            throw NSError(domain: "MusicFetchMac", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: message])
        }
    }

    private static func readPipe(_ pipe: Pipe) -> String {
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        guard let value = String(data: data, encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines),
            !value.isEmpty else {
            return ""
        }
        return value
    }

    static func defaultWorkingDirectory() -> URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? FileManager.default.homeDirectoryForCurrentUser
        let directory = base.appendingPathComponent("Music Fetch", isDirectory: true)
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    private func resolveBackendCommand(_ command: String) throws -> (executableURL: URL, arguments: [String]) {
        if command.contains("/") || command.hasPrefix("~") {
            let expanded = NSString(string: command).expandingTildeInPath
            return (URL(fileURLWithPath: expanded), [])
        }
        if let located = loginShellLookup(command), located.hasPrefix("/") {
            return (URL(fileURLWithPath: located), [])
        }
        return (URL(fileURLWithPath: "/usr/bin/env"), [command])
    }

    private func loginShellLookup(_ command: String) -> String? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/zsh")
        process.arguments = ["-lc", "command -v \(command) 2>/dev/null || true"]
        let stdout = Pipe()
        process.standardOutput = stdout
        process.standardError = Pipe()
        do {
            try process.run()
            process.waitUntilExit()
            let data = stdout.fileHandleForReading.readDataToEndOfFile()
            let value = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
        return value?.isEmpty == false ? value : nil
        } catch {
            return nil
        }
    }

    static func baseEnvironment() -> [String: String] {
        var environment: [String: String] = [:]
        let processInfo = ProcessInfo.processInfo.environment
        let home = processInfo["HOME"] ?? FileManager.default.homeDirectoryForCurrentUser.path
        environment["HOME"] = home
        environment["PATH"] = processInfo["PATH"] ?? "/usr/bin:/bin:/usr/sbin:/sbin"
        if let lang = processInfo["LANG"], !lang.isEmpty {
            environment["LANG"] = lang
        }
        if let lcAll = processInfo["LC_ALL"], !lcAll.isEmpty {
            environment["LC_ALL"] = lcAll
        }
        if let tmpdir = processInfo["TMPDIR"], !tmpdir.isEmpty {
            environment["TMPDIR"] = tmpdir
        }
        return environment
    }

    private func clearDeadProcess() {
        process = nil
        baseURL = nil
    }

    private func stopCurrentProcess() {
        process?.terminationHandler = nil
        if let process, process.isRunning {
            process.terminate()
        }
        process = nil
        baseURL = nil
    }
}
