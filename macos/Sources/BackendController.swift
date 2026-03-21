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
    private var process: Process?
    private var baseURL: URL?
    private var token = UUID().uuidString
    private let session = URLSession(configuration: .default)

    func ensureServer(command: String) async throws -> URL {
        if let baseURL, await healthCheck(baseURL: baseURL) {
            return baseURL
        }

        let port = Int.random(in: 18000 ... 26000)
        let baseURL = URL(string: "http://127.0.0.1:\(port)")!
        let process = Process()
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw NSError(domain: "MusicFetchMac", code: 1, userInfo: [NSLocalizedDescriptionKey: "Backend command is empty"])
        }

        if trimmed.contains("/") || trimmed.hasPrefix("~") {
            let expanded = NSString(string: trimmed).expandingTildeInPath
            process.executableURL = URL(fileURLWithPath: expanded)
            process.arguments = ["serve", "--host", "127.0.0.1", "--port", "\(port)"]
        } else {
            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = [trimmed, "serve", "--host", "127.0.0.1", "--port", "\(port)"]
        }

        var environment = ProcessInfo.processInfo.environment
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
        process.currentDirectoryURL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
        process.standardOutput = Pipe()
        process.standardError = Pipe()
        try process.run()

        self.process = process
        self.baseURL = baseURL
        for _ in 0 ..< 30 {
            if await healthCheck(baseURL: baseURL) {
                return baseURL
            }
            try? await Task.sleep(for: .milliseconds(200))
        }
        throw NSError(domain: "MusicFetchMac", code: 2, userInfo: [NSLocalizedDescriptionKey: "Backend server did not start in time"])
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
        process?.terminate()
        process = nil
        baseURL = nil
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
}
