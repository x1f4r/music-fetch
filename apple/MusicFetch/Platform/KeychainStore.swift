import Foundation
import MusicFetchCore
import Security

/// Generic-password items in the login keychain. Values are never logged.
nonisolated struct KeychainStore: Sendable {
    /// Service name for provider credentials.
    static let credentialsService = "local.musicfetch.app.credentials"

    struct Failure: LocalizedError {
        let status: OSStatus
        var errorDescription: String? {
            (SecCopyErrorMessageString(status, nil) as String?) ?? "Keychain error \(status)."
        }
    }

    let service: String

    init(service: String = KeychainStore.credentialsService) {
        self.service = service
    }

    func string(for account: String) -> String? {
        var query = baseQuery(account)
        query[kSecReturnData as String] = true
        query[kSecMatchLimit as String] = kSecMatchLimitOne
        var result: CFTypeRef?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
            let data = result as? Data
        else { return nil }
        return String(data: data, encoding: .utf8)
    }

    /// Stores `value`, or deletes the item when `value` is nil or blank.
    func set(_ value: String?, for account: String) throws {
        let trimmed = value?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !trimmed.isEmpty else {
            try delete(account)
            return
        }
        let data = Data(trimmed.utf8)
        let status = SecItemUpdate(baseQuery(account) as CFDictionary, [kSecValueData as String: data] as CFDictionary)
        switch status {
        case errSecSuccess:
            return
        case errSecItemNotFound:
            var item = baseQuery(account)
            item[kSecValueData as String] = data
            item[kSecAttrLabel as String] = "Music Fetch (\(account))"
            let addStatus = SecItemAdd(item as CFDictionary, nil)
            guard addStatus == errSecSuccess else { throw Failure(status: addStatus) }
        default:
            throw Failure(status: status)
        }
    }

    func delete(_ account: String) throws {
        let status = SecItemDelete(baseQuery(account) as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw Failure(status: status) }
    }

    private func baseQuery(_ account: String) -> [String: Any] {
        [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
        ]
    }
}

/// Provider credentials as edited in Settings › Services.
nonisolated struct CredentialFields: Equatable, Sendable {
    var auddToken = ""
    var acrHost = ""
    var acrAccessKey = ""
    var acrAccessSecret = ""
    var humHost = ""
    var humAccessKey = ""
    var humAccessSecret = ""

    enum Account: String, CaseIterable {
        case auddToken = "audd.token"
        case acrHost = "acrcloud.host"
        case acrAccessKey = "acrcloud.access-key"
        case acrAccessSecret = "acrcloud.access-secret"
        case humHost = "acrcloud-humming.host"
        case humAccessKey = "acrcloud-humming.access-key"
        case humAccessSecret = "acrcloud-humming.access-secret"
    }

    subscript(account: Account) -> String {
        get {
            switch account {
            case .auddToken: auddToken
            case .acrHost: acrHost
            case .acrAccessKey: acrAccessKey
            case .acrAccessSecret: acrAccessSecret
            case .humHost: humHost
            case .humAccessKey: humAccessKey
            case .humAccessSecret: humAccessSecret
            }
        }
        set {
            switch account {
            case .auddToken: auddToken = newValue
            case .acrHost: acrHost = newValue
            case .acrAccessKey: acrAccessKey = newValue
            case .acrAccessSecret: acrAccessSecret = newValue
            case .humHost: humHost = newValue
            case .humAccessKey: humAccessKey = newValue
            case .humAccessSecret: humAccessSecret = newValue
            }
        }
    }

    static func load(from store: KeychainStore) -> CredentialFields {
        var fields = CredentialFields()
        for account in Account.allCases {
            fields[account] = store.string(for: account.rawValue) ?? ""
        }
        return fields
    }

    func save(to store: KeychainStore) throws {
        for account in Account.allCases {
            try store.set(self[account], for: account.rawValue)
        }
    }

    /// The engine's view: a provider is configured only when all its fields are set.
    var credentials: Credentials {
        func value(_ text: String) -> String? {
            let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
            return trimmed.isEmpty ? nil : trimmed
        }
        func acr(_ host: String, _ key: String, _ secret: String) -> AcrCredentials? {
            guard let host = value(host), let key = value(key), let secret = value(secret) else { return nil }
            return AcrCredentials(host: host, accessKey: key, accessSecret: secret)
        }
        return Credentials(
            auddToken: value(auddToken),
            acrFingerprint: acr(acrHost, acrAccessKey, acrAccessSecret),
            acrHumming: acr(humHost, humAccessKey, humAccessSecret)
        )
    }
}
