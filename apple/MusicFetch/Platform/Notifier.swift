import Foundation
import UserNotifications

/// Posts "result arrived" notifications for analyses that finish while Music Fetch is
/// in the background, and routes clicks back into the app.
final class Notifier: NSObject {
    static let openAction = "open-in-service"
    private static let category = "result"

    /// Called with the session id when a notification is clicked; the second argument
    /// is true for the "Open in …" action.
    var onOpen: ((String, Bool) -> Void)?

    private let center: UNUserNotificationCenter?

    override init() {
        // The notification center requires a real app bundle (not unit tests).
        center = Bundle.main.bundleURL.pathExtension == "app" ? UNUserNotificationCenter.current() : nil
        super.init()
        center?.delegate = self
    }

    /// Requests permission (once; the system remembers the answer).
    func requestAuthorization() async -> Bool {
        guard let center else { return false }
        return (try? await center.requestAuthorization(options: [.alert, .sound])) ?? false
    }

    /// Posts a notification; `actionTitle` adds an "Open in …" button.
    func post(sessionID: String, title: String, body: String, actionTitle: String?) async {
        guard let center else { return }
        let settings = await center.notificationSettings()
        guard settings.authorizationStatus == .authorized || settings.authorizationStatus == .provisional else { return }

        var categoryID = ""
        if let actionTitle {
            categoryID = "\(Self.category).\(actionTitle)"
            let action = UNNotificationAction(identifier: Self.openAction, title: actionTitle, options: [.foreground])
            let category = UNNotificationCategory(identifier: categoryID, actions: [action], intentIdentifiers: [])
            let existing = await center.notificationCategories()
            center.setNotificationCategories(existing.filter { $0.identifier != categoryID }.union([category]))
        }

        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.categoryIdentifier = categoryID
        content.userInfo = ["session": sessionID]
        let request = UNNotificationRequest(identifier: sessionID, content: content, trigger: nil)
        try? await center.add(request)
    }
}

extension Notifier: UNUserNotificationCenterDelegate {
    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse
    ) async {
        guard let sessionID = response.notification.request.content.userInfo["session"] as? String else { return }
        let isOpenAction = response.actionIdentifier == Self.openAction
        await MainActor.run {
            onOpen?(sessionID, isOpenAction)
        }
    }

    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification
    ) async -> UNNotificationPresentationOptions {
        [.banner]
    }
}
