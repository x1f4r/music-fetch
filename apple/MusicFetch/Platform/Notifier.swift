import Foundation
@preconcurrency import UserNotifications

/// Posts "result arrived" notifications for analyses that finish while Music Fetch is
/// in the background, and routes clicks back into the app.
final class Notifier: NSObject {
    nonisolated static let openAction = "open-in-service"
    private nonisolated static let category = "result"

    /// Called with the session id when a notification is clicked; the second argument
    /// is true for the "Open in …" action.
    var onOpen: ((String, Bool) -> Void)?

    private let center: UNUserNotificationCenter?

    override init() {
        // The notification center requires a real app bundle (not unit tests).
        // Nor the offscreen layout harness.
        center = Bundle.main.bundleURL.pathExtension == "app" && !RenderHarness.isActive ? UNUserNotificationCenter.current() : nil
        super.init()
        center?.delegate = self
    }

    /// Requests permission (once; the system remembers the answer).
    func requestAuthorization() async -> Bool {
        guard let center else { return false }
        return await withCheckedContinuation { continuation in
            center.requestAuthorization(options: [.alert, .sound]) { granted, error in
                if let error {
                    let code = (error as NSError).code
                    Log.app.info("Notification permission unavailable (\(code, privacy: .public))")
                }
                continuation.resume(returning: granted)
            }
        }
    }

    /// Posts a notification; `actionTitle` adds an "Open in …" button.
    func post(sessionID: String, title: String, body: String, actionTitle: String?) async {
        guard let center else { return }
        let authorized = await withCheckedContinuation { continuation in
            center.getNotificationSettings { settings in
                continuation.resume(returning: settings.authorizationStatus == .authorized || settings.authorizationStatus == .provisional)
            }
        }
        guard authorized else { return }

        var categoryID = ""
        if let actionTitle {
            categoryID = "\(Self.category).\(actionTitle)"
            let action = UNNotificationAction(identifier: Self.openAction, title: actionTitle, options: [.foreground])
            let category = UNNotificationCategory(identifier: categoryID, actions: [action], intentIdentifiers: [])
            let id = categoryID
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                center.getNotificationCategories { existing in
                    center.setNotificationCategories(existing.filter { $0.identifier != id }.union([category]))
                    continuation.resume()
                }
            }
        }

        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.categoryIdentifier = categoryID
        content.userInfo = ["session": sessionID]
        let request = UNNotificationRequest(identifier: sessionID, content: content, trigger: nil)
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            center.add(request) { error in
                if let error { Log.app.error("Notification failed: \(error.localizedDescription, privacy: .public)") }
                continuation.resume()
            }
        }
    }
}

extension Notifier: UNUserNotificationCenterDelegate {
    // Completion-handler forms: Swift 6.2 crashes emitting the ObjC thunks of the async forms.
    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completionHandler: @escaping () -> Void
    ) {
        let sessionID = response.notification.request.content.userInfo["session"] as? String
        let isOpenAction = response.actionIdentifier == Self.openAction
        if let sessionID {
            Task { @MainActor in
                self.onOpen?(sessionID, isOpenAction)
            }
        }
        completionHandler()
    }

    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        completionHandler([.banner])
    }
}
