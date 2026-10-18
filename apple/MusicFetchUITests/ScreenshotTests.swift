import XCTest

/// Screenshots of every demo state (`-MFDemoState`) at several window sizes in light and
/// dark, plus toolbar geometry checks. Runs on CI only (it launches the app); the
/// screenshots workflow turns the attachments into a contact sheet.
///
/// Attachment names: `shot__<state>__<variant>__` (PNG) and `geom__<state>__<variant>__`
/// (JSON with the toolbar frames and any violations).
final class ScreenshotTests: XCTestCase {
    struct Variant {
        var name: String
        var width: Int
        var height = 760
        var dark = false
        var inspector = true

        static let regular = Variant(name: "1180-light", width: 1180)
        static let regularDark = Variant(name: "1180-dark", width: 1180, dark: true)
        static let narrow = Variant(name: "1000-light", width: 1000)
        static let wide = Variant(name: "1440-dark-noinspector", width: 1440, dark: true, inspector: false)

        static let all: [Variant] = [.regular, .regularDark, .narrow, .wide]
        static let pair: [Variant] = [.regular, .regularDark]
    }

    /// States shown at every size, and states where light and dark at 1180 are enough.
    static let fullStates = ["idle", "result", "candidates", "mix", "library", "library-selection", "session", "noMatch"]
    static let pairStates = [
        "listening", "hum", "hum-answers", "matching", "collection", "failed", "micDenied", "prePrompt", "captureSilent",
        "providers", "lyrics", "melodyPrompt", "library-pinned", "settings-general", "settings-melody", "candidates-unsure",
        "speechPrePrompt", "settings-melody-speech-off", "settings-melody-model", "settings-storage",
    ]
    /// The idle stage hides the toolbar Listen button (the big button is the same action).
    static let statesWithoutToolbarListen: Set<String> = [
        "idle", "noMatch", "failed", "micDenied", "prePrompt", "speechPrePrompt", "listening", "hum", "hum-answers", "matching",
        "captureSilent",
        "providers", "melodyPrompt", "downloading", "lyrics",
    ]

    override func setUp() {
        continueAfterFailure = true
    }

    /// The screen the runner gives us, for the contact sheet's notes.
    func testScreen() throws {
        let size = XCUIScreen.main.screenshot().image.size
        let attachment = XCTAttachment(string: "\(Int(size.width)) x \(Int(size.height))")
        attachment.name = "screen"
        attachment.lifetime = .keepAlways
        add(attachment)
        XCTAssertGreaterThanOrEqual(size.width, 1480, "the screen is too small for the 1440 pt variants")
    }

    func testStates() throws {
        let only = ProcessInfo.processInfo.environment["MF_SHOT_STATES"].map { Set($0.split(separator: ",").map(String.init)) }
        let plan = Self.fullStates.map { ($0, Variant.all) } + Self.pairStates.map { ($0, Variant.pair) }
        for (state, variants) in plan where only?.contains(state) ?? true {
            for variant in variants {
                capture(state, variant)
            }
        }
    }

    /// Menus, hover, method switching and back navigation, driven like a person would.
    func testInteractions() throws {
        for variant in Variant.pair {
            // The Listen split button's method menu.
            run("result", variant) { app, window in
                guard let listen = self.toolbarListen(in: window) else {
                    XCTFail("result \(variant.name): no toolbar Listen")
                    return
                }
                let chevron = window.toolbars.firstMatch.descendants(matching: .any)
                    .matching(NSPredicate(format: "label == %@", "Other Ways to Identify")).firstMatch
                if chevron.exists {
                    chevron.click()
                } else {
                    listen.coordinate(withNormalizedOffset: CGVector(dx: 0.93, dy: 0.5)).click()
                }
                sleep(1)
                self.attachScreen("listen-menu", variant)
                app.typeKey(.escape, modifierFlags: [])
            }
            // Back from a result to the stage.
            run("result", variant) { app, window in
                let back = self.element("mf.backToStage", in: window)
                XCTAssertTrue(back.waitForExistence(timeout: 3), "result \(variant.name): no back button")
                back.click()
                XCTAssertTrue(self.element("mf.listenButton", in: window).waitForExistence(timeout: 3),
                              "result \(variant.name): back did not show the stage")
                sleep(1)
                self.attachWindow(window, "result-back", variant)
            }
            // The sidebar's Listen row returns to the stage too.
            run("result", variant) { app, window in
                let row = self.element("mf.sidebar.listen", in: window)
                XCTAssertTrue(row.exists, "result \(variant.name): no Listen row in the sidebar")
                if row.exists { row.click() }
                XCTAssertTrue(self.element("mf.listenButton", in: window).waitForExistence(timeout: 3),
                              "result \(variant.name): the sidebar's Listen row did not show the stage")
                sleep(1)
                self.attachWindow(window, "result-sidebar-listen", variant)
            }
            // After no match, a source toggle switches the method without listening.
            run("noMatch", variant) { app, window in
                let hum = self.element("mf.source.hum", in: window)
                XCTAssertTrue(hum.waitForExistence(timeout: 3), "noMatch \(variant.name): no Hum toggle")
                hum.click()
                sleep(1)
                self.attachWindow(window, "noMatch-hum", variant)
            }
            // Clicking a live answer while humming stops and selects it in the final list.
            run("hum-answers", variant) { app, window in
                let answer = window.buttons.matching(NSPredicate(format: "label BEGINSWITH %@", "Dancing Queen")).firstMatch
                XCTAssertTrue(answer.waitForExistence(timeout: 5), "hum-answers \(variant.name): no live answer")
                guard answer.exists else { return }
                answer.click()
                XCTAssertTrue(self.element("mf.humAgain", in: window).waitForExistence(timeout: 6),
                              "hum-answers \(variant.name): the final list did not appear")
                sleep(1)
                self.attachWindow(window, "hum-answers-chosen", variant)
            }
            // "Not Now" on the speech explanation hums without words.
            run("speechPrePrompt", variant) { app, window in
                let notNow = self.element("mf.action.skipSpeech", in: window)
                XCTAssertTrue(notNow.waitForExistence(timeout: 3), "speechPrePrompt \(variant.name): no Not Now")
                guard notNow.exists else { return }
                notNow.click()
                sleep(2)
                self.attachWindow(window, "speechPrePrompt-notNow", variant)
            }
            // ⌘⇧P pins the whole selection. (Synthetic hover and right clicks don't reach
            // list rows on the runners, so row hover and context menus aren't shot.)
            run("library-selection", variant) { app, window in
                app.typeKey("p", modifierFlags: [.command, .shift])
                sleep(1)
                self.attachWindow(window, "library-selection-pinned", variant)
            }
        }
    }

    // MARK: - Running states

    private func launch(_ state: String, _ variant: Variant, extra: [String] = []) -> XCUIApplication {
        let app = XCUIApplication()
        app.launchArguments = [
            "-MFDemoState", state,
            "-MFWindowSize", "\(variant.width)x\(variant.height)",
            "-MFAppearance", variant.dark ? "dark" : "light",
            "-MFReduceMotion", "YES",
            "-MFInspector", variant.inspector ? "YES" : "NO",
            "-ApplePersistenceIgnoreState", "YES",
        ] + extra
        app.launch()
        return app
    }

    private func run(_ state: String, _ variant: Variant, _ body: (XCUIApplication, XCUIElement) -> Void) {
        let app = launch(state, variant)
        defer { app.terminate() }
        guard let window = mainWindow(app, state: state) else { return }
        body(app, window)
    }

    private func mainWindow(_ app: XCUIApplication, state: String) -> XCUIElement? {
        let marker = element("MFDemoReady", in: app)
        if !marker.waitForExistence(timeout: 30) {
            XCTFail("\(state): the demo state never became ready")
        }
        let window = app.windows.containing(.any, identifier: "MFDemoReady").firstMatch
        if window.exists { return window }
        let first = app.windows.firstMatch
        return first.exists ? first : nil
    }

    private func capture(_ state: String, _ variant: Variant) {
        let app = launch(state, variant)
        defer { app.terminate() }
        guard let main = mainWindow(app, state: state) else {
            XCTFail("\(state) \(variant.name): no window")
            return
        }
        var window = main
        if state.hasPrefix("settings-") {
            sleep(1)
            if let settings = app.windows.allElementsBoundByIndex.first(where: { !$0.descendants(matching: .any)["MFDemoReady"].exists }) {
                window = settings
            }
        }
        attachWindow(window, state, variant)
        if !state.hasPrefix("settings-") {
            checkGeometry(main, state: state, variant: variant)
        }
    }

    // MARK: - Attachments

    private func attachWindow(_ window: XCUIElement, _ state: String, _ variant: Variant) {
        let shot = XCTAttachment(screenshot: window.screenshot())
        shot.name = "shot__\(state)__\(variant.name)__"
        shot.lifetime = .keepAlways
        add(shot)
    }

    private func attachScreen(_ state: String, _ variant: Variant) {
        let shot = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
        shot.name = "shot__\(state)__\(variant.name)__"
        shot.lifetime = .keepAlways
        add(shot)
    }

    // MARK: - Geometry

    /// The first element with `identifier` under `root` (SwiftUI may repeat an
    /// identifier on a container's children).
    private func element(_ identifier: String, in root: XCUIElement) -> XCUIElement {
        root.descendants(matching: .any).matching(identifier: identifier).firstMatch
    }

    private func toolbarListen(in window: XCUIElement) -> XCUIElement? {
        let listen = element("mf.listen", in: window.toolbars.firstMatch)
        return listen.exists ? listen : nil
    }

    /// Toolbar controls must not cross a split divider (sidebar, inspector), share one
    /// center line, and Listen must be there unless the idle stage hides it.
    private func checkGeometry(_ window: XCUIElement, state: String, variant: Variant) {
        var violations: [String] = []
        let toolbar = window.toolbars.firstMatch
        let windowFrame = window.frame
        let dividers = window.splitters.allElementsBoundByIndex.map(\.frame).filter { $0.height > $0.width }
        let types: [XCUIElement.ElementType] = [
            .button, .menuButton, .popUpButton, .textField, .searchField, .segmentedControl, .radioGroup, .checkBox,
        ]
        var items: [[String: Any]] = []
        if toolbar.exists {
            for type in types {
                for element in toolbar.descendants(matching: type).allElementsBoundByIndex where element.exists {
                    let frame = element.frame
                    guard frame.width > 1, frame.height > 1 else { continue }
                    let id = element.identifier
                    let label = element.label
                    items.append([
                        "type": String(describing: type.rawValue), "id": id, "label": label,
                        "x": frame.minX - windowFrame.minX, "y": frame.minY - windowFrame.minY,
                        "w": frame.width, "h": frame.height,
                    ])
                    for divider in dividers {
                        let x = divider.midX
                        if frame.minX + 0.5 < x, x < frame.maxX - 0.5 {
                            violations.append("\(label.isEmpty ? id : label) crosses the divider at x=\(Int(x - windowFrame.minX))")
                        }
                    }
                }
            }
        }
        let listen = toolbarListen(in: window)
        if !Self.statesWithoutToolbarListen.contains(state) {
            if listen == nil || listen?.isHittable == false {
                violations.append("Listen is missing from the toolbar (overflow?)")
            }
        }
        let toggle = element("mf.inspectorToggle", in: toolbar)
        if let listen, toggle.exists, abs(listen.frame.midY - toggle.frame.midY) > 1 {
            violations.append("Listen and the inspector toggle are not on one line")
        }
        let report: [String: Any] = [
            "state": state, "variant": variant.name, "ok": violations.isEmpty, "violations": violations,
            "window": ["w": windowFrame.width, "h": windowFrame.height],
            "dividers": dividers.map { $0.midX - windowFrame.minX }, "items": items,
        ]
        if let data = try? JSONSerialization.data(withJSONObject: report, options: [.prettyPrinted, .sortedKeys]) {
            let attachment = XCTAttachment(data: data, uniformTypeIdentifier: "public.json")
            attachment.name = "geom__\(state)__\(variant.name)__"
            attachment.lifetime = .keepAlways
            add(attachment)
        }
        for violation in violations {
            XCTFail("\(state) \(variant.name): \(violation)")
        }
    }
}
