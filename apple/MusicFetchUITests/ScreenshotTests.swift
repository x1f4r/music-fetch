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
        "listening", "hum", "matching", "collection", "failed", "micDenied", "prePrompt", "captureSilent", "providers",
        "lyrics", "melodyPrompt", "library-pinned", "settings-general", "settings-melody",
    ]
    /// The idle stage hides the toolbar Listen button (the big button is the same action).
    static let statesWithoutToolbarListen: Set<String> = [
        "idle", "noMatch", "failed", "micDenied", "prePrompt", "listening", "hum", "matching", "captureSilent",
        "providers", "melodyPrompt", "downloading",
    ]

    override func setUp() {
        continueAfterFailure = true
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
                listen.coordinate(withNormalizedOffset: CGVector(dx: 0.93, dy: 0.5)).click()
                sleep(1)
                self.attachScreen("listen-menu", variant)
                app.typeKey(.escape, modifierFlags: [])
            }
            // Back from a result to the stage.
            run("result", variant) { app, window in
                let back = window.buttons["mf.backToStage"]
                XCTAssertTrue(back.waitForExistence(timeout: 3), "result \(variant.name): no back button")
                back.click()
                XCTAssertTrue(window.descendants(matching: .any)["mf.listenButton"].waitForExistence(timeout: 3),
                              "result \(variant.name): back did not show the stage")
                sleep(1)
                self.attachWindow(window, "result-back", variant)
            }
            // The sidebar's Listen row returns to the stage too.
            run("result", variant) { app, window in
                let row = window.outlines.firstMatch.cells.element(boundBy: 0)
                if row.exists { row.click() }
                XCTAssertTrue(window.descendants(matching: .any)["mf.listenButton"].waitForExistence(timeout: 3),
                              "result \(variant.name): the sidebar's Listen row did not show the stage")
                sleep(1)
                self.attachWindow(window, "result-sidebar-listen", variant)
            }
            // After no match, a source toggle switches the method without listening.
            run("noMatch", variant) { app, window in
                let hum = window.descendants(matching: .any)["mf.source.hum"]
                XCTAssertTrue(hum.waitForExistence(timeout: 3), "noMatch \(variant.name): no Hum toggle")
                hum.click()
                sleep(1)
                self.attachWindow(window, "noMatch-hum", variant)
            }
            // Hovering a library row shows its pin button.
            run("library", variant) { app, window in
                let rows = window.outlines.firstMatch.cells
                if rows.count > 2 { rows.element(boundBy: 2).hover() }
                sleep(1)
                self.attachWindow(window, "library-hover", variant)
            }
            // The context menu of a multiple selection, then ⌘⇧P pins all of it.
            run("library-selection", variant) { app, window in
                let rows = window.outlines.firstMatch.cells
                if rows.count > 1 { rows.element(boundBy: 1).rightClick() }
                sleep(1)
                self.attachScreen("library-selection-menu", variant)
                app.typeKey(.escape, modifierFlags: [])
                sleep(1)
                app.typeKey("p", modifierFlags: [.command, .shift])
                sleep(1)
                self.attachWindow(window, "library-selection-pinned", variant)
            }
        }
    }

    // MARK: - Running states

    private func launch(_ state: String, _ variant: Variant) -> XCUIApplication {
        let app = XCUIApplication()
        app.launchArguments = [
            "-MFDemoState", state,
            "-MFWindowSize", "\(variant.width)x\(variant.height)",
            "-MFAppearance", variant.dark ? "dark" : "light",
            "-MFReduceMotion", "YES",
            "-MFInspector", variant.inspector ? "YES" : "NO",
            "-ApplePersistenceIgnoreState", "YES",
        ]
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
        let marker = app.descendants(matching: .any)["MFDemoReady"]
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

    private func toolbarListen(in window: XCUIElement) -> XCUIElement? {
        let listen = window.toolbars.firstMatch.descendants(matching: .any)["mf.listen"]
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
        let toggle = toolbar.descendants(matching: .any)["mf.inspectorToggle"]
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
