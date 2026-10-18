import AppKit
import SwiftUI

/// Offscreen toolbar geometry check (Debug builds): `-MFRender <dir>` lays out demo
/// states in windows that are never shown, at several widths, with and without the
/// inspector, in light and dark. It writes `geometry.json` and rough PNGs to `<dir>` and
/// exits non-zero when a toolbar item crosses a split divider, the toolbar overflows,
/// items don't share one line, or Listen is missing. No window appears, the app never
/// becomes active, and nothing is written to the app's preferences. Liquid Glass does
/// not draw offscreen, so the PNGs only show layout; CI screenshots show the real thing.
///
/// Run it through `apple/tools/render-offscreen.sh`, which uses a copy of the Debug
/// app under its own bundle identifier.
enum RenderHarness {
    nonisolated static let output: URL? = {
        #if DEBUG
        UserDefaults.standard.string(forKey: "MFRender").map { URL(fileURLWithPath: $0) }
        #else
        nil
        #endif
    }()

    nonisolated static var isActive: Bool { output != nil }

    #if DEBUG
    /// Every state the screenshot tests cover, minus sheets and alerts.
    static let states = [
        "idle", "result", "candidates", "mix", "collection", "library", "library-selection", "library-pinned",
        "session", "noMatch", "failed", "listening",
    ]
    static let widths: [CGFloat] = [900, 1000, 1180, 1440]
    /// States where the idle stage hides the toolbar's Listen button.
    static let withoutListen: Set<String> = ["idle", "noMatch", "failed", "listening"]

    static func run() async {
        guard let output else { return }
        try? FileManager.default.createDirectory(at: output, withIntermediateDirectories: true)
        let only = UserDefaults.standard.string(forKey: "MFRenderStates").map { Set($0.split(separator: ",").map(String.init)) }
        var results: [[String: Any]] = []
        for state in states where only?.contains(state) ?? true {
            for width in widths {
                for inspector in [true, false] {
                    for dark in [false, true] where !(dark && width != 1180) {
                        results.append(await render(state, width: width, inspector: inspector, dark: dark, into: output))
                    }
                }
            }
        }
        let failures = results.filter { $0["ok"] as? Bool != true }
        if let data = try? JSONSerialization.data(withJSONObject: results, options: [.prettyPrinted, .sortedKeys]) {
            try? data.write(to: output.appending(path: "geometry.json"))
        }
        for failure in failures {
            let name = "\(failure["state"] ?? "") \(failure["width"] ?? "") inspector=\(failure["inspector"] ?? "")"
            print("FAIL \(name): \((failure["violations"] as? [String] ?? []).joined(separator: "; "))")
        }
        print("\(results.count) layouts, \(failures.count) failures")
        exit(failures.isEmpty ? 0 : 1)
    }

    private static func render(_ state: String, width: CGFloat, inspector: Bool, dark: Bool, into output: URL) async
        -> [String: Any]
    {
        var demo = DemoState(name: state)
        demo.offscreen = true
        demo.reduceMotion = true
        demo.inspector = inspector
        let app = AppModel(demo: demo)
        let host = NSHostingController(rootView: RootView().environment(app))
        host.sceneBridgingOptions = [.toolbars, .title]
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: width, height: 760),
            styleMask: [.titled, .closable, .miniaturizable, .resizable, .fullSizeContentView], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.isRestorable = false
        window.appearance = NSAppearance(named: dark ? .darkAqua : .aqua)
        window.toolbarStyle = .unified
        window.contentViewController = host
        window.setContentSize(NSSize(width: width, height: 760))
        await app.start()
        // The inspector folds 150 ms after the width settles.
        try? await Task.sleep(for: .milliseconds(300))
        await settle(window)
        var result = ToolbarGeometry(window: window).check(
            listenExpected: !withoutListen.contains(state), inspectorShown: app.isInspectorPresented)
        let name = "\(state)__\(Int(width))-\(inspector ? "inspector" : "closed")-\(dark ? "dark" : "light")"
        result["state"] = state
        result["width"] = Int(width)
        result["inspector"] = inspector
        result["inspectorShown"] = app.isInspectorPresented
        result["debug"] = "pref=\(app.showInspector) collapsed=\(app.inspectorCollapsed) window=\(Int(window.frame.width)) "
            + (window.toolbar?.items ?? []).map { "\($0.itemIdentifier.rawValue)[\($0.label)]" }.joined(separator: ", ")
        result["dark"] = dark
        result["png"] = "\(name).png"
        writePNG(window, to: output.appending(path: "\(name).png"))
        window.close()
        app.listen.cancel()
        return result
    }

    /// Lets layout, toolbar and animations settle: until frames stop changing.
    private static func settle(_ window: NSWindow) async {
        var last = ""
        for _ in 0..<40 {
            try? await Task.sleep(for: .milliseconds(40))
            window.layoutIfNeeded()
            let now = ToolbarGeometry(window: window).signature
            if now == last { return }
            last = now
        }
    }

    private static func writePNG(_ window: NSWindow, to url: URL) {
        guard let view = window.contentView?.superview,
            let bitmap = view.bitmapImageRepForCachingDisplay(in: view.bounds)
        else { return }
        view.cacheDisplay(in: view.bounds, to: bitmap)
        try? bitmap.representation(using: .png, properties: [:])?.write(to: url)
    }
    #endif
}

#if DEBUG
/// The toolbar's frames, from AppKit's views (platters, dividers, overflow) and the
/// accessibility identifiers of our items.
@MainActor
struct ToolbarGeometry {
    let window: NSWindow

    private var themeFrame: NSView? { window.contentView?.superview }
    private var toolbarHeight: CGFloat { window.frame.height - window.contentLayoutRect.height }

    struct Box {
        var kind: String
        var frame: CGRect
    }

    /// Toolbar platters (macOS 26) or item viewers (before), dividers, overflow.
    var boxes: [Box] {
        var out: [Box] = []
        guard let root = themeFrame else { return out }
        func walk(_ view: NSView) {
            let name = String(describing: type(of: view))
            if name == "NSToolbarPlatterView" || name == "NSToolbarItemViewer" || name == "NSSplitDividerView"
                || name.hasPrefix("NSToolbarClippedItemsIndicator") || name == "NSSearchToolbarItemView"
            {
                out.append(Box(kind: name, frame: view.convert(view.bounds, to: nil)))
            }
            view.subviews.forEach(walk)
        }
        walk(root)
        return out
    }

    var signature: String {
        boxes.map { "\($0.kind)\(Int($0.frame.minX)),\(Int($0.frame.width))" }.joined(separator: "|")
    }

    /// The frame (window coordinates) of the toolbar item labeled `label`.
    func item(_ label: String) -> CGRect? {
        guard let item = window.toolbar?.items.first(where: { $0.label == label }), let view = item.view, view.window != nil
        else { return nil }
        return view.convert(view.bounds, to: nil)
    }

    func check(listenExpected: Bool, inspectorShown: Bool) -> [String: Any] {
        var violations: [String] = []
        let all = boxes
        // Items in the toolbar row: their top is within the toolbar band.
        let band = window.frame.height - toolbarHeight
        let items = all.filter {
            ($0.kind == "NSToolbarPlatterView" || $0.kind == "NSToolbarItemViewer" || $0.kind == "NSSearchToolbarItemView")
                && $0.frame.minY >= band - 1 && $0.frame.height < 60
        }
        let dividers = all.filter { $0.kind == "NSSplitDividerView" && $0.frame.minX < window.frame.width - 1 }.map(\.frame.midX)
        for item in items {
            for x in dividers where item.frame.minX + 0.5 < x && x < item.frame.maxX - 0.5 {
                violations.append("\(item.kind) \(Int(item.frame.minX))–\(Int(item.frame.maxX)) crosses the divider at \(Int(x))")
            }
        }
        let platters = items.filter { $0.kind == "NSToolbarPlatterView" }
        if let first = platters.first {
            for platter in platters where abs(platter.frame.midY - first.frame.midY) > 0.5 || abs(platter.frame.height - first.frame.height) > 0.5 {
                violations.append("platter \(Int(platter.frame.minX)) is off the toolbar line")
            }
        }
        if window.frame.width >= 1000, all.contains(where: { $0.kind.hasPrefix("NSToolbarClippedItemsIndicator") }) {
            violations.append("toolbar overflows")
        }
        let listen = item(Copy.listenAction)
        let toggle = item(Copy.inspector)
        if listenExpected, listen == nil || listen?.width == 0 {
            violations.append("Listen is missing")
        }
        if inspectorShown, let inspectorDivider = dividers.max() {
            if let listen, listen.maxX > inspectorDivider {
                violations.append("Listen ends at \(Int(listen.maxX)), past the inspector divider at \(Int(inspectorDivider))")
            }
            if let toggle, toggle.minX < inspectorDivider {
                violations.append("the inspector toggle starts at \(Int(toggle.minX)), before the inspector divider")
            }
        }
        return [
            "ok": violations.isEmpty, "violations": violations, "dividers": dividers.map { Int($0) },
            "items": items.map { ["kind": $0.kind, "x": Int($0.frame.minX), "w": Int($0.frame.width), "h": Int($0.frame.height)] },
            "listen": listen.map { [Int($0.minX), Int($0.maxX)] } ?? [], "toggle": toggle.map { [Int($0.minX), Int($0.maxX)] } ?? [],
        ]
    }
}
#endif
