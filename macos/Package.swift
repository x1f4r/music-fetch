// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "MusicFetchMac",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "MusicFetchMac", targets: ["MusicFetchMac"])
    ],
    targets: [
        .executableTarget(
            name: "MusicFetchMac",
            path: "Sources",
            resources: [.copy("Resources")],
            linkerSettings: [
                .linkedFramework("SwiftUI"),
                .linkedFramework("AppKit"),
                .linkedFramework("AVFoundation"),
                .linkedFramework("ScreenCaptureKit"),
                .linkedFramework("CoreMedia"),
                .linkedFramework("UserNotifications"),
                .linkedFramework("ServiceManagement")
            ]
        )
    ]
)
