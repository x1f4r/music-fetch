import AppKit
import SwiftUI

/// Loads and caches artwork images.
final class ArtworkLoader {
    static let shared = ArtworkLoader()

    private let cache = NSCache<NSString, NSImage>()
    private var inflight: [String: Task<Data?, Never>] = [:]

    init() {
        cache.countLimit = 300
    }

    func cached(_ url: String) -> NSImage? {
        cache.object(forKey: url as NSString)
    }

    func image(for url: String) async -> NSImage? {
        if let image = cached(url) { return image }
        let task: Task<Data?, Never>
        if let running = inflight[url] {
            task = running
        } else {
            guard let remote = URL(string: url), remote.scheme == "https" || remote.scheme == "http" else { return nil }
            task = Task.detached {
                var request = URLRequest(url: remote)
                request.timeoutInterval = 20
                guard let (data, response) = try? await URLSession.shared.data(for: request),
                    (response as? HTTPURLResponse)?.statusCode ?? 200 < 400
                else { return nil }
                return data
            }
            inflight[url] = task
        }
        let data = await task.value
        inflight[url] = nil
        if let cachedImage = cached(url) { return cachedImage }
        guard let data, let image = NSImage(data: data) else { return nil }
        cache.setObject(image, forKey: url as NSString)
        return image
    }
}

/// Artwork with continuous rounded corners. Missing artwork shows a quiet tile;
/// `hatched` draws the unresolved ("ID") tile.
struct ArtworkView: View {
    var url: String?
    var size: CGFloat
    var cornerRadius: CGFloat
    var hatched = false
    /// Wider-than-square thumbnails (video) keep their aspect inside `size × height`.
    var height: CGFloat?

    @State private var image: NSImage?

    var body: some View {
        let shape = RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
        ZStack {
            if hatched {
                shape.fill(.fill.quaternary)
                HatchPattern().clipShape(shape)
            } else if let palette = DemoArtwork.palette(for: url) {
                shape.fill(palette)
            } else if let image = image ?? url.flatMap(ArtworkLoader.shared.cached) {
                Image(nsImage: image)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
                    .frame(width: size, height: height ?? size)
                    .clipShape(shape)
            } else {
                shape.fill(.fill.tertiary)
                Image(systemName: "music.note")
                    .font(.system(size: size * 0.32))
                    .foregroundStyle(.tertiary)
            }
        }
        .frame(width: size, height: height ?? size)
        .overlay(shape.strokeBorder(.separator.opacity(0.5), lineWidth: 0.5))
        .task(id: url) {
            guard let url, DemoArtwork.palette(for: url) == nil, ArtworkLoader.shared.cached(url) == nil else { return }
            image = await ArtworkLoader.shared.image(for: url)
        }
        .accessibilityHidden(true)
    }
}

/// Diagonal hatch for unidentified music: label color at 22 %, 1.5 pt every 6 pt.
struct HatchPattern: View {
    var opacity: Double = 0.22

    var body: some View {
        Canvas { context, size in
            var path = Path()
            var x = -size.height
            while x < size.width {
                path.move(to: CGPoint(x: x, y: size.height))
                path.addLine(to: CGPoint(x: x + size.height, y: 0))
                x += 6
            }
            context.stroke(path, with: .color(.primary.opacity(opacity)), lineWidth: 1.5)
        }
    }
}

/// Gradient artwork for `mfdemo://artwork/<palette>` (demo data and previews only).
enum DemoArtwork {
    static func palette(for url: String?) -> LinearGradient? {
        guard let url, url.hasPrefix("mfdemo://artwork/") else { return nil }
        let name = String(url.dropFirst("mfdemo://artwork/".count))
        let colors: [Color] =
            switch name {
            case "ocean": [Color(red: 0.25, green: 0.62, blue: 0.98), Color(red: 0.05, green: 0.16, blue: 0.45)]
            case "sage": [Color(red: 0.62, green: 0.72, blue: 0.69), Color(red: 0.36, green: 0.48, blue: 0.46)]
            case "ember": [Color(red: 0.95, green: 0.28, blue: 0.22), Color(red: 0.45, green: 0.04, blue: 0.1)]
            case "sand": [Color(red: 0.84, green: 0.78, blue: 0.68), Color(red: 0.55, green: 0.47, blue: 0.36)]
            case "sky": [Color(red: 0.62, green: 0.8, blue: 0.99), Color(red: 0.38, green: 0.6, blue: 0.95)]
            case "paper": [Color(red: 0.95, green: 0.93, blue: 0.88), Color(red: 0.82, green: 0.8, blue: 0.75)]
            case "sunset": [Color(red: 0.98, green: 0.67, blue: 0.25), Color(red: 0.82, green: 0.24, blue: 0.16)]
            case "dune": [Color(red: 0.86, green: 0.74, blue: 0.53), Color(red: 0.35, green: 0.23, blue: 0.12)]
            case "night": [Color(red: 0.32, green: 0.32, blue: 0.34), Color(red: 0.12, green: 0.12, blue: 0.14)]
            case "navy": [Color(red: 0.16, green: 0.22, blue: 0.38), Color(red: 0.09, green: 0.12, blue: 0.24)]
            case "fog": [Color(red: 0.9, green: 0.88, blue: 0.85), Color(red: 0.72, green: 0.7, blue: 0.66)]
            case "forest": [Color(red: 0.3, green: 0.5, blue: 0.38), Color(red: 0.14, green: 0.3, blue: 0.22)]
            case "violet": [Color(red: 0.38, green: 0.2, blue: 0.5), Color(red: 0.62, green: 0.4, blue: 0.85)]
            case "peach": [Color(red: 0.96, green: 0.78, blue: 0.56), Color(red: 0.86, green: 0.55, blue: 0.3)]
            case "gold": [Color(red: 0.3, green: 0.24, blue: 0.08), Color(red: 0.92, green: 0.75, blue: 0.24)]
            case "hazard": [Color(red: 0.2, green: 0.2, blue: 0.2), Color(red: 0.1, green: 0.1, blue: 0.1)]
            case "stage": [Color(red: 0.16, green: 0.16, blue: 0.17), Color(red: 0.06, green: 0.06, blue: 0.07)]
            default: [Color.gray, Color.gray.opacity(0.6)]
            }
        return LinearGradient(colors: colors, startPoint: .topLeading, endPoint: .bottomTrailing)
    }
}
