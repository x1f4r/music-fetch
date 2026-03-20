from __future__ import annotations

import math
import shutil
import subprocess
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter


ROOT = Path(__file__).resolve().parents[1]
ASSET_DIR = ROOT / "assets" / "app_icon"
ICONSET_DIR = ASSET_DIR / "MusicFetch.iconset"
BASE_PNG = ASSET_DIR / "music-fetch-1024.png"
ICNS_PATH = ASSET_DIR / "MusicFetch.icns"


def rounded_rectangle_mask(size: int, radius: int) -> Image.Image:
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, size - 1, size - 1), radius=radius, fill=255)
    return mask


def vertical_gradient(size: int, top: tuple[int, int, int], bottom: tuple[int, int, int]) -> Image.Image:
    gradient = Image.new("RGB", (1, size))
    pixels = []
    for y in range(size):
        t = y / (size - 1)
        pixels.append(
            (
                int(top[0] * (1 - t) + bottom[0] * t),
                int(top[1] * (1 - t) + bottom[1] * t),
                int(top[2] * (1 - t) + bottom[2] * t),
            )
        )
    gradient.putdata(pixels)
    return gradient.resize((size, size))


def radial_glow(size: int, center: tuple[float, float], radius: float, color: tuple[int, int, int, int]) -> Image.Image:
    image = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    cx, cy = center
    px = image.load()
    for y in range(size):
        for x in range(size):
            dx = x - cx
            dy = y - cy
            distance = math.sqrt(dx * dx + dy * dy)
            t = max(0.0, 1.0 - distance / radius)
            alpha = int(color[3] * (t**2))
            if alpha:
                px[x, y] = (color[0], color[1], color[2], alpha)
    return image.filter(ImageFilter.GaussianBlur(radius=18))


def draw_icon(size: int = 1024) -> Image.Image:
    image = vertical_gradient(size, (34, 44, 62), (11, 18, 31)).convert("RGBA")
    mask = rounded_rectangle_mask(size, radius=230)
    image.putalpha(mask)

    overlay = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    overlay.alpha_composite(radial_glow(size, (size * 0.28, size * 0.2), size * 0.65, (83, 211, 255, 105)))
    overlay.alpha_composite(radial_glow(size, (size * 0.72, size * 0.82), size * 0.55, (91, 120, 255, 120)))
    overlay.alpha_composite(radial_glow(size, (size * 0.5, size * 0.48), size * 0.45, (255, 255, 255, 28)))
    image = Image.alpha_composite(image, overlay)

    glass = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    glass_draw = ImageDraw.Draw(glass)
    glass_draw.rounded_rectangle(
        (82, 82, size - 82, size - 82),
        radius=190,
        fill=(255, 255, 255, 34),
        outline=(255, 255, 255, 48),
        width=3,
    )
    glass = glass.filter(ImageFilter.GaussianBlur(radius=0.5))
    image = Image.alpha_composite(image, glass)

    glyph = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(glyph)

    cx = size * 0.5
    cy = size * 0.5
    plate_w = size * 0.52
    plate_h = size * 0.52
    plate_box = (cx - plate_w / 2, cy - plate_h / 2, cx + plate_w / 2, cy + plate_h / 2)
    draw.rounded_rectangle(
        plate_box,
        radius=int(size * 0.16),
        fill=(255, 255, 255, 218),
        outline=(255, 255, 255, 235),
        width=2,
    )

    # Stylized waveform bars
    bar_width = size * 0.035
    gap = size * 0.025
    heights = [0.16, 0.28, 0.40, 0.28, 0.16]
    start_x = cx - ((len(heights) * bar_width) + ((len(heights) - 1) * gap)) / 2
    for index, factor in enumerate(heights):
        x0 = start_x + index * (bar_width + gap)
        x1 = x0 + bar_width
        y0 = cy - size * factor / 2
        y1 = cy + size * factor / 2
        draw.rounded_rectangle((x0, y0, x1, y1), radius=bar_width / 2, fill=(23, 41, 71, 255))

    # Magnifying glass ring
    lens_radius = size * 0.105
    lens_cx = cx + size * 0.17
    lens_cy = cy + size * 0.16
    ring_width = int(size * 0.032)
    draw.ellipse(
        (
            lens_cx - lens_radius,
            lens_cy - lens_radius,
            lens_cx + lens_radius,
            lens_cy + lens_radius,
        ),
        outline=(23, 41, 71, 255),
        width=ring_width,
    )
    handle_len = size * 0.12
    handle_thickness = int(size * 0.028)
    angle = math.radians(44)
    hx0 = lens_cx + math.cos(angle) * (lens_radius - size * 0.01)
    hy0 = lens_cy + math.sin(angle) * (lens_radius - size * 0.01)
    hx1 = hx0 + math.cos(angle) * handle_len
    hy1 = hy0 + math.sin(angle) * handle_len
    draw.line((hx0, hy0, hx1, hy1), fill=(23, 41, 71, 255), width=handle_thickness)

    glyph = glyph.filter(ImageFilter.GaussianBlur(radius=0.25))
    image = Image.alpha_composite(image, glyph)

    return image


def write_iconset(base_image: Image.Image) -> None:
    if ICONSET_DIR.exists():
        shutil.rmtree(ICONSET_DIR)
    ICONSET_DIR.mkdir(parents=True, exist_ok=True)
    sizes = {
        "icon_16x16.png": 16,
        "icon_16x16@2x.png": 32,
        "icon_32x32.png": 32,
        "icon_32x32@2x.png": 64,
        "icon_128x128.png": 128,
        "icon_128x128@2x.png": 256,
        "icon_256x256.png": 256,
        "icon_256x256@2x.png": 512,
        "icon_512x512.png": 512,
        "icon_512x512@2x.png": 1024,
    }
    for filename, size in sizes.items():
        base_image.resize((size, size), Image.Resampling.LANCZOS).save(ICONSET_DIR / filename)


def build_icns() -> None:
    subprocess.run(["/usr/bin/iconutil", "-c", "icns", str(ICONSET_DIR), "-o", str(ICNS_PATH)], check=True)


def main() -> None:
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    image = draw_icon()
    image.save(BASE_PNG)
    write_iconset(image)
    build_icns()
    print(BASE_PNG)
    print(ICNS_PATH)


if __name__ == "__main__":
    main()
