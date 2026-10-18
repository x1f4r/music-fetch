#!/usr/bin/env python3
"""Write the contour mark (DESIGN.md §2.1) as a custom SF Symbol.

  apple/tools/make-contour-symbol.py

Output: apple/MusicFetch/Resources/Assets.xcassets/contour.mark.symbolset

The mark is one round-capped stroke. Symbols are filled outlines, so the stroke is
offset to both sides of its centre line and closed with round caps. Every weight uses
the same number of points in the same order, which makes the Ultralight, Regular and
Black sources interpolatable: the system derives all nine weights and three scales.
"""

import json
import math
from pathlib import Path

# The centre line in the mark's 48 x 20 unit box.
START = (3.0, 11.0)
CURVES = [
    ((7, 11), (8, 5), (12, 5)),
    ((16, 5), (17, 15), (21, 15)),
    ((26, 15), (26, 3), (31, 3)),
    ((36, 3), (36, 13), (40, 13)),
    ((43, 13), (44, 9), (45, 9)),
]
# Template units per box unit at the small scale: the centre line is 120 units wide,
# which the system scales to about 21 x 7 pt next to 13 pt text at the medium scale.
SCALE = 120 / 42
# Stroke widths in template units; Regular is the mark's own 3-unit stroke.
WEIGHTS = {"Ultralight": 2.6, "Regular": 3 * SCALE, "Black": 16.5}
COLUMNS = {"Ultralight": 660.0, "Regular": 1650.0, "Black": 2640.0}
BASELINE_S = 696.0
CAP_HEIGHT = 70.459
SIDE_BEARING = 3.0
SAMPLES_PER_CURVE = 48
CAP_POINTS = 16


def cubic(p0, p1, p2, p3, t):
    u = 1 - t
    return tuple(u**3 * a + 3 * u * u * t * b + 3 * u * t * t * c + t**3 * d for a, b, c, d in zip(p0, p1, p2, p3))


def tangent(p0, p1, p2, p3, t):
    u = 1 - t
    return tuple(3 * u * u * (b - a) + 6 * u * t * (c - b) + 3 * t * t * (d - c) for a, b, c, d in zip(p0, p1, p2, p3))


def second(p0, p1, p2, p3, t):
    return tuple(6 * (1 - t) * (c - 2 * b + a) + 6 * t * (d - 2 * c + b) for a, b, c, d in zip(p0, p1, p2, p3))


def centre_line():
    """Points, unit tangents and signed curvatures along the centre line, in template units."""
    points, tangents, curvatures = [], [], []
    p0 = START
    for index, (c1, c2, p3) in enumerate(CURVES):
        for step in range(SAMPLES_PER_CURVE + 1):
            if index > 0 and step == 0:
                continue
            t = step / SAMPLES_PER_CURVE
            x, y = cubic(p0, c1, c2, p3, t)
            dx, dy = tangent(p0, c1, c2, p3, t)
            ddx, ddy = second(p0, c1, c2, p3, t)
            length = math.hypot(dx, dy)
            # Box y grows downward like SVG; centre the mark's 3...15 band on the cap height.
            points.append(((x - START[0]) * SCALE, (y - 9) * SCALE - CAP_HEIGHT / 2))
            tangents.append((dx / length, dy / length))
            curvatures.append((dx * ddy - dy * ddx) / length**3 / SCALE)
        p0 = p3
    return points, tangents, curvatures


def outline(width):
    """The stroke's outline: both sides of the centre line and two round caps. On the
    inside of a bend tighter than the stroke, the offset stops short of the centre of
    curvature so the side never folds over itself."""
    points, tangents, curvatures = centre_line()
    half = width / 2

    def side(sign):
        result = []
        for (x, y), (tx, ty), k in zip(points, tangents, curvatures):
            # N = (-ty, tx) points to the centre of curvature when k > 0.
            reach = half
            if k * sign > 0:
                reach = min(half, 0.85 / abs(k))
            result.append((x - sign * ty * reach, y + sign * tx * reach))
        return result

    right, left = side(1), side(-1)
    path = list(left)
    # End cap: half circle from the left side over the end to the right side.
    (ex, ey), (tx, ty) = points[-1], tangents[-1]
    start_angle = math.atan2(-tx, ty)
    for k in range(1, CAP_POINTS):
        a = start_angle + math.pi * k / CAP_POINTS
        path.append((ex + half * math.cos(a), ey + half * math.sin(a)))
    path.extend(reversed(right))
    (sx, sy), (tx, ty) = points[0], tangents[0]
    start_angle = math.atan2(tx, -ty)
    for k in range(1, CAP_POINTS):
        a = start_angle + math.pi * k / CAP_POINTS
        path.append((sx + half * math.cos(a), sy + half * math.sin(a)))
    return path


def path_data(points, dx):
    head, *rest = points
    parts = [f"M{head[0] + dx:.3f} {head[1]:.3f}"] + [f"L{x + dx:.3f} {y:.3f}" for x, y in rest]
    return " ".join(parts) + " Z"


def main():
    guides = [
        f'<line id="Baseline-S" style="fill:none;stroke:#27AAE1;opacity:1;stroke-width:0.5;" x1="263.676" x2="3036.32" y1="{BASELINE_S}" y2="{BASELINE_S}"/>',
        f'<line id="Capline-S" style="fill:none;stroke:#27AAE1;opacity:1;stroke-width:0.5;" x1="263.676" x2="3036.32" y1="{BASELINE_S - CAP_HEIGHT:.3f}" y2="{BASELINE_S - CAP_HEIGHT:.3f}"/>',
        '<line id="Baseline-M" style="fill:none;stroke:#27AAE1;opacity:1;stroke-width:0.5;" x1="263.676" x2="3036.32" y1="1126" y2="1126"/>',
        '<line id="Capline-M" style="fill:none;stroke:#27AAE1;opacity:1;stroke-width:0.5;" x1="263.676" x2="3036.32" y1="1055.54" y2="1055.54"/>',
        '<line id="Baseline-L" style="fill:none;stroke:#27AAE1;opacity:1;stroke-width:0.5;" x1="263.676" x2="3036.32" y1="1556" y2="1556"/>',
        '<line id="Capline-L" style="fill:none;stroke:#27AAE1;opacity:1;stroke-width:0.5;" x1="263.676" x2="3036.32" y1="1485.54" y2="1485.54"/>',
    ]
    symbols = []
    for weight, width in WEIGHTS.items():
        points = outline(width)
        min_x = min(x for x, _ in points)
        max_x = max(x for x, _ in points)
        glyph_width = max_x - min_x + 2 * SIDE_BEARING
        left = COLUMNS[weight] - glyph_width / 2
        # Inside the group x = 0 is the left margin and y = 0 the baseline.
        shift = SIDE_BEARING - min_x
        for side, x in (("left", left), ("right", left + glyph_width)):
            guides.append(
                f'<line id="{side}-margin-{weight}-S" style="fill:none;stroke:#00AEEF;stroke-width:0.5;opacity:1.0;" '
                f'x1="{x:.3f}" x2="{x:.3f}" y1="600.785" y2="720.121"/>'
            )
        symbols.append(
            f'<g id="{weight}-S" transform="matrix(1 0 0 1 {left:.3f} {BASELINE_S})">'
            f'<path d="{path_data(points, shift)}"/></g>'
        )
    svg = f"""<?xml version="1.0" encoding="UTF-8"?>
<!--Generated by apple/tools/make-contour-symbol.py-->
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
<svg version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="3300" height="2200">
 <g id="Notes">
  <rect height="2200" id="artboard" style="fill:white;opacity:1" width="3300" x="0" y="0"/>
  <text style="stroke:none;fill:black;font-family:sans-serif;font-size:13;font-weight:bold;" transform="matrix(1 0 0 1 263.676 322)">Weight/Scale Variations</text>
  <text id="template-version" style="stroke:none;fill:black;font-family:sans-serif;font-size:13;" transform="matrix(1 0 0 1 3036.32 1933)">Template v.3.0</text>
  <text style="stroke:none;fill:black;font-family:sans-serif;font-size:13;" transform="matrix(1 0 0 1 263.676 1933)">contour.mark</text>
 </g>
 <g id="Guides">
  {chr(10).join("  " + g for g in guides).strip()}
 </g>
 <g id="Symbols">
  {chr(10).join("  " + s for s in symbols).strip()}
 </g>
</svg>
"""
    root = Path(__file__).resolve().parent.parent
    folder = root / "MusicFetch/Resources/Assets.xcassets/contour.mark.symbolset"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "contour.mark.svg").write_text(svg)
    contents = {"info": {"author": "xcode", "version": 1}, "symbols": [{"filename": "contour.mark.svg", "idiom": "universal"}]}
    (folder / "Contents.json").write_text(json.dumps(contents, indent=2) + "\n")

    print(f"wrote {folder}")


if __name__ == "__main__":
    main()
