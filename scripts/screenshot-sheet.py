#!/usr/bin/env python3
"""Turn the screenshot tests' attachments into a contact sheet.

  scripts/screenshot-sheet.py collect --label macos-26 <exported-attachments> <out-dir>
  scripts/screenshot-sheet.py sheet --out <dir> <collected-dir> [<collected-dir> ...]

`collect` reads the folder written by `xcrun xcresulttool export attachments`
(manifest.json plus files), keeps the attachments named `shot__<state>__<variant>__`
(PNG) and `geom__<state>__<variant>__` (JSON), and writes them as
`<state>__<variant>.png|json` with a label file.

`sheet` copies one or more collected folders side by side and writes index.html:
one section per state, one row per system, one column per variant, with the toolbar
geometry check under each picture.
"""

import argparse
import html
import json
import re
import shutil
import sys
from pathlib import Path

NAME = re.compile(r"(shot|geom)__(.+?)__(.+?)__")


def collect(args: argparse.Namespace) -> None:
    source = Path(args.attachments)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest = source / "manifest.json"
    entries = json.loads(manifest.read_text()) if manifest.exists() else []
    count = 0
    for test in entries:
        for attachment in test.get("attachments", []):
            name = attachment.get("suggestedHumanReadableName", "")
            match = NAME.search(name)
            exported = source / attachment.get("exportedFileName", "")
            if not match or not exported.is_file():
                continue
            kind, state, variant = match.groups()
            suffix = ".png" if kind == "shot" else ".json"
            shutil.copyfile(exported, out / f"{state}__{variant}{suffix}")
            count += 1
    (out / "label.txt").write_text(args.label + "\n")
    print(f"{count} attachments from {source} -> {out}")


def load(folder: Path) -> tuple[str, dict]:
    label_file = folder / "label.txt"
    label = label_file.read_text().strip() if label_file.exists() else folder.name
    shots: dict = {}
    for path in sorted(folder.iterdir()):
        if "__" not in path.stem or path.suffix not in (".png", ".json"):
            continue
        state, variant = path.stem.split("__", 1)
        shot = shots.setdefault(state, {}).setdefault(variant, {})
        if path.suffix == ".png":
            shot["png"] = path.name
        else:
            try:
                shot["geom"] = json.loads(path.read_text())
            except json.JSONDecodeError:
                shot["geom"] = {"ok": False, "violations": ["unreadable geometry"]}
    return label, shots


STYLE = """
:root { color-scheme: light dark; --bg: #f5f5f7; --card: #fff; --ink: #1d1d1f; --dim: #6e6e73; --bad: #d70015; --good: #248a3d; }
@media (prefers-color-scheme: dark) { :root { --bg: #161617; --card: #232325; --ink: #f5f5f7; --dim: #98989d; --bad: #ff6961; --good: #30d158; } }
* { box-sizing: border-box; }
body { margin: 0; font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: var(--bg); color: var(--ink); }
header { position: sticky; top: 0; z-index: 2; padding: 14px 28px; background: color-mix(in srgb, var(--bg) 85%, transparent);
  backdrop-filter: blur(12px); border-bottom: 1px solid color-mix(in srgb, var(--ink) 10%, transparent); display: flex; gap: 24px; align-items: baseline; flex-wrap: wrap; }
header h1 { margin: 0; font-size: 17px; }
header .summary { color: var(--dim); }
header .summary b.bad { color: var(--bad); }
nav { display: flex; flex-wrap: wrap; gap: 6px; }
nav a { color: var(--ink); text-decoration: none; padding: 2px 9px; border-radius: 99px; background: var(--card); font-size: 12px; }
nav a.bad { color: var(--bad); }
section { padding: 22px 28px 8px; }
section h2 { margin: 0 0 12px; font-size: 20px; }
.row { display: grid; grid-template-columns: 92px repeat(var(--cols), minmax(260px, 1fr)); gap: 14px; margin-bottom: 18px; align-items: start; }
.os { font-weight: 600; padding-top: 6px; }
figure { margin: 0; background: var(--card); border-radius: 12px; padding: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }
figure img { width: 100%; display: block; border-radius: 6px; cursor: zoom-in; }
figcaption { font-size: 12px; color: var(--dim); padding: 6px 2px 0; }
.ok { color: var(--good); }
.bad { color: var(--bad); }
ul.v { margin: 4px 0 0; padding-left: 16px; color: var(--bad); font-size: 12px; }
.missing { color: var(--dim); font-size: 12px; padding: 30px 0; text-align: center; }
dialog { padding: 0; border: 0; background: transparent; max-width: 96vw; max-height: 96vh; }
dialog img { max-width: 96vw; max-height: 96vh; display: block; }
dialog::backdrop { background: rgba(0,0,0,.75); }
"""

SCRIPT = """
const d = document.querySelector('dialog'), big = d.querySelector('img');
document.querySelectorAll('figure img').forEach(i => i.addEventListener('click', () => { big.src = i.src; d.showModal(); }));
d.addEventListener('click', () => d.close());
"""


def sheet(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    systems = []
    for folder in map(Path, args.folders):
        if not folder.is_dir():
            print(f"skipping missing {folder}", file=sys.stderr)
            continue
        label, shots = load(folder)
        target = out / label
        target.mkdir(exist_ok=True)
        for state in shots.values():
            for shot in state.values():
                if "png" in shot:
                    shutil.copyfile(folder / shot["png"], target / shot["png"])
        systems.append((label, shots))

    order: list[str] = []
    for _, shots in systems:
        for state in shots:
            if state not in order:
                order.append(state)

    failures = 0
    sections = []
    nav = []
    for state in order:
        variants: list[str] = []
        for _, shots in systems:
            for variant in shots.get(state, {}):
                if variant not in variants:
                    variants.append(variant)
        state_bad = False
        rows = []
        for label, shots in systems:
            cells = []
            for variant in variants:
                shot = shots.get(state, {}).get(variant)
                if not shot or "png" not in shot:
                    cells.append(f'<div class="missing">{html.escape(variant)}: no screenshot</div>')
                    continue
                geom = shot.get("geom")
                check = ""
                if geom is not None:
                    violations = geom.get("violations", [])
                    if violations:
                        failures += 1
                        state_bad = True
                        items = "".join(f"<li>{html.escape(v)}</li>" for v in violations)
                        check = f'<ul class="v">{items}</ul>'
                    else:
                        check = ' · <span class="ok">toolbar geometry OK</span>'
                src = f"{html.escape(label)}/{html.escape(shot['png'])}"
                cells.append(
                    f'<figure><img loading="lazy" src="{src}" alt="{html.escape(state)} {html.escape(variant)}">'
                    f"<figcaption>{html.escape(variant)}{check}</figcaption></figure>"
                )
            rows.append(f'<div class="row" style="--cols:{len(variants)}"><div class="os">{html.escape(label)}</div>{"".join(cells)}</div>')
        anchor = re.sub(r"[^a-z0-9-]", "-", state.lower())
        nav.append(f'<a href="#{anchor}" class="{"bad" if state_bad else ""}">{html.escape(state)}</a>')
        sections.append(f'<section id="{anchor}"><h2>{html.escape(state)}</h2>{"".join(rows)}</section>')

    labels = " · ".join(html.escape(label) for label, _ in systems)
    summary = f'<b class="bad">{failures} geometry failures</b>' if failures else '<span class="ok">no geometry failures</span>'
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Music Fetch screenshots</title><style>{STYLE}</style></head>
<body><header><h1>Music Fetch screenshots</h1><span class="summary">{labels} · {len(order)} states · {summary}</span>
<nav>{"".join(nav)}</nav></header>
{"".join(sections)}
<dialog><img alt=""></dialog><script>{SCRIPT}</script></body></html>
"""
    (out / "index.html").write_text(page)
    print(f"{out / 'index.html'}: {len(order)} states, {failures} geometry failures")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    c = commands.add_parser("collect")
    c.add_argument("--label", required=True)
    c.add_argument("attachments")
    c.add_argument("out")
    s = commands.add_parser("sheet")
    s.add_argument("--out", required=True)
    s.add_argument("folders", nargs="+")
    args = parser.parse_args()
    collect(args) if args.command == "collect" else sheet(args)


if __name__ == "__main__":
    main()
