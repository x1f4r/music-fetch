#!/usr/bin/env python3
"""Drive Music Fetch on an emulator and take screenshots (headless; nothing plays or shows).

    python3 -I tools/android-ux/ux.py <serial> <out-dir> <scenario> [<scenario> ...]
    python3 -I tools/android-ux/ux.py <serial> <out-dir> list

Needs a debug or benchmark build (test tags are resource ids there) and adb on PATH. Run
push-seed.sh first for the library scenarios and set UX_SEED to the seed directory. Screenshots and UI dumps go to
<out-dir>/NN-name.png / .xml. Views are found by test tag (resource-id), text or content
description.
"""
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

PKG = "app.musicfetch"
DEV_DIR = f"/sdcard/Android/data/{PKG}/files"


class Device:
    def __init__(self, serial, out):
        self.serial = serial
        self.out = out
        os.makedirs(out, exist_ok=True)
        # Continue the numbering of earlier runs into the same directory.
        self.n = len([f for f in os.listdir(out) if f.endswith(".png")])

    # adb
    def adb(self, *args, check=True, capture=True, timeout=120):
        cmd = ["adb", "-s", self.serial, *args]
        r = subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=capture, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)}: {r.stderr.decode(errors='replace')}")
        return r.stdout.decode(errors="replace") if capture else ""

    def sh(self, command, check=True):
        return self.adb("shell", command, check=check)

    # UI tree
    def dump(self):
        for _ in range(4):
            out = self.sh("uiautomator dump /sdcard/ux.xml >/dev/null 2>&1; cat /sdcard/ux.xml", check=False)
            if out.strip().startswith("<?xml"):
                return ET.fromstring(out[out.index("<?xml"):].split("</hierarchy>")[0] + "</hierarchy>")
            time.sleep(1)
        raise RuntimeError("uiautomator dump failed")

    def nodes(self, root=None):
        return list((root if root is not None else self.dump()).iter("node"))

    @staticmethod
    def matches(node, key):
        rid = node.get("resource-id", "")
        return rid == key or rid.endswith("/" + key) or node.get("text") == key or node.get("content-desc") == key

    def find(self, key, root=None, contains=False):
        for node in self.nodes(root):
            if self.matches(node, key):
                return node
            if contains and (key in (node.get("text") or "") or key in (node.get("content-desc") or "")):
                return node
        return None

    def wait(self, key, timeout=20, contains=False):
        end = time.time() + timeout
        while time.time() < end:
            root = self.dump()
            node = self.find(key, root=root, contains=contains)
            if node is not None:
                return node
            self.dismiss_anr(root)
            time.sleep(0.7)
        raise RuntimeError(f"not found: {key}")

    def dismiss_anr(self, root):
        """A loaded host makes the emulator's System UI slow: answer "Wait" to its dialog."""
        wait = self.find("android:id/aerr_wait", root=root)
        if wait is not None:
            self.sh("input tap %d %d" % self.center(wait))
            time.sleep(1)

    def exists(self, key, contains=False):
        return self.find(key, contains=contains) is not None

    @staticmethod
    def center(node):
        x1, y1, x2, y2 = map(int, re.findall(r"\d+", node.get("bounds")))
        return (x1 + x2) // 2, (y1 + y2) // 2

    def tap(self, key, timeout=20, contains=False):
        node = self.wait(key, timeout, contains)
        x, y = self.center(node)
        self.sh(f"input tap {x} {y}")
        time.sleep(0.6)
        return node

    def long_press(self, key, ms=900):
        node = self.wait(key)
        x, y = self.center(node)
        self.sh(f"input swipe {x} {y} {x} {y} {ms}")
        time.sleep(0.6)

    def swipe(self, x1, y1, x2, y2, ms=300):
        self.sh(f"input swipe {x1} {y1} {x2} {y2} {ms}")
        time.sleep(0.6)

    def size(self):
        m = re.search(r"(\d+)x(\d+)", self.sh("wm size").splitlines()[-1])
        return int(m.group(1)), int(m.group(2))

    def back(self):
        self.sh("input keyevent KEYCODE_BACK")
        time.sleep(0.6)

    def text(self, value):
        self.sh("input text " + value.replace(" ", "%s"))
        time.sleep(0.4)

    # Captures
    def shot(self, name, settle=1.5):
        time.sleep(settle)
        self.n += 1
        base = os.path.join(self.out, f"{self.n:02d}-{name}")
        with open(base + ".png", "wb") as f:
            f.write(subprocess.run(["adb", "-s", self.serial, "exec-out", "screencap", "-p"], capture_output=True, check=True).stdout)
        try:
            root = self.dump()
            with open(base + ".xml", "w") as f:
                f.write(ET.tostring(root, encoding="unicode"))
        except RuntimeError:
            pass
        print("shot", base + ".png", flush=True)

    # App
    def launch(self, uri=None, ready=None, timeout=90):
        """Start the app (at [uri]) and wait until [ready] (a tag) is on screen."""
        if uri:
            self.sh(f"am start -W -a android.intent.action.VIEW -d '{uri}' {PKG}/.MainActivity >/dev/null")
        else:
            self.sh(f"am start -W -n {PKG}/.MainActivity >/dev/null")
        if ready:
            self.wait(ready, timeout)
        time.sleep(1.0)

    def open_session(self, key):
        self.launch(f"musicfetch://session/{self.ids()[key]}", ready="session.back")

    def stop_app(self):
        self.sh(f"am force-stop {PKG}")

    def setup(self, night=False, font=1.0):
        self.sh("settings put global window_animation_scale 0; settings put global transition_animation_scale 0; "
                "settings put global animator_duration_scale 0")
        self.sh(f"cmd uimode night {'yes' if night else 'no'}")
        self.sh(f"settings put system font_scale {font}")
        self.sh("svc power stayon true; input keyevent KEYCODE_WAKEUP; wm dismiss-keyguard", check=False)

    @staticmethod
    def ids():
        """Session ids of the seed library (UX_SEED: the directory seed.py wrote)."""
        import json
        with open(os.path.join(os.environ["UX_SEED"], "ids.json")) as f:
            return json.load(f)

    def dev_file(self, local, name):
        """Put a replay WAV (or other development file) where DevFiles looks."""
        self.adb("push", local, f"{DEV_DIR}/{name}")

    def clear_dev_file(self, name):
        self.sh(f"rm -f {DEV_DIR}/{name}", check=False)

    def listen_state(self):
        node = self.find("listen.state")
        return node.get("text") if node is not None else None


SCENARIOS = {}


def scenario(fn):
    SCENARIOS[fn.__name__] = fn
    return fn


def to_listen_root(d):
    d.tap("nav.listen")
    if d.exists("session.back"):
        d.tap("nav.listen")


def to_library_root(d):
    """The Library's list, scrolled to the top (selecting the destination again does that)."""
    d.tap("nav.library")
    d.tap("nav.library")
    if d.exists("session.back"):
        d.tap("nav.library")


@scenario
def first_launch(d):
    d.stop_app()
    d.sh(f"pm clear {PKG}")
    d.sh(f"pm grant {PKG} android.permission.RECORD_AUDIO", check=False)
    d.sh(f"pm grant {PKG} android.permission.POST_NOTIFICATIONS", check=False)
    d.launch()
    d.wait("provider.skip", 40)
    d.shot("provider-panel")
    d.tap("provider.skip")
    d.wait("listen.button", 30)
    d.shot("listen-empty")


@scenario
def stage(d):
    d.launch(ready="nav.listen")
    to_listen_root(d)
    d.wait("listen.button")
    for source in ("mic", "phone", "hum"):
        d.tap("listen.source." + source)
        d.shot("stage-" + source)
    d.tap("listen.source.mic")


@scenario
def library(d):
    d.launch(ready="nav.library")
    to_library_root(d)
    d.wait("library.search")
    d.wait("library.pinned", 30)
    d.shot("library")
    d.swipe(*_mid(d, 0.5, 0.8), *_mid(d, 0.5, 0.3))
    d.shot("library-scrolled")
    to_library_root(d)


def _mid(d, fx, fy):
    w, h = d.size()
    return int(w * fx), int(h * fy)


def library_row(d, key, scrolls=4):
    """The library row of seed session [key], scrolling down to it if needed."""
    tag = "library.row." + d.ids()[key]
    _, height = d.size()
    for _ in range(scrolls + 1):
        node = d.find(tag)
        if node is not None:
            _, y1, _, y2 = map(int, re.findall(r"\d+", node.get("bounds")))
            # Only a row that is fully on screen, clear of the navigation bar.
            if y2 - y1 >= height * 0.06 and y2 < height * 0.8:
                return node
        d.swipe(*_mid(d, 0.5, 0.7), *_mid(d, 0.5, 0.45), 500)
    raise RuntimeError("row not found: " + key)


@scenario
def selection(d):
    d.launch(ready="nav.library")
    to_library_root(d)
    d.wait("library.pinned", 30)
    x, y = d.center(library_row(d, "hum"))
    d.sh(f"input swipe {x} {y} {x} {y} 900")
    time.sleep(0.8)
    d.shot("select-one")
    for key in ("shape", "billie"):
        d.sh("input tap %d %d" % d.center(library_row(d, key)))
        time.sleep(0.5)
    d.shot("select-three")
    d.tap("library.selection.pin")
    d.shot("select-pinned-snackbar", settle=0.3)
    time.sleep(5.5)
    d.tap("nav.library")  # reselect: scroll to the top
    d.shot("pinned-section")
    # Select pinned sessions only: the bar offers Unpin.
    x, y = d.center(library_row(d, "nggyu"))
    d.sh(f"input swipe {x} {y} {x} {y} 900")
    time.sleep(0.6)
    d.sh("input tap %d %d" % d.center(library_row(d, "mix")))
    time.sleep(0.5)
    d.shot("select-all-pinned-unpin")
    d.tap("library.selection.delete")
    d.shot("delete-confirm")
    d.tap("Cancel")
    d.back()
    d.shot("selection-closed-by-back")


@scenario
def swipe_pin(d):
    d.launch(ready="nav.library")
    to_library_root(d)
    d.wait("library.pinned", 30)
    node = library_row(d, "levitating")
    x1, y1, x2, y2 = map(int, re.findall(r"\d+", node.get("bounds")))
    y = (y1 + y2) // 2
    d.sh(f"input swipe {x1 + 30} {y} {x2 - 30} {y} 350")
    d.shot("swipe-pinned", settle=0.4)


@scenario
def pinned_filter(d):
    d.launch(ready="nav.library")
    to_library_root(d)
    d.wait("library.pinned", 30)
    d.swipe(*_mid(d, 0.9, 0.17), *_mid(d, 0.1, 0.17))
    d.tap("library.filter.pinned")
    d.shot("filter-pinned")
    d.tap("library.filter.all", timeout=5) if d.exists("library.filter.all") else d.swipe(*_mid(d, 0.1, 0.17), *_mid(d, 0.9, 0.17))
    if d.exists("library.filter.all"):
        d.tap("library.filter.all")
    d.tap("library.search")
    d.text("zzzz")
    d.sh("input keyevent KEYCODE_BACK")  # keyboard
    d.shot("search-no-results")
    d.back()
    d.shot("search-cleared-by-back")


@scenario
def result(d):
    d.open_session("blinding")
    d.shot("result-listen-tab")
    d.tap("session.pin")
    d.shot("result-pinned")
    d.tap("session.more")
    d.shot("result-overflow")
    d.back()
    d.swipe(*_mid(d, 0.5, 0.75), *_mid(d, 0.5, 0.2), 400)
    d.shot("result-scrolled")
    d.swipe(*_mid(d, 0.5, 0.3), *_mid(d, 0.5, 0.9), 300)
    d.tap("result.preview")
    d.shot("result-preview-playing", settle=2.5)
    d.tap("result.preview")
    d.tap("session.pin")


@scenario
def listen_again(d, hum_wav=None):
    """Phone-audio result → one tap on Hum → the stage hums."""
    ids = d.ids()
    if hum_wav:
        d.dev_file(hum_wav, "debug-capture.wav")
    d.open_session("blinding")
    d.wait("listenAgain.hum")
    d.shot("listen-again-dock")
    d.tap("listenAgain.hum")
    # First hum without the melody library: decline the download for now.
    try:
        d.wait("Not now", 10)
        d.shot("melody-prompt")
        d.tap("Not now")
    except RuntimeError:
        pass
    d.shot("listen-again-hum", settle=2.5)
    d.shot("listen-again-hum-later", settle=3)
    # Stop and cancel what it heard.
    if d.exists("listen.button"):
        d.back()
    time.sleep(2)
    d.clear_dev_file("debug-capture.wav")


@scenario
def library_and_back(d):
    """Decision 1: what "Library and back to Listen" shows after a result on the Listen tab."""
    d.open_session("shape")
    d.shot("lab-result")
    d.tap("nav.library")
    d.shot("lab-library")
    d.tap("nav.listen")
    d.shot("lab-back-to-listen")


@scenario
def candidates(d):
    d.open_session("hum")
    d.wait("cand.confirm", 30)
    d.shot("candidates-listen-tab")
    to_library_root(d)
    d.wait("library.pinned", 30)
    d.sh("input tap %d %d" % d.center(library_row(d, "hum")))
    d.wait("cand.confirm", 30)
    d.shot("candidates-library")
    d.back()


@scenario
def empty(d):
    d.open_session("nomatch")
    d.shot("empty-listen-tab")
    d.open_session("waiting")
    d.shot("waiting-listen-tab")
    to_library_root(d)
    d.wait("library.pinned", 30)
    d.sh("input tap %d %d" % d.center(library_row(d, "nomatch")))
    d.wait("session.back")
    d.shot("empty-library-tab")
    d.back()


@scenario
def mix(d):
    d.open_session("mix")
    d.shot("mix", settle=2)
    d.open_session("playlist")
    d.shot("playlist", settle=2)
    d.open_session("link")
    d.shot("link-result", settle=2)


@scenario
def delete_undo(d):
    d.open_session("levitating")
    d.tap("session.more")
    d.tap("Delete")
    d.shot("deleted-undo-on-stage", settle=0.6)
    d.tap("Undo")
    d.shot("undone")


@scenario
def no_match(d, quiet_wav=None):
    """A quiet mic listen ends with suggestions (started by a deep link, as from a notification)."""
    if quiet_wav:
        d.dev_file(quiet_wav, "debug-capture.wav")
    d.launch("musicfetch://listen?source=mic", ready="listen.button")
    if d.exists("Not now"):
        d.tap("Not now")
    for _ in range(40):
        time.sleep(1)
        if d.exists("listen.action.tryAgain") or d.exists("listen.action.humIt"):
            break
    d.shot("no-match-mic")
    d.clear_dev_file("debug-capture.wav")


SCENARIO_ARGS = {}


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    serial, out, *names = sys.argv[1:]
    if names == ["list"]:
        print("\n".join(SCENARIOS))
        return
    d = Device(serial, out)
    kwargs = {}
    for name in names:
        if "=" in name:
            k, v = name.split("=", 1)
            kwargs[k] = v
    for name in names:
        if "=" in name:
            continue
        fn = SCENARIOS[name]
        params = fn.__code__.co_varnames[1:fn.__code__.co_argcount]
        try:
            fn(d, **{k: v for k, v in kwargs.items() if k in params})
        except Exception as e:  # keep going; the shots so far are still useful
            print(f"scenario {name} failed: {e}", flush=True)
            try:
                d.shot(f"FAILED-{name}")
            except Exception:
                pass


if __name__ == "__main__":
    main()
