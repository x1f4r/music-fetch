#!/usr/bin/env python3
"""Humming on an emulator: answers while humming, Try Google, sung words, the melody settings.

    python3 -I tools/android-hum/hum.py <serial> <out-dir> <scenario> [key=value ...] [<scenario> ...]
    python3 -I tools/android-hum/hum.py <serial> <out-dir> list

Builds on tools/android-ux/ux.py (same Device, debug build, adb on PATH). Hums are replayed as
the microphone through the debug capture file; bring your own WAV (wav=...) and melody index
(index=...): none are in the repository. Nothing plays; the only network requests are the app's
own (enrichment of the final candidates) and, in the Google scenarios, the Google app's.

Scenarios:
  index index=<file.mfi>           install a melody index into the app (stops the app)
  hum wav=<file.wav> [label=x]     hum from the stage; shots while the answer forms, a timeline
                                   (timeline-<label>.json), and the candidates or no-match
  google                           on the candidates screen: Try Google opens Google's song search
  nomatch_google wav=<file.wav>    a hum that finds nothing; Try Google from the stage
  settings                         the melody library section of Settings
"""
import json
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "android-ux"))
import ux  # noqa: E402

PKG = ux.PKG
SOUND_SEARCH = "com.google.android.apps.search.soundsearch.SoundSearchActivity"
SCENARIOS = {}


def scenario(fn):
    SCENARIOS[fn.__name__] = fn
    return fn


def top_activity(d):
    out = d.sh("dumpsys activity activities | grep -m1 topResumedActivity", check=False)
    m = re.search(r"u0 (\S+)", out)
    return m.group(1) if m else ""


def logcat_clear(d):
    d.adb("logcat", "-c", check=False)


def logcat_lines(d, tag):
    return d.adb("logcat", "-d", "-s", f"{tag}:I", check=False).splitlines()


@scenario
def index(d, index=None):
    """Install a melody index file as the engine's installed index."""
    if not index:
        raise RuntimeError("index=<file.mfi> is required")
    d.stop_app()
    d.adb("push", index, "/data/local/tmp/mf-index.mfi", timeout=600)
    d.sh(f"run-as {PKG} sh -c 'mkdir -p files/engine/melody && rm -f files/engine/melody/manifest.json'")
    d.sh(f"cat /data/local/tmp/mf-index.mfi | run-as {PKG} sh -c 'cat > files/engine/melody/melody-index.mfi'")
    d.sh("rm -f /data/local/tmp/mf-index.mfi")
    print("index installed", flush=True)


def to_hum_stage(d):
    d.launch(ready="nav.listen")
    ux.to_listen_root(d)
    d.wait("listen.button")
    # After the second answer the stage offers its Quick Settings tile once.
    time.sleep(1.5)
    if d.exists("Do not add tile"):
        d.tap("Do not add tile")
    d.tap("listen.source.hum")


@scenario
def hum(d, wav=None, label="hum", shots="1"):
    """Hum from the stage and follow the answer: preview chip, early stop, candidates.

    UI dumps take seconds while the stage animates, so the run is followed with plain
    screenshots (frames/<label>-<seconds>.png) and the app's log; one dump at the end tells
    which screen it reached.
    """
    if not wav:
        raise RuntimeError("wav=<file.wav> is required")
    d.dev_file(wav, ux_dev_name())
    to_hum_stage(d)
    logcat_clear(d)
    frames = os.path.join(d.out, "frames")
    os.makedirs(frames, exist_ok=True)
    start = time.time()
    d.tap("listen.button")
    ended_at = None
    while time.time() - start < 45:
        t = time.time() - start
        if shots == "1":
            data = subprocess.run(["adb", "-s", d.serial, "exec-out", "screencap", "-p"], capture_output=True).stdout
            with open(os.path.join(frames, f"{label}-{t:05.1f}.png"), "wb") as f:
                f.write(data)
        log = logcat_lines(d, "ListenController")
        if any("session ended" in line for line in log):
            ended_at = ended_at or time.time()
            # Keep a few frames of the hand-over to the result.
            if time.time() - ended_at > 2.5:
                break
        time.sleep(0.4)
    root = d.dump()
    screen = "candidates" if d.find("cand.confirm", root=root) is not None else (
        "session" if d.find("session.back", root=root) is not None else "stage")
    actions = [n.get("resource-id", "").split("/")[-1] for n in d.nodes(root)]
    actions = [a for a in actions if a.startswith("listen.action.")]
    d.shot(f"{label}-end", settle=1.0)
    log = [line for line in logcat_lines(d, "ListenController") if "session ended" in line or "interim" in line]
    with open(os.path.join(d.out, f"timeline-{label}.json"), "w") as f:
        json.dump({"wav": os.path.basename(wav), "end": screen, "actions": actions, "log": log}, f, indent=1)
    print(json.dumps({"label": label, "end": screen, "actions": actions, "log": log}), flush=True)
    d.clear_dev_file(ux_dev_name())


def ux_dev_name():
    return "debug-capture.wav"


@scenario
def google(d):
    """Candidates whose best answer is not Strong: Try Google opens Google's song search."""
    node = d.wait("cand.tryGoogle", 20)
    d.shot("candidates-try-google")
    d.sh("input tap %d %d" % d.center(node))
    time.sleep(4)
    top = top_activity(d)
    d.shot("google-song-search")
    print(json.dumps({"tryGoogle": top, "opensSongSearch": SOUND_SEARCH in top}), flush=True)
    d.back()
    time.sleep(1)


@scenario
def nomatch_google(d, wav=None):
    """A hum that finds nothing: the stage offers Try Google second."""
    hum(d, wav=wav, label="nomatch", shots="0")
    node = d.find("listen.action.tryGoogle")
    if node is None:
        print(json.dumps({"tryGoogle": None}), flush=True)
        return
    d.sh("input tap %d %d" % d.center(node))
    time.sleep(4)
    top = top_activity(d)
    d.shot("nomatch-google-song-search")
    print(json.dumps({"tryGoogle": top, "opensSongSearch": SOUND_SEARCH in top}), flush=True)
    d.back()
    time.sleep(1)


@scenario
def settings(d):
    d.launch(ready="nav.library")
    ux.to_library_root(d)
    d.tap("library.settings")
    d.wait("Melody library", 20)
    d.shot("settings-melody")
    for _ in range(3):
        if d.exists("settings.sungLyrics"):
            break
        d.swipe(*ux._mid(d, 0.5, 0.7), *ux._mid(d, 0.5, 0.4), 400)
    d.shot("settings-while-humming")
    d.back()


def main():
    if len(sys.argv) < 4:
        sys.exit(__doc__)
    serial, out, *args = sys.argv[1:]
    if args == ["list"]:
        print("\n".join(SCENARIOS))
        return
    d = ux.Device(serial, out)
    # Scenarios run in order, each with the key=value arguments that follow it.
    plan = []
    for arg in args:
        if "=" in arg and plan:
            k, v = arg.split("=", 1)
            plan[-1][1][k] = v
        else:
            plan.append((arg, {}))
    for name, kwargs in plan:
        try:
            SCENARIOS[name](d, **kwargs)
        except Exception as e:  # keep going; the shots so far are still useful
            print(f"scenario {name} failed: {e}", flush=True)
            try:
                d.shot(f"FAILED-{name}")
            except Exception:
                pass


if __name__ == "__main__":
    main()
