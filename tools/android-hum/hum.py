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
  hum wav=<file.wav> [label=x] [gfx=1] [shots=0]
                                   hum from the stage; shots while the answer forms, a timeline
                                   (timeline-<label>.json), and the candidates or no-match;
                                   gfx=1 adds frame statistics and the app's CPU use
  google                           on the candidates screen: Try Google opens Google's song search
  nomatch_google wav=<file.wav>    a hum that finds nothing; Try Google from the stage
  settings                         the melody library section of Settings
  takes files=<file>               hum every WAV listed in <file>; <out>/takes.jsonl has the answers
  melody_download manifest=<url>   remove the index, download it from <url> (index + encoder)
  melody_encoder manifest=<url>    an installed index whose encoder is new: just the encoder
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
    # topResumedActivity (Android 10+ multi-resume) or mResumedActivity (Android 10 and older).
    out = d.sh("dumpsys activity activities | grep -m1 -E 'topResumedActivity|mResumedActivity'", check=False)
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


def launch(d, ready):
    """Start the app; answer the system's "add tile" dialog (offered once after the second answer)."""
    d.launch()
    for _ in range(3):
        if d.exists("Do not add tile"):
            d.tap("Do not add tile")
        if d.exists(ready):
            return
        time.sleep(1)
    d.wait(ready, 60)


def to_hum_stage(d):
    # A fresh process: an earlier no-match would show its suggestions instead of the toggles.
    d.stop_app()
    launch(d, "nav.listen")
    ux.to_listen_root(d)
    d.wait("listen.button")
    time.sleep(1.5)
    if d.exists("Do not add tile"):
        d.tap("Do not add tile")
    d.tap("listen.source.hum")


def gfx_summary(d):
    """Frame statistics of the app since the last reset (dumpsys gfxinfo)."""
    out = d.sh(f"dumpsys gfxinfo {PKG}", check=False)
    keys = ["Total frames rendered", "Janky frames", "50th percentile", "90th percentile", "95th percentile",
            "99th percentile", "Number Slow UI thread", "Number Frame deadline missed"]
    summary = {}
    for line in out.splitlines():
        for k in keys:
            if line.strip().startswith(k) and k not in summary:
                summary[k] = line.split(":", 1)[1].strip()
    return summary


def cpu_sample(d):
    """CPU use of the app's process right now (top, one iteration)."""
    pid = d.sh(f"pidof {PKG}", check=False).strip()
    if not pid:
        return None
    line = d.sh(f"top -b -n 1 -p {pid} | tail -1", check=False).split()
    # PID USER PR NI VIRT RES SHR S %CPU %MEM TIME+ ARGS
    try:
        return float(line[8])
    except (IndexError, ValueError):
        return None


@scenario
def hum(d, wav=None, label="hum", shots="1", gfx="0"):
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
    if gfx == "1":
        d.sh(f"dumpsys gfxinfo {PKG} reset", check=False)
    start = time.time()
    d.tap("listen.button")
    ended_at = None
    cpu = []
    while time.time() - start < 75:
        t = time.time() - start
        if gfx == "1" and ended_at is None:
            cpu.append(cpu_sample(d))
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
    frames_stats = gfx_summary(d) if gfx == "1" else None
    root = d.dump()
    screen = "candidates" if d.find("cand.confirm", root=root) is not None else (
        "session" if d.find("session.back", root=root) is not None else "stage")
    actions = [n.get("resource-id", "").split("/")[-1] for n in d.nodes(root)]
    actions = [a for a in actions if a.startswith("listen.action.")]
    d.shot(f"{label}-end", settle=1.0)
    log = [line for line in logcat_lines(d, "ListenController")
           if "session ended" in line or "interim" in line or "transcript" in line or "capturing" in line]
    result = {"wav": os.path.basename(wav), "end": screen, "actions": actions, "log": log}
    if gfx == "1":
        result["gfx"] = frames_stats
        result["cpu"] = [c for c in cpu if c is not None]
    with open(os.path.join(d.out, f"timeline-{label}.json"), "w") as f:
        json.dump(result, f, indent=1)
    print(json.dumps({"label": label, **result}), flush=True)
    d.clear_dev_file(ux_dev_name())


def answers(d):
    """What the screen ended on: the candidate cards (title, artist, tier) or the no-match line."""
    root = d.dump()
    cards = []
    for i in range(5):
        card = d.find(f"cand.card.{i}", root=root)
        if card is None:
            break
        x1, y1, x2, y2 = map(int, re.findall(r"\d+", card.get("bounds")))
        texts = []
        for n in root.iter("node"):
            b = n.get("bounds")
            if not b or not n.get("text"):
                continue
            a1, b1, a2, b2 = map(int, re.findall(r"\d+", b))
            if a1 >= x1 and b1 >= y1 and a2 <= x2 and b2 <= y2:
                texts.append(n.get("text"))
        cards.append(texts[:3])
    state = d.find("listen.state", root=root)
    return {"cards": cards, "state": state.get("text") if state is not None and not cards else None}


@scenario
def takes(d, files=None, label="takes"):
    """Hum every WAV named in [files] (one path per line) and record what the app answered."""
    with open(files) as f:
        paths = [line.strip() for line in f if line.strip()]
    out = os.path.join(d.out, f"{label}.jsonl")
    for path in paths:
        name = os.path.basename(path)[:-4]
        try:
            hum(d, wav=path, label=name, shots="0")
            row = {"take": name, **answers(d)}
        except Exception as e:  # keep going with the next take
            row = {"take": name, "error": str(e)}
        try:
            with open(os.path.join(d.out, f"timeline-{name}.json")) as f:
                timeline = json.load(f)
            row["log"] = timeline.get("log")
        except OSError:
            pass
        with open(out, "a") as f:
            f.write(json.dumps(row) + "\n")
        print(json.dumps({k: row.get(k) for k in ("take", "cards", "state", "error")}), flush=True)


def ux_dev_name():
    return "debug-capture.wav"


@scenario
def google(d):
    """Candidates whose best answer is not Strong: Try Google opens Google's song search."""
    d.wait("cand.card.0", 20)
    # Below the list in landscape.
    scroll_to(d, "cand.tryGoogle")
    node = d.wait("cand.tryGoogle", 5)
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
def settings(d, speech_pack="0"):
    """The melody library and "While humming" sections; speech_pack=1 also asks the system for
    the missing speech pack and waits (up to 10 min) until it is installed."""
    to_melody_settings(d)
    d.shot("settings-melody")
    scroll_to(d, "settings.autoStop")
    d.shot("settings-while-humming")
    if speech_pack == "1" and d.exists("settings.speechPack"):
        d.tap("settings.speechPack")
        # The system asks first ("Download English (US) update … Download").
        d.wait("Download", 20)
        d.shot("settings-speech-pack-confirm", settle=0.5)
        d.tap("Download")
        d.shot("settings-speech-pack-downloading", settle=3)
        end = time.time() + 600
        while time.time() < end:
            time.sleep(15)
            # Settings checks the pack again when it resumes.
            d.sh("input keyevent KEYCODE_HOME")
            time.sleep(1)
            launch(d, "settings.autoStop")
            if not d.exists("settings.speechPack") and "Downloading" not in str([n.get("text") for n in d.nodes()]):
                break
        d.shot("settings-speech-pack-installed")
    d.back()


def to_melody_settings(d):
    launch(d, "nav.library")
    ux.to_library_root(d)
    d.tap("library.settings")
    scroll_to(d, "Melody library")


def scroll_to(d, key, swipes=6):
    """Scroll a list down until [key] is on screen, clear of the bottom bar (lazy lists hold only
    what is visible)."""
    _, height = d.size()
    for _ in range(swipes):
        node = d.find(key)
        if node is not None and d.center(node)[1] < height * 0.8:
            return
        # Slow and short: no fling past the target.
        d.swipe(*ux._mid(d, 0.5, 0.7), *ux._mid(d, 0.5, 0.5), 900)
    d.wait(key, 5)


def use_manifest(d, url):
    """Point the app at a melody-index manifest (DevFiles); restarts the app."""
    d.stop_app()
    local = os.path.join(d.out, "manifest-url.txt")
    with open(local, "w") as f:
        f.write(url + "\n")
    d.dev_file(local, "debug-melody-manifest-url.txt")


@scenario
def melody_download(d, manifest=None, label="melody"):
    """No index installed: Settings offers the download (index and encoder), then shows it installed."""
    use_manifest(d, manifest)
    # Debug builds: start without an index (benchmark builds start from a cleared app).
    d.sh(f"run-as {PKG} sh -c 'rm -rf files/engine/melody'", check=False)
    to_melody_settings(d)
    # The manifest check gives the size; the button sits below the header.
    time.sleep(3)
    scroll_to(d, "Download")
    d.shot(f"{label}-offer")
    d.tap("Download")
    for i in range(6):
        d.shot(f"{label}-downloading-{i}", settle=0.8)
        if d.exists("Remove"):
            break
    d.wait("Remove", 300)
    d.shot(f"{label}-installed")
    d.back()


@scenario
def melody_encoder(d, manifest=None, label="encoder"):
    """The index is current but its encoder is new: Settings offers just the encoder."""
    use_manifest(d, manifest)
    to_melody_settings(d)
    # Settings checks the manifest when it opens; tap only if it has not offered anything yet.
    time.sleep(3)
    if not d.exists("melody.update"):
        scroll_to(d, "Check for updates")
        d.tap("Check for updates")
        d.wait("melody.update", 15)
    scroll_to(d, "melody.update")
    d.shot(f"{label}-offer")
    if d.exists("melody.update"):
        d.tap("melody.update")
        for i in range(4):
            d.shot(f"{label}-downloading-{i}", settle=0.8)
        d.wait("Check for updates", 300)
        d.shot(f"{label}-installed")
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
