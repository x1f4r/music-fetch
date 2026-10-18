#!/usr/bin/env python3
"""Turn answers recorded by `mf dev youtube-probe --record <raw>` into fixtures.

Usage:
    python3 tools/fixtures/scrub_youtube.py <raw-dir> <out-dir> NAME=N[,N…] …

Each NAME=N list names the recorded answers (by their request number) that
become `youtube_<kind>_<NAME>[_<i>].(html|json)` in <out-dir>, e.g.
`mix=1,2,3 chapters=4 playlist=5,6,7`.

What goes:
- everything a page carries for the session or the requester: visitor ids,
  tracking parameters, the `responseContext` (remote host, experiment flags),
  streaming and playback URLs (they embed the requester's IP), ads, captions,
  storyboards, related videos;
- commenters: names become "@commenter-N", channel ids, avatars and links go,
  and the text of every comment that is neither pinned nor the uploader's
  becomes "Comment N." (the uploader's and pinned comments carry the tracklists
  the tests read);
- anything shaped like an IP address.

Watch and playlist pages shrink to a shell with `ytcfg` (client version only),
`ytInitialPlayerResponse` and `ytInitialData`. The raw folder stays private.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

DROP_KEYS = {
    "trackingParams", "clickTrackingParams", "visitorData", "serviceTrackingParams", "responseContext",
    "mainAppWebResponseContext", "webResponseContextExtensionData", "loggingContext", "loggingDirectives",
    "adSignalsInfo", "streamingData", "playbackTracking", "playerAds", "adPlacements", "adSlots", "adBreakHeartbeatParams",
    "attestation", "heartbeatParams", "storyboards", "captions", "annotations", "cards", "endscreen", "playerConfig",
    "topbar", "secondaryResults", "overlay", 
    "playerSettingsMenuData", "messages", "paidContentOverlay", "trackingUrl",
    "avatar", "avatarThumbnailUrl", "creatorThumbnailUrl", "authorThumbnail", "authorEndpoint", "authorButtonA11y",
    "channelThumbnailSupportedRenderers", "serializedShareEntity", "sharedKey",
    "desktopTopbar", "pageVisualEffects", "webPlayerShareEntityServiceEndpoint", "replies", "teaserAvatar",
    "creatorThumbnail", "voiceReplyContainerViewModel",
}
IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
IPV6 = re.compile(r"(?<![0-9A-Fa-f:])(?:[0-9A-Fa-f]{1,4}:){3,7}[0-9A-Fa-f]{1,4}(?![0-9A-Fa-f:])")
VISITOR = re.compile(r"Cg[t-v][A-Za-z0-9_-]{8,}(?:%3D|=){0,2}")


class Scrubber:
    def __init__(self) -> None:
        self.commenters: dict[str, str] = {}
        self.comment_n = 0

    def handle(self, name: str) -> str:
        if name not in self.commenters:
            self.commenters[name] = f"@commenter-{len(self.commenters) + 1}"
        return self.commenters[name]

    def text(self, s: str) -> str:
        s = IPV4.sub("0.0.0.0", s)
        s = IPV6.sub("::", s)
        return VISITOR.sub("SCRUBBED", s)

    def value(self, v):
        if isinstance(v, dict):
            return self.obj(v)
        if isinstance(v, list):
            return [self.value(x) for x in v]
        if isinstance(v, str):
            return self.text(v)
        return v

    def obj(self, d: dict) -> dict:
        if "commentEntityPayload" in d:
            d = dict(d)
            d["commentEntityPayload"] = self.comment_payload(d["commentEntityPayload"])
        if "commentRenderer" in d:
            d = dict(d)
            d["commentRenderer"] = self.legacy_comment(d["commentRenderer"])
        out = {}
        for k, v in d.items():
            if k in DROP_KEYS:
                continue
            out[k] = self.value(v)
        return out

    def comment_payload(self, p: dict) -> dict:
        p = json.loads(json.dumps(p))
        author = p.get("author", {})
        creator = bool(author.get("isCreator"))
        if not creator and "displayName" in author:
            author["displayName"] = self.handle(author["displayName"])
        if not creator:
            author.pop("channelId", None)
        for k in ("avatarThumbnailUrl", "channelCommand", "isCurrentUser"):
            author.pop(k, None)
        props = p.get("properties", {})
        props.pop("authorButtonA11y", None)
        if not creator and not p.get("_pinned"):
            self.comment_n += 1
            content = props.get("content")
            if isinstance(content, dict):
                content["content"] = f"Comment {self.comment_n}."
                content.pop("commandRuns", None)
                content.pop("styleRuns", None)
                content.pop("attachmentRuns", None)
        p.pop("_pinned", None)
        return p

    def legacy_comment(self, r: dict) -> dict:
        r = json.loads(json.dumps(r))
        owner = bool(r.get("authorIsChannelOwner"))
        if not owner:
            if "authorText" in r:
                r["authorText"] = {"simpleText": self.handle(json.dumps(r["authorText"]))}
            r.pop("authorEndpoint", None)
            if "pinnedCommentBadge" not in r:
                self.comment_n += 1
                r["contentText"] = {"runs": [{"text": f"Comment {self.comment_n}."}]}
        r.pop("authorThumbnail", None)
        return r


def pinned_keys(page: dict) -> set[str]:
    """Comment keys whose thread is pinned (their text is kept)."""
    keys = set()

    def walk(v):
        if isinstance(v, dict):
            vm = v.get("commentViewModel")
            if isinstance(vm, dict) and isinstance(vm.get("commentViewModel"), dict):
                inner = vm["commentViewModel"]
                if inner.get("pinnedText"):
                    keys.add(inner.get("commentKey"))
            for x in v.values():
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)

    walk(page)
    return keys


def mark_pinned(page: dict) -> None:
    keys = pinned_keys(page)
    for m in page.get("frameworkUpdates", {}).get("entityBatchUpdate", {}).get("mutations", []):
        p = m.get("payload", {}).get("commentEntityPayload")
        if p and p.get("key") in keys:
            p["_pinned"] = True


def json_after(html: str, marker: str):
    i = html.find(marker)
    if i < 0:
        return None
    j = html.find("{", i + len(marker))
    if j < 0 or html[i + len(marker):j].strip():
        return None
    obj, _ = json.JSONDecoder().raw_decode(html[j:])
    return obj


def page_shell(html: str, s: Scrubber) -> str:
    version = re.search(r'"INNERTUBE_CLIENT_VERSION"\s*:\s*"([0-9.]+)"', html)
    player = json_after(html, "var ytInitialPlayerResponse =")
    initial = json_after(html, "var ytInitialData =") or json_after(html, 'window["ytInitialData"] =')
    parts = ["<!DOCTYPE html>", "<html><head></head><body>"]
    if version:
        parts.append('<script>ytcfg.set({"INNERTUBE_CLIENT_VERSION":"%s"});</script>' % version.group(1))
    if player is not None:
        parts.append("<script>var ytInitialPlayerResponse = %s;</script>" % json.dumps(s.value(player), ensure_ascii=False))
    if initial is not None:
        parts.append("<script>var ytInitialData = %s;</script>" % json.dumps(s.value(initial), ensure_ascii=False))
    parts.append("</body></html>")
    return "\n".join(parts) + "\n"


def main() -> int:
    if len(sys.argv) < 4:
        print(__doc__)
        return 2
    raw, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    for spec in sys.argv[3:]:
        name, numbers = spec.split("=", 1)
        scrubber = Scrubber()  # one commenter numbering per fixture set
        json_index = 0
        for n in [int(x) for x in numbers.split(",") if x]:
            matches = sorted(raw.glob(f"{n:02d}-*"))
            body = [m for m in matches if not m.name.endswith(".request.json")]
            if len(body) != 1:
                raise SystemExit(f"request {n}: expected one answer, found {[m.name for m in matches]}")
            src = body[0]
            kind = src.name.split("-", 1)[1].rsplit(".", 1)[0]
            if src.suffix == ".html":
                dest = out / f"youtube_{kind}_{name}.html"
                dest.write_text(page_shell(src.read_text(), scrubber))
            else:
                json_index += 1
                page = json.loads(src.read_text())
                mark_pinned(page)
                dest = out / f"youtube_{kind}_{name}_{json_index}.json"
                dest.write_text(json.dumps(scrubber.value(page), ensure_ascii=False, indent=None, separators=(",", ":")) + "\n")
            print(f"{src.name} -> {dest.name} ({dest.stat().st_size} bytes)")
    # Last line of defence: nothing session-shaped may remain.
    bad = re.compile(r'visitorData|VISITOR_DATA|remoteHost|"ip":|[?&]ip=|googlevideo\.com')
    for f in sorted(out.glob("youtube_*")):
        text = f.read_text()
        hit = bad.search(text) or next((m for m in IPV4.finditer(text) if m.group(0) != "0.0.0.0"), None)
        if hit:
            raise SystemExit(f"{f.name}: still contains {hit.group(0)!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
