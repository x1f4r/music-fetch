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
- continuation tokens and entity keys become placeholders (consistent within
  a fixture set, so answers still chain);
- anything shaped like an IP address, and email addresses (uploaders put
  theirs in descriptions).

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
    "creatorThumbnail", "voiceReplyContainerViewModel", "watchEndpointSupportedOnesieConfig",
    "html5PlaybackOnesieConfig", "watchEndpointSupportedPrefetchConfig", "prefetchHintConfig", "menuButton",
    "accessibilityContext", "rendererContext",
}
IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
IPV6 = re.compile(r"(?<![0-9A-Fa-f:])(?:[0-9A-Fa-f]{1,4}:){3,7}[0-9A-Fa-f]{1,4}(?![0-9A-Fa-f:])")
IP_PARAM = re.compile(r"([?&]|\\u0026)ip=[^&\"\\]+")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
VISITOR = re.compile(r"Cg[t-v][A-Za-z0-9_-]{8,}(?:%3D|=){0,2}")


class Scrubber:
    def __init__(self) -> None:
        self.commenters: dict[str, str] = {}
        self.comment_n = 0
        self.opaque: dict[str, str] = {}

    def placeholder(self, value: str, prefix: str) -> str:
        """Opaque tokens and entity keys, renamed consistently across a fixture set."""
        if value not in self.opaque:
            self.opaque[value] = f"{prefix}-{len(self.opaque) + 1}"
        return self.opaque[value]

    def handle(self, name: str) -> str:
        if name not in self.commenters:
            self.commenters[name] = f"@commenter-{len(self.commenters) + 1}"
        return self.commenters[name]

    def text(self, s: str) -> str:
        if "googlevideo.com" in s or "initplayback" in s:
            return ""  # stream hosts near the requester, with its address in the query
        s = IP_PARAM.sub(r"\1ip=0", s)
        s = EMAIL.sub("contact@example.com", s)
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
        if "lockupViewModel" in d:
            d = dict(d)
            d["lockupViewModel"] = lockup(d["lockupViewModel"])
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
            if isinstance(v, str) and k == "token":
                out[k] = self.placeholder(v, "CONTINUATION")
            elif isinstance(v, str) and (k == "key" or k.endswith("Key")) and len(v) > 16:
                out[k] = self.placeholder(v, "KEY")
            else:
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
        if isinstance(props.get("content"), dict):
            props["content"] = {"content": props["content"].get("content", "")}
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


def lockup(v: dict) -> dict:
    """A playlist lockup without its UI: id, type, title, channel row, length badge."""
    meta = v.get("metadata", {}).get("lockupMetadataViewModel", {})
    rows = meta.get("metadata", {}).get("contentMetadataViewModel", {}).get("metadataRows", [])
    badges = []
    for o in v.get("contentImage", {}).get("thumbnailViewModel", {}).get("overlays", []):
        for b in o.get("thumbnailBottomOverlayViewModel", {}).get("badges", []):
            t = b.get("thumbnailBadgeViewModel", {}).get("text")
            if t:
                badges.append({"thumbnailBadgeViewModel": {"text": t}})
    def part(p):
        text = p.get("text", {})
        return {"text": {"content": text.get("content")}} if isinstance(text, dict) and "content" in text else {}
    return {
        "contentId": v.get("contentId"),
        "contentType": v.get("contentType"),
        "contentImage": {"thumbnailViewModel": {"overlays": [{"thumbnailBottomOverlayViewModel": {"badges": badges}}]}},
        "metadata": {"lockupMetadataViewModel": {
            "title": {"content": meta.get("title", {}).get("content")},
            "metadata": {"contentMetadataViewModel": {"metadataRows": [
                {"metadataParts": [part(p) for p in r.get("metadataParts", [])]} for r in rows]}},
        }},
    }


def prune_watch(initial: dict) -> dict:
    """Keep what a probe reads: the comments section, the panels (Music cards only)."""
    initial = dict(initial)
    # microformat repeats top comments with their authors (schema.org).
    for k in ("playerOverlays", "frameworkUpdates", "microformat"):
        initial.pop(k, None)
    results = initial.get("contents", {}).get("twoColumnWatchNextResults", {}).get("results", {}).get("results", {})
    if "contents" in results:
        results["contents"] = [c for c in results["contents"] if "itemSectionRenderer" in c]
    panels = []
    for p in initial.get("engagementPanels", []):
        r = p.get("engagementPanelSectionListRenderer", {})
        target = r.get("targetId") or r.get("panelIdentifier") or ""
        if target in ("engagement-panel-ads", "PAsearch_preview", "engagement-panel-searchable-transcript") or not target:
            continue
        items = r.get("content", {}).get("structuredDescriptionContentRenderer", {}).get("items")
        if items is not None:
            r["content"]["structuredDescriptionContentRenderer"]["items"] = [i for i in items if "horizontalCardListRenderer" in i]
        panels.append(p)
    initial["engagementPanels"] = panels
    return initial


def prune_next(page: dict) -> dict:
    """Comment threads and their comment entities only."""
    page = dict(page)
    batch = page.get("frameworkUpdates", {}).get("entityBatchUpdate", {})
    if "mutations" in batch:
        batch["mutations"] = [m for m in batch["mutations"] if "commentEntityPayload" in m.get("payload", {})]
    for e in page.get("onResponseReceivedEndpoints", []):
        for cmd in e.values():
            if not isinstance(cmd, dict) or "continuationItems" not in cmd:
                continue
            for item in cmd["continuationItems"]:
                h = item.get("commentsHeaderRenderer")
                if h is not None:
                    item["commentsHeaderRenderer"] = {k: v for k, v in h.items() if k in ("countText", "commentsCount")}
                t = item.get("commentThreadRenderer")
                if t is not None:
                    keep = {k: v for k, v in t.items() if k == "comment"}
                    vm = t.get("commentViewModel", {}).get("commentViewModel")
                    if isinstance(vm, dict):
                        keep["commentViewModel"] = {"commentViewModel": {
                            k: v for k, v in vm.items() if k in ("commentKey", "commentId", "pinnedText", "toolbarStateKey")}}
                    item["commentThreadRenderer"] = keep
    return page


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
        if player is not None:
            initial = prune_watch(initial)
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
                if kind == "next":
                    page = prune_next(page)
                dest = out / f"youtube_{kind}_{name}_{json_index}.json"
                dest.write_text(json.dumps(scrubber.value(page), ensure_ascii=False, indent=None, separators=(",", ":")) + "\n")
            print(f"{src.name} -> {dest.name} ({dest.stat().st_size} bytes)")
    # Last line of defence: nothing session-shaped may remain.
    bad = re.compile(r'visitorData|VISITOR_DATA|remoteHost|"ip":|ip=(?!0\b)|googlevideo\.com|initplayback|%3A[0-9a-fA-F]{1,4}%3A')
    for f in sorted(out.glob("youtube_*")):
        text = f.read_text()
        hit = bad.search(text) or next((m for m in IPV4.finditer(text) if m.group(0) != "0.0.0.0"), None)
        if hit:
            raise SystemExit(f"{f.name}: still contains {hit.group(0)!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
