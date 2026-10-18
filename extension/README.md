# Music Fetch browser extension

One codebase for Chromium (Chrome, Edge, Brave, Opera, Vivaldi, Helium) and Firefox
(desktop and Android), both Manifest V3: [WXT](https://wxt.dev) 0.21, TypeScript and
Svelte 5. Until the engine is compiled to WebAssembly it runs on a **mock engine** that
keeps the core's event contract, so every surface works end to end.

## Layout

```
entrypoints/
  background.ts         Chromium: routes and starts the offscreen host. Firefox: also the engine host.
  offscreen/            Chromium only: hosts the engine (USER_MEDIA, WORKERS).
  popup/  sidepanel/    Toolbar popup; Chromium side panel and Firefox sidebar (one entrypoint).
  app/                  Full-page tab: #/listen, #/library, #/session/:id, #/settings/:pane,
                        #/welcome, #/grant-mic, #/open?url=
  capture.content.ts    The capture agent, injected on demand (isolated world); page.content.ts
                        the site adapters (MAIN world), injected right after it.
src/
  engine/               Types mirrored from core model.rs, EngineClient and the remote client
                        over Ports, the mock (catalog, scripts, persistence, export), and the
                        engine host (`host-*.ts`, see Runtime).
  audio/                Sources: tab capture, element capture, microphone; the tap worklet,
                        framing, media-time stamping, silence watch.
  capture/              The capture agent (in tabs) and the capture coordinator (in the host).
  net/                  Hosts, the header policy as DNR session rules, the fetch policy,
                        optional host permissions.
  messaging/            Typed request/response and event bus; control messages to the background.
  models/               Pure state: the listen stage (folded from SessionEvents), library sections.
  providers/            "Open in": services, search links (port of catalog::search_links),
                        custom provider (port of custom_provider_link).
  settings/             storage.sync (provider decision, custom provider, …) and storage.local.
  platform/             Manifest per browser, capability probe, offscreen, surfaces.
  page/                 Page facts and the site adapters (YouTube, SoundCloud, Mixcloud, Twitch,
                        Bandcamp, streaming now-playing, generic), the agent ↔ page bridge.
  i18n/                 Copy from docs/design/DESIGN.md §7 and the extension plan §5.9.
  ui/                   Svelte components, surfaces and routes; ui/theme holds every style.
tests/unit, tests/e2e, tests/fixtures
```

## Scripts

| | |
|---|---|
| `npm run dev` / `dev:firefox` | Development build with reload (opens a browser) |
| `npm run build` | `build:chrome` and `build:firefox` into `.output/` |
| `npm run zip` | Store zips plus the Firefox sources zip |
| `npm test` | Unit tests (Vitest, jsdom, WXT's fake browser) |
| `npm run check` | `svelte-check` (types in `.ts` and `.svelte`) |
| `npm run lint:firefox` | `web-ext lint` on the Firefox build |
| `npm run e2e` | Builds, then loads the extension into Chromium (Puppeteer) and Firefox (web-ext + WebDriver BiDi). `CHROMIUM_BIN` / `FIREFOX_BIN` pick the browsers; with `DISPLAY` set they run headful |
| `npm run build:e2e` | `--mode e2e` builds in `.output/*-e2e`: they may script pages on 127.0.0.1, know the local echo server and honour `<meta name="mf-test-adapter">`. Release builds contain none of it |
| `npm run e2e:runtime` | The runtime end-to-end tests (`tests/e2e/runtime-*`) on the e2e builds. On the Legion run `tests/e2e/legion-runtime.sh` (private Xvfb display and PipeWire null sink) |

## Runtime

- **Engine host** (`src/engine/host-core.ts`): one per extension, in the offscreen document
  on Chromium (`host-chromium.ts`) and in the event page on Firefox (`host-firefox.ts`). It
  serves the engine to every page, owns live capture, keeps sessions running when pages
  close (results end in a notification), relays events, and starts the **engine Worker**
  (`host-worker*.ts`) on demand, recycling it after 10 idle minutes (2 in low power). The
  Firefox host keeps its event page alive while a session runs (`host-keepalive.ts`).
- **The engine** runs in that Worker behind `EngineBackend` (`host-backend.ts`).
  `host-backend-factory.ts` is the one place the wasm engine replaces the mock.
- **Audio** (`src/audio/`): Chromium's "This tab" is `tabCapture` in the offscreen
  document, routed back to the speakers; Firefox captures the page's element through the
  injected agent. The microphone is captured in the offscreen document on Chromium and,
  because Firefox's background never can, in the page that asked on Firefox.
- **Analyses of media pages** go through `CaptureHost` (`src/capture/`): the coordinator
  injects the agent, saves the player's state, plays the media silently at up to 4× with
  `preservesPitch` off (tab capture not routed back, or the element muted), stamps the
  audio with media time, follows ads, stalls, user actions and navigation, and restores
  the player afterwards.
- **Network** (`src/net/`): the header policy becomes declarativeNetRequest session rules
  scoped to the extension's own requests (`tabIds: [-1]`, `initiatorDomains: [own host]`)
  in both browsers; engine requests never send cookies.

## Mock engine

The mock answers every `EngineClient` call and plays scripted sessions with the core's
guarantees (one `Started`, exactly one terminal event, upserted segments). It runs in the
engine Worker and keeps its library in IndexedDB. Live sessions use the captured audio:
real levels, `CaptureSilent` for a silent capture, and the **synthetic test songs**
(`host-mock-audio.ts`, pure tones the tests render) are recognized from the PCM; other
audio keeps the scripted outcome. "Analyze this page" on a media page drives the capture
coordinator. To reach hard states:

- live sessions: in any extension page's console,
  `localStorage.setItem('mf.mock.next', '<directive>')` with `nomatch`, `quiet`, `silent`,
  `ratelimited`, `offline`, `fail`, or `hold` (keeps listening until Stop, then matches);
  a directive session opens no capture;
- links: add `mf-mock=fail`, `mf-mock=mix` or `mf-mock=unsupported` to the link's query.

Streams recorded from the native CLI (`mf listen … > events.jsonl`) replay through
`fromRecording(parseRecording(text))`.

## Design

`src/ui/theme/design-tokens.css` and `components.css` come from the D1 design deliverable
unchanged (minus mockup-only rules); `extension.css` adapts them to real surfaces (rem type
scale, full-size panels, page layouts). Components carry no styles of their own.
