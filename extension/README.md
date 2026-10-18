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
  capture.content.ts    Injected on demand (isolated world); page.content.ts (MAIN world).
src/
  engine/               Types mirrored from core model.rs, EngineClient, host and remote
                        client over Ports, the mock (catalog, scripts, persistence, export).
  messaging/            Typed request/response and event bus; control messages to the background.
  models/               Pure state: the listen stage, sessions (timeline, tracklist, status row),
                        library rows and selection, notifications and the badge.
  providers/            "Open in": services, search links (port of catalog::search_links),
                        custom provider (port of custom_provider_link).
  settings/             storage.sync (provider decision, custom provider, …) and storage.local.
  platform/             Manifest per browser, capability probe, offscreen, surfaces.
  page/                 Page facts read in the page (site adapters are task X4).
  i18n/                 Copy from docs/design/DESIGN.md §7 and the extension plan §5.9.
  notify/               The background's badge and result notifications (wired by the background).
  ui/                   Svelte components, surfaces, routes and controllers; ui/theme holds every
                        style; ui/demo the demo states.
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
| `npm run screens` | After `npm run build:chrome`: every demo state, light and dark, at 2× into `.screens/` (with `manifest.json`), and an axe-core audit of each (`.screens/a11y.json`). `SCREENS=<id,…>` picks states |

## Mock engine

The mock answers every `EngineClient` call and plays scripted sessions with the core's
guarantees (one `Started`, exactly one terminal event, upserted segments). Its library
lives in the extension origin's `localStorage`. To reach hard states:

- live sessions: in any extension page's console,
  `localStorage.setItem('mf.mock.next', '<directive>')` with `nomatch`, `quiet`, `silent`,
  `ratelimited`, `offline`, `fail`, or `hold` (keeps listening until Stop, then matches);
- links: add `mf-mock=fail`, `mf-mock=mix` or `mf-mock=unsupported` to the link's query.

Streams recorded from the native CLI (`mf listen … > events.jsonl`) replay through
`fromRecording(parseRecording(text))`.

## Demo states

Every state of the extension plan §5 and the design spec opens from a URL on the real
pages, like the apps' `-MFDemoState`: `popup.html?demo=popup-hum`, `sidepanel.html?demo=panel-mix`,
`app.html?demo=app-library` (add `&theme=dark`, or `&still=1` for screenshots). The gallery of
all of them is `app.html?demo=gallery`. A demo runs on the mock engine with its own library
(`src/ui/demo/library.ts`), keeps settings in memory and never touches the user's data; the
states and the clicks that reach them are in `src/ui/demo/states.ts`.

## Design

`src/ui/theme/design-tokens.css` and `components.css` come from the D1 design deliverable
unchanged (minus mockup-only rules); `extension.css` adapts them to real surfaces (rem type
scale, full-size panels, page layouts). Components carry no styles of their own.
