# Music Fetch 1.0 — design system and screen specification

This is the implementation spec for the macOS app (SwiftUI, macOS 26 Tahoe, Liquid Glass) and the Android app (Jetpack Compose, Material 3 Expressive, `material3:1.5.0-beta01`, Android 16/17). Both apps sit on the same Rust engine (`docs/ARCHITECTURE.md`, `docs/CORE_API.md`); every state below maps to a `SessionEvent`, `Notice`, `Outcome` or `NoMatchReason` from `core/crates/mfcore/src/model.rs`.

Static mockups live in `docs/design/mockups/` (HTML) and `docs/design/mockups/png/` (2x renders). Where a number here and a mockup disagree, this document wins.

Units: `pt` on macOS, `dp` on Android. Where a value is shared it is written once ("28pt/dp").

---

## 1. Principles

1. **One obvious action per screen.** Listen: the button. Result: Open in your service. Candidates: This is it. Mix: the timeline. Everything else is quiet.
2. **The line is the sound.** One motif carries the product: a single round-capped contour stroke. It is the idle glyph, it goes live with the input level, it unfurls into the pitch trace when you hum, it becomes the "your hum vs. the song" pair, it is the timeline strip, and it is the app icon. Spend all the boldness here.
3. **Native controls, system type, system color.** `List`, `Table`, `Form`, `Menu`, `ShareLink`, `Picker` on macOS; `ListItem`, `ButtonGroup`, `SplitButton`, `FilterChip`, `NavigationSuiteScaffold` on Android. No custom chrome, no custom fonts on macOS, no fixed point sizes.
4. **Glass and expressive shape only on the functional layer:** the listen button, toolbars and floating controls. Results, lists, timelines and inspectors sit on plain surfaces.
5. **Nothing that repeats what is visible.** No "Results" headings, no "Success!" lines, no subtitles explaining the obvious. A heading exists only when the content beneath it would otherwise be ambiguous.
6. **Stable layouts.** Each slot on the Listen stage has a fixed size and position in every state. Text that changes keeps its line box; a control that disappears is replaced by one of the same height or by nothing.
7. **Honest uncertainty.** Fingerprint matches show no score. Hums always show a ranked list with Strong / Possible / Long shot. Mix segments show tiers by fill style and text, never by color alone.
8. **Secondary information only when it helps the next action.** Evidence, ISRC, lyrics and the contour overlay live in the inspector (macOS) or below the fold / in a sheet (Android).

---

## 2. The signature

### 2.1 The contour mark

A single stroke in a 48 × 20 unit box, round caps and joins, no fill:

```
M3 11 C7 11 8 5 12 5 C16 5 17 15 21 15 C26 15 26 3 31 3 C36 3 36 13 40 13 C43 13 44 9 45 9
```

Three humps of growing amplitude that settle: a hummed phrase, deliberately asymmetric so it never reads as a signal waveform. Implement it once as a `Shape` (`ContourMark`) and once as a Compose `Path`. Stroke width is 3 units at the 48-unit box; scale the whole thing, never the stroke alone.

| Use | Size | Stroke | Color |
|---|---|---|---|
| macOS listen button glyph | 56 × 24pt | 3.5pt | white |
| macOS menu bar extra button | 40 × 18pt | 3.2pt | white |
| macOS menu bar status item | 18 × 8pt template | 2pt | template |
| Android listen button glyph | 72 × 32dp | 4dp | `onPrimary` |
| Android navigation bar "Listen" icon | 24dp box | 2.4dp | icon color |
| App icon | see §9 | | |

### 2.2 The choreography (both platforms)

| Moment | What happens | Timing |
|---|---|---|
| Idle | Mark at rest. No ambient motion. | — |
| Press | Button scales to 0.96; Android morphs 40 % toward `MaterialShapes.Circle`. | spring fast (response 0.22, damping 0.6) |
| Listening | The mark becomes the **live line**: the same stroke, now a sum of three sines whose amplitude is the input level. A halo behind the button breathes with level. Android also morphs the container shape with level (§5.1). | continuous, level smoothed by a 100 ms EMA |
| Impression detected (`Notice::ImpressionDetected`) | The live line **unfurls**: a copy grows from the glyph bounds into the trace band above the button (matched geometry), then starts scrolling. The glyph inside the button keeps a low-amplitude live line. The hum ring appears. | spring hero (0.32, 0.8), ring fades in 150 ms |
| Matching (`Phase::Recognizing` / `MatchingMelody` after capture stops) | The line freezes at its last frame and fades to 70 %. State line shows a small progress indicator and "Matching". | fade 150 ms |
| Matched (`Outcome::Matched`) | The button's glass **morphs into the primary "Open in …" button** of the result; the artwork rises into place; title, artist, meta follow. | §4.4 "Result reveal" |
| Candidates (`Outcome::Candidates`) | The trace band shrinks to the hum strip at the top of the candidates view; the list appears beneath. | spring hero |
| No match | The line returns to the resting mark; the state line shows the reason. macOS: up to three actions fill the recovery row under the source toggles, which stay. Android: the action row replaces the source toggles. | fade 150 ms |

Reduce Motion / "Remove animations": every morph and matched-geometry move becomes a 150 ms crossfade; the halo stops scaling (opacity may still follow level); Android shape morphing is disabled (static `Cookie9Sided`); the trace band still scrolls (it is content, not decoration).

---

## 3. Information architecture and navigation

### 3.1 macOS

**Scenes**

- `Window` — main window, default 1180 × 760, minimum 900 × 600. `NavigationSplitView` (sidebar + detail) with one `.inspector(isPresented:)` attached to the split view (not to the screens inside it). Below 1060pt with the sidebar shown (832pt without), the inspector folds away and comes back when the window widens; the preference (View › Show Inspector) is unchanged, and opening it by hand in a narrow window keeps it open.
- `Settings` — five panes (§6.9). Opened with ⌘, from the app menu only; never a toolbar button.
- `MenuBarExtra("Music Fetch", systemImage: …)` with `.menuBarExtraStyle(.window)` and `isInserted` bound to the "Show in menu bar" setting (§6.10).
- `ControlWidget` "Identify with Music Fetch" for Control Center and the menu bar (runs the Listen App Intent).
- App Intents: Listen, Hum, Identify Link, Open Last Result. Share extension "Identify with Music Fetch" for links and media files.
- The app stays running after the last window closes only while the menu bar extra is shown.

**Sidebar** (220pt, min 180, max 280; floating glass panel on Tahoe — system default, no custom background)

```
Listen            (symbol: the contour mark, template)
Library           (square.stack)
Pinned
  ▪ Fred again.. | Boiler Room: London      (waveform.path.ecg → use "rectangle.split.3x1" for mixes)
  ♪ Nights                                  (music.note)
```

- `List(selection:)` with `NavigationLink(value:)`. Two levels maximum. Not hidden by default. Pinned sessions use the kind symbol; selection uses the system row style. Nothing critical at the bottom.
- Choosing Listen, or clicking it again while a result shows, returns to the stage (like ⌘1); a mix still being analyzed stays on screen.
- Library carries a badge with the number of links and files still being analyzed.
- The "Pinned" section appears only when at least one session is pinned. It is collapsible, shows the newest 12 and then "Show All" (Library with the Pinned scope). A pinned row's context menu: Open in ‹service› ▸, Export ▸, Show in Library, Unpin, Delete.

**Detail column**

| Selection | Content | Window title (`.navigationTitle`) |
|---|---|---|
| Listen | Listen stage (idle / listening / hum / link progress), or the last session's result/candidates after a match | "Listen", removed from the toolbar (the sidebar already says it) |
| Library | Session list; selecting a row pushes the session in a `NavigationStack` | "Library"; pushed sessions set the session title but remove it from the toolbar with `.toolbar(removing: .title)` so the content header carries it |
| Pinned session | Same as a pushed Library session | as above |

**Toolbar** (≤ 3 groups; symbols without borders; one prominent item; nothing centered on the window)

| View | Leading | Content trailing | Over the inspector |
|---|---|---|---|
| Listen, stage | link field (240pt, prompt "Paste a link") | — | [Inspector toggle] |
| Listen, showing a session | [‹ Back to Listen] link field | [Export ▾ · Pin] [Listen ▾] | [Inspector toggle] |
| Library | title | [Pin] [Listen ▾] | [Inspector toggle] · search field |
| Session (pushed or pinned) | back | [Export ▾ · Pin] [Listen ▾] | [Inspector toggle] |

- The inspector toggle lives in the inspector's own `.toolbar`, which makes SwiftUI add the inspector's tracking separator: every content item stays left of the inspector divider, the toggle is the first item over the inspector, and nothing ever sits on the divider when the inspector is resized or toggled. With the inspector closed the toggle is the last item.
- **Listen ▾** is a split button: on macOS 26 a `.borderedProminent` button and a chevron `Menu` in one toolbar item, which the toolbar draws as one accent-tinted glass capsule, with a hairline between the parts (a `Menu(primaryAction:)` is drawn untinted there); before macOS 26, where toolbars draw every button plain, an accent capsule with a chevron segment like "Open in". The main part listens again with the current method (its icon: `mic`, `macbook` or the contour mark) and reads "Stop" while capturing, "Matching" (disabled) while matching. The chevron lists Listen with Microphone · Listen to This Mac · Hum, Whistle or Sing, then Identify Link… · Open File… · Type a Lyric…. It is **hidden on the Listen stage** (the big button and the source toggles are the same actions; one place for one action): `ToolbarItem(...).hidden(isOnListenStage)`.
- Export ▾ (sessions with a tracklist or a single answer) and Pin sit together before Listen; Pin shows `pin` "Pin", or `pin.slash` "Unpin" when every target is pinned.
- Every toolbar action also exists in the menu bar.

**Inspector** (300pt, min 260, max 360; `.inspector`, system material)

| Selection | Inspector shows |
|---|---|
| Matched track | Album, Released, Length, ISRC · "Heard": Source, When, Matched by · "Lyrics" |
| Hum candidate | "Your hum vs. ‹title›" contour overlay + legend · Album, Released, Matched by, Where |
| Mix segment | artwork 64, title, artists, cue range · Match, Evidence, Album · [Open in ‹service› ▾] · [‹Site› at ‹cue›] |
| Library row (selected, not opened) | the same as its kind |
| Several library rows | "‹n› sessions" (`.headline`) · [Pin] and/or [Unpin] · [Delete] (bordered, large, full width) |
| Nothing | "No Selection", `.title3` tertiary, centered (the AppKit convention; `ContentUnavailableView` is **not** used) |

**Keyboard**

| Shortcut | Action | Menu |
|---|---|---|
| ⌘L | Listen with the current method (toggle; Esc also stops) | Listen |
| ⌘⇧H | Hum | Listen |
| — | Listen with Microphone, Listen to This Mac (one item per method; the Source picker below them only chooses) | Listen |
| ⌘[ / Esc | Back to Listen from a result; Esc also leaves a no-match or an error | — |
| ⌘K | Identify Link… (focuses the toolbar field) | File |
| ⌘O | Open File… | File |
| ⌘V | Paste a link and identify it (when no text field has focus and the pasteboard holds a URL) | Edit |
| ⌘1 / ⌘2 | Listen / Library | View |
| ⌥⌘I | Show/Hide Inspector | View |
| ⌃⌘S | Show/Hide Sidebar | View |
| ⌘F | Search Library | Edit |
| ⌘E | Export… | File |
| ⌘⇧C | Copy Tracklist | Edit |
| ⌘C | Copy "Artist – Title" (selected track) | Edit |
| ⌘↩ | Open in default service | Session |
| Space | Play/pause preview | Session |
| ⌘⇧P | Pin / Unpin the session on screen or every selected library row (Undo with ⌘Z) | Session |
| ⌫ | Delete (Library) | Edit |
| ⌘+ / ⌘− / ⌘0 | Zoom timeline strip / reset | View |
| ↑ ↓ | Move selection in candidates / tracklist / library | — |
| ↩ | Candidates: "This is it" for the selected row; tracklist/library: open | — |
| ⌘, | Settings | App |

**Drag and drop, paste.** The whole Listen pane and the sidebar "Listen" item are `dropDestination`s for file URLs, web URLs and text containing a URL. While targeted: the button tint goes to 100 % accent, scale 1.04, state line "Drop to identify". ⌘V anywhere in the main window starts link analysis immediately (no sheet) when the pasteboard holds a URL; otherwise the toolbar field takes the text.

**Dock menu**: Listen, Hum, the last three results. Dock badge: none.

### 3.2 Android

**Theme.** `MaterialExpressiveTheme(colorScheme = dynamic (API 31+) else seed #1FA48A, motionScheme = MotionScheme.expressive())`. Lists, navigation chrome and dialogs are wrapped in a nested `MaterialTheme(motionScheme = MotionScheme.standard())`. `enableEdgeToEdge()`; inset with `WindowInsets.safeDrawing`. Predictive back is on (target 36).

**Navigation.** `NavigationSuiteScaffold` with two destinations: Listen (contour mark icon) and Library (`Icons.Outlined.VideoLibrary`-style stack). Compact: `ShortNavigationBar`; expanded (sw ≥ 600): `WideNavigationRail`. Navigation 3 back stacks per destination; detail screens (result, candidates, mix, lyric search) push inside their destination and keep the navigation bar visible. On expanded screens Library and Mix use `ListDetailPaneScaffold`.

| Route | Screen |
|---|---|
| `listen` | Listen stage (§6.2) |
| `session/{id}` | Result, Candidates or Mix according to `SessionStatus` |
| `library` | Library (§6.8) |
| `lyrics` | Typed lyric search (§6.7) |
| `settings` | Settings (§6.9), from the Library app bar |

**Entry points.**

- **Share sheet** — `ShareIdentifyActivity` (translucent theme, `ACTION_SEND` for `text/plain`, `audio/*`, `video/*`). Opens as a `ModalBottomSheet` over the source app (§6.11).
- **Quick Settings tile** — `TOGGLEABLE_TILE`, label "Music Fetch" (≤ 18 chars), 24dp white vector of the mark. Tap → `startActivityAndCollapse(PendingIntent)` → translucent `ListenOverlayActivity` which starts the microphone foreground service (§6.11). Offer `requestAddTileService()` once, after the second successful listen.
- **Widgets** — Glance `SizeMode.Responsive`: 2×2 (the button), 4×2 (button + last result). Widget tap starts the microphone service (exempt from while-in-use restriction) and opens the overlay for feedback.
- **Notifications** — background mix progress (`Notification.ProgressStyle`, segments per identified track, Stop action; never requested as a Live Update) and background results ("Houdini — Dua Lipa", actions: Open in Spotify).
- **Predictive back** on the Listen stage while listening: `PredictiveBackHandler` scrubs the button to 0.85 scale and the halo to 0; committing cancels the session (`cancel()`), releasing springs back.

---

## 4. Tokens

### 4.1 Color

**macOS.** Only semantic system colors: `.primary`, `.secondary`, `.tertiary`, `.quaternary`, `Color(.windowBackgroundColor)`, `.controlBackgroundColor`, `.separator`, `.accentColor`. The glass tint on the listen button is `.accentColor`. No custom fills; capsules use `.fill.tertiary` / `.fill.quaternary`.

**Android.** Dynamic color scheme; fallback seed `#1FA48A` ("fetch teal"). Surfaces follow the M3 tonal roles (`surface`, `surfaceContainerLow/…/Highest`). Never hard-code a hex in a composable.

**Artwork-derived color** appears in exactly two places: the result hero wash (both platforms) and the content-based scheme of the Android result screen (`MaterialKolor` 5.0.1, `PaletteStyle.Expressive`, `ColorSpec.SpecVersion.SPEC_2025`, seed = dominant artwork color from the core's palette function). Never in toolbars, navigation or lists.

**Meaning never by color alone.**

| Tier (`Tier`) | Strip fill | Row |
|---|---|---|
| Confirmed | solid accent/primary | no tag |
| Likely | accent/primary at 60 % | no tag |
| Possible | 1.5pt accent/primary outline, no fill | tag "Possible" (outlined) + ✓ ✕ |
| Unknown | diagonal hatch (label color 22 %, 1.5pt every 6pt) | title "ID" in secondary, hatched 40pt tile, "Hum it" |
| Speech (`SegmentKind::Speech`) | 6pt-tall bar, tertiary label color | not listed |
| Silence | gap | not listed |

| Candidate tier (`CandidateTier`) | Capsule / tag |
|---|---|
| Strong | filled, accent-soft background, accent text (macOS) · `filled` tag on `primaryContainer` (Android) |
| Possible | filled, quaternary fill, primary text · `tonal` tag |
| LongShot | outlined, secondary text · `outlined` tag |

Destructive actions use `role: .destructive` / `error` roles. Orange, red and green carry no other meaning in this app.

### 4.2 Typography

**macOS — system text styles only** (Dynamic Type and the Tahoe metrics come for free). `.monospacedDigit()` on every cue, time and duration.

| Role | Style |
|---|---|
| Result title | `.largeTitle` bold, 2 lines max |
| Result artist | `.title3` |
| Result meta ("Album · 2024") | `.callout`, secondary |
| State line under the button | `.title3`; secondary when idle, primary when active |
| Row title | `.body` (sessions, tracklist) / `.headline` (candidates, recent row) |
| Row subtitle | `.callout`, secondary |
| Cue times | `.callout`, secondary, monospaced digits |
| Section headers (inspector, settings) | `.headline` |
| Footers, ruler labels | `.caption`, secondary |
| Day headers (Library) | `.headline`, secondary |

**Android — M3 type roles**, Google Sans Flex (bundled variable font, SIL OFL) for display/headline/title, Roboto Flex for body/label. `fontFeatureSettings = "tnum"` on times.

| Role | Style |
|---|---|
| Result title | `displaySmallEmphasized`, 2 lines max, centered |
| Result artist | `titleMedium` |
| Result meta | `bodyMedium`, `onSurfaceVariant` |
| State line | `titleMedium`; `onSurfaceVariant` idle, `onSurface` active |
| App bar title | `titleLarge` (+ `bodyMedium` subtitle for mixes) |
| List title / subtitle | `bodyLarge` / `bodyMedium` `onSurfaceVariant` |
| Candidate title | `titleMedium` |
| Cue times, times | `labelMedium`, tnum |
| Day headers | `titleSmall`, `primary` |
| Tags, chips | `labelMedium` / `labelLarge` |
| Share sheet title | `titleLargeEmphasized` |

### 4.3 Spacing and shape

- Spacing scale: 4 · 8 · 12 · 16 · 20 · 24 · 32 · 48.
- macOS radii: artwork 40–56pt → 6–8pt; artwork 240pt → 12pt (continuous); rows 8pt; inspector/sidebar panels are system; buttons are system capsules. Android: M3 shape scale (`extraSmall` 4, `small` 8, `medium` 12, `large` 16, `extraLarge` 28, `full`); artwork 240dp → 20dp; cards 16dp; list selection full-bleed.
- Minimum hit targets: 28 × 28pt (macOS icon buttons), 48 × 48dp (Android, including the ✓ ✕ in tracklist rows).

### 4.4 Motion

Same physics on both platforms (Material expressive tokens; SwiftUI response ≈ 2π/√stiffness).

| Role | Android (damping, stiffness) | SwiftUI |
|---|---|---|
| Hero spatial (button ↔ result, sheets, unfurl) | expressive default spatial (0.8, 380) | `.spring(response: 0.32, dampingFraction: 0.8)` |
| Press, small spatial, chips | expressive fast spatial (0.6, 800) | `.spring(response: 0.22, dampingFraction: 0.6)` |
| Large layout (timeline zoom, inspector) | expressive slow spatial (0.8, 200) | `.spring(response: 0.44, dampingFraction: 0.8)` |
| Color, opacity | effects (1.0, 1600) | `.spring(response: 0.16, dampingFraction: 1)` |
| Chrome, navigation, lists | standard (0.9, 700) | `.spring(response: 0.24, dampingFraction: 0.9)` |

**Listen button, listening (macOS).**
- Level input: `SessionEvent::Level { rms }` at ~30 Hz → `level = ema(rms, 100 ms)` in 0…1 (rms normalised by the core; clamp).
- Halo: a circle 176pt behind the button, `RadialGradient(accent → clear)`, `.blur(radius: 14)`, `opacity = 0.15 + 0.45·level`, `scale = 1.0 + 0.12·level`. Drawn in a `Canvas` inside `TimelineView(.animation)`.
- Live line: 64 × 40pt `Path`, 65 samples, `y = h/2 + env(u)·level·(h/2)·Σ aₖ·sin(2π·fₖ·(2u) + φₖ(t))` with `a = [0.6, 0.3, 0.15]`, `f = [1.0, 2.3, 3.7]`, phases advancing at 1.0 / 2.3 / 3.7 rad·s⁻¹, `env = sin(πu)`. Stroke 3pt white, round caps.
- Glass tint: `.tint(.accentColor.opacity(0.85 + 0.15·level))`.
- Click again or Esc stops: the line collapses to the mark over the fast spring.

**Listen button, listening (Android).**
- Container shape = `Morph` between `MaterialShapes.Cookie9Sided` (level 0) → `SoftBurst` (0.5) → `Sunny` (1.0), progress = level; plus a slow rotation of 360° per 12 s while listening. Drawn with `RoundedPolygon.toShape()` / `Morph.toPath()` (`androidx.graphics.shapes` 1.1.0; `MaterialShapes` is `@ExperimentalMaterial3ExpressiveApi` — isolate in `ListenButtonShape.kt`).
- Halo and live line exactly as macOS, in `primary` / `onPrimary`.

**Impression detected → trace band.** A `matchedGeometryEffect(id: "line")` (Compose: `SharedTransitionLayout` + `sharedElement`) moves a copy of the live line from the glyph frame to the trace band's frame over the hero spring, scaling the stroke from 3 to 4pt/dp. The band then starts scrolling. The glyph inside the button resumes a live line at 50 % amplitude.

**Hum ring.** macOS: 1.5pt ring, radius 62pt (6pt outside the button), track `.quaternary`, progress `.accentColor`, trimmed from 12 o'clock clockwise, `progress = voicedSeconds / 12`, `.animation(.linear(duration: 0.1))`. Android: `CircularWavyProgressIndicator(progress)` 188dp, stroke 4dp, wavelength ≈ 24dp, amplitude 2dp.

**Result reveal.** Inside one `GlassEffectContainer(spacing: 24)` the listen button carries `.glassEffectID("primary", in: ns)` and the "Open in …" button carries the same id, so the glass morphs between them (`glassEffectTransition(.matchedGeometry)`), hero spring. Artwork: opacity 0 → 1 and scale 0.92 → 1, hero spring, 50 ms delay. Title / artist / meta: `+8pt` offset → 0 with opacity, hero spring, staggered 40 ms. Hero wash: opacity 0 → 0.42 over 300 ms. Android: `sharedBounds` from the button to the `SplitButton`, same timings; `HapticFeedbackType.Confirm` fires when the artwork lands.

**Lists and chrome.** Standard springs; inserted tracklist rows slide in from +6pt with opacity (streamed mix results); the strip's new segment scales from 0.6 width around its center.

### 4.5 Haptics

Android only: `Confirm` on `Outcome::Matched`; `Confirm` on "This is it"; `SegmentTick` when the hum ring completes (12 s); nothing on no-match or errors. macOS: none.

### 4.6 Iconography

macOS uses SF Symbols: `mic`, `macbook`, the custom contour mark (`contour.mark`, a custom symbol generated from §2.1 by `apple/tools/make-contour-symbol.py`; Ultralight, Regular and Black sources, so it takes every weight and scale like the system symbols), `link`, `chevron.backward`, `pin.fill`, `pin.slash`, `doc`, `rectangle.split.3x1` (mixes), `music.note`, `square.stack`, `sidebar.trailing`, `pin`, `play.fill`, `square.and.arrow.up`, `doc.on.doc`, `checkmark`, `xmark`, `magnifyingglass`, `text.alignleft` (lyrics), `arrow.up.right.square`, `gearshape`, `key`, `internaldrive`, `info.circle`. Android uses Material Symbols (outlined): `mic`, `smartphone`, the custom mark, `link`, `attach_file`, `description`, `view_timeline`, `music_note`, `video_library`, `push_pin`, `play_arrow`, `pause`, `share`, `content_copy`, `check`, `close`, `search`, `lyrics`, `open_in_new`, `settings`, `delete`.

---

## 5. Components

### 5.1 Listen button

| | macOS | Android |
|---|---|---|
| Size | 112pt circle | 160dp `Cookie9Sided` |
| API | `Button(action:) { glyph }.buttonStyle(.plain).frame(112).glassEffect(.regular.tint(.accentColor).interactive(), in: .circle).glassEffectID("primary", in: ns)` inside `GlassEffectContainer(spacing: 24)` | `Surface(shape = cookie9.toShape(), color = primary, onClick)` with `Modifier.size(160.dp)`; `interactionSource` drives the press morph |
| Glyph | `ContourMark` 56 × 24, white | `ContourMark` 72 × 32, `onPrimary` |
| Hover | glass `.interactive()` highlight | — |
| Accessibility | label "Listen"; value "Idle" / "Listening" / "Humming" / "Matching"; hint "Identifies what's playing" | `contentDescription = "Listen"`, `stateDescription` |

### 5.2 Source toggles

Three options, always in this order: **Microphone · This Mac / Phone · Hum**. Selecting Hum sets `ListenMode::Hum` with the microphone; Microphone and This Mac/Phone use `ListenMode::Auto` (the core detects humming on its own, so humming into "Microphone" also works). While a session runs the toggles are disabled at 45 % opacity, never hidden.

- macOS: three `Toggle`s with `.toggleStyle(.button)` and `.buttonStyle(.glass)` in a `GlassEffectContainer(spacing: 12)`; 28pt tall; symbol + label. They are on the stage in **every** state, so the method can change after any result. Choosing one never starts listening; after a no-match, an error or a prompt it returns the stage to idle ("Identify what's playing") for the new method.
- Android: `ButtonGroup` (connected) of three `ToggleButton`s, 40dp, `connectedLeading/Middle/TrailingButtonShapes()`, icon 20dp + label.

### 5.3 State line

A single `.title3` / `titleMedium` line centered under the button. Fixed height (20pt / 24dp) in every state; text crossfades (`.contentTransition(.opacity)` / `AnimatedContent`). It is a live region for assistive tech (`AccessibilityNotification.Announcement` / `liveRegion = Polite`). Full string table in §7.

### 5.4 Action row

macOS: the **recovery row**, a 28pt slot under the source toggles, holds up to three actions after a no-match, an error or a prompt, as accent text buttons (`.borderless`, medium weight) so they read as actions rather than more toggles: Try again (the same method, or the same link or file again), Hum it (only when the last try wasn't a hum), Type a lyric, Use Microphone, Continue, Open System Settings, OK, Stop. It is empty otherwise but keeps its slot.

Android: the slot under the state line (40dp tall) holds either the source toggles or, after a no-match, up to three actions of the same height (`FilledTonalButton` + `TextButton`s). Never both.

### 5.5 Link field

macOS: `TextField("Paste a link", text:)` in the toolbar's leading group (`ToolbarItem(placement: .navigation)`, never `.principal`, which centers on the window), 240pt, leading `link` symbol, submits on ↩. On macOS 26 the toolbar's glass capsule is its border; before, a rounded field border. Android: a docked `SearchBar` input field styled surface (`surfaceContainerHigh`, 56dp, 28dp radius) with leading `link`, placeholder "Paste a link", trailing `attach_file` (opens `OpenDocument` for audio/video). When the clipboard holds a URL on resume, Android shows it as a suggestion row beneath the field ("youtube.com/watch… — Identify"); macOS relies on ⌘V.

### 5.6 Pitch trace (live)

- Band: macOS 560 × 96pt (width = min(560, pane − 64)); Android (screen − 48) × 96dp.
- Window: the last 8 s, newest at the right edge; y spans ±12 semitones around the running median (4pt/dp per semitone); no grid, no note names, no axis.
- Line: 4pt/dp round-capped, accent / `primary`; opacity = voicing confidence (0.6…1.0); unvoiced frames break the path (gaps). A 4pt/dp dot marks "now" at the right end of the live segment.
- Source: `SessionEvent::Pitch { frames }` (~10 Hz, batched 20 fps frames); interpolate between frames; scroll continuously in a `Canvas` / `drawBehind` inside `TimelineView(.animation)` / `withFrameNanos`.
- Accessibility: label "Your pitch", value "‹n› seconds hummed"; the band is `accessibilityHidden` for VoiceOver during capture except the summary.

### 5.7 Contour overlay ("your hum vs. the song")

- macOS inspector 268 × 120pt; Android 356 × 96dp (inside the expanded candidate card).
- Reference (`MelodyAlignment.reference`) 4pt/dp, `.tertiary` / `outlineVariant`, drawn first. Hum (`MelodyAlignment.query`, DTW-aligned to the reference time base by the core) 2.5pt/dp accent / `primary`, drawn on top. Both share the ±12-semitone scale around the reference's median and are transposed by the core.
- Legend beneath: two 14pt/dp swatches, "You" and the song title. Heading "Your hum vs. ‹title›" (macOS inspector only; the Android card already shows the title).

### 5.8 Match label / tier tag

Capsule 18pt tall (`.subheadline` medium) on macOS; `AssistChip`-sized 22dp tag on Android. Text is the label; style per §4.1.

### 5.9 Track row (results inside collections and tracklists)

50pt / 64dp: cue (44pt/dp, monospaced digits, secondary) · artwork 40 · title + artists · trailing (tier tag, ✓ ✕, "Hum it", or nothing). Selection: system row selection (`List(selection:)`) / full-bleed `surfaceContainer`.

### 5.10 Result hero

§6.3.

### 5.11 "Open in" split button

- macOS: a split capsule on accent-tinted glass ("Open in Spotify" | chevron menu), 28pt tall like the large glass buttons beside it, joined to the listen button's glass morph. The primary part opens the default service (Settings › General); the chevron lists every available `ServiceLink` in the order Spotify, Apple Music, YouTube Music, YouTube, Deezer, Tidal, Amazon Music, SoundCloud, Shazam, then "Search the web". Links with `is_search` are suffixed "(search)".
- Android: `SplitButton` (filled): leading "Open in Spotify", trailing chevron → `DropdownMenu` with the same list.
- Beside it on macOS: `ShareLink` and Copy as `.glass` icon buttons (28pt). On Android, Share and the overflow (Copy title and artist, Copy link, Pin, Delete) live in the top app bar.

### 5.12 Timeline strip

- 28pt/dp tall; a 16pt/dp ruler above with labels at 0, ¼, ½, ¾, 1 of the duration (first label left-aligned, last right-aligned), `.caption` / `labelSmall` secondary.
- Segment rendering per §4.1; minimum segment width 3pt/dp; overlapping segments (`overlap_with`) are drawn as two half-height bars stacked in the overlap.
- Analysis sweep: everything right of `Progress.analyzed_ms` is veiled at 75 % window color; a 1.5pt/dp label-colored frontier line marks the edge.
- Selection: 1.5pt/dp label-colored outline with a 1.5pt inner window-colored gap; selecting a row scrolls/zooms the strip to it and vice versa.
- Zoom: ⌘+/⌘− (×1.6 steps, 1…40) and pinch; the strip becomes horizontally scrollable with the selected segment kept in view.
- Playhead (when local audio exists): 1pt label line, dragging scrubs the preview.
- macOS: pinned with `safeAreaBar(edge: .top)` above the `List`, `scrollEdgeEffectStyle(.hard, for: .top)`. Android: pinned below the `TopAppBar` inside the `Scaffold` content, the list beneath is a `LazyColumn`.
- Accessibility: `accessibilityChartDescriptor` (AXChartDescriptor; one series "Tracks" with cue and tier per segment) on macOS; on Android the strip is `invisibleToUser` and the list is the accessible representation.

### 5.13 Progress and ETA

`Progress { analyzed_ms, total_ms, eta_ms }` → "About ‹n› min left" (ceil; under 60 s → "Less than a minute left"; `eta_ms == nil` → "Analyzing"). macOS shows `ProgressView().controlSize(.small)` + text + a bordered **Stop**; Android shows a 120dp `LinearWavyProgressIndicator(progress)` + text + a **Stop** text button.

### 5.14 Notices and errors

Notices (`Notice`) change the state line only. Session failures (`Failed { code, message }`) show the core's message in the state line with "Try again". Nothing uses an alert except permission pre-prompts and destructive confirmations.

---

## 6. Screens and states

### 6.1 Listen stage — macOS

Pane = detail column minus the 52pt toolbar. Everything is horizontally centered on the pane. Vertical positions are relative to the pane center `c`:

| Slot | Frame | Content |
|---|---|---|
| Trace band | 560 × 96, bottom edge at `c − 120` | empty unless humming |
| Listen button | 112 × 112, center at `c − 44` | §5.1 (+ halo, + ring) |
| State line | 20 tall, top at `c + 30` | §7 |
| Source toggles | 28 tall, top at `c + 70` | §5.2, in every state |
| Recovery row | 28 tall, top at `c + 110` | §5.4; empty unless there is something to recover from |
| Recent row | 520 × 56, bottom margin 24 | last result (artwork 40, `.headline` title, `.callout` artist, time, "Open in ‹service›" bordered) — only when a previous result exists; highlights on hover, click opens the session; context menu Open, Open in ▸, Copy, Pin/Unpin |

**States**

| State (engine) | Glyph / button | State line | Recovery row (macOS) | Other |
|---|---|---|---|---|
| Idle | mark | "Identify what's playing" (secondary) | — | — |
| Permission pre-prompt (first "This Mac") | mark | "macOS will ask to allow system audio recording." | [Continue] [Use Microphone] | the system prompt follows Continue |
| Microphone denied | mark | "Microphone access is off." | [Open System Settings] | deep link to Privacy › Microphone |
| Listening (`Started`, `Phase::Listening`) | live line + halo | "Listening" | — (toggles disabled) | Esc / click stops |
| `Notice::MoveCloser` (6 s) | same | "Move closer to the sound" | same | — |
| `Notice::ImpressionDetected` | low live line + ring | "Keep going" | — (toggles disabled) | trace band scrolling |
| Hum mode start (`ListenMode::Hum`) | low live line + ring at 0 | "Hum, whistle or sing" | same | trace band visible, empty |
| `Notice::KeepGoing` | same | "Keep going" | same | — |
| Matching (`Phase::Recognizing` / `MatchingMelody` / `SearchingLyrics` after capture ends) | frozen line at 70 % | "⟳ Matching" (small `ProgressView` inline) | — (toggles disabled) | — |
| `Outcome::Matched` | morphs into the result | — | — | §6.3 replaces the stage |
| `Outcome::Candidates` | — | — | — | §6.4 replaces the stage |
| `NoMatch(NotFound)` | mark | "No match found" | [Try again] [Hum it] [Type a lyric] | — |
| `NoMatch(TooQuiet)` | mark | "Too quiet to hear" | [Try again] [Hum it] | — |
| `NoMatch(Noisy)` | mark | "Too noisy to hear the music" | [Try again] [Hum it] | — |
| `NoMatch(TooShort)` | mark | "Too short to tell" | [Try again] | — |
| `NoMatch(MostlySpeech)` | mark | "Mostly talking, not music" | [Try again] [Hum it] | — |
| `Notice::CaptureSilent` / `NoMatch(CaptureSilent)` | mark | "This app blocks audio capture. Use the microphone instead." | [Use Microphone] | switches the source toggle |
| `Notice::MelodyIndexMissing` (on Hum, or first impression) | mark | — | — | sheet: "Download the melody library?" / "Humming needs a 48 MB library that stays on this Mac." / [Download] [Not Now]. Declining continues with lyrics and ACRCloud humming only. |
| `Notice::RateLimited { retry_in_ms }` | mark | "Too many requests. Trying again in ‹n› s." | [Stop] | counts down |
| `Notice::ProviderUnavailable` | unchanged | unchanged | unchanged | nothing shown; logged |
| Offline (no route at session start) | mark | "You're offline. Saved to identify later." | [OK] | the session is stored with status Running and retried on reconnect; Library row subtitle "Waiting for connection" |
| Drop targeted | mark, tint 100 %, scale 1.04 | "Drop to identify" | — | — |
| Link: `Phase::Resolving` | `link` symbol replaces the mark (crossfade) | "Resolving link" | [Stop] | — |
| Link: `Phase::Downloading` (fraction) | `link` symbol + determinate ring (reuse the hum ring) | "Downloading" | [Stop] | — |
| Link: `Phase::Decoding` / `Segmenting` | ring indeterminate (`ProgressView`-style rotating 90° arc) | "Preparing" | [Stop] | — |
| Link: `Phase::Recognizing` for short media | ring indeterminate | "Matching" | [Stop] | then §6.3 / no-match |
| Link: media longer than 4 min or first `Segment` | — | — | — | navigates to §6.5 immediately; results stream in |
| Streaming link (Spotify/Apple Music/Deezer track) | — | — | — | §6.3 directly (`Phase::Resolving` is usually < 1 s; show "Resolving link" only if it exceeds 300 ms) |
| Album / playlist link | — | — | — | §6.6 |
| `Failed { code, message }` | mark | `message` | [Try again] | `UnsupportedLink`: "This link isn't supported." · `MediaUnavailable`: "This video isn't available." · `Network`: "Couldn't reach the server." |

### 6.2 Listen stage — Android

Stage = between the link field (ends at 116dp from the top including the status bar) and the navigation bar. Positions relative to the stage center `c`:

| Slot | Frame | Content |
|---|---|---|
| Trace band | (screen − 48) × 96, bottom at `c − 144` | empty unless humming |
| Listen button | 160 × 160, center at `c − 40` | §5.1 |
| State line | 24 tall, top at `c + 60` | §7 |
| Action row | 40 tall, top at `c + 124` | `ButtonGroup` or no-match actions |
| Recent card | 72 tall, 16dp margins, 16dp above the navigation bar, `surfaceContainerLow` 16dp radius | artwork 56, `titleMedium`, `bodyMedium` "Dua Lipa · 14:32", chevron |

States mirror §6.1 with these platform differences:

- Permission: microphone → the system dialog directly; if previously denied, state line "Microphone access is off." + [Open Settings]. Phone audio → state line "Android will ask to allow capture for this session." + [Continue] (MediaProjection consent is required for every session on Android 14+), and the source is never the default.
- `Notice::MelodyIndexMissing` → `AlertDialog` with the same copy and buttons.
- Matching uses `LoadingIndicator(color = onPrimary)` (48dp) in place of the glyph; the state line reads "Matching" without an extra spinner.
- Back gesture while listening cancels (§3.2).
- Offline: state line "You're offline. Saved to identify later." and a `Snackbar` is not shown (the state line is enough).

### 6.3 Result (matched track) — both platforms

**macOS** (content column; vertically centered group; inspector open by default when the window is ≥ 1100pt wide):

```
hero wash: blurred artwork (blur 50, saturation 1.3, opacity 0.42 light / 0.5 dark) covering the pane,
           fading to the window background from 20 % to 100 % of height; one instance, with backgroundExtensionEffect()
artwork 240 × 240, radius 12, shadow (0, 10, 28, 22 %)
24  title        .largeTitle bold, centered, max 2 lines, max width 520
 4  artist       .title3
 4  meta         .callout secondary   "Radical Optimism · 2024"   (album only if known; year only if known)
12  context      .callout secondary, only for links/files: "At 12:34 in ‹source title›" / "From ‹file name›"
22  actions      [Open in Spotify ▾] .glassProminent .large   [share] [copy] .glass icon 28 — one GlassEffectContainer
```

Toolbar: [Listen ▸] [Inspector]. Inspector per §3.1. Keyboard: ⌘↩ opens, Space previews (30 s `Track.preview_url`; the artwork shows a 2pt accent progress ring while playing), ⌘C copies "Artist – Title".

**Android**: transparent `TopAppBar` (back; Share; More). Content scrolls:

```
wash: primaryContainer → transparent, 440dp, 55 % (content-based scheme active for this screen only)
artwork 240dp, radius 20, elevation 16dp shadow, top at 128dp from the status bar
24  title    displaySmallEmphasized
 2  artist   titleMedium
 2  meta     bodyMedium onSurfaceVariant
12  context  (links/files only)
22  SplitButton xl (56dp): "Open in Spotify" | ▾
40  "Lyrics" titleSmall + bodyLarge onSurfaceVariant (only if lyrics exist)
```

Preview playback: tapping the artwork toggles the 30 s preview; a 3dp `primary` ring draws progress around the artwork's rounded rect.

### 6.4 Hum candidates — both platforms

Ranked 1–5 `Candidate`s, `is_final` may arrive several times: rows update in place, never reorder after the first final list (new candidates append; rank changes animate with `.animation(.spring(...), value:)` / `animateItem`).

**macOS** (content column + inspector):

```
hum row  72 tall, padding 8 28: frozen hum trace (fills, 56 tall, 2.5pt accent) · "Your hum · 11 s" .callout secondary · [Hum again] bordered
rows     68 tall, 16 side margins, radius 10, List(selection:)
         artwork 48 · title .headline + artist .callout secondary · match capsule · [▶] 28 icon · [This is it]
         The selected row's "This is it" is .borderedProminent and is the default button (↩). Others are .bordered.
footer   18 below: "Not here?" .callout secondary · [Type a lyric] bordered
```

Selecting a row updates the inspector (§5.7 overlay + details). "This is it" → `confirm_candidate` → the view morphs into §6.3 (the row's artwork is the matched-geometry source). ▶ plays the 30 s preview; one at a time; the row icon becomes `pause.fill`.

**Android**:

```
top bar: back · [Hum again] text button (trailing)
hum row  64dp: trace (fills, 48dp) · "Your hum · 11 s" labelMedium onSurfaceVariant
cards    8dp gap, 16dp margins, radius 16, surfaceContainerLow; expanded card surfaceContainer
         head: artwork 56 · titleMedium + bodyMedium · tag (· [▶] IconButton when collapsed)
         expanded: overlay 96dp + legend · actions: [▶] FilledTonalIconButton … [This is it] Button(filled)
footer   "Not here?" bodyMedium + [Type a lyric] TextButton
```

The first card is expanded by default. Tapping a collapsed card expands it (`animateContentSize`, hero spring) and collapses the other.

### 6.5 Mix session — both platforms

**macOS** (pushed in the Library stack, or shown in Listen while analyzing a link):

```
header   padding 6 20 14: video thumbnail 100 × 56 (radius 6) · title .headline (1 line) + "YouTube · 1:00:33" .callout secondary
         trailing: [⟳ About 4 min left] .callout secondary + [Stop] bordered   — while running
                   "12 of 14 identified" .callout secondary                     — when finished
timeline padding 0 20 10: ruler 16 + strip 28 (safeAreaBar, pinned)
list     List(selection:) rows 50 (§5.9); rows appear as Segment events arrive, in cue order
```

Toolbar: [Export ▾] [Listen ▸] [Inspector]. Export menu: Copy Tracklist ⌘⇧C · Export as Text… · Export as CSV… · Export as CUE… · Export as JSON… (`fileExporter`, `ExportFormat`). Inspector per §3.1.

Row actions: Possible rows show ✓ ("Confirm") and ✕ ("Not this") icon buttons (28pt); ID rows show [Hum it] (bordered; starts a Hum session linked to the segment — on confirmation the segment is updated with `user_state = Edited`). Context menu on every row: Open in ‹service› ▸, Confirm, Not This, Hum This Part, Retry This Part, Copy, Play on ‹site› at ‹cue›. Return opens, Space previews the segment (local audio) or opens the source at the cue (deep link: YouTube `&t=905s`, SoundCloud `#t=15:05`).

Stop: cancels the session (`cancel()`); identified segments stay; header shows "Stopped · 8 of ? identified" → "8 identified".

**Android**:

```
TopAppBar: back · title titleLarge (1 line) + subtitle bodyMedium "YouTube · 1:00:33" · Share · More (Copy tracklist, Export…, Pin, Delete)
timeline 16dp margins: ruler 16 + strip 28 (pinned)
status   40dp: LinearWavyProgressIndicator 120dp · "About 4 min left" bodyMedium onSurfaceVariant · [Stop] TextButton
list     LazyColumn rows 64dp (§5.9), selection full-bleed surfaceContainer
```

Tapping a row opens a `ModalBottomSheet`: artwork 64, title, artists, cue range, Match, Evidence, [Open in ‹service› ▾] SplitButton, [YouTube at 15:05] outlined, and for Possible rows [Confirm] [Not this] tonal. Export… opens a sheet listing Text, CSV, CUE, JSON → `CreateDocument`. A `ProgressStyle` notification mirrors the header while the app is in the background.

### 6.6 Collections (album / playlist links)

Streaming links resolve in under a second to `Segment`s of kind `Track` with `start_ms = end_ms = 0`. Show the mix layout without the timeline: header (collection artwork 56, "Short n' Sweet", "Sabrina Carpenter · 12 tracks · open.spotify.com"), then rows with the item index instead of a cue (`item_index`, right-aligned, monospaced). Export offers the same formats.

### 6.7 Typed lyric search

Reachable from the no-match action row and the candidates footer.

- macOS: a `.sheet` 480 × 360: `TextField("Type a lyric you remember")` with `.searchable`-style capsule, results `List` beneath (rows like §6.4 without the overlay; capsule "Lyrics"); ↩ searches; selecting a row → §6.3. Cancel closes.
- Android: `ExpandedFullScreenSearchBar` with the same placeholder; results as candidate cards; tapping → result.

Empty result: a single line in the list area, "No songs with that lyric." + [Try different words]. Copy avoids blaming the user.

### 6.8 Library — both platforms

Grouped by day (Today, Yesterday, then "Sunday, October 11" via `Date.FormatStyle(date: .complete)` without the year in the current year). Filter: All · Heard · Hummed · Links · Mixes · Pinned (`SessionKind` → Heard = Listen + SystemAudio, Hummed = Hum, Links = Link + File + Lyrics, Mixes = sessions with ≥ 2 segments or `AnalyzeMode::Mix`, Pinned = pinned sessions). Search matches title, artist, source title and host.

**Pin is the one way to keep sessions** (there is no separate favorite): pinned sessions are listed in the sidebar like a playlist, keep their audio (Settings › Storage) and ask before deleting.

Row (56pt / 72dp): artwork 40 / 56 (video thumbnail for mixes, collection art for playlists, hatched tile for unresolved) · title · subtitle = kind symbol + artist / "8 tracks so far" / "a-ha · 4 candidates" / "Sabrina Carpenter · tiktok.com" / "Waiting for connection" · trailing time (monospaced) · pin symbol if pinned · small spinner while Running.

- macOS: toolbar per §3.1; the filter is a one-click scope bar pinned above the list (`Picker(.segmented)`, a menu when the column is too narrow); `List(selection:)` with multiple selection (⌘-click, ⇧-click, ⌘A); day headers `.headline` secondary. The row's pin is a button: `pin.fill` while pinned, an outline `pin` in tertiary on hover or selection. Pin and unpin work on any selection — context menu (Pin when any is unpinned, Unpin when any is pinned, both for mixed selections, like Finder's tags), ⌘⇧P, the toolbar's Pin, the inspector, a leading swipe (orange) — update at once and undo with ⌘Z. Context menu for one row: Open, Open in ‹service› ▸, Pin/Unpin, Export ▸, Delete; for several: Pin/Unpin, Delete. Trailing swipe deletes. ⌫ deletes. Space previews the selected row. Empty Pinned scope: "Nothing pinned yet" / "Pin songs and mixes to keep them here and in the sidebar."
- Android: `AppBarWithSearch` ("Search") with a Settings action; `FilterChip` row (32dp, 8dp gap) beneath; `LazyColumn` with `stickyHeader` day headers (`titleSmall` primary); swipe start→end pins, end→start deletes.

Delete semantics: Heard/Hummed/Link sessions delete immediately with Undo (⌘Z for 10 s / `Snackbar` "Deleted" + Undo, 5 s). Mixes and pinned sessions confirm first: title "Delete “‹title›”?", message "The tracklist and its saved audio will be removed.", [Delete] destructive / [Cancel].

Empty: `ContentUnavailableView("Nothing identified yet", systemImage: "waveform")` with description "Songs you identify will show up here." · Android: the mark at 48dp in `outline`, "Nothing identified yet" `titleMedium`, "Songs you identify will show up here." `bodyMedium` onSurfaceVariant, centered.

### 6.9 Settings

**macOS** — `Settings` scene, `TabView` with five panes, `Form` with `.formStyle(.grouped)`, window 620pt wide, minimize and zoom dimmed, last pane restored. The window title is the pane name.

| Pane (symbol) | Rows |
|---|---|
| General (`gearshape`) | "Open songs in" popup (Spotify, Apple Music, YouTube Music, YouTube, Deezer, Tidal, Amazon Music, SoundCloud) · "Show in menu bar" toggle · "Launch at login" toggle (`SMAppService`) · "Notify when a result arrives in the background" toggle |
| Services (`key`) | **AudD**: "API token" secure field, footer "Optional. Tried when the free match fails." · **ACRCloud**: Host, Access key, Access secret; footer "Optional. Used for recordings." · **ACRCloud humming**: Host, Access key, Access secret; footer "Optional. A second opinion when you hum." Credentials go to the Keychain; fields show "••••" when set. |
| Melody (the mark) | installed: "Melody library" → "41,234 songs · 48 MB"; "Updated" → "October 1, 2026"; buttons [Check for Updates] [Remove]; footer "Needed to recognize humming, whistling and singing. Stays on this Mac." · not installed: "Melody library" → "Not installed · 48 MB" + [Download] · downloading: `ProgressView(value:)` "23 of 48 MB" + [Cancel] · update available: "Update available · 52 MB" + [Update] · error: "Couldn't download. Check your connection." + [Try Again] · group "While humming": "Also search lyrics you sing" toggle, "Stop automatically after a pause" toggle |
| Storage (`internaldrive`) | "On disk" → "1.2 GB" · "Keep audio for pinned sessions" toggle · [Clear Cache] with confirmation "Clear 1.2 GB of cached audio? Pinned sessions keep their audio." · "Location" → path + [Show in Finder] |
| About (`info.circle`) | app icon, "Music Fetch 1.0 (build)", [Licenses] (pushes a list of third-party licenses), "Acknowledgements" |

Settings mockups: `mac-settings-services.png` and `mac-settings-melody.png` show the Services pane and the installed Melody state.

**Android** — `settings` route, `LazyColumn` of `ListItem`s with section headers (`titleSmall` primary): Open songs in (dialog list) · Quick Settings tile [Add tile] · Home screen widget [Add widget] · Notifications (system link) · AudD, ACRCloud, ACRCloud humming (expandable `OutlinedTextField`s; stored in Keystore-encrypted prefs) · Melody library (same states; `LinearProgressIndicator` while downloading) · Storage (size, Clear cache) · About (version, Licenses).

### 6.10 Menu bar extra — macOS

`.window` style, 320pt wide, padding 20 16 10. Status item: the contour mark as a template symbol (18 × 8pt in the 24pt bar). Content, top to bottom:

```
listen button 72pt (glass, accent tint; same choreography at 64 % scale; halo and live line included)
12  state line .body
12  source toggles (compact, 24pt)
 8  recovery row (compact; only while it has actions)
16  divider · last result row: artwork 40 · .headline title + .callout artist · [Open in Spotify] bordered
 8  divider · footer: "Open Music Fetch" (plain) … gear (opens Settings)
```

After a match the result row updates in place and the button returns to idle (no result hero in the extra). Candidates and mixes open the main window. The window can't be opened programmatically with first-party API; the ControlWidget and the global shortcut start listening in the main window instead.

### 6.11 Android entry points

**Share sheet** (`ShareIdentifyActivity`, `ModalBottomSheet`, `surfaceContainerLow`, 28dp top radius):

```
handle
source row: link icon + "youtube.com/shorts/x9Zk3pQ7lTw" bodyMedium onSurfaceVariant (the shared content, confirmable; for files: the file name)
state      while working: LinearWavyProgressIndicator + "Downloading" / "Matching" / "Analyzing · about 2 min left" + [Stop]
result     artwork 64 · titleLargeEmphasized title · bodyLarge artist · bodyMedium meta
           SplitButton xl "Open in Spotify" | ▾
           [Open in Music Fetch] TextButton
mix        after the first segment: "3 tracks found so far" + [Open in Music Fetch] (analysis continues in the foreground service)
no match   "No match found" titleMedium + [Hum it] [Type a lyric] tonal/text
```

**QS tile overlay** (`ListenOverlayActivity`, translucent, scrim 32 %): a 112dp listen button variant centered with the state line beneath and [Cancel] below; it follows the listening choreography; on a match it becomes the share-sheet result layout; on candidates it opens the app.

**Widgets** (Glance, `GlanceTheme`): 2×2 — the mark on a `primary` cookie shape, label "Listen"; 4×2 — the button at 64dp plus the last result (artwork 48 downscaled, title, artist). Artwork bitmaps are downscaled to ≤ 96px (Android 17 RemoteViews memory limit).

**Notifications**: channel "Identifying" (low importance, silent) for mix progress with `ProgressStyle` segments (one filled segment per identified track over the analyzed span) and a Stop action; channel "Results" (default) for background matches with "Open in ‹service›". Never promoted to Live Updates.

---

## 7. Copy

All user-facing strings, final wording. Sentence case; no exclamation marks; no apologies.

**Listen stage**

| Key | String |
|---|---|
| state.idle | Identify what's playing |
| state.listening | Listening |
| state.moveCloser | Move closer to the sound |
| state.humStart | Hum, whistle or sing |
| state.keepGoing | Keep going |
| state.matching | Matching |
| state.resolving | Resolving link |
| state.downloading | Downloading |
| state.preparing | Preparing |
| state.drop | Drop to identify |
| state.noMatch | No match found |
| state.tooQuiet | Too quiet to hear |
| state.noisy | Too noisy to hear the music |
| state.tooShort | Too short to tell |
| state.mostlySpeech | Mostly talking, not music |
| state.captureSilent | This app blocks audio capture. Use the microphone instead. |
| state.rateLimited | Too many requests. Trying again in %d s. |
| state.offline | You're offline. Saved to identify later. |
| state.micOff.mac | Microphone access is off. |
| state.systemAudioPrePrompt.mac | macOS will ask to allow system audio recording. |
| state.phoneAudioPrePrompt.android | Android will ask to allow capture for this session. |
| action.tryAgain | Try again |
| action.humIt | Hum it |
| action.typeLyric | Type a lyric |
| action.useMicrophone | Use Microphone (macOS) / Use microphone (Android) |
| action.continue | Continue |
| action.openSystemSettings | Open System Settings (macOS) / Open settings (Android) |
| action.stop | Stop |
| source.microphone | Microphone |
| source.thisMac | This Mac |
| source.phone | Phone |
| source.hum | Hum |
| field.link | Paste a link |
| field.linkSuggestion.android | %@ — Identify |

**Melody library**

| Key | String |
|---|---|
| melody.prompt.title | Download the melody library? |
| melody.prompt.message | Humming needs a 48 MB library that stays on this Mac. (Android: "…on this phone.") |
| melody.download | Download |
| melody.notNow | Not Now (macOS) / Not now (Android) |
| melody.status.installed | %@ songs · %@ |
| melody.status.notInstalled | Not installed · %@ |
| melody.status.updateAvailable | Update available · %@ |
| melody.status.downloading | %@ of %@ |
| melody.error | Couldn't download. Check your connection. |
| melody.footer | Needed to recognize humming, whistling and singing. Stays on this Mac. (Android: "…on this phone.") |
| melody.checkForUpdates | Check for Updates |
| melody.remove | Remove |
| melody.update | Update |
| melody.tryAgain | Try Again |
| melody.alsoLyrics | Also search lyrics you sing |
| melody.autoStop | Stop automatically after a pause |

**Results**

| Key | String |
|---|---|
| result.openIn | Open in %@ |
| result.search | %@ (search) |
| result.searchWeb | Search the web |
| result.share | Share |
| result.copy | Copy |
| result.copied | Copied |
| result.context.link | At %@ in %@ |
| result.context.file | From %@ |
| result.lyrics | Lyrics |
| result.album | Album |
| result.released | Released |
| result.length | Length |
| result.isrc | ISRC |
| result.heard | Heard |
| result.source | Source |
| result.when | When |
| result.matchedBy | Matched by |
| result.matchedBy.shazam | Shazam, after %d s |
| result.matchedBy.audd | AudD |
| result.matchedBy.acr | ACRCloud |
| result.matchedBy.melody | Melody |
| result.matchedBy.lyrics | Sung lyrics |
| result.matchedBy.metadata | Link metadata |
| result.matchedBy.user | You |
| result.where | Where |

**Candidates**

| Key | String |
|---|---|
| cand.yourHum | Your hum · %d s |
| cand.humAgain | Hum again |
| cand.strong | Strong |
| cand.possible | Possible |
| cand.longShot | Long shot |
| cand.thisIsIt | This is it |
| cand.notHere | Not here? |
| cand.overlayTitle | Your hum vs. %@ |
| cand.legend.you | You |
| lyrics.prompt | Type a lyric you remember |
| lyrics.empty | No songs with that lyric. |
| lyrics.tryDifferent | Try different words |

**Mix**

| Key | String |
|---|---|
| mix.subtitle | %@ · %@ (site · duration) |
| mix.eta | About %d min left |
| mix.etaUnderMinute | Less than a minute left |
| mix.analyzing | Analyzing |
| mix.identified | %d of %d identified |
| mix.identifiedOnly | %d identified |
| mix.stopped | Stopped · %d identified |
| mix.tracksSoFar | %d tracks so far |
| mix.id | ID |
| mix.possible | Possible |
| mix.confirm | Confirm |
| mix.notThis | Not this |
| mix.humIt | Hum it |
| mix.retryPart | Retry this part |
| mix.humPart | Hum this part |
| mix.openSourceAt | %@ at %@ |
| mix.match | Match |
| mix.evidence | Evidence |
| mix.evidence.shazam.n | %d Shazam matches |
| mix.evidence.shazam.skew | 1 Shazam match, pitch-shifted |
| mix.evidence.acr | ACRCloud, score %d |
| mix.evidence.hint | Page attribution only |
| export.menu | Export |
| export.copyTracklist | Copy Tracklist |
| export.text | Export as Text… |
| export.csv | Export as CSV… |
| export.cue | Export as CUE… |
| export.json | Export as JSON… |

**Library and sessions**

| Key | String |
|---|---|
| library.title | Library |
| library.search | Search Library |
| library.filter.all / heard / hummed / links / mixes / pinned | All / Heard / Hummed / Links / Mixes / Pinned |
| library.today / yesterday | Today / Yesterday |
| library.candidates | %@ · %d candidates |
| library.tracks | %@ · %d tracks |
| library.waiting | Waiting for connection |
| library.empty.title | Nothing identified yet |
| library.empty.message | Songs you identify will show up here. |
| session.pin / unpin | Pin / Unpin |
| session.delete | Delete |
| session.deleted | Deleted |
| session.undo | Undo |
| session.deleteConfirm.title | Delete “%@”? |
| session.deleteConfirm.message | The tracklist and its saved audio will be removed. |
| session.cancel | Cancel |

**Settings**

| Key | String |
|---|---|
| settings.general / services / melody / storage / about | General / Services / Melody / Storage / About |
| settings.openSongsIn | Open songs in |
| settings.menuBar | Show in menu bar |
| settings.launchAtLogin | Launch at login |
| settings.notify | Notify when a result arrives in the background |
| settings.audd | AudD |
| settings.audd.token | API token |
| settings.audd.footer | Optional. Tried when the free match fails. |
| settings.acr | ACRCloud |
| settings.acrHum | ACRCloud humming |
| settings.host / accessKey / accessSecret | Host / Access key / Access secret |
| settings.acr.footer | Optional. Used for recordings. |
| settings.acrHum.footer | Optional. A second opinion when you hum. |
| settings.onDisk | On disk |
| settings.keepPinnedAudio | Keep audio for pinned sessions |
| settings.clearCache | Clear Cache |
| settings.clearCache.confirm | Clear %@ of cached audio? Pinned sessions keep their audio. |
| settings.location | Location |
| settings.showInFinder | Show in Finder |
| settings.licenses | Licenses |
| settings.qsTile | Quick Settings tile |
| settings.addTile | Add tile |
| settings.widget | Home screen widget |
| settings.addWidget | Add widget |

**Menu bar extra**: "Open Music Fetch". **Share sheet**: "Open in Music Fetch", "%d tracks found so far". **QS tile**: label "Music Fetch". **Notifications**: title "%@ — %@" (title — artist); mix "Analyzing %@" with "%@ of %@ · %d tracks".

**Failures** (`ErrorCode` → state line): `UnsupportedLink` "This link isn't supported." · `MediaUnavailable` "This video isn't available." · `Network` "Couldn't reach the server." · `RateLimited` "Too many requests. Try again in a minute." · `Decode` "Couldn't read this file." · `IndexUnavailable` "The melody library isn't installed." · `Storage` / `Platform` / `Internal` "Something went wrong." All with [Try again].

---

## 8. Accessibility

- **Labels.** Every icon-only control has a label: Listen (value: idle/listening/humming/matching), Inspector, Share, Copy, Play preview / Pause preview, Confirm, Not this, Pin / Unpin, Stop, Export, Back to Listen, Other Ways to Identify. Library rows read "Pinned" as their value and offer Pin / Unpin as an accessibility action. Source toggles announce "Microphone, selected".
- **Announcements.** `AccessibilityNotification.Announcement` / `announceForAccessibility` for: "Listening", "Move closer to the sound", "Keep going", "Found ‹title› by ‹artist›", "‹n› possible matches. Best: ‹title› by ‹artist›, ‹tier›", "No match found. ‹reason›", "‹n› of ‹m› identified" every 5 new segments while a mix runs.
- **Live regions.** The state line is a polite live region on Android; on macOS it is read through announcements only (avoid double reads).
- **Reduce Motion** (`accessibilityReduceMotion` / "Remove animations"): §2.2. **Reduce Transparency**: the glass button becomes an opaque accent circle (`.glassEffect` adapts; for custom halo use `accessibilityReduceTransparency` to disable it). **Increase Contrast**: capsules and tags gain a 1pt outline in label color; the strip's Likely tier uses 75 % instead of 60 %. **Differentiate Without Color**: already satisfied by fill styles and text.
- **Dynamic Type / font scale**: all text uses system styles; the result title clamps at `.accessibility2` on macOS; rows grow with text (no fixed heights in code; the heights above are the default-size results). Android: `sp` everywhere, `nonScaledSp` nowhere.
- **Keyboard**: full keyboard navigation on macOS (§3.1); focus rings are system; the trace band and strip are not focusable (their lists are). Android: `Modifier.focusable()` order follows reading order; D-pad works in the candidates list.
- **Hit targets**: §4.3.
- **VoiceOver on the trace**: the band reports "Your pitch, ‹n› seconds hummed"; the overlay reports "Your hum compared with ‹title›: matches ‹n› of ‹m› notes" (the core supplies the aligned note count).
- **TalkBack on the strip**: `invisibleToUser`; the list rows read "‹cue›, ‹title› by ‹artist›, ‹tier›".
- **Color contrast**: all text pairs follow system/M3 roles (≥ 4.5:1); the glass button's white glyph sits on accent at ≥ 85 % opacity; in the content-based scheme the `SplitButton` uses `primary`/`onPrimary` which M3 guarantees.

---

## 9. App icon brief

**Concept — "the fetched line".** One stroke on a deep surface: a melody that enters flat from the left, lifts through three growing humps and settles. It is the contour mark (§2.1) at icon scale. No magnifier, no note symbol, no waveform bars. The idea is a tune being pulled out of the dark and held still.

**Composition.** The mark fills ~68 % of the width, centered, with its baseline slightly below center so the humps sit in the upper half. Stroke width ≈ 9 % of the icon width, round caps. Light enters from the top-left.

**Palette.** Surface: ink navy `#10173A` → `#1B2A5E` (vertical, darker at the bottom). Line: mint `#5BE4C7` warming to white `#F4FFFB` at the terminals (a two-stop gradient along the stroke, not across it). No other color.

**macOS 26 (Icon Composer, layered).** Background layer: the navy gradient (squircle, system mask). Foreground layer 1: the stroke, with Icon Composer's specular highlight on and a 2 % blur on a duplicate beneath for a soft glow. Foreground layer 2: a very faint secondary stroke (10 % white) one hump behind, as a receding echo — omit in the mono/tinted variants. Provide Default, Dark (surface `#0B1029`, line unchanged), Clear (stroke only, white at 90 %), and Tinted (single-layer monochrome stroke). Export the 1024 master plus the layered `.icon` bundle.

**Android.** Adaptive icon: background = navy gradient (full 108dp canvas); foreground = the stroke within the 66dp safe zone, scaled so the humps clear the circle mask; monochrome layer = the stroke alone (white). Provide `ic_launcher_foreground`, `_background`, `_monochrome` as vectors.

**Also derive:** the macOS template status-bar symbol (stroke only, 2pt at 18 × 8), the QS tile vector (24dp, white), the Glance widget button mark, and a Dock-size (512) variant with the stroke slightly thicker (11 %).

Generate with the image model, then trace the stroke as a vector for the Android layers and the template symbols; the gradient layers may stay raster for macOS.

---

## 10. Engineering notes

**Platform availability and opt-ins**

- macOS 26.0: `glassEffect`, `Glass`, `GlassEffectContainer`, `glassEffectID`, `glassEffectTransition`, `.buttonStyle(.glass / .glassProminent)`, `backgroundExtensionEffect`, `ToolbarSpacer`, `sharedBackgroundVisibility`, `scrollEdgeEffectStyle`, `safeAreaBar`, `ControlWidget`. `toolbar(removing: .title)` is macOS 15+. `visibilityPriority` is **macOS 26.1** — gate with `#available(macOS 26.1, *)`.
- `MenuBarExtra` still has no first-party open/close; don't design flows that need it.
- `backgroundExtensionEffect()` on exactly one view (the hero wash).
- `glassEffectID` morphs only between views inside the same `GlassEffectContainer`; keep the listen button and the result actions in one container and one view hierarchy (switch content with `if`, not navigation).
- `AccessibilityNotification.Announcement` for all state announcements; `accessibilityChartDescriptor` on the strip.
- material3 `1.5.0-beta01`: stable — `MaterialExpressiveTheme`, `MotionScheme`, `ButtonGroup`, `ToggleButton`, `SplitButton`, `Linear/CircularWavyProgressIndicator`, `ShortNavigationBar`, `WideNavigationRail`, `AppBarWithSearch`, `ExpandedFullScreenSearchBar`, emphasized type styles. **`@OptIn(ExperimentalMaterial3ExpressiveApi::class)`** — `LoadingIndicator`, `MaterialShapes`, `RoundedPolygon.toShape()`, `Morph.toPath()`. Keep these in `ListenButtonShape.kt` and `MatchingIndicator.kt` so API churn stays contained. Pin material3 above the Compose BOM 2026.09.00 (which maps to 1.4.0).
- `androidx.graphics:graphics-shapes:1.1.0` for `Morph`; `glance-appwidget` 1.2.0; MaterialKolor 5.0.1 (or the core's `material-colors` palette, preferred, so both platforms share one palette).
- Android 14+: MediaProjection consent **every** session, one-shot token, `mediaProjection` FGS type; Android 15 QPR1+ shows a capture chip and stops on lock. Android 17 (target 37): foreground service with while-in-use capability for any background audio; RemoteViews bitmap limit.
- QS tile → overlay activity → microphone FGS (tiles are not exempt from the while-in-use restriction; widgets are).

**Risky interactions to prototype first**

1. The glass morph from the listen button to the "Open in" split button: `Menu` with `primaryAction` plus `.glassProminent` must accept `glassEffectID`; if it doesn't, morph into a plain `Button` and overlay the chevron `Menu` as a separate glass item.
2. The unfurl (matched geometry of a `Canvas` path between two frames): if `matchedGeometryEffect` can't carry a live-drawn path, animate a plain `Rectangle` placeholder and crossfade the drawn line at the end.
3. Hum auto-stop feel: the ring fills to 12 s but the core may stop earlier (≥ 6 s voiced + 1.5 s silence) or later (20 s cap). The state line must not promise a count; the ring is guidance only.
4. `safeAreaBar` + `scrollEdgeEffectStyle(.hard)` with a pinned strip that also scrolls horizontally; verify the edge effect doesn't blur the strip.
5. Streaming segment upserts into `List(selection:)` / `LazyColumn` without losing selection or scroll position; `SegmentRemoved` must animate the merge.
6. Share-sheet activity lifecycle: the foreground service must outlive the sheet when the user taps "Open in Music Fetch" mid-analysis.
7. Content-based color on the Android result: compute the scheme off the main thread and crossfade (`animateColorAsState`, effects spring) to avoid a flash.
8. Dynamic Type at `.accessibility3` on the Listen stage: the state line may wrap to two lines — reserve two lines when `dynamicTypeSize.isAccessibilitySize`.

**Open questions for the core**

- A normalised `rms` range for the halo/live line (currently assumed 0…1 after the core's own normalisation).
- `Candidate.sources` → "Matched by" phrasing when several sources agree ("Melody and lyrics").
- Whether `MelodyAlignment` includes an aligned-note count for the VoiceOver summary.
- A `preview_url` refresh path for expired previews (some services expire them).

---

## Appendix — mockup index

| File | Shows |
|---|---|
| `mockups/mac-listen-idle.html` (+ `-dark`) | Listen stage, idle, with the recent row |
| `mockups/mac-listening.html` (+ `-dark`) | Listening: live line, halo, disabled toggles |
| `mockups/mac-hum.html` (+ `-dark`) | Impression: trace band, hum ring, "Keep going" |
| `mockups/mac-result.html` (+ `-dark`) | Matched result with inspector (details, heard, lyrics) |
| `mockups/mac-candidates.html` | Hum candidates with the contour overlay in the inspector |
| `mockups/mac-mix.html` (+ `-dark`) | Mix session: strip, tracklist, streaming progress, segment inspector |
| `mockups/mac-library.html` | Library with filters, search, day groups |
| `mockups/mac-settings-services.html` | Settings › Services |
| `mockups/mac-settings-melody.html` | Settings › Melody (installed state) |
| `mockups/mac-menubar.html` (+ `-dark`) | Menu bar extra window |
| `mockups/android-listen-idle.html` (+ `-dark`) | Listen, idle |
| `mockups/android-listening.html` (+ `-dark`) | Listening (shape morph, halo) |
| `mockups/android-hum.html` (+ `-dark`) | Hum with trace band and wavy ring |
| `mockups/android-result.html` (+ `-dark`) | Result with content-based color and SplitButton |
| `mockups/android-candidates.html` (+ `-dark`) | Candidates with the expanded overlay card |
| `mockups/android-mix.html` | Mix session |
| `mockups/android-library.html` | Library |
| `mockups/android-share.html` (+ `-dark`) | Share-sheet result |

Render everything with `mockups/render.sh` (headless Chrome, 2x). Shared helpers: `mockups/mac.css`, `mockups/android.css`, `mockups/mock.js` (pitch-trace, waveform and shape generators used by the static pages).
