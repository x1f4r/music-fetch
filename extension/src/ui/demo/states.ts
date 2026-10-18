import type { CaptureStatus } from '../../engine/client';
import { humScript, matchScript, noMatchScript } from '../../engine/mock/scripts';
import type { SessionDetail, SessionEvent, SessionKind } from '../../engine/types';
import type { PageFacts } from '../../page/facts';
import type { ProviderId, StoredKeys } from '../../platform/secrets';
import type { CheckResult } from '../settings/services-check';
import { MIX_LENGTH, MIX_TITLE, MIX_URL, TRACKS, humCandidates, mixSession, playlistSession } from './library';

/**
 * Demo states (like the apps' `-MFDemoState`): every state of extension plan §5 and the
 * design spec, reachable from a URL on the real extension pages:
 *
 *   popup.html?demo=popup-hum&theme=dark   sidepanel.html?demo=panel-mix   app.html?demo=app-library
 *
 * A demo runs on the mock engine with its own library, keeps every setting in memory and
 * never touches the user's data. `&still=1` freezes animation (screenshots). The gallery of
 * all states is `app.html?demo=gallery`.
 */

export type DemoSurface = 'popup' | 'panel' | 'app';

export interface DemoTab {
  url: string;
  title: string;
  audible: boolean;
  facts?: PageFacts | null;
}

/** A session the stage is following when the page opens, replayed from its events. */
export interface DemoLive {
  kind: SessionKind;
  source: 'Microphone' | 'Tab' | null;
  events: SessionEvent[];
  /** Played when the user stops the replayed session (so a demo can go on). */
  onFinish?: SessionEvent[];
}

/** Live state of a running library session (a mix or playlist being analyzed). */
export interface DemoRunning {
  events: SessionEvent[];
  capture?: CaptureStatus;
}

export type DemoAction =
  | { click: string; within?: string }
  | { key: string; on?: string; shift?: boolean; mod?: boolean }
  | { type: string; into: string }
  | { wait: number }
  | { waitFor: string };

export interface DemoState {
  id: string;
  surface: DemoSurface;
  /** One line for the gallery. */
  title: string;
  width: number;
  height: number;
  browser?: 'chromium' | 'firefox';
  /** Firefox for Android (touch, full-screen popup). */
  android?: boolean;
  /** The app tab's route (hash without `#`). */
  route?: string;
  /** Stored settings by key (`sync:…`, `local:…`); first-run states leave the provider out. */
  settings?: Record<string, unknown>;
  tab?: DemoTab | null;
  sessions?: 'all' | 'none' | (() => SessionDetail[]);
  melody?: 'none' | 'installed' | 'update' | 'encoder';
  /** The melody library download fails (Settings › Melody, failed). */
  melodyFails?: boolean;
  /** Paid providers' keys (Settings › Services); kept in memory like the settings. */
  secrets?: StoredKeys;
  /** What "Check" answers per provider (no request leaves a demo). Default: works. */
  checks?: Partial<Record<ProviderId, CheckResult>>;
  /** The browser's storage estimate for Settings › Storage. */
  storage?: { usage: number; quota: number };
  live?: DemoLive;
  running?: Record<string, DemoRunning>;
  /** The mock directive for the next live session (e.g. `busy`). */
  directive?: string;
  /** The melody library download stops at this fraction (a still picture of it). */
  holdDownload?: number;
  /** Sites the engine may not read yet (the optional host permission). */
  needsHost?: string[];
  /** Clicks and keys after the page is up (menus open, rows selected, …). */
  actions?: DemoAction[];
  /** The design prototype to compare with (`ext-design/png/<name>.png`). */
  prototype?: string;
}

// MARK: building blocks

const POPUP = { surface: 'popup', width: 380, height: 560 } as const;
const PANEL = { surface: 'panel', width: 360, height: 800 } as const;
const APP = { surface: 'app', width: 1280, height: 800 } as const;
const MOBILE = { surface: 'popup', width: 412, height: 780, android: true, browser: 'firefox' } as const;
const MOBILE_APP = { ...MOBILE, surface: 'app' } as const;

export const DECIDED = {
  'sync:providerDecision': { provider: 'spotify', skipped: false },
  'local:welcomeStep': 3,
  'local:micGrant': 'granted',
  'local:lastSource': 'Tab',
} as const;

/** Demo keys for Settings › Services: obviously not real, never sent anywhere. */
const DEMO_KEYS: StoredKeys = {
  acrHumming: { host: 'identify-eu-west-1.acrcloud.com', accessKey: 'demo-access-key-4f2a', accessSecret: 'demo-access-secret' },
  audd: { token: 'demo-api-token' },
};

const YOUTUBE_SONG: DemoTab = {
  url: 'https://www.youtube.com/watch?v=demo-song',
  title: 'Dua Lipa - Houdini (Official Music Video) - YouTube',
  audible: true,
  facts: {
    adapter: 'youtube',
    url: 'https://www.youtube.com/watch?v=demo-song',
    title: 'Dua Lipa - Houdini (Official Music Video)',
    ogTitle: null,
    ogType: 'video.other',
    nowPlaying: null,
    media: [{ tag: 'video', duration: 185, currentTime: 41, paused: false, muted: false }],
  },
};

const YOUTUBE_MIX: DemoTab = {
  url: MIX_URL,
  title: `${MIX_TITLE} - YouTube`,
  audible: true,
  facts: {
    adapter: 'youtube',
    url: MIX_URL,
    title: MIX_TITLE,
    ogTitle: MIX_TITLE,
    ogType: 'video.other',
    nowPlaying: null,
    media: [{ tag: 'video', duration: MIX_LENGTH / 1000, currentTime: 312, paused: false, muted: false }],
  },
};

/** The same page, paused: "This tab" isn't preselected, the last used source is. */
const YOUTUBE_PAUSED: DemoTab = { ...YOUTUBE_SONG, audible: false };

const BLANK_TAB: DemoTab = { url: 'chrome://newtab/', title: 'New Tab', audible: false, facts: null };

/** The events of a script up to `untilMs` of capture (frozen mid-session). */
function captureUntil(script: { capture: { wall_ms: number; event: SessionEvent }[] }, untilMs: number): SessionEvent[] {
  return script.capture.filter((l) => l.wall_ms <= untilMs).map((l) => l.event);
}

function whole(script: { capture: { event: SessionEvent }[]; outcome: { event: SessionEvent }[] }): SessionEvent[] {
  return [...script.capture, ...script.outcome].map((l) => l.event);
}

const houdiniMatch = matchScript(TRACKS.houdini);
const finishHoudini: SessionEvent[] = houdiniMatch.outcome.map((l) => l.event);

function hum(untilMs: number): SessionEvent[] {
  return captureUntil(humScript([TRACKS.takeonme], { indexInstalled: true, captureMs: 12_000 }), untilMs);
}

const humFinish: SessionEvent[] = [
  { Phase: { phase: 'MatchingMelody', fraction: null } },
  { Candidates: { candidates: humCandidates(), is_final: true } },
  { Finished: { outcome: { Candidates: { count: 4 } } } },
];

/** A stage that failed to start: the mock rejects the next start, the demo presses Listen. */
function rejected(id: string, title: string, directive: string, source: 'Tab' | 'Microphone' = 'Tab'): DemoState {
  return {
    ...POPUP,
    id,
    title,
    settings: { ...DECIDED, 'local:lastSource': source },
    tab: source === 'Tab' ? YOUTUBE_SONG : YOUTUBE_PAUSED,
    directive,
    actions: [{ click: '.listen .disc' }, { wait: 150 }],
    prototype: 'popup-states',
  };
}

function replay(id: string, title: string, live: DemoLive, extra: Partial<DemoState> = {}): DemoState {
  return { ...POPUP, id, title, settings: DECIDED, tab: YOUTUBE_SONG, live, ...extra };
}

// MARK: the states

export const DEMO_STATES: DemoState[] = [
  // Popup
  { ...POPUP, id: 'popup-first-run', title: 'Popup · first run: where do you listen?', settings: {}, tab: { ...YOUTUBE_SONG, url: 'https://open.spotify.com/track/demo' }, prototype: 'app-welcome' },
  { ...POPUP, id: 'popup-idle', title: 'Popup · idle', settings: DECIDED, tab: YOUTUBE_SONG, prototype: 'popup-idle' },
  replay('popup-listening', 'Popup · listening', { kind: 'SystemAudio', source: 'Tab', events: captureUntil(houdiniMatch, 2600), onFinish: finishHoudini }, { prototype: 'popup-listening' }),
  replay('popup-hum', 'Popup · humming (keep going, ring)', { kind: 'Hum', source: 'Microphone', events: hum(7000), onFinish: humFinish }, { prototype: 'popup-hum', settings: { ...DECIDED, 'local:lastSource': 'Hum' } }),
  replay('popup-matching', 'Popup · matching', { kind: 'SystemAudio', source: 'Tab', events: [...captureUntil(houdiniMatch, 2600), { Phase: { phase: 'Recognizing', fraction: null } }] }, { prototype: 'popup-matching' }),
  replay('popup-result', 'Popup · matched', { kind: 'SystemAudio', source: 'Tab', events: whole(houdiniMatch) }, { prototype: 'popup-result' }),
  replay(
    'popup-result-menu',
    'Popup · provider menu, details, pinned',
    { kind: 'SystemAudio', source: 'Tab', events: whole(houdiniMatch) },
    {
      prototype: 'popup-result-menu',
      settings: { ...DECIDED, 'sync:customProvider': { name: 'Plex', template: 'https://plex.example/search?q={query}' } },
      actions: [{ click: '.details .dhead' }, { click: '.hero .actions .btn.icon[aria-pressed]' }, { click: '.split .chev-part' }],
    },
  ),
  replay('popup-nomatch', 'Popup · no match', { kind: 'SystemAudio', source: 'Tab', events: whole(noMatchScript('NotFound')) }, { prototype: 'popup-nomatch' }),
  replay('popup-candidates', 'Popup · hum candidates', { kind: 'Hum', source: 'Microphone', events: [...hum(11_000), ...humFinish] }, { prototype: 'popup-candidates', settings: { ...DECIDED, 'local:lastSource': 'Hum' } }),
  {
    ...POPUP,
    id: 'popup-melody',
    title: 'Popup · melody library prompt',
    settings: { ...DECIDED, 'local:lastSource': 'Hum' },
    tab: YOUTUBE_PAUSED,
    melody: 'none',
    actions: [{ click: '.listen .disc' }],
    prototype: 'popup-melody',
  },
  {
    ...POPUP,
    id: 'popup-downloading',
    title: 'Popup · melody library downloading',
    settings: { ...DECIDED, 'local:lastSource': 'Hum' },
    tab: YOUTUBE_PAUSED,
    melody: 'none',
    holdDownload: 0.42,
    actions: [{ click: '.listen .disc' }, { click: '.sheet .btn.filled' }, { waitFor: '.dl-bar' }],
    prototype: 'popup-states',
  },
  {
    ...POPUP,
    id: 'popup-analyzing',
    title: 'Popup · a mix analyzing in the background',
    settings: { ...DECIDED, 'local:lastSource': 'Microphone' },
    tab: BLANK_TAB,
    sessions: () => [mixSession({ running: true, pinned: false })],
    running: { 'demo-mix-running': { events: [{ Progress: { analyzed_ms: 23 * 60_000, total_ms: MIX_LENGTH, eta_ms: 17 * 60_000 } }], capture: { state: 'capturing', rate: 4, muted: true } } },
    prototype: 'popup-analyzing',
  },
  rejected('popup-nothing-playing', 'Popup · nothing is playing in this tab', 'nothing'),
  rejected('popup-blocked', 'Popup · this tab blocks capture', 'blocked'),
  rejected('popup-cant-listen', "Popup · can't listen to this page", 'cantlisten'),
  rejected('popup-busy', 'Popup · another capture holds the tab', 'busy'),
  rejected('popup-mic-off', 'Popup · microphone access is off', 'micdenied', 'Microphone'),
  replay(
    'popup-rate-limited',
    'Popup · rate limited (counts down)',
    { kind: 'SystemAudio', source: 'Tab', events: [{ Phase: { phase: 'Recognizing', fraction: null } }, { Notice: { notice: { RateLimited: { retry_in_ms: 42_000 } } } }] },
    { prototype: 'popup-states' },
  ),
  replay(
    'popup-offline',
    'Popup · offline, saved for later',
    { kind: 'SystemAudio', source: 'Tab', events: [{ Phase: { phase: 'Recognizing', fraction: null } }, { Finished: { outcome: 'Waiting' } }] },
    { prototype: 'popup-states' },
  ),
  replay(
    'popup-network',
    'Popup · network failure',
    { kind: 'SystemAudio', source: 'Tab', events: [{ Phase: { phase: 'Recognizing', fraction: null } }, { Failed: { code: 'Network', message: 'offline' } }] },
    { prototype: 'popup-states' },
  ),
  replay('popup-firefox-hum', 'Firefox popup · humming: keep this open', { kind: 'Hum', source: 'Microphone', events: hum(5000), onFinish: humFinish }, { browser: 'firefox', settings: { ...DECIDED, 'local:lastSource': 'Hum' } }),
  {
    ...POPUP,
    id: 'popup-from-page',
    title: 'Popup · answered by the page (Spotify), listen instead',
    settings: DECIDED,
    tab: {
      url: 'https://open.spotify.com/track/demo',
      title: 'Houdini - song and lyrics by Dua Lipa | Spotify',
      audible: true,
      facts: { adapter: 'streaming', url: 'https://open.spotify.com/track/demo', title: 'Houdini', ogTitle: 'Houdini', ogType: 'music.song', nowPlaying: { title: 'Houdini', artist: 'Dua Lipa', album: 'Radical Optimism' }, media: [] },
    },
    actions: [{ click: '.chip' }, { waitFor: '.hero .ttl' }],
  },
  { ...POPUP, id: 'popup-check-video', title: 'Popup · check a short video?', settings: DECIDED, tab: YOUTUBE_SONG, actions: [{ click: '.chip' }, { waitFor: '.sheet' }] },
  {
    ...POPUP,
    id: 'popup-plays-on-page',
    title: 'Popup · a pasted video link plays on its page',
    settings: DECIDED,
    tab: BLANK_TAB,
    actions: [{ type: 'https://www.youtube.com/watch?v=demo-video', into: '.top input' }, { key: 'Enter', on: '.top input' }],
  },
  {
    ...POPUP,
    id: 'popup-allow-host',
    title: 'Popup · allow reading a site',
    settings: DECIDED,
    tab: { url: 'https://radio.example.org/live', title: 'Live radio', audible: true, facts: null },
    needsHost: ['radio.example.org'],
    actions: [{ click: '.chip' }],
  },
  {
    ...POPUP,
    id: 'popup-ask',
    title: 'Popup · analyze the whole video?',
    settings: DECIDED,
    tab: YOUTUBE_MIX,
    actions: [{ click: '.chip' }],
    prototype: 'panel-ask',
  },

  // Side panel and sidebar
  { ...PANEL, id: 'panel-listen', title: 'Side panel · listen and recent', settings: DECIDED, tab: YOUTUBE_SONG, prototype: 'panel-listen' },
  { ...PANEL, id: 'panel-ask', title: 'Side panel · analyze the whole video?', settings: DECIDED, tab: YOUTUBE_MIX, actions: [{ click: '.chip' }], prototype: 'panel-ask' },
  {
    ...PANEL,
    id: 'panel-mix',
    title: 'Side panel · a mix being listened through',
    settings: DECIDED,
    tab: YOUTUBE_MIX,
    sessions: () => [mixSession({ running: true, pinned: true })],
    running: { 'demo-mix-running': { events: [{ Progress: { analyzed_ms: 23 * 60_000, total_ms: MIX_LENGTH, eta_ms: 17 * 60_000 } }], capture: { state: 'capturing', rate: 4, muted: true } } },
    actions: [{ click: '.list .row' }, { wait: 50 }, { click: '.tracks .track:nth-child(6) .track-main' }],
    prototype: 'panel-mix',
  },
  {
    ...PANEL,
    id: 'panel-playlist',
    title: 'Side panel · a playlist resolving',
    settings: DECIDED,
    tab: { url: 'https://open.spotify.com/album/demo', title: "Short n' Sweet - Spotify", audible: false, facts: null },
    sessions: () => [playlistSession({ resolved: 7 })],
    running: { 'demo-playlist-running': { events: [{ Item: { index: 7, count: 12, title: 'Dumb & Poetic' } }] } },
    actions: [{ click: '.list .row' }],
    prototype: 'panel-playlist',
  },
  {
    ...PANEL,
    id: 'panel-mix-done',
    title: 'Side panel · a finished mix',
    settings: DECIDED,
    tab: YOUTUBE_MIX,
    actions: [{ click: '.list .row[data-id="demo-mix"]' }],
  },
  { ...PANEL, id: 'panel-result', title: 'Side panel · matched', settings: DECIDED, tab: YOUTUBE_SONG, live: { kind: 'SystemAudio', source: 'Tab', events: whole(houdiniMatch) } },
  { ...PANEL, id: 'panel-first-run', title: 'Side panel · first run', settings: {}, tab: YOUTUBE_SONG },
  { ...PANEL, id: 'sidebar-narrow', title: 'Firefox sidebar · narrower than 320', width: 280, browser: 'firefox', settings: DECIDED, tab: YOUTUBE_SONG },

  // App tab
  { ...APP, id: 'app-listen', title: 'App tab · listen', route: '/listen', settings: DECIDED },
  { ...APP, id: 'app-library', title: 'App tab · library with details', route: '/library', settings: DECIDED, actions: [{ click: '.list .row[data-id="demo-hum"]' }], prototype: 'app-library' },
  {
    ...APP,
    id: 'app-library-select',
    title: 'App tab · library, two selected',
    route: '/library',
    settings: DECIDED,
    actions: [{ click: '.list .row[data-id="demo-hum"] .cb' }, { click: '.list .row[data-id="demo-blinding"] .cb' }],
    prototype: 'app-library',
  },
  {
    ...APP,
    id: 'app-library-undo',
    title: 'App tab · deleted, with Undo',
    route: '/library',
    settings: DECIDED,
    actions: [{ click: '.list .row[data-id="demo-espresso"] .cb' }, { click: '.ctx .delete' }, { wait: 60 }],
    prototype: 'app-library',
  },
  {
    ...APP,
    id: 'app-delete-confirm',
    title: 'App tab · delete a pinned mix?',
    route: '/library',
    settings: DECIDED,
    actions: [{ click: '.list .row[data-id="demo-mix"] .cb' }, { click: '.ctx .delete' }],
  },
  { ...APP, id: 'app-library-empty', title: 'App tab · empty library', route: '/library', settings: DECIDED, sessions: 'none' },
  { ...APP, id: 'app-session-mix', title: 'App tab · mix with segment details', route: '/session/demo-mix', settings: DECIDED, actions: [{ click: '.tracks .track[data-id="mix-14"] .track-main' }], prototype: 'app-session-mix' },
  {
    ...APP,
    id: 'app-session-mix-running',
    title: 'App tab · mix being listened through',
    route: '/session/demo-mix-running',
    settings: DECIDED,
    sessions: () => [mixSession({ running: true, pinned: true })],
    running: { 'demo-mix-running': { events: [{ Progress: { analyzed_ms: 23 * 60_000, total_ms: MIX_LENGTH, eta_ms: 17 * 60_000 } }], capture: { state: 'ad', rate: 4, muted: true } } },
  },
  { ...APP, id: 'app-export', title: 'App tab · export menu', route: '/session/demo-mix', settings: DECIDED, actions: [{ click: '.toolbar .export' }] },
  { ...APP, id: 'app-session-playlist', title: 'App tab · playlist with track details', route: '/session/demo-playlist', settings: DECIDED, actions: [{ click: '.tracks .track[data-id="item-7"] .track-main' }], prototype: 'app-session-playlist' },
  { ...APP, id: 'app-session-song', title: 'App tab · a song', route: '/session/demo-houdini', settings: DECIDED },
  { ...APP, id: 'app-session-hum', title: 'App tab · hum candidates and the overlay', route: '/session/demo-hum', settings: DECIDED },
  { ...APP, id: 'app-settings-general', title: 'Settings · general', route: '/settings/general', settings: { ...DECIDED, 'sync:openInApp': true, 'sync:customProvider': { name: 'Plex', template: 'https://plex.example/search?q={query}' } }, prototype: 'app-settings-general' },
  { ...APP, id: 'app-settings-listening', title: 'Settings · listening', route: '/settings/listening', settings: DECIDED, prototype: 'app-settings-listening' },
  { ...APP, id: 'app-settings-melody', title: 'Settings · melody, installed', route: '/settings/melody', settings: DECIDED, prototype: 'app-settings-melody' },
  { ...APP, id: 'app-settings-melody-none', title: 'Settings · melody, not installed', route: '/settings/melody', settings: DECIDED, melody: 'none', prototype: 'app-settings-melody' },
  {
    ...APP,
    id: 'app-settings-melody-downloading',
    title: 'Settings · melody, downloading',
    route: '/settings/melody',
    settings: DECIDED,
    melody: 'none',
    holdDownload: 0.42,
    actions: [{ click: '.melody-row .btn.filled' }, { waitFor: '.melody-bar' }],
    prototype: 'app-settings-melody',
  },
  { ...APP, id: 'app-settings-melody-update', title: 'Settings · melody, update available', route: '/settings/melody', settings: DECIDED, melody: 'update', prototype: 'app-settings-melody' },
  { ...APP, id: 'app-settings-storage', title: 'Settings · storage', route: '/settings/storage', settings: DECIDED },
  { ...APP, id: 'app-settings-privacy', title: 'Settings · privacy', route: '/settings/privacy', settings: DECIDED },
  { ...APP, id: 'app-settings-shortcuts', title: 'Settings · shortcuts', route: '/settings/shortcuts', settings: DECIDED },
  { ...APP, id: 'app-settings-about', title: 'Settings · about', route: '/settings/about', settings: DECIDED },
  { ...APP, id: 'app-settings-about-license', title: 'Settings · about, a license open', route: '/settings/about', settings: DECIDED, actions: [{ waitFor: '.licenses .license .btn' }, { click: '.licenses .license .btn' }] },
  { ...APP, id: 'app-settings-melody-encoder', title: 'Settings · melody, installed with the hum encoder', route: '/settings/melody', settings: DECIDED, melody: 'encoder' },
  {
    ...APP,
    id: 'app-settings-melody-failed',
    title: 'Settings · melody, the download failed',
    route: '/settings/melody',
    settings: DECIDED,
    melody: 'none',
    melodyFails: true,
    actions: [{ click: '.melody-row .btn.filled' }, { waitFor: '.melody-row .problem' }],
    prototype: 'app-settings-melody',
  },
  {
    ...APP,
    id: 'app-settings-melody-current',
    title: 'Settings · melody, checked: up to date',
    route: '/settings/melody',
    settings: DECIDED,
    actions: [{ waitFor: '.melody-row .btn:not(.text)' }, { click: '.melody-row .btn:not(.text)' }, { wait: 120 }],
  },
  { ...APP, id: 'app-settings-storage-low', title: 'Settings · storage, little room left', route: '/settings/storage', settings: DECIDED, storage: { usage: 1_930_000_000, quota: 2_000_000_000 } },
  { ...APP, id: 'app-settings-listening-low-power', title: 'Settings · listening, low power on', route: '/settings/listening', settings: { ...DECIDED, 'sync:lowPower': 'on' } },
  { ...APP, id: 'app-settings-services', title: 'Settings · services, nothing set up', route: '/settings/services', settings: DECIDED },
  {
    ...APP,
    id: 'app-settings-services-typing',
    title: 'Settings · services, entering ACRCloud humming keys',
    route: '/settings/services',
    settings: DECIDED,
    actions: [
      { click: '#svc-acrHumming-title ~ .group .add-keys' },
      { type: 'identify-eu-west-1.acrcloud.com', into: '#svc-acrHumming-host' },
      { type: 'demo-access-key-4f2a', into: '#svc-acrHumming-key' },
      { type: 'demo-access-secret', into: '#svc-acrHumming-secret' },
    ],
  },
  {
    ...APP,
    id: 'app-settings-services-set',
    title: 'Settings · services: humming works, the AudD token was rejected',
    route: '/settings/services',
    settings: { ...DECIDED, 'local:providerChecks': { acrHumming: { state: 'ok', at: 0 }, audd: { state: 'rejected', code: 900, at: 0 } } },
    secrets: DEMO_KEYS,
  },
  {
    ...APP,
    id: 'app-settings-services-saved',
    title: 'Settings · services, saved and checked',
    route: '/settings/services',
    settings: DECIDED,
    actions: [
      { click: '#svc-acrHumming-title ~ .group .add-keys' },
      { type: 'identify-eu-west-1.acrcloud.com', into: '#svc-acrHumming-host' },
      { type: 'demo-access-key-4f2a', into: '#svc-acrHumming-key' },
      { type: 'demo-access-secret', into: '#svc-acrHumming-secret' },
      { click: '.svc form .btn.filled' },
      { waitFor: '.svc-status.ok' },
    ],
  },
  {
    ...APP,
    id: 'app-settings-services-permission',
    title: 'Settings · services, the browser refused the host',
    route: '/settings/services',
    settings: { ...DECIDED, 'local:providerChecks': { acrHumming: { state: 'permission', at: 0 } } },
    secrets: { acrHumming: DEMO_KEYS.acrHumming },
  },
  { ...APP, id: 'app-settings-privacy-services', title: 'Settings · privacy, with paid services', route: '/settings/privacy', settings: DECIDED, secrets: DEMO_KEYS },
  {
    ...APP,
    id: 'app-narrow-settings-services',
    title: 'App tab, narrow · settings: entering keys',
    width: 390,
    height: 780,
    route: '/settings/services',
    settings: DECIDED,
    actions: [{ click: '#svc-acrHumming-title ~ .group .add-keys' }, { type: 'identify-eu-west-1.acrcloud.com', into: '#svc-acrHumming-host' }],
  },
  { ...APP, id: 'app-settings-privacy-firefox', title: 'Firefox settings · privacy and data consent', route: '/settings/privacy', settings: DECIDED, browser: 'firefox' },
  { ...APP, id: 'app-welcome', title: 'Welcome · where do you listen?', route: '/welcome', settings: {}, sessions: 'none', prototype: 'app-welcome' },
  { ...APP, id: 'app-welcome-custom', title: 'Welcome · more services and a custom link', route: '/welcome', settings: {}, sessions: 'none', actions: [{ click: '.opt.more' }, { click: '.opt.custom' }] },
  { ...APP, id: 'app-welcome-shortcut', title: 'Welcome · the shortcut', route: '/welcome', settings: { ...DECIDED, 'local:welcomeStep': 2 }, sessions: 'none' },
  { ...APP, id: 'app-grant-mic', title: 'Grant tab · allow the microphone', route: '/grant-mic', settings: { ...DECIDED, 'local:micGrant': 'unknown' }, prototype: 'app-grant-mic' },
  { ...APP, id: 'app-grant-granted', title: 'Grant tab · allowed', route: '/grant-mic?state=granted', settings: DECIDED, prototype: 'app-grant-mic-states' },
  { ...APP, id: 'app-grant-denied', title: 'Grant tab · denied', route: '/grant-mic?state=denied', settings: DECIDED, prototype: 'app-grant-mic-states' },
  { ...APP, id: 'app-grant-session', title: 'Grant tab · allowed this time', route: '/grant-mic?state=session', settings: DECIDED, prototype: 'app-grant-mic-states' },
  { ...APP, id: 'app-narrow-library', title: 'App tab, narrow · library', width: 390, height: 780, route: '/library', settings: DECIDED, prototype: 'app-narrow-library' },
  { ...APP, id: 'app-narrow-session', title: 'App tab, narrow · a song', width: 390, height: 780, route: '/session/demo-houdini', settings: DECIDED },

  // Firefox for Android: extension pages never get the microphone there (S2), so
  // Microphone and Hum open the Music Fetch app; This tab and links work in the extension.
  { ...MOBILE, id: 'mobile-idle', title: 'Firefox for Android · idle', settings: DECIDED, tab: YOUTUBE_SONG, prototype: 'mobile-popup-idle' },
  { ...MOBILE, id: 'mobile-hum', title: 'Firefox for Android · Hum opens the app', settings: { ...DECIDED, 'local:lastSource': 'Hum' }, tab: YOUTUBE_PAUSED },
  { ...MOBILE, id: 'mobile-mic', title: 'Firefox for Android · Microphone opens the app', settings: { ...DECIDED, 'local:lastSource': 'Microphone' }, tab: YOUTUBE_PAUSED },
  replay('mobile-listening', 'Firefox for Android · listening to this tab', { kind: 'SystemAudio', source: 'Tab', events: captureUntil(houdiniMatch, 2600), onFinish: finishHoudini }, { ...MOBILE }),
  replay(
    'mobile-result',
    'Firefox for Android · matched',
    { kind: 'SystemAudio', source: 'Tab', events: whole(matchScript(TRACKS.blinding)) },
    { ...MOBILE, prototype: 'mobile-popup-result' },
  ),
  { ...MOBILE_APP, id: 'mobile-welcome', title: 'Firefox for Android · welcome: where do you listen?', route: '/welcome', settings: {}, sessions: 'none' },
  { ...MOBILE_APP, id: 'mobile-welcome-find', title: 'Firefox for Android · welcome: where to find it', route: '/welcome', settings: { ...DECIDED, 'local:welcomeStep': 2 }, sessions: 'none' },
  { ...MOBILE_APP, id: 'mobile-settings-listening', title: 'Firefox for Android · settings: listening', route: '/settings/listening', settings: DECIDED },
  {
    ...MOBILE_APP,
    id: 'mobile-settings-services',
    title: 'Firefox for Android · settings: services',
    route: '/settings/services',
    settings: { ...DECIDED, 'local:providerChecks': { acrHumming: { state: 'ok', at: 0 } } },
    secrets: { acrHumming: DEMO_KEYS.acrHumming },
  },
];

export function demoState(id: string): DemoState | undefined {
  return DEMO_STATES.find((s) => s.id === id);
}

/** The page a demo state opens on. */
export function demoPage(state: DemoState): string {
  return state.surface === 'popup' ? 'popup.html' : state.surface === 'panel' ? 'sidepanel.html' : 'app.html';
}

export function demoUrl(state: DemoState, options: { theme?: 'light' | 'dark'; still?: boolean } = {}): string {
  const params = new URLSearchParams({ demo: state.id });
  if (options.theme) params.set('theme', options.theme);
  if (options.still) params.set('still', '1');
  return `${demoPage(state)}?${params.toString()}${state.route ? `#${state.route}` : ''}`;
}
