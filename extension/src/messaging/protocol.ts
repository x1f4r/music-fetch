import type { ActiveSession, CaptureStatus } from '../engine/client';
import type { SessionEvent, SessionKind } from '../engine/types';

/** Name of the Port UI pages open to the engine host. */
export const ENGINE_PORT = 'mf-engine';

/**
 * The capture agent in a tab (isolated-world content script) opens this Port to the
 * engine host: player control, page facts, element PCM and media-clock samples.
 */
export const CAPTURE_PORT = 'mf-capture';

/**
 * Firefox: the background can never use the microphone, so the UI page that started a
 * microphone session captures it and streams the PCM to the host over this Port.
 */
export const MIC_PORT = 'mf-mic';

/** Extension pages only: the host's own state and controls (diagnostics, tests, settings). */
export const HOST_PORT = 'mf-host';

/** Events the engine host broadcasts to every connected UI port. */
export interface EngineTopics {
  session: { sessionId: string; event: SessionEvent };
  'library:changed': null;
  /** A page capture's state next to the session's events (listen-through, ads, pauses). */
  capture: { sessionId: string; status: CaptureStatus };
  /**
   * Firefox only, to the one page that called `startListening('Microphone', …)`: capture
   * the microphone here and stream it to the host on a `MIC_PORT` that sends `token`.
   */
  'capture:mic': MicCaptureRequest;
}

export interface MicCaptureRequest {
  token: string;
  /** Preferred input devices, most preferred first (Settings → Listening). */
  deviceIds: string[];
}

/**
 * Why the host could not start or keep a capture, finer than the `CaptureErrorCode` the
 * `EngineError` carries as its code (`client.ts`): engine calls reject with both, the
 * code for the line of extension plan §5.3/§5.5 and this `reason` for what to offer
 * (e.g. `NeedsInvocation`: "Click the toolbar button on this tab").
 */
export type CaptureReason =
  /** chrome://, about:, the Web Store or an extension page: `state.tab.cantListen`. */
  | 'CantCapture'
  /** Another capture already holds the tab: `state.tab.busy`. */
  | 'TabBusy'
  /** Chromium: tab capture needs the toolbar button, a shortcut or the menu on this tab. */
  | 'NeedsInvocation'
  /** DRM/EME media or a player that refuses capture: `state.tab.blocked`. */
  | 'CaptureBlocked'
  /** No playing audio or video element (element capture): `state.tab.nothingPlaying`. */
  | 'NothingPlaying'
  /** The microphone was never allowed: open `#/grant-mic`. */
  | 'MicNotGranted'
  /** No microphone here (none connected, or Firefox for Android): offer the app. */
  | 'MicUnavailable'
  /** The tab was closed or navigated away while capturing. */
  | 'TabGone';

/**
 * One-shot messages to the background (`runtime.sendMessage`). Each type maps to its
 * response. Only the background answers them.
 */
export interface ControlMessages {
  /** Make sure the engine host runs (creates the offscreen document on Chromium). */
  'mf:host.ensure': { request: Record<never, never>; response: { host: EngineHostKind } };
  /** Describe a tab; without `tabId`, the active tab of the last focused window. */
  'mf:tab.resolve': { request: { tabId?: number }; response: TabInfo };
  /**
   * Chromium: a `tabCapture` stream id for the engine host. The service worker calls
   * `getMediaStreamId` (it needs the extension to have been invoked on the tab); the
   * offscreen document must consume the id within about 3 s.
   */
  'mf:tab.stream': { request: { tabId?: number }; response: { streamId: string; tab: TabInfo } };
  /** Inject the capture agent (isolated world) and the page script (MAIN world) into a tab. */
  'mf:tab.inject': { request: { tabId: number }; response: { injected: boolean } };
  /** The host's sessions changed: badge, and a notification when one ends unattended. */
  'mf:host.activity': { request: HostActivity; response: null };
  /** Chromium: the host is idle (no sessions, no pages, the Worker recycled): close the offscreen document. */
  'mf:host.idle': { request: Record<never, never>; response: { closed: boolean } };
  /** Install the header rules for the engine's requests (idempotent; also done on startup). */
  'mf:net.ensure': { request: Record<never, never>; response: { rules: number } };
  /** Chromium: the settings the host needs (offscreen documents have no `storage` API). */
  'mf:host.settings': { request: Record<never, never>; response: HostSettings };
}

/** Settings the engine host reads (extension plan §3.5). */
export interface HostSettings {
  /** Microphone device ids, most preferred first. */
  micPriority: string[];
  lowPower: 'auto' | 'on' | 'off';
}

export type EngineHostKind = 'offscreen' | 'background';

/** A tab as the capture side sees it. */
export interface TabInfo {
  id: number;
  windowId: number;
  /** Known only while the extension may read the tab (activeTab, host permission). */
  url: string | null;
  title: string | null;
  audible: boolean;
  /** Whether "This tab" can work here; a `CaptureReason` when it can't. */
  capture: 'ok' | Extract<CaptureReason, 'CantCapture' | 'TabBusy'>;
}

export interface HostActivity {
  /** Sessions still running, newest first. */
  active: ActiveSession[];
  /** A session that just ended, with what the notification needs. */
  ended: EndedSession | null;
  /** UI pages connected to the host (popup, side panel, sidebar, app tabs). */
  surfaces: number;
  /**
   * The session events the background's badge and notifications follow, since the last
   * report: `Started`, `Phase`, `Progress` (at most about once a second) and the terminal one.
   */
  events?: { sessionId: string; event: SessionEvent }[];
}

export interface EndedSession {
  id: string;
  kind: SessionKind;
  event: SessionEvent;
}

export type ControlType = keyof ControlMessages;
export type ControlRequest<K extends ControlType> = { type: K } & ControlMessages[K]['request'];
export type ControlResponse<K extends ControlType> = ControlMessages[K]['response'];
