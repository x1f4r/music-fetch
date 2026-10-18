import type { CaptureStatus } from '../engine/client';
import type { SessionEvent } from '../engine/types';

/** Name of the Port UI pages open to the engine host. */
export const ENGINE_PORT = 'mf-engine';

/** Events the engine host broadcasts to every connected UI port. */
export interface EngineTopics {
  session: { sessionId: string; event: SessionEvent };
  'library:changed': null;
  /** A page capture's state next to the session's events (listen-through, ads, pauses). */
  capture: { sessionId: string; status: CaptureStatus };
}

/**
 * One-shot messages to the background (`runtime.sendMessage`). Each type maps to its
 * response. Only the background answers them.
 */
export interface ControlMessages {
  /** Make sure the engine host runs (creates the offscreen document on Chromium). */
  'mf:host.ensure': { request: Record<never, never>; response: { host: EngineHostKind } };
}

export type EngineHostKind = 'offscreen' | 'background';

export type ControlType = keyof ControlMessages;
export type ControlRequest<K extends ControlType> = { type: K } & ControlMessages[K]['request'];
export type ControlResponse<K extends ControlType> = ControlMessages[K]['response'];
