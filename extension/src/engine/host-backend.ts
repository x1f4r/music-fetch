import type { AudioSourceKind, PcmChunk } from '../audio/types';
import type { CaptureEvent, CaptureHost } from '../capture/types';
import { ENGINE_METHODS, type EngineClient } from './client';
import type { CaptureSource, ListenMode, ListenOptions } from './types';

/**
 * The engine as the engine host drives it, inside the engine Worker (extension plan
 * §2.8, §3.1). It is the UI's `EngineClient` plus what only the host does: live sessions
 * get their audio pushed in, and analyses of media pages call back into the host's
 * `CaptureHost`.
 *
 * `host-backend-factory.ts` picks the implementation. Today that is the mock
 * (`host-mock-backend.ts`); the wasm engine replaces it there and nowhere else.
 */
export interface EngineBackend extends EngineClient {
  /** `startListening` with the input the host opened for it. */
  startLive(source: CaptureSource, mode: ListenMode, options: ListenOptions, input: LiveInput): Promise<string>;
  /** Audio of a live session or of an analysis's capture. Fire and forget. */
  pushPcm(sessionId: string, chunk: PcmChunk): void;
  /**
   * The input of a live session ended without the user stopping it (the tab closed,
   * the device went away, the page holding the microphone closed): identify what was
   * heard, as `finishSession` does.
   */
  captureEnded(sessionId: string, reason: string): Promise<void>;
  /** Something happened to an analysis's capture (ad, user pause, navigation, …). */
  captureEvent(sessionId: string, event: CaptureEvent): Promise<void>;
  /** Close cleanly before the Worker is terminated: finish writes, close the database. */
  shutdown(): Promise<void>;
}

export interface LiveInput {
  kind: AudioSourceKind | 'none';
  sampleRate: number;
  label: string;
  /**
   * Mock only: the directive from `localStorage['mf.mock.next']` (an extension page's
   * console). A directive plays the scripted session without audio.
   */
  directive: string | null;
}

/** Every `EngineBackend` method the Worker serves to the host. */
export const BACKEND_METHODS = [...ENGINE_METHODS, 'startLive', 'captureEnded', 'captureEvent', 'shutdown'] as const satisfies readonly (keyof EngineBackend)[];

/** What the Worker knows about its surroundings (structured-cloneable, sent at spawn). */
export interface BackendConfig {
  target: 'chromium' | 'firefox';
  android: boolean;
  lowPower: boolean;
  /** `hardwareConcurrency` of the host context. */
  cpus: number;
}

export interface BackendEnv extends BackendConfig {
  /** The host's capture side, for analyses of media pages. */
  capture: CaptureHost;
}

export type BackendFactory = (env: BackendEnv) => Promise<EngineBackend>;
