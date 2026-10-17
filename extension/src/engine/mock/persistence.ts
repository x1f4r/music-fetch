import type { MelodyIndexStatus, SessionDetail } from '../types';

/** Everything the mock engine keeps between page loads. */
export interface MockSnapshot {
  version: 1;
  /** Newest first. */
  sessions: SessionDetail[];
  melody: MelodyIndexStatus;
  lowPower: boolean;
}

export interface MockPersistence {
  load(): MockSnapshot | null;
  save(snapshot: MockSnapshot): void;
}

export function memoryPersistence(initial: MockSnapshot | null = null): MockPersistence & { current: MockSnapshot | null } {
  return {
    current: initial,
    load() {
      return this.current ? structuredClone(this.current) : null;
    },
    save(snapshot) {
      this.current = structuredClone(snapshot);
    },
  };
}

/**
 * `localStorage` of the extension origin. The engine host runs in the offscreen document
 * (Chromium), where `chrome.storage` is not available, or in the Firefox event page; both
 * have the origin's `localStorage`. The real engine uses SQLite on OPFS instead.
 */
export function localStoragePersistence(key = 'mf.mock.engine'): MockPersistence {
  return {
    load() {
      try {
        const raw = globalThis.localStorage?.getItem(key);
        const parsed = raw ? (JSON.parse(raw) as MockSnapshot) : null;
        return parsed?.version === 1 ? parsed : null;
      } catch {
        return null;
      }
    },
    save(snapshot) {
      try {
        globalThis.localStorage?.setItem(key, JSON.stringify(snapshot));
      } catch (error) {
        console.warn('mock engine: could not save the library', error);
      }
    },
  };
}
