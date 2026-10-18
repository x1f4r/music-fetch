import type { Coverage } from '../capture/types';
import type { Kv } from './host-idb';
import type { CaptureSource, SessionKind } from './types';

/**
 * Sessions the host is running, kept outside the host's memory (IndexedDB) so that a
 * host that went away mid-session (Firefox suspended the event page, Chromium closed the
 * offscreen document, the browser restarted) leaves a record: which sessions were
 * running, what they captured from where, and how far an analysis got. The engine marks
 * such sessions failed in its own store; the coverage tells a resumed analysis where to
 * pick up (extension plan §5.4 "The tab was closed · Resume").
 */
export interface RegistryEntry {
  id: string;
  kind: SessionKind;
  source: CaptureSource | null;
  startedAt: number;
  capture: { via: 'tab' | 'element' | 'mic' | 'none'; tabId: number | null; url: string | null };
  /** Media ranges captured so far (analyses). */
  coverage: Coverage;
  updatedAt: number;
}

export class SessionRegistry {
  private readonly entries = new Map<string, RegistryEntry>();
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(
    private readonly kv: Promise<Kv | null>,
    /** Coverage updates are written at most this often. */
    private readonly throttleMs = 2000,
  ) {}

  /** What a previous host left running, removed from the registry as it is read. */
  async takeInterrupted(): Promise<RegistryEntry[]> {
    const kv = await this.kv;
    if (!kv) return [];
    const stale = (await kv.entries<RegistryEntry>()).map(([, e]) => e).filter((e) => !this.entries.has(e.id));
    await Promise.all(stale.map((e) => kv.delete(e.id)));
    return stale.sort((a, b) => b.startedAt - a.startedAt);
  }

  list(): RegistryEntry[] {
    return [...this.entries.values()].sort((a, b) => b.startedAt - a.startedAt);
  }

  get(id: string): RegistryEntry | undefined {
    return this.entries.get(id);
  }

  async put(entry: Omit<RegistryEntry, 'updatedAt' | 'coverage'> & { coverage?: Coverage }): Promise<void> {
    const full: RegistryEntry = { coverage: [], ...entry, updatedAt: Date.now() };
    this.entries.set(full.id, full);
    await (await this.kv)?.put(full.id, full).catch(() => undefined);
  }

  /** Update an entry; the write is throttled (coverage changes many times a second). */
  update(id: string, patch: Partial<Pick<RegistryEntry, 'coverage' | 'capture'>>): void {
    const entry = this.entries.get(id);
    if (!entry) return;
    Object.assign(entry, patch, { updatedAt: Date.now() });
    if (this.timers.has(id)) return;
    this.timers.set(
      id,
      setTimeout(() => {
        this.timers.delete(id);
        const current = this.entries.get(id);
        if (current) void this.kv.then((kv) => kv?.put(id, current)).catch(() => undefined);
      }, this.throttleMs),
    );
  }

  async remove(id: string): Promise<void> {
    this.entries.delete(id);
    const timer = this.timers.get(id);
    if (timer) clearTimeout(timer);
    this.timers.delete(id);
    await (await this.kv)?.delete(id).catch(() => undefined);
  }
}
