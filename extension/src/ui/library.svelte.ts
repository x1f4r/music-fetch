import type { EngineClient } from '../engine/client';
import { libraryQuery, type LibraryEntry } from '../engine/types';
import type { Message } from '../i18n';
import {
  copyText,
  deleteNeedsConfirm,
  displayOrder,
  librarySections,
  matchesFilter,
  pinsSelection,
  rangeSelection,
  toggleSelection,
  type LibraryFilter,
  type LibrarySection,
} from '../models/library';

export interface Toast {
  message: Message;
  /** Undo restores what the action changed. */
  undo: (() => Promise<void>) | null;
}

export const TOAST_MS = 5000;

/**
 * The library list (app tab, side panel, narrow app): entries, filter and search, the
 * multi-selection with its batch actions and Undo (DESIGN §6.8, extension plan §5.7).
 */
export class LibraryController {
  entries: LibraryEntry[] = $state([]);
  loaded = $state(false);
  filter: LibraryFilter = $state('all');
  search = $state('');
  selection: Set<string> = $state(new Set());
  /** The row the details column shows: the last one clicked. */
  focusedId: string | null = $state(null);
  /** Candidate counts for hum and lyric rows ("a-ha · 4 candidates"). */
  candidateCounts: Record<string, number> = $state({});
  toast: Toast | null = $state(null);
  /** Deleting these needs a confirmation (pinned sessions or mixes). */
  confirmDelete: LibraryEntry[] | null = $state(null);

  private anchor: string | null = null;
  private toastTimer: ReturnType<typeof setTimeout> | null = null;
  private stop: (() => void) | null = null;
  /** Deleted rows are hidden at once and removed from the engine when the toast ends. */
  private pendingDelete: { ids: string[]; timer: ReturnType<typeof setTimeout> } | null = $state.raw(null);

  constructor(
    private readonly engine: EngineClient,
    private readonly options: { limit?: number; now?: () => number; pinnedSection?: boolean } = {},
  ) {}

  get visible(): LibraryEntry[] {
    const hidden = new Set(this.pendingDelete?.ids ?? []);
    return this.entries.filter((e) => !hidden.has(e.id) && matchesFilter(e, this.filter));
  }

  get sections(): LibrarySection[] {
    return librarySections(this.visible, this.options.now?.() ?? Date.now(), { pinnedSection: (this.options.pinnedSection ?? true) && this.filter !== 'pinned' });
  }

  get order(): LibraryEntry[] {
    return displayOrder(this.sections);
  }

  get selected(): LibraryEntry[] {
    return this.order.filter((e) => this.selection.has(e.id));
  }

  get selecting(): boolean {
    return this.selection.size > 0;
  }

  get focused(): LibraryEntry | null {
    return this.entries.find((e) => e.id === this.focusedId) ?? null;
  }

  get pinLabel(): 'pin' | 'unpin' {
    return pinsSelection(this.selected) ? 'pin' : 'unpin';
  }

  async start(): Promise<void> {
    this.stop = this.engine.subscribe({ libraryChanged: () => void this.load() });
    await this.load();
  }

  dispose(): void {
    this.stop?.();
    if (this.pendingDelete) {
      clearTimeout(this.pendingDelete.timer);
      void this.engine.deleteSessions(this.pendingDelete.ids);
    }
    if (this.toastTimer) clearTimeout(this.toastTimer);
  }

  async load(): Promise<void> {
    const query = libraryQuery({ search: this.search.trim() || null, limit: this.options.limit ?? 500 });
    this.entries = await this.engine.library(query).catch(() => this.entries);
    this.loaded = true;
    const ids = new Set(this.entries.map((e) => e.id));
    if ([...this.selection].some((id) => !ids.has(id))) this.selection = new Set([...this.selection].filter((id) => ids.has(id)));
    void this.loadCandidateCounts();
  }

  private async loadCandidateCounts() {
    for (const entry of this.entries) {
      if ((entry.kind !== 'Hum' && entry.kind !== 'Lyrics') || entry.track || entry.status !== 'Candidates' || entry.id in this.candidateCounts) continue;
      const detail = await this.engine.session(entry.id).catch(() => null);
      if (detail) this.candidateCounts = { ...this.candidateCounts, [entry.id]: detail.candidates.length };
    }
  }

  setFilter(filter: LibraryFilter): void {
    this.filter = filter;
  }

  async setSearch(text: string): Promise<void> {
    this.search = text;
    await this.load();
  }

  // MARK: selection

  /**
   * A click on a row: plain clicks open it (and focus it for the details column);
   * ⌘/Ctrl toggles it into the selection, Shift selects the range from the last one.
   */
  click(entry: LibraryEntry, modifiers: { shift?: boolean; mod?: boolean } = {}): 'open' | 'select' {
    if (modifiers.shift) {
      this.selection = rangeSelection(this.selection, this.order.map((e) => e.id), this.anchor ?? this.focusedId, entry.id);
    } else if (modifiers.mod || this.selecting) {
      this.selection = toggleSelection(this.selection, entry.id);
      this.anchor = entry.id;
    } else {
      this.focusedId = entry.id;
      this.anchor = entry.id;
      return 'open';
    }
    this.focusedId = entry.id;
    return 'select';
  }

  /** The row's checkbox (and long-press on touch). */
  toggle(entry: LibraryEntry): void {
    this.selection = toggleSelection(this.selection, entry.id);
    this.anchor = entry.id;
    this.focusedId = entry.id;
  }

  selectAll(): void {
    this.selection = new Set(this.order.map((e) => e.id));
  }

  clearSelection(): void {
    this.selection = new Set();
    this.anchor = null;
  }

  // MARK: batch actions

  private showToast(message: Message, undo: Toast['undo']) {
    if (this.toastTimer) clearTimeout(this.toastTimer);
    this.toast = { message, undo };
    this.toastTimer = setTimeout(() => (this.toast = null), TOAST_MS);
  }

  dismissToast(): void {
    if (this.toastTimer) clearTimeout(this.toastTimer);
    this.toast = null;
  }

  async undo(): Promise<void> {
    const undo = this.toast?.undo;
    this.dismissToast();
    await undo?.();
  }

  /** Pin all unless all are pinned, then unpin; with Undo. */
  async togglePinned(entries: LibraryEntry[] = this.selected): Promise<void> {
    if (entries.length === 0) return;
    const pin = pinsSelection(entries);
    const before = entries.map((e) => [e.id, e.pinned] as const);
    const ids = entries.map((e) => e.id);
    this.entries = this.entries.map((e) => (ids.includes(e.id) ? { ...e, pinned: pin } : e));
    await this.engine.setPinnedMany(ids, pin);
    this.showToast({ key: pin ? 'session.pinnedToast' : 'session.unpinnedToast', args: [ids.length] }, async () => {
      for (const [id, pinned] of before) await this.engine.setPinned(id, pinned);
    });
  }

  /** "Artist – Title" lines of the selected sessions. */
  copyText(entries: LibraryEntry[] = this.selected): string {
    return copyText(entries);
  }

  /** Delete: Undo for 5 s, or a confirmation first when a pinned session or a mix is among them. */
  async requestDelete(entries: LibraryEntry[] = this.selected): Promise<void> {
    if (entries.length === 0) return;
    if (deleteNeedsConfirm(entries)) {
      this.confirmDelete = entries;
      return;
    }
    this.deleteWithUndo(entries.map((e) => e.id));
  }

  async confirmDeletion(): Promise<void> {
    const entries = this.confirmDelete ?? [];
    this.confirmDelete = null;
    const ids = entries.map((e) => e.id);
    this.clearSelection();
    if (this.focusedId && ids.includes(this.focusedId)) this.focusedId = null;
    await this.engine.deleteSessions(ids);
  }

  private deleteWithUndo(ids: string[]) {
    if (this.pendingDelete) {
      clearTimeout(this.pendingDelete.timer);
      void this.engine.deleteSessions(this.pendingDelete.ids);
    }
    const timer = setTimeout(() => {
      if (this.pendingDelete?.timer !== timer) return;
      this.pendingDelete = null;
      void this.engine.deleteSessions(ids);
    }, TOAST_MS);
    this.pendingDelete = { ids, timer };
    this.selection = new Set([...this.selection].filter((id) => !ids.includes(id)));
    if (this.focusedId && ids.includes(this.focusedId)) this.focusedId = null;
    this.showToast({ key: 'session.deleted', args: [ids.length] }, async () => {
      if (this.pendingDelete?.timer === timer) {
        clearTimeout(timer);
        this.pendingDelete = null;
        this.entries = [...this.entries];
      }
    });
  }
}
