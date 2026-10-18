<script lang="ts">
  import type { LibraryEntry } from '../../engine/types';
  import { t, tm } from '../../i18n';
  import type { LibraryController } from '../library.svelte';
  import SessionRow from './SessionRow.svelte';

  /**
   * The library as a listbox (design spec §7): one tab stop, ↑ ↓ move, ↩ opens, Space and
   * ⌘/Ctrl-click select, Shift extends, ⌘/Ctrl+A selects all, Delete deletes with Undo.
   * Day headings are announced through each row's description.
   */
  interface Props {
    library: LibraryController;
    /** Checkboxes and multi-select (app tab). */
    selectable?: boolean;
    /** Trailing chevrons (side panel: rows push the session). */
    chevrons?: boolean;
    /** Show the Pinned section (off in the side panel's recent list). */
    art?: 40 | 44;
    label?: string;
    onopen: (entry: LibraryEntry) => void;
    oncopy?: (text: string) => void;
  }

  let { library, selectable = false, chevrons = false, art = 40, label = t('library.title'), onopen, oncopy }: Props = $props();
  const sections = $derived(library.sections);
  const order = $derived(library.order);
  const cursor = $derived(library.focusedId && order.some((e) => e.id === library.focusedId) ? library.focusedId : (order[0]?.id ?? null));
  const now = Date.now();
  let list = $state<HTMLElement | undefined>();

  function click(entry: LibraryEntry, event: MouseEvent) {
    const mod = event.metaKey || event.ctrlKey;
    if (!selectable) {
      library.focusedId = entry.id;
      onopen(entry);
      return;
    }
    if (library.click(entry, { shift: event.shiftKey, mod }) === 'open') onopen(entry);
  }

  function move(step: number, extend: boolean) {
    const index = Math.max(0, order.findIndex((e) => e.id === cursor));
    const next = order[Math.min(order.length - 1, Math.max(0, index + step))];
    if (!next) return;
    if (extend && selectable) library.click(next, { shift: true });
    library.focusedId = next.id;
    list?.querySelector(`#row-${CSS.escape(next.id)}`)?.scrollIntoView({ block: 'nearest' });
  }

  function onkeydown(event: KeyboardEvent) {
    const mod = event.metaKey || event.ctrlKey;
    const entry = order.find((e) => e.id === cursor);
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      move(event.key === 'ArrowDown' ? 1 : -1, event.shiftKey);
    } else if (event.key === 'Home' || event.key === 'End') {
      event.preventDefault();
      move(event.key === 'Home' ? -order.length : order.length, false);
    } else if (event.key === 'Enter' && entry) {
      event.preventDefault();
      onopen(entry);
    } else if (event.key === ' ' && entry && selectable) {
      event.preventDefault();
      library.toggle(entry);
    } else if (mod && event.key.toLowerCase() === 'a' && selectable) {
      event.preventDefault();
      library.selectAll();
    } else if (mod && event.key.toLowerCase() === 'c' && oncopy) {
      const targets = library.selecting ? library.selected : entry ? [entry] : [];
      if (targets.length) {
        event.preventDefault();
        oncopy(library.copyText(targets));
      }
    } else if ((event.key === 'Delete' || event.key === 'Backspace') && selectable) {
      const targets = library.selecting ? library.selected : entry ? [entry] : [];
      if (targets.length) {
        event.preventDefault();
        void library.requestDelete(targets);
      }
    } else if (event.key === 'Escape' && library.selecting) {
      event.preventDefault();
      library.clearSelection();
    }
  }
</script>

{#if library.loaded && order.length === 0}
  <div class="empty">
    <span class="ic ic-mark"></span>
    {#if library.search.trim()}
      <div class="headline">{t('library.noResults', library.search.trim())}</div>
    {:else}
      <div class="headline">{t('library.empty.title')}</div>
      <div class="callout">{t('library.empty.message')}</div>
    {/if}
  </div>
{:else}
  <div
    class="rows"
    class:selecting={library.selecting}
    role="listbox"
    aria-label={label}
    aria-multiselectable={selectable}
    aria-activedescendant={cursor ? `row-${cursor}` : undefined}
    tabindex="0"
    bind:this={list}
    {onkeydown}
  >
    {#each sections as section (section.id)}
      <div class="day" class:pinned={section.pinned} id="day-{section.id}" role="presentation">{typeof section.label === 'string' ? section.label : tm(section.label)}</div>
      {#each section.entries as entry (entry.id)}
        <SessionRow
          {entry}
          {art}
          {now}
          candidateCount={library.candidateCounts[entry.id] ?? null}
          inDaySection={!section.pinned}
          headingId="day-{section.id}"
          {selectable}
          checked={library.selection.has(entry.id)}
          current={entry.id === cursor && (library.focusedId !== null || !selectable)}
          chevron={chevrons}
          onclick={(event) => click(entry, event)}
          ontoggle={() => library.toggle(entry)}
        />
      {/each}
    {/each}
  </div>
{/if}
