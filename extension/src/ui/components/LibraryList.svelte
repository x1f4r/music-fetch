<script lang="ts">
  import type { LibraryEntry } from '../../engine/types';
  import { t, tm } from '../../i18n';
  import { librarySections } from '../../models/library';
  import SessionRow from './SessionRow.svelte';

  interface Props {
    entries: readonly LibraryEntry[];
    selectedId?: string | null;
    pinnedSection?: boolean;
    onopen: (entry: LibraryEntry) => void;
  }

  let { entries, selectedId = null, pinnedSection = true, onopen }: Props = $props();
  const sections = $derived(librarySections(entries, Date.now(), { pinnedSection }));
</script>

{#if entries.length === 0}
  <div class="empty">
    <span class="ic ic-mark"></span>
    <div class="headline">{t('library.empty.title')}</div>
    <div class="callout">{t('library.empty.message')}</div>
  </div>
{:else}
  {#each sections as section (section.id)}
    <div class="day" class:pinned={section.pinned}>{typeof section.label === 'string' ? section.label : tm(section.label)}</div>
    {#each section.entries as entry (entry.id)}
      <SessionRow {entry} selected={entry.id === selectedId} onclick={() => onopen(entry)} />
    {/each}
  {/each}
{/if}
