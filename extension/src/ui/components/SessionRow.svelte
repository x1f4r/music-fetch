<script lang="ts">
  import type { LibraryEntry } from '../../engine/types';
  import { t } from '../../i18n';
  import Artwork from './Artwork.svelte';

  interface Props {
    entry: LibraryEntry;
    /** Extra classes from the surface (`recent` in the popup). */
    extra?: string;
    selected?: boolean;
    /** A trailing "Open in ‹provider›" button (the popup's last-session row). */
    openIn?: { label: string; onclick: () => void } | null;
    onclick: () => void;
  }

  let { entry, extra = '', selected = false, openIn = null, onclick }: Props = $props();

  function subtitle(e: LibraryEntry): string {
    if (e.status === 'Running') return t('library.running');
    if (e.status === 'Waiting') return t('library.waiting');
    if (e.status === 'NoMatch') return t('library.noMatch');
    if (e.status === 'Failed') return t('library.failed');
    if (e.status === 'Candidates') return e.subtitle ?? '';
    if (e.track_count > 1) return t('library.tracks', e.subtitle ?? e.title, e.track_count);
    return e.subtitle ?? '';
  }

  const time = $derived(new Date(entry.created_at_ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }));
  const sub = $derived(subtitle(entry));
</script>

{#snippet body()}
  <Artwork url={entry.artwork_url} size={40} />
  <div class="text">
    <div class="headline truncate">{entry.title}</div>
    {#if sub}<div class="callout secondary truncate">{sub}</div>{/if}
  </div>
{/snippet}

{#if openIn}
  <!-- Two actions: the row opens the session, the trailing button opens the song. -->
  <div class="row {extra}" class:selected>
    <button class="row-main" aria-label="{entry.title}{sub ? `, ${sub}` : ''}, {time}" {onclick}>{@render body()}</button>
    <div class="trail">
      <span class="callout tnum">{time}</span>
      <button class="btn small outlined" onclick={openIn.onclick}>{openIn.label}</button>
    </div>
  </div>
{:else}
  <button class="row {extra}" class:selected aria-label="{entry.title}{sub ? `, ${sub}` : ''}, {time}" {onclick}>
    {@render body()}
    <div class="trail">
      {#if entry.pinned}<span class="ic ic-pin-fill s14" aria-label={t('result.pin')}></span>{/if}
      <span class="callout tnum">{time}</span>
    </div>
  </button>
{/if}
