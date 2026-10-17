<script lang="ts">
  import type { LibraryEntry } from '../../engine/types';
  import { t } from '../../i18n';
  import Artwork from './Artwork.svelte';

  interface Props {
    entry: LibraryEntry;
    /** Extra classes from the surface (`recent` in the popup). */
    extra?: string;
    selected?: boolean;
    onclick: () => void;
  }

  let { entry, extra = '', selected = false, onclick }: Props = $props();

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

<button class="row {extra}" class:selected aria-label="{entry.title}{sub ? `, ${sub}` : ''}, {time}" {onclick}>
  <Artwork url={entry.artwork_url} size={40} />
  <div class="text">
    <div class="headline truncate">{entry.title}</div>
    {#if sub}<div class="callout secondary truncate">{sub}</div>{/if}
  </div>
  <div class="trail">
    {#if entry.pinned}<span class="ic ic-pin-fill s14" aria-label={t('result.pin')}></span>{/if}
    <span class="callout tnum">{time}</span>
  </div>
</button>
