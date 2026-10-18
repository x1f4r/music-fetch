<script lang="ts">
  import type { Segment } from '../../engine/types';
  import { t } from '../../i18n';
  import { mixRows, playlistRows, sourceSite, type SessionView } from '../../models/session';
  import Artwork from './Artwork.svelte';

  /**
   * A mix's tracklist (cue, track, row actions) or a playlist's numbered entries
   * (DESIGN §6.5–6.6). One tab stop (roving): ↑ ↓ move the selection, ↩ opens the selected
   * track in the default provider, Tab reaches the focused row's own buttons. Possible rows carry ✓ ✕; unidentified parts offer "Hum it";
   * the rest play the source at their cue.
   */
  interface Props {
    view: SessionView;
    kind: 'mix' | 'playlist';
    selectedId?: string | null;
    onselect?: (id: string) => void;
    onplay?: (segment: Segment) => void;
    onconfirm?: (segment: Segment) => void;
    onreject?: (segment: Segment) => void;
    onhum?: (segment: Segment) => void;
    onactivate?: (id: string) => void;
  }

  let { view, kind, selectedId = null, onselect, onplay, onconfirm, onreject, onhum, onactivate }: Props = $props();
  const site = $derived(sourceSite(view.detail) ?? '');
  const mix = $derived(kind === 'mix' ? mixRows(view) : []);
  const playlist = $derived(kind === 'playlist' ? playlistRows(view) : []);
  const ids = $derived(kind === 'mix' ? mix.map((r) => r.segment.id) : playlist.map((r) => r.segment?.id ?? `pending-${r.index}`));
  // The newest analyzed row slides in (DESIGN "provisional rows" and live updates).
  const newestId = $derived(view.running ? (mix.filter((r) => !r.provisional).at(-1)?.segment.id ?? null) : null);
  let list = $state<HTMLElement | undefined>();
  /** The row that holds the list's one tab stop (roving tabindex). */
  const cursor = $derived(selectedId && ids.includes(selectedId) ? selectedId : (ids[0] ?? null));

  // A selection made elsewhere (the timeline, the details column) brings its row into view.
  $effect(() => {
    if (!selectedId) return;
    list?.querySelector(`[data-id="${CSS.escape(selectedId)}"]`)?.scrollIntoView({ block: 'nearest' });
  });

  function focusRow(id: string) {
    onselect?.(id);
    const row = list?.querySelector<HTMLElement>(`[data-id="${CSS.escape(id)}"]`);
    row?.querySelector<HTMLElement>('.track-main')?.focus();
    row?.scrollIntoView({ block: 'nearest' });
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp' || event.key === 'Home' || event.key === 'End') {
      event.preventDefault();
      const index = cursor ? ids.indexOf(cursor) : -1;
      const to = event.key === 'Home' ? 0 : event.key === 'End' ? ids.length - 1 : index + (event.key === 'ArrowDown' ? 1 : -1);
      const next = ids[Math.min(ids.length - 1, Math.max(0, to))];
      if (next && !next.startsWith('pending-')) focusRow(next);
    } else if (event.key === 'Enter' && cursor) {
      event.preventDefault();
      onactivate?.(cursor);
    }
  }
</script>

<div class="tracks" role="list" bind:this={list}>
  {#if kind === 'mix'}
    {#each mix as row (row.segment.id)}
      {@const track = row.segment.track}
      {@const tab = row.segment.id === cursor ? 0 : -1}
      <div
        class="track"
        class:unknown={row.unknown}
        class:prov={row.provisional}
        class:selected={row.segment.id === selectedId}
        class:new={row.segment.id === newestId}
        role="listitem"
        data-id={row.segment.id}
      >
        <!-- One tab stop for the list (roving): the row's main button takes focus, ↑ ↓ move it. -->
        <button class="track-main" tabindex={tab} aria-current={row.segment.id === selectedId ? 'true' : undefined} onclick={() => onselect?.(row.segment.id)} {onkeydown}>
          <span class="cue">{row.cue}</span>
          {#if track}<Artwork url={track.artwork_url} size={32} />{:else}<span class="art art-32 unknown" aria-hidden="true"></span>{/if}
          <span class="text">
            <span class="truncate ttl">{track ? track.title : t('mix.id')}</span>
            <span class="callout secondary truncate">
              {#if track}{track.artists.join(', ')}{#if row.provisional} · {t('mix.fromTracklist')}{/if}{:else}{row.lengthLabel}{/if}
            </span>
          </span>
        </button>
        <div class="trail">
          {#if row.unknown}
            {#if onhum}<button class="btn small" tabindex={tab} onclick={() => onhum(row.segment)}><span class="ic ic-hum s12"></span>{t('mix.humIt')}</button>{/if}
          {:else if row.possible}
            <span class="tag possible">{t('mix.possible')}</span>
            {#if !row.provisional || row.segment.id === mix.find((r) => r.provisional)?.segment.id}
              <button class="btn icon" tabindex={tab} aria-label={t('mix.confirm')} onclick={() => onconfirm?.(row.segment)}><span class="ic ic-check s12"></span></button>
              <button class="btn icon" tabindex={tab} aria-label={t('mix.notThis')} onclick={() => onreject?.(row.segment)}><span class="ic ic-x s12"></span></button>
            {/if}
          {:else if onplay}
            <button class="btn icon" tabindex={tab} aria-label={t('mix.playAt', site, row.cue)} onclick={() => onplay(row.segment)}><span class="ic ic-play s12"></span></button>
          {/if}
        </div>
      </div>
    {/each}
  {:else}
    {#each playlist as row (row.index)}
      {@const id = row.segment?.id ?? `pending-${row.index}`}
      <div class="track" class:pending={row.pending} class:selected={id === selectedId} role="listitem" data-id={id}>
        <button class="track-main" tabindex={id === cursor ? 0 : -1} disabled={row.pending} aria-current={id === selectedId ? 'true' : undefined} onclick={() => onselect?.(id)} {onkeydown}>
          <span class="idx">{row.index + 1}</span>
          {#if row.track}<Artwork url={row.track.artwork_url} size={32} />{:else}<span class="art art-32 placeholder" aria-hidden="true"></span>{/if}
          <span class="text">
            <span class="truncate ttl">{row.title}</span>
            <span class="callout secondary truncate">{row.artist}</span>
          </span>
        </button>
        <div class="trail">
          {#if row.resolving}
            <span class="spinner small" role="img" aria-label={t('state.resolving')}></span>
          {:else if row.duration && !row.pending}
            <span class="callout tnum secondary">{row.duration}</span>
          {/if}
        </div>
      </div>
    {/each}
  {/if}
</div>
