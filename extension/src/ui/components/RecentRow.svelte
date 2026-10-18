<script lang="ts">
  import type { LibraryEntry } from '../../engine/types';
  import { t } from '../../i18n';
  import { rowSubtitle } from '../../models/library';
  import { clockTime, text } from '../format';
  import Artwork from './Artwork.svelte';

  /**
   * The popup's last-session row (design spec §4): the newest result with "Open in
   * ‹provider›", or a mix still analyzing in the background with its progress and "Open".
   */
  interface Props {
    entry: LibraryEntry;
    /** The trailing "Open in ‹provider›" for a song. */
    openIn?: { label: string; onclick: () => void } | null;
    /** A mix analyzing in the background: minutes heard of the whole. */
    progress?: { analyzedMs: number; totalMs: number } | null;
    touch?: boolean;
    onclick: () => void;
  }

  let { entry, openIn = null, progress = null, touch = false, onclick }: Props = $props();
  const running = $derived(entry.status === 'Running');
  const subtitle = $derived(text(rowSubtitle(entry)));
  const time = $derived(clockTime(entry.created_at_ms));
  const minutes = $derived(progress ? { done: Math.round(progress.analyzedMs / 60_000), all: Math.max(1, Math.round(progress.totalMs / 60_000)) } : null);
</script>

{#if running}
  <div class="recent row analyzing">
    <button class="row-main" aria-label="{t('mix.analyzingBackground')}: {entry.title}{minutes ? `, ${t('mix.tracksMinutes', entry.track_count, minutes.done, minutes.all)}` : ''}" {onclick}>
      <Artwork url={entry.artwork_url} size="video" />
      <div class="text">
        <div class="headline truncate">{entry.title}</div>
        <div class="callout secondary truncate">
          <span class="spinner small" aria-hidden="true"></span>{#if minutes}{t('mix.tracksMinutes', entry.track_count, minutes.done, minutes.all)}{:else}{t('mix.analyzingBackground')}{/if}
        </div>
        {#if minutes}
          <div class="progress" role="progressbar" aria-label={entry.title} aria-valuemin="0" aria-valuemax={minutes.all} aria-valuenow={minutes.done}>
            <i style:width="{Math.round((minutes.done / minutes.all) * 100)}%"></i>
          </div>
        {/if}
      </div>
    </button>
    <div class="trail"><button class="btn small outlined" {onclick}>{t('action.open')}</button></div>
  </div>
{:else}
  <div class="recent row">
    <button class="row-main" aria-label="{entry.title}{subtitle ? `, ${subtitle}` : ''}, {time}" {onclick}>
      <Artwork url={entry.artwork_url} size={touch ? 44 : 40} />
      <div class="text">
        <div class="headline truncate">{entry.title}</div>
        {#if subtitle}<div class="callout secondary truncate">{subtitle}</div>{/if}
      </div>
    </button>
    <div class="trail">
      {#if !touch}<span class="callout tnum">{time}</span>{/if}
      {#if openIn}<button class="btn small outlined" onclick={openIn.onclick}>{openIn.label}</button>{/if}
    </div>
  </div>
{/if}
