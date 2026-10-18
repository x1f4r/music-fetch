<script lang="ts">
  import { t, tm } from '../../i18n';
  import { clock, identifiedCounts, sourceSite, statusRow, type SessionShape, type SessionView } from '../../models/session';
  import { text } from '../format';
  import Artwork from './Artwork.svelte';

  /**
   * The header of a mix or playlist (design spec "Mix header and status"): thumbnail or
   * cover, the source and its length, and the status row (spinner, wording, Stop or
   * Resume) that only changes its text.
   */
  interface Props {
    view: SessionView;
    shape: SessionShape;
    /** The app tab's larger header (100 × 56 thumbnail, 64 cover, a second meta line). */
    large?: boolean;
    onstop?: () => void;
    onresume?: () => void;
  }

  let { view, shape, large = false, onstop, onresume }: Props = $props();
  const detail = $derived(view.detail);
  const site = $derived(sourceSite(detail));
  const length = $derived(detail.source?.duration_ms ?? detail.entry.duration_ms);
  const status = $derived(statusRow(view, shape));
  const counts = $derived(identifiedCounts(detail));
  const host = $derived.by(() => {
    try {
      const url = new URL(detail.source?.url ?? detail.entry.source_url ?? '');
      return `${url.hostname.replace(/^(www|m)\./, '')}${url.pathname}${url.search}`;
    } catch {
      return null;
    }
  });
  const items = $derived(detail.source?.item_count ?? 0);
  const date = $derived(new Date(detail.entry.created_at_ms).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }));
</script>

<div class="mix-head" class:large>
  {#if shape === 'mix'}
    <Artwork url={detail.entry.artwork_url} size={large ? 'video-l' : 'video'} />
    <div class="text">
      {#if !large}<div class="headline truncate">{detail.entry.title}</div>{/if}
      <div class="callout secondary truncate">{[site, length ? clock(length) : null, large ? host : null].filter(Boolean).join(' · ')}</div>
      {#if view.running && view.capture?.rate}
        <div class="callout secondary muted-line"><span class="ic ic-mute s12"></span>{t('mix.mutedAt', String(view.capture.rate))}</div>
      {:else if large && !view.running}
        <div class="callout secondary"><span class="tnum">{t('mix.identified', counts.identified, counts.total)}</span> · {t('mix.heardAt', date)}</div>
      {/if}
    </div>
  {:else}
    <Artwork url={detail.entry.artwork_url} size={large ? 64 : 56} />
    <div class="text">
      <div class="headline truncate">{detail.entry.title}</div>
      <div class="callout secondary truncate">
        {[detail.source?.uploader ?? detail.entry.subtitle, items ? t('collection.tracks', items) : null, large && length ? clock(length) : null, site].filter(Boolean).join(' · ')}
      </div>
      {#if large && !view.running}
        <div class="callout secondary"><span class="tnum">{t('mix.identified', counts.identified, counts.total)}</span> · {t('mix.fromLink', date)}</div>
      {/if}
    </div>
  {/if}
</div>
{#if status && (view.running || !large)}
  <div class="mix-status" aria-live="polite">
    {#if status.spinner}<span class="spinner" aria-hidden="true"></span>{/if}
    <span class="grow truncate">{tm(status.message)}</span>
    {#if status.progress !== null && shape === 'playlist'}
      <div class="progress small-bar" role="progressbar" aria-label={text(status.message)} aria-valuemin="0" aria-valuemax="100" aria-valuenow={Math.round(status.progress * 100)}>
        <i style:width="{Math.round(status.progress * 100)}%"></i>
      </div>
    {/if}
    {#if status.action === 'stop' && onstop}
      <button class="btn small outlined" onclick={onstop}><span class="ic ic-stop s12"></span>{t('action.stop')}</button>
    {:else if status.action === 'resume' && onresume}
      <button class="btn small outlined" onclick={onresume}><span class="ic ic-play s12"></span>{t('action.resume')}</button>
    {/if}
  </div>
{/if}
