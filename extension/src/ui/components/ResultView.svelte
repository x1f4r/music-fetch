<script lang="ts">
  import type { Track } from '../../engine/types';
  import { t } from '../../i18n';
  import type { Fact } from '../../models/session';
  import { trackFacts } from '../../models/session';
  import type { CustomProvider, Provider } from '../../providers/links';
  import Artwork from './Artwork.svelte';
  import Facts from './Facts.svelte';
  import OpenInButton from './OpenInButton.svelte';

  /**
   * A matched song (DESIGN §6.3): artwork on a wash of its colour, title, artist, album ·
   * year, where it was heard, then "Open in ‹provider›" ▾, Pin and Copy (Share on touch).
   * In narrow surfaces the details fold into an expandable block.
   */
  interface Props {
    track: Track;
    preferred: Provider;
    custom: CustomProvider | null;
    pinned?: boolean;
    /** Where it was heard, e.g. "From this tab · youtube.com". */
    context?: string | null;
    contextIcon?: string;
    /** The "Heard" rows of the details block (source, when, matched by). */
    heard?: Fact[];
    /** Show the expandable details block (narrow surfaces). */
    details?: boolean;
    menuUp?: boolean;
    artSize?: 120 | 160 | 200;
    /** Touch: Share instead of Copy, the large split button. */
    touch?: boolean;
    /** The dominant artwork colour from the engine, for the wash. */
    wash?: string | null;
    onopen: (url: string) => void;
    oncopy: (text: string) => void;
    onshare?: () => void;
    onpin?: (pinned: boolean) => void;
  }

  let {
    track,
    preferred,
    custom,
    pinned = false,
    context = null,
    contextIcon = 'ic-tab',
    heard = [],
    details = false,
    menuUp,
    artSize = 160,
    touch = false,
    wash = null,
    onopen,
    oncopy,
    onshare,
    onpin,
  }: Props = $props();
  let copied = $state(false);
  let open = $state(false);

  const meta = $derived([track.album, track.release_year].filter(Boolean).join(' · '));
  const facts = $derived(trackFacts(track));

  function copy() {
    oncopy(`${track.artist} – ${track.title}`);
    copied = true;
    setTimeout(() => (copied = false), 1500);
  }
</script>

<section class="hero" aria-label={t('result.label')} style:--wash={wash}>
  <div class="wash" aria-hidden="true"></div>
  <Artwork url={track.artwork_url} size={artSize} />
  <h1 class="ttl title-l clamp2">{track.title}</h1>
  <div class="artist">{track.artist}</div>
  {#if meta}<div class="meta callout secondary">{meta}</div>{/if}
  {#if context}<div class="context callout"><span class="ic {contextIcon} s14"></span>{context}</div>{/if}
  <div class="actions">
    <OpenInButton {track} {preferred} {custom} up={menuUp} {onopen} />
    {#if onpin}
      <button class="btn icon" class:pinned aria-pressed={pinned} aria-label={t('result.pin')} onclick={() => onpin(!pinned)}>
        <span class="ic {pinned ? 'ic-pin-fill' : 'ic-pin'}"></span>
      </button>
    {/if}
    {#if touch && onshare}
      <button class="btn icon" aria-label={t('result.share')} onclick={onshare}><span class="ic ic-share s18"></span></button>
    {:else}
      <button class="btn icon" aria-label={copied ? t('result.copied') : t('result.copyArtistTitle')} onclick={copy}>
        <span class="ic {copied ? 'ic-check' : 'ic-copy'}"></span>
      </button>
    {/if}
  </div>
  <p class="sr-only" aria-live="polite">{t('result.found', track.title, track.artist)}</p>
</section>
{#if details && (facts.length > 0 || heard.length > 0)}
  <div class="details" class:open>
    <button class="dhead" aria-expanded={open} onclick={() => (open = !open)}><span class="ic ic-chev-right {touch ? 's18' : 's14'}"></span>{t('result.details')}</button>
    {#if open}
      <Facts {facts} />
      {#if heard.length > 0}
        <div class="dsec">
          <div class="h">{t('result.heard')}</div>
          <Facts facts={heard} />
        </div>
      {/if}
    {/if}
  </div>
{/if}
