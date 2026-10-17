<script lang="ts">
  import type { Track } from '../../engine/types';
  import { t } from '../../i18n';
  import type { CustomProvider, Provider } from '../../providers/links';
  import Artwork from './Artwork.svelte';
  import OpenInButton from './OpenInButton.svelte';

  interface Props {
    track: Track;
    preferred: Provider;
    custom: CustomProvider | null;
    pinned?: boolean;
    /** The popup opens the provider menu upward. */
    menuUp?: boolean;
    artSize?: 120 | 160 | 200;
    onopen: (url: string) => void;
    oncopy: (text: string) => void;
    onpin?: (pinned: boolean) => void;
  }

  let { track, preferred, custom, pinned = false, menuUp = false, artSize = 160, onopen, oncopy, onpin }: Props = $props();
  let copied = $state(false);

  const meta = $derived([track.album, track.release_year].filter(Boolean).join(' · '));

  function copy() {
    oncopy(`${track.artist} – ${track.title}`);
    copied = true;
    setTimeout(() => (copied = false), 1500);
  }
</script>

<div class="hero" aria-live="polite">
  <div class="wash"></div>
  <Artwork url={track.artwork_url} size={artSize} />
  <div class="ttl title-l clamp2">{track.title}</div>
  <div class="artist secondary">{track.artist}</div>
  {#if meta}<div class="meta callout tertiary">{meta}</div>{/if}
  <div class="actions">
    <OpenInButton {track} {preferred} {custom} up={menuUp} {onopen} />
    {#if onpin}
      <button class="btn icon" class:pinned aria-pressed={pinned} aria-label={pinned ? t('result.unpin') : t('result.pin')} onclick={() => onpin(!pinned)}>
        <span class="ic {pinned ? 'ic-pin-fill' : 'ic-pin'}"></span>
      </button>
    {/if}
    <button class="btn icon" aria-label={copied ? t('result.copied') : t('result.copy')} onclick={copy}>
      <span class="ic {copied ? 'ic-check' : 'ic-copy'}"></span>
    </button>
  </div>
</div>
