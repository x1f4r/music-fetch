<script lang="ts">
  import { onDestroy } from 'svelte';
  import type { Candidate, CandidateTier } from '../../engine/types';
  import { t } from '../../i18n';
  import { contourPaths, voicedSeconds } from '../draw/contour';
  import Artwork from './Artwork.svelte';

  /**
   * Hum and lyric candidates (DESIGN §6.4, design spec "Candidates"): the hum strip on
   * top, then the matches with their tier. The artwork plays the preview; "This is it" is
   * the one action, on the selected row and on hover or focus.
   */
  interface Props {
    candidates: readonly Candidate[];
    /** The row the details column shows. */
    selected?: number;
    /** Hide the hum strip (lyric searches; the app tab shows the hum in the details column). */
    humStrip?: boolean;
    onchoose: (candidate: Candidate) => void;
    onselect?: (index: number) => void;
    onhumagain?: () => void;
    ontypelyric?: () => void;
  }

  let { candidates, selected = $bindable(0), humStrip = true, onchoose, onselect, onhumagain, ontypelyric }: Props = $props();

  const TAGS: Record<CandidateTier, { cls: string; label: string }> = {
    Strong: { cls: 'strong', label: t('cand.strong') },
    Possible: { cls: 'possible', label: t('cand.possible') },
    LongShot: { cls: 'longshot', label: t('cand.longShot') },
  };

  const hum = $derived(candidates.find((c) => c.alignment)?.alignment ?? null);
  const humSeconds = $derived(hum ? voicedSeconds(hum.query, hum.frame_ms) : 0);
  const humPaths = $derived(hum ? contourPaths(hum.query, { width: 220, height: 48 }) : []);
  let playing = $state<number | null>(null);
  let audio: HTMLAudioElement | null = null;
  let list = $state<HTMLElement | undefined>();

  function select(index: number) {
    selected = index;
    onselect?.(index);
  }

  function preview(index: number) {
    const url = candidates[index]?.track.preview_url;
    if (!url) return;
    if (playing === index) {
      audio?.pause();
      playing = null;
      return;
    }
    audio?.pause();
    audio = new Audio(url);
    audio.addEventListener('ended', () => (playing = null));
    void audio.play().catch(() => (playing = null));
    playing = index;
  }

  onDestroy(() => audio?.pause());

  function focusRow(index: number) {
    select(index);
    const row = list?.querySelector<HTMLElement>(`#cand-${index}`);
    row?.querySelector<HTMLElement>('.cand-main')?.focus();
    row?.scrollIntoView({ block: 'nearest' });
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const step = event.key === 'ArrowDown' ? 1 : -1;
      focusRow(Math.min(candidates.length - 1, Math.max(0, selected + step)));
    } else if (event.key === 'Enter') {
      const candidate = candidates[selected];
      if (!candidate) return;
      event.preventDefault();
      onchoose(candidate);
    } else if (event.key === ' ') {
      event.preventDefault();
      preview(selected);
    }
  }
</script>

{#if humStrip && hum}
  <div class="hum-row">
    <div class="trace" role="img" aria-label={t('cand.yourPitch', humSeconds)}>
      <svg viewBox="0 0 220 48" preserveAspectRatio="none">
        {#each humPaths as d, i (i)}<path {d} fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke" />{/each}
      </svg>
    </div>
    <span class="lbl tnum">{t('cand.yourHum', humSeconds)}</span>
    {#if onhumagain}<button class="btn small" onclick={onhumagain}><span class="ic ic-hum s12"></span>{t('cand.humAgain')}</button>{/if}
  </div>
{/if}

<div class="cands" role="list" aria-label={t('cand.list')} bind:this={list}>
  {#each candidates as candidate, i (candidate.track.identity_key || i)}
    {@const tag = TAGS[candidate.tier]}
    <div class="cand" class:selected={i === selected} id="cand-{i}" role="listitem">
      {#if candidate.track.preview_url}
        <button
          class="art art-44 preview"
          class:playing={playing === i}
          aria-label={playing === i ? t('cand.pausePreview') : t('cand.playPreview')}
          tabindex="-1"
          onclick={() => preview(i)}
        ><Artwork url={candidate.track.artwork_url} size={44} bare /></button>
      {:else}
        <Artwork url={candidate.track.artwork_url} size={44} />
      {/if}
      <!-- One tab stop for the list (roving): ↑ ↓ move, ↩ chooses, Space plays the preview. -->
      <button class="cand-main" tabindex={i === selected ? 0 : -1} aria-current={i === selected ? 'true' : undefined} onclick={() => select(i)} {onkeydown}>
        <span class="headline truncate">{candidate.track.title}</span>
        <span class="callout secondary truncate">{candidate.track.artist}</span>
      </button>
      <div class="trail">
        <span class="tag {tag.cls}">{tag.label}</span>
        <button class="btn this" class:filled={i === selected} tabindex={i === selected ? 0 : -1} onclick={() => onchoose(candidate)}>{t('cand.thisIsIt')}</button>
      </div>
    </div>
  {/each}
</div>
{#if candidates[0]}
  <p class="sr-only" aria-live="polite">{t('cand.announce', candidates.length, candidates[0].track.title, TAGS[candidates[0].tier].label)}</p>
{/if}
{#if ontypelyric}
  <div class="cand-foot">{t('cand.notHere')} <button class="btn small" onclick={ontypelyric}><span class="ic ic-text s12"></span>{t('action.typeLyric')}</button></div>
{/if}
