<script lang="ts">
  import type { MelodyAlignment } from '../../engine/types';
  import { t } from '../../i18n';
  import { contourPaths } from '../draw/contour';

  /**
   * "Your hum vs. ‹title›" (DESIGN §6.4): the song's line (4 px, outline-variant) under
   * the hum (2.5 px, primary), both in semitones around the hum's median, and a legend.
   */
  interface Props {
    alignment: MelodyAlignment;
    title: string;
    height?: number;
  }

  let { alignment, title, height = 96 }: Props = $props();
  let width = $state(268);
  const reference = $derived(contourPaths(alignment.reference, { width, height }));
  const hum = $derived(contourPaths(alignment.query, { width, height }));
</script>

<div class="overlay-wrap" bind:clientWidth={width}>
  <svg class="overlay" style:height="{height}px" viewBox="0 0 {width} {height}" role="img" aria-label={t('cand.overlayLabel', title)}>
    {#each reference as d, i (i)}<path {d} fill="none" stroke="var(--outline-variant)" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />{/each}
    {#each hum as d, i (i)}<path {d} fill="none" stroke="var(--primary)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />{/each}
  </svg>
  <div class="legend"><span><i style:background="var(--primary)"></i>{t('cand.legend.you')}</span><span><i style:background="var(--outline-variant)"></i>{title}</span></div>
</div>
