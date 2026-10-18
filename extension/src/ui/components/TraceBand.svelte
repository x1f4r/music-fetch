<script lang="ts">
  import type { PitchFrame } from '../../engine/types';
  import { traceGeometry } from '../draw/trace';

  interface Props {
    frames: readonly PitchFrame[];
    /** Accessible description, e.g. "Your pitch, 6 seconds hummed". */
    label: string;
    visible: boolean;
  }

  let { frames, label, visible }: Props = $props();

  // Drawn at the band's real size so the 4 px line keeps its width at any panel width.
  let width = $state(340);
  let height = $state(96);
  const geometry = $derived(traceGeometry(frames, { width, height }));
</script>

<div class="trace-band" role={visible ? 'img' : undefined} aria-label={visible ? label : undefined} aria-hidden={!visible} bind:clientWidth={width} bind:clientHeight={height}>
  {#if visible && width > 0}
    <svg viewBox="0 0 {width} {height}">
      {#each geometry.segments as segment, i (i)}
        <path d={segment.d} fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" opacity={segment.opacity} />
      {/each}
      {#if geometry.now}
        <circle cx={geometry.now.x} cy={geometry.now.y} r="4" fill="currentColor" />
      {/if}
    </svg>
  {/if}
</div>
