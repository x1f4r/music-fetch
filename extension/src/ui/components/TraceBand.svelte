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

  const W = 340;
  const H = 96;
  const geometry = $derived(traceGeometry(frames, { width: W, height: H }));
</script>

<div class="trace-band" role={visible ? 'img' : undefined} aria-label={visible ? label : undefined} aria-hidden={!visible}>
  {#if visible}
    <svg viewBox="0 0 {W} {H}" preserveAspectRatio="none">
      {#each geometry.segments as segment, i (i)}
        <path d={segment.d} fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" opacity={segment.opacity} />
      {/each}
      {#if geometry.now}
        <circle cx={geometry.now.x} cy={geometry.now.y} r="4" fill="currentColor" />
      {/if}
    </svg>
  {/if}
</div>
