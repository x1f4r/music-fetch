<script lang="ts">
  import { LIVE_VIEWBOX, livePath, MARK_PATH, MARK_VIEWBOX } from '../draw/wave';

  interface Props {
    /** Capture is running: halo and the live line. */
    live?: boolean;
    /** Humming: the ring fills with voiced time (0–100). */
    hum?: boolean;
    ring?: number;
    /** The engine is identifying: the glyph dims. */
    matching?: boolean;
    level?: number;
    label: string;
    describedBy?: string;
    onclick: () => void;
  }

  let { live = false, hum = false, ring = 0, matching = false, level = 0, label, describedBy, onclick }: Props = $props();

  // The live line drifts a little between level updates so it reads as sound, not a chart.
  let phase = $state(0);
  $effect(() => {
    if (!live) return;
    let frame = 0;
    const tick = () => {
      phase = (phase + 0.12) % (Math.PI * 2);
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  });
</script>

<div class="listen" class:live class:hum class:matching>
  <div class="halo"></div>
  <div class="ring" style:--ring={ring}></div>
  <button class="disc" aria-label={label} aria-describedby={describedBy} {onclick}>
    {#if live}
      <svg class="mark" viewBox={LIVE_VIEWBOX} fill="none" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d={livePath(Math.max(level, 0.08), phase)} />
      </svg>
    {:else}
      <svg class="mark" viewBox={MARK_VIEWBOX} fill="none" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d={MARK_PATH} />
      </svg>
    {/if}
  </button>
</div>
