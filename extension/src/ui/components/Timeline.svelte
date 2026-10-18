<script lang="ts">
  import { t } from '../../i18n';
  import { identifiedCounts, timeline, type SessionView } from '../../models/session';

  /**
   * The ruler and timeline strip of a mix (DESIGN §6.5): tiers by fill, the unanalyzed
   * veil and the frontier while it runs. A picture for assistive technology (one-line
   * description); the tracklist is its accessible form. Clicking a segment selects it.
   */
  interface Props {
    view: SessionView;
    selectedId?: string | null;
    /** 1 = the whole length fits; ⌘/Ctrl + and − zoom the strip. */
    zoom?: number;
    onselect?: (segmentId: string) => void;
  }

  let { view, selectedId = null, zoom = 1, onselect }: Props = $props();
  const geometry = $derived(timeline(view));
  const counts = $derived(identifiedCounts(view.detail));
  const label = $derived.by(() => {
    if (!geometry) return '';
    if (geometry.frontier !== null && view.progress) {
      return t('mix.timelineRunning', counts.identified, counts.total, Math.round(view.progress.analyzed_ms / 60_000), Math.round(geometry.totalMs / 60_000));
    }
    return t('mix.timeline', counts.total, counts.identified);
  });
  const pct = (n: number) => `${(n * 100).toFixed(3)}%`;
</script>

{#if geometry}
  <div class="tl" role="img" aria-label={label}>
    <div class="tl-scroll" class:zoomed={zoom > 1}>
      <div class="tl-inner" style:width="{zoom * 100}%">
        <div class="ruler" aria-hidden="true">
          {#each geometry.ticks as tick, i (i)}<span style:left={i === 0 ? undefined : pct(tick.at)}>{tick.label}</span>{/each}
        </div>
        <div class="strip" aria-hidden="true">
          {#each geometry.segments as segment (segment.id)}
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <!-- svelte-ignore a11y_no_static_element_interactions -->
            <div class="seg {segment.cls}" class:selected={segment.id === selectedId} style:left={pct(segment.left)} style:width={pct(segment.width)} onclick={() => onselect?.(segment.id)}></div>
          {/each}
          {#if geometry.frontier !== null}
            <div class="unanalyzed" style:width={pct(1 - geometry.frontier)}></div>
            <div class="frontier" style:left={pct(geometry.frontier)}></div>
          {/if}
        </div>
      </div>
    </div>
  </div>
{/if}
