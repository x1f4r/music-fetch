<script lang="ts">
  import type { Candidate, CandidateTier } from '../../engine/types';
  import { t } from '../../i18n';
  import Artwork from './Artwork.svelte';

  interface Props {
    candidates: readonly Candidate[];
    onchoose: (candidate: Candidate) => void;
  }

  let { candidates, onchoose }: Props = $props();
  let selected = $state(0);

  const TAGS: Record<CandidateTier, { cls: string; label: string }> = {
    Strong: { cls: 'strong', label: t('cand.strong') },
    Possible: { cls: 'possible', label: t('cand.possible') },
    LongShot: { cls: 'longshot', label: t('cand.longShot') },
  };

  function onkeydown(event: KeyboardEvent) {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const step = event.key === 'ArrowDown' ? 1 : -1;
      selected = (selected + step + candidates.length) % candidates.length;
    } else if (event.key === 'Enter') {
      const candidate = candidates[selected];
      if (!candidate) return;
      event.preventDefault();
      onchoose(candidate);
    }
  }
</script>

<div class="cands" role="listbox" tabindex="0" aria-activedescendant="cand-{selected}" {onkeydown}>
  {#each candidates as candidate, i (candidate.track.identity_key || i)}
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <div class="cand" class:selected={i === selected} id="cand-{i}" role="option" aria-selected={i === selected} tabindex="-1" onclick={() => (selected = i)}>
      <Artwork url={candidate.track.artwork_url} size={44} />
      <div class="text">
        <div class="headline truncate">{candidate.track.title}</div>
        <div class="callout secondary truncate">{candidate.track.artist}</div>
      </div>
      <div class="trail">
        <span class="tag {TAGS[candidate.tier].cls}">{TAGS[candidate.tier].label}</span>
        <button class="btn filled this" onclick={() => onchoose(candidate)}>{t('cand.thisIsIt')}</button>
      </div>
    </div>
  {/each}
</div>
