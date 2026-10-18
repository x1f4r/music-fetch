<script lang="ts">
  import { t } from '../../i18n';
  import { clock } from '../../models/session';
  import type { AnalyzeChoice } from '../../settings/settings';
  import Artwork from './Artwork.svelte';
  import Sheet from './Sheet.svelte';

  /**
   * "Analyze the whole video?" (extension plan §5.4): listen through at 4× with the sound
   * off (every track, exact times), or a quick check that samples it. "Don't ask again"
   * remembers the choice for the site.
   */
  interface Props {
    title: string;
    site: string;
    durationMs: number;
    thumbnail: string | null;
    onanalyze: (choice: AnalyzeChoice, remember: boolean) => void;
    oncancel: () => void;
  }

  let { title, site, durationMs, thumbnail, onanalyze, oncancel }: Props = $props();
  let choice = $state<AnalyzeChoice>('whole');
  let remember = $state(false);
  // Listen-through at 4×; a quick check samples about one window per four minutes.
  const wholeMinutes = $derived(Math.max(1, Math.round(durationMs / 4 / 60_000)));
  const quickMinutes = $derived(Math.max(1, Math.min(5, Math.round(durationMs / 30 / 60_000))));
  const OPTIONS = $derived([
    { id: 'whole' as const, icon: 'ic-ff', label: t('ask.whole'), meta: t('ask.wholeMeta', wholeMinutes) },
    { id: 'quick' as const, icon: 'ic-bolt', label: t('ask.quick'), meta: t('ask.quickMeta', quickMinutes) },
  ]);

  function onkeydown(event: KeyboardEvent) {
    if (!['ArrowDown', 'ArrowUp', 'ArrowLeft', 'ArrowRight'].includes(event.key)) return;
    event.preventDefault();
    choice = choice === 'whole' ? 'quick' : 'whole';
    (event.currentTarget as HTMLElement).querySelector<HTMLElement>(`[data-choice="${choice}"]`)?.focus();
  }
</script>

<Sheet title={t('ask.title')} onclose={oncancel}>
  <div class="ask-source">
    {#if thumbnail}<Artwork url={thumbnail} size="video" />{/if}
    <div class="text">
      <div class="headline truncate">{title}</div>
      <div class="callout secondary">{site} · <span class="tnum">{clock(durationMs)}</span></div>
    </div>
  </div>
  <div class="secondary">{t('ask.message')}</div>
  <div class="ask-opts" role="radiogroup" aria-label={t('ask.title')} tabindex="-1" {onkeydown}>
    {#each OPTIONS as option (option.id)}
      <button class="opt" class:on={choice === option.id} role="radio" aria-checked={choice === option.id} data-choice={option.id} tabindex={choice === option.id ? 0 : -1} onclick={() => (choice = option.id)}>
        <span class="ic {option.icon} s18"></span>
        <span class="t"><span class="semibold">{option.label}</span><span class="callout secondary">{option.meta}</span></span>
        <span class="radio" class:on={choice === option.id}></span>
      </button>
    {/each}
  </div>
  <label class="check"><input type="checkbox" class="sr-only" bind:checked={remember} /><span class="cb" class:on={remember} aria-hidden="true">{#if remember}<span class="ic ic-check"></span>{/if}</span>{t('ask.dontAsk', site)}</label>
  {#snippet actions()}
    <button class="btn text" onclick={oncancel}>{t('action.cancel')}</button>
    <button class="btn filled" onclick={() => onanalyze(choice, remember)}>{t('ask.analyze')}</button>
  {/snippet}
</Sheet>
