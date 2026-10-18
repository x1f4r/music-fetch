<script lang="ts">
  import StepDots from './StepDots.svelte';
  import type { Service } from '../../engine/types';
  import { t } from '../../i18n';
  import {
    customProviderLink,
    isValidTemplate,
    providerKey,
    type CustomProvider,
    type Provider,
  } from '../../providers/links';
  import { COMMON_PROVIDERS, MORE_PROVIDERS, serviceName } from '../../providers/services';
  import { chooseProvider, skipProviderPanel } from '../../settings/settings';

  /**
   * "Where do you listen?" (docs/APPS.md "Opening links"). Nothing is stored until the
   * user presses Choose or Skip; Skip stores Spotify. Browsers can't see installed apps,
   * so the only pre-selection is the streaming site in the current tab.
   */
  interface Props {
    suggested?: Service | null;
    /** A custom provider saved earlier, to prefill the form. */
    savedCustom?: CustomProvider | null;
    /** Popup and side panel: smaller title, no step dots. */
    compact?: boolean;
    /** Steps in the welcome flow (the dots under the panel). */
    steps?: number;
    /** Called after the decision is stored. */
    ondecided?: () => void;
  }

  let { suggested = null, savedCustom = null, compact = false, steps = 3, ondecided }: Props = $props();

  // Seeded once from the props: the suggestion only pre-selects, the user's taps win.
  // svelte-ignore state_referenced_locally
  let selected = $state<Provider | null>(suggested ? { kind: 'service', service: suggested } : null);
  // svelte-ignore state_referenced_locally
  let showMore = $state(suggested !== null && MORE_PROVIDERS.includes(suggested));
  // svelte-ignore state_referenced_locally
  let customName = $state(savedCustom?.name ?? '');
  // svelte-ignore state_referenced_locally
  let customTemplate = $state(savedCustom?.template ?? '');
  let showProblem = $state(false);
  let busy = $state(false);

  const SAMPLE = { title: 'Take On Me', artist: 'a-ha', artists: ['a-ha'], isrc: 'NOA018500030' };

  const isCustom = $derived(selected?.kind === 'custom');
  const problem = $derived.by(() => {
    if (!isCustom) return null;
    if (!customName.trim()) return t('provider.customNeedsName');
    if (!isValidTemplate(customTemplate)) return t('provider.customInvalid');
    return null;
  });
  const preview = $derived(isCustom && !problem ? customProviderLink(SAMPLE, customTemplate) : null);
  const chooseLabel = $derived.by(() => {
    if (!selected) return t('action.continue');
    if (selected.kind === 'custom') return t('provider.choose', customName.trim() || t('provider.customDefault'));
    return t('provider.choose', serviceName(selected.service));
  });

  function asProvider(service: Service): Provider {
    return { kind: 'service', service };
  }

  function isSelected(provider: Provider): boolean {
    return selected !== null && providerKey(selected) === providerKey(provider);
  }

  let nameField = $state<HTMLInputElement | undefined>();

  function select(provider: Provider) {
    selected = provider;
    showProblem = false;
    // The custom form opens under the list: start typing its name.
    if (provider.kind === 'custom') queueMicrotask(() => nameField?.focus());
  }

  /** One tab stop for the radio group: the selected option, else the first. */
  function rovingIndex(provider: Provider): number {
    if (selected) return isSelected(provider) ? 0 : -1;
    return providerKey(provider) === providerKey({ kind: 'service', service: COMMON_PROVIDERS[0]! }) ? 0 : -1;
  }

  function onkeydown(event: KeyboardEvent) {
    if (!['ArrowDown', 'ArrowUp', 'ArrowRight', 'ArrowLeft'].includes(event.key)) return;
    const radios = [...(event.currentTarget as HTMLElement).querySelectorAll<HTMLButtonElement>('[role="radio"]')];
    const index = radios.indexOf(document.activeElement as HTMLButtonElement);
    if (index < 0) return;
    event.preventDefault();
    const next = radios[(index + (event.key === 'ArrowDown' || event.key === 'ArrowRight' ? 1 : -1) + radios.length) % radios.length];
    next?.focus();
    next?.click();
  }

  async function choose() {
    if (!selected || busy) return;
    if (problem) {
      showProblem = true;
      return;
    }
    busy = true;
    try {
      await chooseProvider(selected, selected.kind === 'custom' ? { name: customName, template: customTemplate } : undefined);
      ondecided?.();
    } finally {
      busy = false;
    }
  }

  async function skip() {
    if (busy) return;
    busy = true;
    try {
      await skipProviderPanel();
      ondecided?.();
    } finally {
      busy = false;
    }
  }
</script>

{#snippet serviceRow(service: Service)}
  {@const provider = asProvider(service)}
  <button class="opt" class:on={isSelected(provider)} role="radio" aria-checked={isSelected(provider)} tabindex={rovingIndex(provider)} onclick={() => select(provider)}>
    <span class="radio" class:on={isSelected(provider)}></span>
    <span class="name">{serviceName(service)}</span>
    {#if service === suggested}<span class="meta">{t('provider.fromTab')}</span>{/if}
  </button>
{/snippet}

<div class="mini-disc" aria-hidden="true"><span class="ic ic-mark"></span></div>
<h1 id="provider-title">{t('provider.title')}</h1>
<p class="lede">{t('provider.message')}</p>
<div class="opts" role="radiogroup" aria-labelledby="provider-title" tabindex="-1" {onkeydown}>
  {#each COMMON_PROVIDERS as service (service)}
    {@render serviceRow(service)}
  {/each}
  {#if showMore}
    {#each MORE_PROVIDERS as service (service)}
      {@render serviceRow(service)}
    {/each}
    <button class="opt custom" class:on={isCustom} role="radio" aria-checked={isCustom} tabindex={rovingIndex({ kind: 'custom' })} onclick={() => select({ kind: 'custom' })}>
      <span class="radio" class:on={isCustom}></span>
      <span class="name">{t('provider.custom')}</span>
    </button>
    {#if isCustom}
      <div class="custom-form">
        <label class="field small"><input bind:this={nameField} bind:value={customName} placeholder={t('provider.customName')} aria-label={t('provider.customName')} /></label>
        <label class="field small">
          <input
            bind:value={customTemplate}
            placeholder={t('provider.customPlaceholder')}
            aria-label={t('provider.customLink')}
            autocomplete="off"
            spellcheck="false"
          />
        </label>
        {#if showProblem && problem}
          <div class="hint problem" role="alert">{problem}</div>
        {:else if preview}
          <div class="hint preview">{t('provider.customPreview', preview)}</div>
        {:else}
          <div class="hint">{t('provider.customHint')}</div>
        {/if}
      </div>
    {/if}
  {:else}
    <button class="opt more" aria-expanded="false" onclick={() => (showMore = true)}>
      <span class="spacer"></span>
      <span class="name">{t('provider.more')}</span>
      <span class="meta">{t('provider.moreMeta', MORE_PROVIDERS.length)}</span>
      <span class="ic ic-chev-right s14 secondary"></span>
    </button>
  {/if}
</div>
<div class="acts">
  <button class="btn text" onclick={skip} disabled={busy}>{t('provider.skip')}</button>
  <button class="btn filled" onclick={choose} disabled={!selected || busy}>{chooseLabel}</button>
</div>
{#if !compact}
  <StepDots index={0} count={steps} />
{/if}
