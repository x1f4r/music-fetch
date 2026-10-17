<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { EngineClient } from '../../engine/client';
  import { isLink } from '../../engine/links';
  import { t } from '../../i18n';
  import { showsResult } from '../../models/stage';
  import type { ActiveTab } from '../../platform/surfaces';
  import { isAnalyzablePage } from '../../platform/surfaces';
  import { suggestProvider } from '../../providers/services';
  import { loadProviderSettings, watchProviderSettings, type ListenChoice, type ProviderSettings } from '../../settings/settings';
  import CandidatesView from '../components/CandidatesView.svelte';
  import LinkField from '../components/LinkField.svelte';
  import ProviderPicker from '../components/ProviderPicker.svelte';
  import ResultView from '../components/ResultView.svelte';
  import SessionRow from '../components/SessionRow.svelte';
  import Stage from '../components/Stage.svelte';
  import { provideSurface, type Platform } from '../context';
  import { ListenController } from '../listen.svelte';

  /**
   * The toolbar popup (380 × 560 on desktop, full screen on Firefox for Android): the
   * listen stage with fixed slots, or the first-run provider panel until a decision exists.
   */
  interface Props {
    engine: EngineClient;
    platform: Platform;
  }

  let { engine, platform }: Props = $props();
  // svelte-ignore state_referenced_locally
  provideSurface({ engine, platform });

  // svelte-ignore state_referenced_locally
  const caps = platform.capabilities;
  const sources: ListenChoice[] = caps.tabCapture || caps.target === 'firefox' ? ['Microphone', 'Tab', 'Hum'] : ['Microphone', 'Hum'];
  // svelte-ignore state_referenced_locally
  const listen = new ListenController(engine, { recentLimit: 1, tabCapture: sources.includes('Tab') });
  let providers = $state<ProviderSettings | null>(null);
  let tab = $state<ActiveTab | null>(null);
  let stopWatching: (() => void) | null = null;
  let field = $state<LinkField | undefined>();

  const stage = $derived(listen.stage);
  const pageHost = $derived(tab && isAnalyzablePage(tab.url) ? new URL(tab.url!).hostname.replace(/^www\./, '') : null);
  const recent = $derived(listen.recent[0] ?? null);
  const panelLabel = caps.panel === 'sidebar' ? t('nav.sidebar') : t('nav.sidePanel');

  onMount(async () => {
    tab = await platform.activeTab();
    providers = await loadProviderSettings();
    stopWatching = watchProviderSettings((value) => (providers = value));
    await listen.start(tab?.audible && sources.includes('Tab') ? 'Tab' : null);
  });

  onDestroy(() => {
    stopWatching?.();
    listen.stop();
  });

  function submit(text: string) {
    if (isLink(text)) void listen.analyze(text);
    else void listen.searchLyrics(text);
  }

  function analyzePage() {
    if (!tab?.url) return;
    void listen.analyze(tab.url, { tab_id: tab.id ?? null, url: tab.url, title: tab.title ?? null, facts: null });
  }

  function focusField() {
    field?.focus();
  }

  async function openPanel() {
    // No await before this call: the browser needs the click's user gesture.
    await platform.openPanel(tab?.windowId).catch(() => platform.openApp('/listen'));
    platform.close();
  }

  async function openApp(route: Parameters<Platform['openApp']>[0]) {
    await platform.openApp(route);
    platform.close();
  }

  async function chooseCandidate(trackIndex: number) {
    const id = stage.sessionId;
    const candidate = stage.candidates[trackIndex];
    if (!id || !candidate) return;
    await engine.confirmCandidate(id, candidate.track);
    listen.stage = { ...stage, phase: 'matched', track: candidate.track };
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key === 'Escape' && showsResult(stage)) {
      event.preventDefault();
      listen.reset();
    }
  }
</script>

<svelte:window {onkeydown} />

{#snippet footer()}
  <div class="footer">
    <button class="btn text quiet" onclick={() => openApp('/library')}><span class="ic ic-stack"></span>{t('nav.library')}</button>
    {#if caps.panel}
      <button class="btn text quiet" onclick={openPanel}><span class="ic ic-panel"></span>{panelLabel}</button>
    {:else if caps.android}
      <button class="btn text quiet" onclick={() => openApp('/listen')}><span class="ic ic-expand"></span>{t('nav.fullView')}</button>
    {/if}
    <span class="grow"></span>
    <button class="btn icon plain" aria-label={t('nav.settings')} onclick={() => openApp('/settings/general')}><span class="ic ic-gear"></span></button>
  </div>
{/snippet}

{#snippet resultBody()}
  {#if stage.phase === 'matched' && stage.track && providers}
    <ResultView
      track={stage.track}
      preferred={providers.provider}
      custom={providers.custom}
      menuUp
      onopen={(url) => void platform.openUrl(url)}
      oncopy={(text) => void platform.copyText(text)}
    />
  {:else if stage.phase === 'candidates'}
    <CandidatesView candidates={stage.candidates} onchoose={(c) => void chooseCandidate(stage.candidates.indexOf(c))} />
  {/if}
{/snippet}

<div class={caps.android ? 'mobile touch' : 'popup'}>
  {#if !providers}
    <!-- Loading the provider decision: render nothing rather than flash the stage. -->
  {:else if providers.needsPanel}
    <div class="first-run">
      <ProviderPicker compact suggested={suggestProvider(tab?.url)} savedCustom={providers.custom} />
    </div>
  {:else if showsResult(stage)}
    <div class="top">
      <button class="back" aria-label={t('action.back')} onclick={() => listen.reset()}><span class="ic ic-chev-left"></span></button>
      <LinkField bind:this={field} onsubmit={submit} />
    </div>
    <div class="scroll">{@render resultBody()}</div>
    {@render footer()}
  {:else if caps.android}
    <div class="top"><LinkField bind:this={field} onsubmit={submit} /></div>
    <div class="chip-slot">
      {#if pageHost}
        <button class="chip" onclick={analyzePage}><span class="ic ic-mix s14"></span>{t('chip.analyzePage')} <span class="host">· {pageHost}</span></button>
      {/if}
    </div>
    <div class="stage">
      <Stage {listen} {sources} ontypelyric={focusField} ondownloadmelody={() => void engine.installMelodyIndex()} />
    </div>
    {#if stage.humming && stage.phase === 'listening'}
      <div class="hint callout">{t('mic.keepOpen')}</div>
    {/if}
    {#if recent}
      <div class="recent"><SessionRow entry={recent} onclick={() => openApp(`/session/${recent.id}`)} /></div>
    {/if}
    {@render footer()}
  {:else}
    <div class="top"><LinkField bind:this={field} onsubmit={submit} /></div>
    <div class="chip-slot">
      {#if pageHost}
        <button class="chip" onclick={analyzePage}><span class="ic ic-mix s14"></span>{t('chip.analyzePage')} <span class="host">· {pageHost}</span></button>
      {/if}
    </div>
    <Stage {listen} {sources} ontypelyric={focusField} ondownloadmelody={() => void engine.installMelodyIndex()} />
    {#if recent}
      <SessionRow entry={recent} extra="recent" onclick={() => openApp(`/session/${recent.id}`)} />
    {/if}
    {@render footer()}
  {/if}
</div>
