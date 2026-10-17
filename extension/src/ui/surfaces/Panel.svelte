<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { EngineClient } from '../../engine/client';
  import { isLink } from '../../engine/links';
  import { t } from '../../i18n';
  import { showsResult } from '../../models/stage';
  import { isAnalyzablePage, type ActiveTab } from '../../platform/surfaces';
  import { suggestProvider } from '../../providers/services';
  import { loadProviderSettings, watchProviderSettings, type ListenChoice, type ProviderSettings } from '../../settings/settings';
  import CandidatesView from '../components/CandidatesView.svelte';
  import LibraryList from '../components/LibraryList.svelte';
  import LinkField from '../components/LinkField.svelte';
  import ProviderPicker from '../components/ProviderPicker.svelte';
  import ResultView from '../components/ResultView.svelte';
  import Stage from '../components/Stage.svelte';
  import { provideSurface, type Platform } from '../context';
  import { ListenController } from '../listen.svelte';

  /**
   * The Chromium side panel and the Firefox sidebar: the listen stage on top and the
   * recent sessions beneath; it stays open across tabs, so long hums and mixes live here.
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
  const listen = new ListenController(engine, { recentLimit: 50, tabCapture: sources.includes('Tab') });
  let providers = $state<ProviderSettings | null>(null);
  let tab = $state<ActiveTab | null>(null);
  let field = $state<LinkField | undefined>();
  let stopWatching: (() => void) | null = null;

  const stage = $derived(listen.stage);
  const pageHost = $derived(tab && isAnalyzablePage(tab.url) ? new URL(tab.url!).hostname.replace(/^www\./, '') : null);

  async function refreshTab() {
    tab = await platform.activeTab();
  }

  onMount(async () => {
    await refreshTab();
    providers = await loadProviderSettings();
    stopWatching = watchProviderSettings((value) => (providers = value));
    await listen.start(null);
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

  async function chooseCandidate(index: number) {
    const id = stage.sessionId;
    const candidate = stage.candidates[index];
    if (!id || !candidate) return;
    await engine.confirmCandidate(id, candidate.track);
    listen.stage = { ...stage, phase: 'matched', track: candidate.track };
  }
</script>

<svelte:window onfocus={() => void refreshTab()} />

<div class="panel">
  {#if !providers}
    <!-- Waiting for the stored provider decision. -->
  {:else if providers.needsPanel}
    <div class="first-run">
      <ProviderPicker compact suggested={suggestProvider(tab?.url)} savedCustom={providers.custom} />
    </div>
  {:else}
    <div class="top">
      {#if showsResult(stage)}
        <button class="back btn icon plain" aria-label={t('action.back')} onclick={() => listen.reset()}><span class="ic ic-chev-left"></span></button>
      {/if}
      <LinkField bind:this={field} onsubmit={submit} />
    </div>
    {#if showsResult(stage)}
      <div class="list">
        {#if stage.phase === 'matched' && stage.track}
          <ResultView
            track={stage.track}
            preferred={providers.provider}
            custom={providers.custom}
            artSize={120}
            onopen={(url) => void platform.openUrl(url)}
            oncopy={(text) => void platform.copyText(text)}
          />
        {:else}
          <CandidatesView candidates={stage.candidates} onchoose={(c) => void chooseCandidate(stage.candidates.indexOf(c))} />
        {/if}
      </div>
    {:else}
      <div class="chip-slot">
        {#if pageHost}
          <button class="chip" onclick={analyzePage}><span class="ic ic-mix s14"></span><span>{t('chip.analyzePage')} <span class="host">· {pageHost}</span></span></button>
        {/if}
      </div>
      <div class="stage">
        <Stage {listen} {sources} ontypelyric={() => field?.focus()} ondownloadmelody={() => void engine.installMelodyIndex()} />
      </div>
      <div class="section-head">{t('library.recent')}</div>
      <div class="list">
        <LibraryList entries={listen.recent} pinnedSection={false} onopen={(entry) => void platform.openApp(`/session/${entry.id}`)} />
      </div>
    {/if}
    <div class="footer">
      <button class="btn text quiet" onclick={() => void platform.openApp('/library')}><span class="ic ic-stack"></span>{t('nav.library')}</button>
      <span class="grow"></span>
      <button class="btn icon plain" aria-label={t('nav.settings')} onclick={() => void platform.openApp('/settings/general')}><span class="ic ic-gear"></span></button>
    </div>
  {/if}
</div>
