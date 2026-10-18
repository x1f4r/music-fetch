<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import type { EngineClient } from '../../engine/client';
  import { t } from '../../i18n';
  import { isRunning, showsResult } from '../../models/stage';
  import { suggestProvider } from '../../providers/services';
  import AskSheet from '../components/AskSheet.svelte';
  import CandidatesView from '../components/CandidatesView.svelte';
  import LinkField from '../components/LinkField.svelte';
  import ProviderPicker from '../components/ProviderPicker.svelte';
  import RecentRow from '../components/RecentRow.svelte';
  import ResultView from '../components/ResultView.svelte';
  import Stage from '../components/Stage.svelte';
  import { provideSurface, type Platform } from '../context';
  import { heardRows } from '../format';
  import { ListenSurface } from '../surface.svelte';

  /**
   * The toolbar popup (380 × 560 on desktop, full screen on Firefox for Android): the
   * listen stage with fixed slots, a result or candidates in place of the stage, or the
   * first-run provider panel until a decision exists.
   */
  interface Props {
    engine: EngineClient;
    platform: Platform;
  }

  let { engine, platform }: Props = $props();
  // svelte-ignore state_referenced_locally
  provideSurface({ engine, platform });
  // svelte-ignore state_referenced_locally
  const surface = new ListenSurface(engine, platform);
  // svelte-ignore state_referenced_locally
  const caps = platform.capabilities;
  const listen = surface.listen;
  const touch = caps.android;
  let field = $state<LinkField | undefined>();
  /** The session pinned from the result's pin button (sessions start unpinned). */
  let pinnedSession = $state<string | null>(null);

  const stage = $derived(listen.stage);
  const providers = $derived(surface.providers);
  const recent = $derived(surface.recentRow);
  const pinned = $derived(pinnedSession !== null && pinnedSession === stage.sessionId);
  const panelLabel = caps.panel === 'sidebar' ? t('nav.sidebar') : t('nav.sidePanel');
  /** Firefox's popup can't keep the microphone once it closes (plan §3.1): say so while humming. */
  const keepOpen = $derived(caps.target === 'firefox' && stage.humming && (stage.phase === 'listening' || stage.phase === 'starting'));
  const resultDetail = $derived.by(() => {
    if (stage.phase !== 'matched' || !stage.track || !stage.sessionId) return null;
    return heardRows({
      entry: {
        id: stage.sessionId,
        kind: stage.kind ?? 'Listen',
        status: 'Matched',
        created_at_ms: Date.now(),
        title: stage.track.title,
        subtitle: stage.track.artist,
        artwork_url: stage.track.artwork_url,
        track: stage.track,
        track_count: 1,
        duration_ms: null,
        source_url: stage.source === 'Tab' ? (surface.tab?.url ?? null) : null,
        pinned,
      },
      source: null,
      segments: [],
      candidates: [],
      transcript: null,
    });
  });

  onMount(() => void surface.start());
  onDestroy(() => surface.stop());

  async function openPanel() {
    // No await before this call: the browser needs the click's user gesture.
    await platform.openPanel(surface.tab?.windowId).catch(() => platform.openApp('/listen'));
    platform.close();
  }

  async function openApp(route: Parameters<Platform['openApp']>[0]) {
    await platform.openApp(route);
    platform.close();
  }

  async function chooseCandidate(index: number) {
    const id = stage.sessionId;
    const candidate = stage.candidates[index];
    if (!id || !candidate) return;
    await engine.confirmCandidate(id, candidate.track);
    listen.stage = { ...stage, phase: 'matched', track: candidate.track };
  }

  function onkeydown(event: KeyboardEvent) {
    const mod = event.metaKey || event.ctrlKey;
    if (mod && stage.phase === 'matched' && stage.track && !(event.target as HTMLElement | null)?.closest('input')) {
      if (event.key === 'Enter') {
        const link = surface.openInFor(stage.track);
        if (link) {
          event.preventDefault();
          void platform.openUrl(link.url);
        }
        return;
      }
      if (event.key.toLowerCase() === 'c' && !window.getSelection()?.toString()) {
        event.preventDefault();
        void platform.copyText(`${stage.track.artist} – ${stage.track.title}`);
        return;
      }
    }
    if (event.key !== 'Escape' || event.defaultPrevented) return;
    if (showsResult(stage)) {
      event.preventDefault();
      listen.reset();
    } else if (isRunning(stage)) {
      event.preventDefault();
      void listen.cancel();
    }
  }

  function onpaste(event: ClipboardEvent) {
    const target = event.target as HTMLElement | null;
    if (target?.closest('input, textarea')) return;
    if (surface.pasted(event.clipboardData?.getData('text') ?? '')) event.preventDefault();
  }

  async function share() {
    const track = stage.track;
    if (!track) return;
    const link = surface.openInFor(track);
    const shared = await platform.share({ title: track.title, text: `${track.artist} – ${track.title}`, url: link?.url });
    if (!shared) await platform.copyText(`${track.artist} – ${track.title}`);
  }
</script>

<svelte:window {onkeydown} {onpaste} />

{#snippet footer()}
  <nav class="footer" aria-label={t('nav.main')}>
    <button class="btn text quiet" onclick={() => openApp('/library')}><span class="ic ic-stack {touch ? 's18' : ''}"></span>{t('nav.library')}</button>
    {#if caps.panel}
      <button class="btn text quiet" onclick={openPanel}><span class="ic ic-panel"></span>{panelLabel}</button>
    {:else if touch}
      <button class="btn text quiet" onclick={() => openApp('/listen')}><span class="ic ic-expand s18"></span>{t('nav.fullView')}</button>
    {/if}
    <span class="grow"></span>
    <button class="btn icon plain" aria-label={t('nav.settings')} onclick={() => openApp('/settings/general')}><span class="ic ic-gear {touch ? 's18' : ''}"></span></button>
  </nav>
{/snippet}

{#snippet chip()}
  <div class="chip-slot">
    {#if surface.chipVisible}
      <button class="chip" onclick={() => void surface.analyzePage()}>
        <span class="ic ic-mix {touch ? 's18' : 's14'}"></span><span>{t('chip.analyzePage')} <span class="host">· {surface.pageHost}</span></span>
      </button>
    {/if}
  </div>
{/snippet}

{#snippet recentRow()}
  {#if keepOpen}
    <div class="hint" role="note">
      <span class="ic ic-info {touch ? 's18' : 's14'}"></span>{t('mic.keepOpen')}
      {#if caps.panel}
        <button class="btn text" onclick={openPanel}>{t('nav.openSidebar')}</button>
      {:else}
        <button class="btn text" onclick={() => openApp('/listen')}>{t('nav.fullView')}</button>
      {/if}
    </div>
  {:else if recent}
    {@const link = surface.openInFor(recent.track)}
    <RecentRow
      entry={recent}
      {touch}
      openIn={link ? { label: link.label, onclick: () => void platform.openUrl(link.url) } : null}
      progress={listen.progress[recent.id] ?? null}
      onclick={() => openApp(`/session/${recent.id}`)}
    />
  {/if}
{/snippet}

<main class={touch ? 'mobile touch' : 'popup'}>
  {#if !providers}
    <!-- Loading the provider decision: render nothing rather than flash the stage. -->
  {:else if providers.needsPanel}
    <div class="first-run">
      <ProviderPicker compact suggested={suggestProvider(surface.tab?.url)} savedCustom={providers.custom} />
    </div>
  {:else if showsResult(stage)}
    <div class="top">
      <button class="back" aria-label={t('action.back')} onclick={() => listen.reset()}><span class="ic ic-chev-left"></span></button>
      <LinkField bind:this={field} onsubmit={(text) => surface.submit(text)} />
    </div>
    <div class="scroll">
      {#if stage.phase === 'matched' && stage.track}
        <ResultView
          track={stage.track}
          preferred={providers.provider}
          custom={providers.custom}
          artSize={touch ? 200 : 160}
          {touch}
          details
          heard={resultDetail ?? []}
          {pinned}
          context={surface.resultContext}
          contextIcon={stage.source === 'Microphone' ? 'ic-mic' : 'ic-tab'}
          onpin={(value) => {
            pinnedSession = value ? stage.sessionId : null;
            if (stage.sessionId) void engine.setPinned(stage.sessionId, value);
          }}
          onopen={(url) => void platform.openUrl(url)}
          oncopy={(text) => void platform.copyText(text)}
          onshare={() => void share()}
        />
      {:else}
        <CandidatesView
          candidates={stage.candidates}
          onchoose={(c) => void chooseCandidate(stage.candidates.indexOf(c))}
          onhumagain={() => void listen.listenWith('Hum')}
          ontypelyric={() => {
            listen.reset();
            queueMicrotask(() => field?.focus());
          }}
        />
      {/if}
    </div>
    {@render footer()}
  {:else}
    <div class="top"><LinkField bind:this={field} onsubmit={(text) => surface.submit(text)} /></div>
    {@render chip()}
    {#if touch}
      <div class="stage">
        <Stage {listen} sources={surface.sources} equal ontypelyric={() => field?.focus()} onopensession={(id) => openApp(`/session/${id}`)} onallowmic={() => openApp('/grant-mic')} onsitesettings={() => void platform.openSiteSettings()} />
      </div>
      {@render recentRow()}
    {:else}
      <Stage {listen} sources={surface.sources} ontypelyric={() => field?.focus()} onopensession={(id) => openApp(`/session/${id}`)} onallowmic={() => openApp('/grant-mic')} onsitesettings={() => void platform.openSiteSettings()} />
      {@render recentRow()}
    {/if}
    {@render footer()}
  {/if}
  {#if surface.ask}
    <AskSheet
      title={surface.ask.title}
      site={surface.ask.site}
      durationMs={surface.ask.durationMs}
      thumbnail={null}
      oncancel={() => (surface.ask = null)}
      onanalyze={(choice, remember) => void surface.answerAsk(choice, remember)}
    />
  {/if}
</main>
