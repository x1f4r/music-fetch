<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { t } from '../../i18n';
  import { isRunning, showsResult } from '../../models/stage';
  import type { ProviderSettings } from '../../settings/settings';
  import CandidatesView from '../components/CandidatesView.svelte';
  import LinkField from '../components/LinkField.svelte';
  import RecentRow from '../components/RecentRow.svelte';
  import ResultView from '../components/ResultView.svelte';
  import Stage from '../components/Stage.svelte';
  import { useSurface } from '../context';
  import { followPendingStart, isDemo } from '../../triggers/pending';
  import { ListenSurface } from '../surface.svelte';

  /**
   * Listen in the app tab: the stage at 112 with the link field, a local file (Open a
   * file, or drop one anywhere on the page) and the last session.
   */
  interface Props {
    providers: ProviderSettings | null;
    narrow: boolean;
    onopen: (sessionId: string) => void;
    onsettings: () => void;
  }

  let { providers, narrow, onopen, onsettings }: Props = $props();
  const { engine, platform } = useSurface();
  const surface = new ListenSurface(engine, platform);
  const listen = surface.listen;
  // The app tab has no tab of its own to capture.
  const sources = surface.sources.filter((s) => s !== 'Tab');
  let field = $state<LinkField | undefined>();
  let picker = $state<HTMLInputElement | undefined>();
  let dragging = $state(false);
  let fileError = $state<string | null>(null);
  const stage = $derived(listen.stage);
  const recent = $derived(surface.recentRow);

  let stopPending: (() => void) | null = null;

  onMount(async () => {
    await surface.start();
    if (listen.source === 'Tab') await listen.chooseSource('Microphone');
    // The Hum shortcut where no side panel or sidebar exists opens this route to hum.
    if (!isDemo()) stopPending = followPendingStart((source) => void listen.listenWith(source));
  });
  onDestroy(() => {
    stopPending?.();
    surface.stop();
  });

  // Links and files finish as sessions: show them in full.
  $effect(() => {
    if (stage.phase === 'completed' && stage.sessionId) onopen(stage.sessionId);
  });

  async function analyzeFile(file: File | undefined) {
    if (!file) return;
    fileError = null;
    try {
      const id = await engine.analyzeFile(file.name, await file.arrayBuffer(), 'Auto');
      onopen(id);
    } catch (error) {
      fileError = error instanceof Error ? error.message : String(error);
    }
  }

  function ondrop(event: DragEvent) {
    event.preventDefault();
    dragging = false;
    void analyzeFile(event.dataTransfer?.files[0]);
  }

  function onkeydown(event: KeyboardEvent) {
    if (event.key !== 'Escape' || event.defaultPrevented) return;
    if (showsResult(stage)) listen.reset();
    else if (isRunning(stage)) void listen.cancel();
  }
</script>

<svelte:window {onkeydown} />

<div class="toolbar">
  <h1 class="wtitle">{t('nav.listen')}</h1>
  <span class="grow"></span>
  {#if narrow}<button class="btn icon plain" aria-label={t('nav.settings')} onclick={onsettings}><span class="ic ic-gear"></span></button>{/if}
</div>
<div
  class="listen-page"
  class:dragging
  role="region"
  aria-label={t('nav.listen')}
  ondragover={(e) => (e.preventDefault(), (dragging = true))}
  ondragleave={() => (dragging = false)}
  {ondrop}
>
  <div class="field-row">
    <LinkField bind:this={field} onsubmit={(text) => surface.submit(text)} />
    <button class="btn icon" aria-label={t('action.openFile')} title={t('action.openFile')} onclick={() => picker?.click()}><span class="ic ic-note"></span></button>
    <input bind:this={picker} class="sr-only" type="file" accept="audio/*,video/*" tabindex="-1" aria-hidden="true" onchange={(e) => void analyzeFile(e.currentTarget.files?.[0])} />
  </div>
  {#if showsResult(stage) && providers}
    <div class="result-area">
      {#if stage.phase === 'matched' && stage.track}
        <ResultView
          track={stage.track}
          preferred={providers.provider}
          custom={providers.custom}
          artSize={200}
          onopen={(url) => void platform.openUrl(url)}
          oncopy={(text) => void platform.copyText(text)}
          onpin={(value) => stage.sessionId && void engine.setPinned(stage.sessionId, value)}
        />
      {:else}
        <CandidatesView
          candidates={stage.candidates}
          onchoose={(c) => {
            if (stage.sessionId) void engine.confirmCandidate(stage.sessionId, c.track).then(() => onopen(stage.sessionId!));
          }}
          onhumagain={() => void listen.listenWith('Hum')}
          ontypelyric={() => (listen.reset(), queueMicrotask(() => field?.focus()))}
        />
      {/if}
      <div class="actions"><button class="btn" onclick={() => listen.reset()}><span class="ic ic-chev-left s14"></span>{t('action.back')}</button></div>
    </div>
  {:else}
    <div class="stage app-stage">
      <Stage {listen} {sources} ontypelyric={() => field?.focus()} onopensession={onopen} onallowmic={() => (location.hash = '#/grant-mic')} onsitesettings={() => void platform.openSiteSettings()} onopenlink={(url) => void surface.openAndAnalyze(url)} />
    </div>
    <p class="drop-hint callout secondary" class:problem={fileError}>{fileError ?? (dragging ? t('state.drop') : t('file.hint'))}</p>
    {#if recent}
      {@const link = surface.openInFor(recent.track)}
      <div class="app-recent">
        <RecentRow entry={recent} openIn={link ? { label: link.label, onclick: () => void platform.openUrl(link.url) } : null} progress={listen.progress[recent.id] ?? null} onclick={() => onopen(recent.id)} />
      </div>
    {/if}
  {/if}
</div>
