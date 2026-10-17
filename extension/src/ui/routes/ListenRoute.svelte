<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { isLink } from '../../engine/links';
  import { t } from '../../i18n';
  import { showsResult } from '../../models/stage';
  import type { ListenChoice, ProviderSettings } from '../../settings/settings';
  import CandidatesView from '../components/CandidatesView.svelte';
  import LinkField from '../components/LinkField.svelte';
  import ResultView from '../components/ResultView.svelte';
  import Stage from '../components/Stage.svelte';
  import { useSurface } from '../context';
  import { ListenController } from '../listen.svelte';

  interface Props {
    providers: ProviderSettings | null;
    onopen: (sessionId: string) => void;
  }

  let { providers, onopen }: Props = $props();
  const { engine, platform } = useSurface();
  const sources: ListenChoice[] = ['Microphone', 'Hum'];
  const listen = new ListenController(engine, { recentLimit: 1, tabCapture: false });
  let field = $state<LinkField | undefined>();
  const stage = $derived(listen.stage);

  onMount(() => void listen.start('Microphone'));
  onDestroy(() => listen.stop());

  function submit(text: string) {
    if (isLink(text)) void listen.analyze(text);
    else void listen.searchLyrics(text);
  }

  // Links and files finish as sessions: show them in full.
  $effect(() => {
    if (stage.phase === 'completed' && stage.sessionId) onopen(stage.sessionId);
  });
</script>

<div class="toolbar"><span class="wtitle">{t('nav.listen')}</span><span class="grow"></span></div>
<div class="scroll-area">
  <div class="listen-page">
    <div class="field-row"><LinkField bind:this={field} onsubmit={submit} /></div>
    {#if showsResult(stage)}
      {#if stage.phase === 'matched' && stage.track && providers}
        <ResultView
          track={stage.track}
          preferred={providers.provider}
          custom={providers.custom}
          artSize={200}
          onopen={(url) => void platform.openUrl(url)}
          oncopy={(text) => void platform.copyText(text)}
        />
      {:else}
        <CandidatesView
          candidates={stage.candidates}
          onchoose={(c) => {
            if (stage.sessionId) void engine.confirmCandidate(stage.sessionId, c.track).then(() => onopen(stage.sessionId!));
          }}
        />
      {/if}
      <div class="actions"><button class="btn" onclick={() => listen.reset()}>{t('action.back')}</button></div>
    {:else}
      <div class="stage">
        <Stage {listen} {sources} ontypelyric={() => field?.focus()} ondownloadmelody={() => void engine.installMelodyIndex()} />
      </div>
    {/if}
  </div>
</div>
