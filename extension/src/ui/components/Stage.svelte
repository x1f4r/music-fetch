<script lang="ts">
  import { t, tm } from '../../i18n';
  import { isRunning, ringPercent } from '../../models/stage';
  import type { ListenChoice } from '../../settings/settings';
  import type { ListenController } from '../listen.svelte';
  import ListenButton from './ListenButton.svelte';
  import SourceToggles from './SourceToggles.svelte';
  import TraceBand from './TraceBand.svelte';

  /**
   * The listen stage's slots: trace band, button, state line, source toggles and the
   * recovery row. It renders no container: the surface places the slots (absolute in the
   * popup, a column in the side panel and app tab).
   */
  interface Props {
    listen: ListenController;
    sources?: readonly ListenChoice[];
    /** "Type a lyric": the surface focuses its link field. */
    ontypelyric?: () => void;
    /** Hum without the melody library. */
    ondownloadmelody?: () => void;
    /** A playlist or mix finished: show it in full. */
    onopensession?: (sessionId: string) => void;
  }

  let { listen, sources, ontypelyric, ondownloadmelody, onopensession }: Props = $props();

  const stage = $derived(listen.stage);
  const running = $derived(isRunning(stage));
  const capturing = $derived(stage.phase === 'listening' || (stage.phase === 'starting' && stage.kind !== 'Link' && stage.kind !== 'Lyrics'));
  const humSeconds = $derived(Math.round(stage.voicedMs / 1000));

  function tryAgain() {
    listen.reset();
    void listen.toggle();
  }

  async function humIt() {
    listen.reset();
    await listen.chooseSource('Hum');
    void listen.toggle();
  }

  async function useMicrophone() {
    listen.reset();
    await listen.chooseSource('Microphone');
    void listen.toggle();
  }
</script>

<TraceBand frames={stage.pitch} visible={capturing && stage.humming} label={t('cand.yourHum', humSeconds)} />
<ListenButton
  live={capturing}
  hum={capturing && stage.humming}
  ring={ringPercent(stage)}
  matching={stage.phase === 'working'}
  level={stage.level}
  label={running ? t('action.stop') : t('action.listen')}
  describedBy="stage-state"
  onclick={() => void listen.toggle()}
/>
<div class="state" class:active={stage.lineActive} id="stage-state" role="status">
  {#if stage.phase === 'working'}<span class="spinner" aria-hidden="true"></span>{/if}{tm(stage.line)}
</div>
<SourceToggles value={listen.source} disabled={running} available={sources} onchange={(s) => void listen.chooseSource(s)} />
<div class="recovery">
  {#if stage.melodyMissing && ondownloadmelody}
    <button class="btn" onclick={ondownloadmelody}>{t('melody.download')}</button>
    <button class="btn text" onclick={() => listen.reset()}>{t('melody.notNow')}</button>
  {:else if stage.phase === 'nomatch' && (stage.reason === 'CaptureSilent' || stage.line.key === 'state.tab.blocked') && stage.source === 'Tab'}
    <button class="btn" onclick={useMicrophone}><span class="ic ic-mic s12"></span>{t('action.useMicrophone')}</button>
  {:else if stage.phase === 'nomatch'}
    <button class="btn" onclick={tryAgain}>{t('action.tryAgain')}</button>
    {#if stage.source !== 'Hum'}<button class="btn" onclick={humIt}><span class="ic ic-hum s12"></span>{t('action.humIt')}</button>{/if}
    {#if ontypelyric}<button class="btn text" onclick={ontypelyric}><span class="ic ic-text s12"></span>{t('action.typeLyric')}</button>{/if}
  {:else if stage.phase === 'completed' && stage.sessionId && onopensession}
    <button class="btn" onclick={() => onopensession(stage.sessionId!)}>{t('action.open')}</button>
  {:else if stage.phase === 'failed'}
    <button class="btn" onclick={() => listen.reset()}>{t('action.tryAgain')}</button>
  {/if}
</div>
