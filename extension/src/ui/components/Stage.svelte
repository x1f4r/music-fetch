<script lang="ts">
  import { t, tm } from '../../i18n';
  import {
    isCapturing,
    isRunning,
    isTwoLine,
    megabytes,
    recoveryActions,
    ringPercent,
    stageLine,
    type RecoveryAction,
  } from '../../models/stage';
  import type { ListenChoice } from '../../settings/settings';
  import { MELODY_DOWNLOAD_BYTES, type ListenController } from '../listen.svelte';
  import ListenButton from './ListenButton.svelte';
  import Sheet from './Sheet.svelte';
  import SourceToggles from './SourceToggles.svelte';
  import TraceBand from './TraceBand.svelte';

  /**
   * The listen stage's slots: trace band, button, state line, source toggles and the
   * recovery row (plus the melody prompt). It renders no container: the surface places the
   * slots (absolute in the popup, a column in the side panel and app tab).
   */
  interface Props {
    listen: ListenController;
    sources?: readonly ListenChoice[];
    /** Narrow sidebar: the toggles drop their text. */
    iconsOnly?: boolean;
    /** Touch: equal-width toggles. */
    equal?: boolean;
    /** "Type a lyric": the surface focuses its link field. */
    ontypelyric?: () => void;
    /** A playlist or mix finished: show it in full. */
    onopensession?: (sessionId: string) => void;
    /** "Allow the microphone": the grant tab. */
    onallowmic?: () => void;
    onsitesettings?: () => void;
  }

  let { listen, sources, iconsOnly = false, equal = false, ontypelyric, onopensession, onallowmic, onsitesettings }: Props = $props();

  const stage = $derived(listen.stage);
  const running = $derived(isRunning(stage));
  const capturing = $derived(isCapturing(stage));
  const humSeconds = $derived(Math.round(stage.voicedMs / 1000));

  // The rate-limit countdown ticks once a second.
  let now = $state(Date.now());
  $effect(() => {
    if (stage.retryAt === null) return;
    const timer = setInterval(() => (now = Date.now()), 1000);
    return () => clearInterval(timer);
  });
  const line = $derived(stageLine(stage, now));
  const lineText = $derived(tm(line));
  const two = $derived(isTwoLine(line));
  const [first, second] = $derived.by(() => {
    if (!two) return [lineText, ''];
    const cut = lineText.indexOf('. ');
    return cut < 0 ? [lineText, ''] : [lineText.slice(0, cut + 1), lineText.slice(cut + 2)];
  });
  const recovery = $derived(recoveryActions(stage).filter((r) => (r.action === 'typeLyric' ? Boolean(ontypelyric) : true)));
  const melodyBytes = $derived(stage.download?.totalBytes ?? MELODY_DOWNLOAD_BYTES);

  const LABELS: Record<RecoveryAction, { key: Parameters<typeof t>[0]; icon?: string; text?: boolean }> = {
    stop: { key: 'action.stop', icon: 'ic-stop' },
    tryAgain: { key: 'action.tryAgain' },
    humIt: { key: 'action.humIt', icon: 'ic-hum' },
    typeLyric: { key: 'action.typeLyric', icon: 'ic-text', text: true },
    useMicrophone: { key: 'action.useMicrophone', icon: 'ic-mic' },
    allowMicrophone: { key: 'mic.allow' },
    siteSettings: { key: 'mic.openSiteSettings', text: true },
    ok: { key: 'action.ok' },
    open: { key: 'action.open' },
    cancelDownload: { key: 'action.cancel' },
  };

  function run(action: RecoveryAction) {
    switch (action) {
      case 'stop':
        return void listen.cancel();
      case 'tryAgain':
        return void listen.tryAgain();
      case 'humIt':
        return void listen.listenWith('Hum');
      case 'typeLyric':
        listen.reset();
        return ontypelyric?.();
      case 'useMicrophone':
        return void listen.listenWith('Microphone');
      case 'allowMicrophone':
        return onallowmic?.();
      case 'siteSettings':
        return onsitesettings?.();
      case 'ok':
        return listen.reset();
      case 'open':
        if (stage.sessionId) onopensession?.(stage.sessionId);
        return;
      case 'cancelDownload':
        return void listen.cancelDownload();
    }
  }
</script>

<TraceBand frames={stage.pitch} visible={capturing && stage.humming} label={t('cand.yourPitch', humSeconds)} />
<ListenButton
  live={capturing}
  hum={capturing && stage.humming}
  ring={ringPercent(stage)}
  ringLabel={t('cand.humLength')}
  ringSeconds={humSeconds}
  matching={stage.phase === 'working' && !stage.download}
  link={stage.kind === 'Link' && running}
  level={stage.level}
  label={running ? t('action.stop') : t('action.listen')}
  describedBy="stage-state"
  onclick={() => void listen.toggle()}
/>
<div class="state" class:active={stage.lineActive} class:two id="stage-state" role="status" aria-busy={stage.phase === 'working'}>
  {#if stage.phase === 'working' && !stage.download}<span class="spinner" aria-hidden="true"></span>{/if}{first}{#if second}<br />{second}{/if}
</div>
{#if stage.download}
  <div
    class="dl-bar progress"
    role="progressbar"
    aria-label={t('melody.title')}
    aria-valuemin="0"
    aria-valuemax={megabytes(melodyBytes)}
    aria-valuenow={megabytes(stage.download.fraction * melodyBytes)}
    aria-valuetext={t('melody.status.downloading', `${megabytes(stage.download.fraction * melodyBytes)} MB`, `${megabytes(melodyBytes)} MB`)}
  >
    <i style:width="{Math.round(stage.download.fraction * 100)}%"></i>
  </div>
{/if}
<SourceToggles value={listen.source} disabled={running || Boolean(stage.download)} available={sources} {iconsOnly} {equal} onchange={(s) => void listen.chooseSource(s)} />
<div class="recovery">
  {#each recovery as item (item.action)}
    {@const label = LABELS[item.action]}
    <button class="btn" class:filled={item.primary} class:text={label.text} onclick={() => run(item.action)}>
      {#if label.icon}<span class="ic {label.icon} s12"></span>{/if}{t(label.key)}
    </button>
  {/each}
</div>
{#if stage.melodyMissing}
  <Sheet title={t('melody.prompt.title')} onclose={() => listen.reset()}>
    <div class="secondary">{t('melody.prompt.message.web', `${megabytes(melodyBytes)} MB`)}</div>
    {#snippet actions()}
      <button class="btn text" onclick={() => listen.reset()}>{t('melody.notNow')}</button>
      <button class="btn filled" onclick={() => void listen.installMelody()}>{t('melody.download')}</button>
    {/snippet}
  </Sheet>
{/if}
