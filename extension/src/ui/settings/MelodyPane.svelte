<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { t } from '../../i18n';
  import { settings } from '../../settings/settings';
  import { useSurface } from '../context';
  import { MelodyController } from './melody.svelte';
  import { mb } from './format';
  import SwitchRow from './SwitchRow.svelte';

  /** Settings › Melody (design `app-settings-melody`): the library's five states and "While humming". */
  const { engine, platform } = useSurface();
  const caps = platform.capabilities;
  const melody = new MelodyController(engine);
  let sungLyrics = $state(false);
  const view = $derived(melody.view);

  onMount(async () => {
    sungLyrics = await settings.searchSungLyrics.getValue();
    await melody.start();
  });
  onDestroy(() => melody.dispose());
</script>

<h2 class="sr-only">{t('settings.melody')}</h2>
<div class="group">
  <div class="frow melody-row">
    <div class="k">
      {t('melody.title')}
      {#if view.phase === 'downloading'}
        <span class="sub tnum">{t('melody.status.downloading', mb(view.bytes * view.fraction), mb(view.bytes))}</span>
      {:else if view.phase === 'failed'}
        <span class="sub problem" role="alert">{t('melody.error')}</span>
      {:else if view.phase === 'update'}
        <span class="sub">{t('melody.status.updateAvailable', mb(view.bytes))}</span>
      {:else if view.phase === 'installed'}
        <span class="sub tnum">
          {view.updated ? t('melody.status.installedDate', view.songs.toLocaleString(), mb(view.bytes), view.updated) : t('melody.status.installed', view.songs.toLocaleString(), mb(view.bytes))}
        </span>
        {#if view.check === 'failed'}<span class="sub problem" role="status">{t('melody.checkFailed')}</span>{/if}
      {:else if view.phase === 'none'}
        <span class="sub">{t('melody.status.notInstalled', mb(view.bytes))}</span>
      {/if}
    </div>
    <div class="v">
      {#if view.phase === 'downloading'}
        <div
          class="progress melody-bar"
          role="progressbar"
          aria-label={t('melody.title')}
          aria-valuemin="0"
          aria-valuemax="100"
          aria-valuenow={Math.round(view.fraction * 100)}
          aria-valuetext={t('melody.status.downloading', mb(view.bytes * view.fraction), mb(view.bytes))}
        >
          <i style:width="{Math.round(view.fraction * 100)}%"></i>
        </div>
        <button class="btn small" onclick={() => void melody.cancel()}>{t('action.cancel')}</button>
      {:else if view.phase === 'failed'}
        <button class="btn small" onclick={() => void melody.download()}>{t('action.tryAgain')}</button>
      {:else if view.phase === 'update'}
        <button class="btn small filled" onclick={() => void melody.download()}>{t('melody.update')}</button>
      {:else if view.phase === 'installed'}
        <button class="btn small" disabled={view.check === 'checking' || view.check === 'upToDate'} onclick={() => void melody.checkForUpdates()}>
          {view.check === 'checking' ? t('melody.status.checking') : view.check === 'upToDate' ? t('melody.upToDate') : t('melody.checkForUpdates')}
        </button>
        <button class="btn small text" onclick={() => void melody.remove()}>{t('melody.remove')}</button>
      {:else if view.phase === 'none'}
        <button class="btn small filled" onclick={() => void melody.download()}>{t('melody.download')}</button>
      {/if}
    </div>
  </div>
</div>
<div class="gfoot">
  {t('melody.footer.web')}
  {#if view.phase === 'installed' && view.encoder}<span class="gfoot-line">{t('melody.encoder')}</span>{/if}
</div>
<h3 class="ghead">{t('melody.whileHumming')}</h3>
<div class="group">
  <SwitchRow
    label={t('melody.alsoLyrics')}
    sub={caps.target === 'chromium' ? t('melody.alsoLyrics.sub') : t('melody.alsoLyrics.unavailable')}
    value={sungLyrics && caps.target === 'chromium'}
    disabled={caps.target !== 'chromium'}
    onchange={(v) => {
      sungLyrics = v;
      void settings.searchSungLyrics.setValue(v);
    }}
  />
</div>
