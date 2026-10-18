<script lang="ts">
  import { onMount } from 'svelte';
  import type { StorageUsage } from '../../engine/types';
  import { t } from '../../i18n';
  import { settings } from '../../settings/settings';
  import { useSurface } from '../context';
  import { mb } from './format';
  import { browserEstimate, storageView } from './storage';
  import SwitchRow from './SwitchRow.svelte';

  /** Settings › Storage: what Music Fetch takes in this browser, Clear cache, pinned audio. */
  const { engine, platform } = useSurface();
  const estimate = () => (platform.storageEstimate ? platform.storageEstimate() : browserEstimate());
  let usage = $state<StorageUsage | null>(null);
  let browserUsage = $state<{ usage?: number; quota?: number } | null>(null);
  let cleared = $state(false);
  let keepPinnedAudio = $state(true);
  const view = $derived(storageView(usage, browserUsage));

  async function load() {
    [usage, browserUsage] = await Promise.all([engine.storageUsage().catch(() => null), estimate()]);
  }

  onMount(async () => {
    keepPinnedAudio = await settings.keepPinnedAudio.getValue();
    await load();
  });

  async function clearCache() {
    await engine.clearCache().catch(() => undefined);
    cleared = true;
    await load();
  }
</script>

<h2 class="sr-only">{t('settings.storage')}</h2>
<div class="group">
  <div class="frow">
    <div class="k">
      {t('settings.onDisk')}
      {#if view}
        <span class="sub tnum">
          {view.other ? t('settings.onDisk.detailOther', mb(view.library), mb(view.cache), mb(view.melody), mb(view.other)) : t('settings.onDisk.detail', mb(view.library), mb(view.cache), mb(view.melody))}
        </span>
      {/if}
    </div>
    <div class="v">
      <span class="tnum">{view ? mb(view.total) : ''}</span>
      <button class="btn small" disabled={cleared} onclick={() => void clearCache()}>{cleared ? t('settings.clearCache.done') : t('settings.clearCache')}</button>
    </div>
  </div>
  <SwitchRow
    label={t('settings.keepPinnedAudio')}
    value={keepPinnedAudio}
    onchange={(v) => {
      keepPinnedAudio = v;
      void settings.keepPinnedAudio.setValue(v);
    }}
  />
</div>
{#if view?.low}<div class="gfoot problem" role="status">{t('settings.onDisk.low')}</div>{/if}
