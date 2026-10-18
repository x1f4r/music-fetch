<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { t } from '../../i18n';
  import { loadProviderKeys, summarize, watchSecrets } from '../../platform/secrets';
  import { useSurface } from '../context';

  /**
   * Settings › Privacy (extension plan §5.8): what leaves the browser and what stays, in
   * plain words. The paid services appear only when the user added keys for them.
   */
  const { platform } = useSurface();
  const firefox = platform.capabilities.target === 'firefox';
  let services = $state<string[]>([]);
  let stop: (() => void) | null = null;

  async function load() {
    const summary = summarize(await loadProviderKeys().catch(() => ({})));
    services = [summary.acrHumming.set || summary.acr.set ? 'ACRCloud' : null, summary.audd.set ? 'AudD' : null].filter((s): s is string => s !== null);
  }

  onMount(() => {
    void load();
    stop = watchSecrets(() => void load());
  });
  onDestroy(() => stop?.());
</script>

<h2 class="sr-only">{t('settings.privacy')}</h2>
<h3 class="ghead">{t('settings.privacy.leaves')}</h3>
<ul class="group plain-list">
  <li class="frow prose">{t('settings.privacy.fingerprints')}</li>
  <li class="frow prose">{t('settings.privacy.links')}</li>
  <li class="frow prose">{t('settings.privacy.lyrics')}</li>
  {#if services.length}<li class="frow prose">{t('settings.privacy.services', services.join(' and '))}</li>{/if}
</ul>
<h3 class="ghead">{t('settings.privacy.stays')}</h3>
<ul class="group plain-list">
  <li class="frow prose">{t('settings.privacy.hums')}</li>
  <li class="frow prose">{t('settings.privacy.library')}</li>
</ul>
<p class="gfoot">{t('settings.privacy.never')}</p>
{#if firefox}<p class="gfoot">{t('settings.privacy.firefoxConsent')}</p>{/if}
