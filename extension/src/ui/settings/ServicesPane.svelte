<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { t } from '../../i18n';
  import { PROVIDER_IDS } from '../../platform/secrets';
  import { useSurface } from '../context';
  import ProviderGroup from './ProviderGroup.svelte';
  import { ServicesController } from './services.svelte';

  /** Settings › Services (X11): the user's own ACRCloud and AudD keys, humming first. */
  const { platform } = useSurface();
  const services = new ServicesController({
    requestHost: (url) => platform.requestHost(url),
    ...(platform.checkProvider ? { check: (id, keys) => platform.checkProvider!(id, keys) } : {}),
    allowLoopbackHttp: import.meta.env.MODE === 'e2e',
  });

  onMount(() => void services.start());
  onDestroy(() => services.dispose());
</script>

<h2 class="sr-only">{t('settings.services')}</h2>
<p class="pane-intro">{t('settings.services.intro')}</p>
{#each PROVIDER_IDS as id (id)}
  <ProviderGroup {id} {services} />
{/each}
<p class="pane-note"><span class="ic ic-shield s12" aria-hidden="true"></span>{t('settings.services.privacy', 'ACRCloud and AudD')}</p>
