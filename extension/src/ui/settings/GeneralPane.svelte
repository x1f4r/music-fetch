<script lang="ts">
  import { onMount } from 'svelte';
  import { t } from '../../i18n';
  import { customProviderLink, isValidCustomProvider, isValidTemplate, providerFromKey, providerKey, type CustomProvider } from '../../providers/links';
  import { ALL_PROVIDERS, serviceName } from '../../providers/services';
  import { chooseProvider, settings, type ProviderSettings } from '../../settings/settings';
  import { useSurface } from '../context';
  import SwitchRow from './SwitchRow.svelte';

  /** Settings › General (design `app-settings-general`): where songs open, the custom provider, the side panel, notifications. */
  interface Props {
    providers: ProviderSettings | null;
  }

  let { providers }: Props = $props();
  const { platform } = useSurface();
  const caps = platform.capabilities;

  let openInApp = $state(false);
  let sidePanelOnClick = $state(false);
  let notify = $state(true);
  let customName = $state('');
  let customTemplate = $state('');

  const SAMPLE = { title: 'Houdini', artist: 'Dua Lipa', artists: ['Dua Lipa'], isrc: 'GBAHT2300520' };
  const customPreview = $derived(isValidTemplate(customTemplate) ? customProviderLink(SAMPLE, customTemplate) : null);
  const currentKey = $derived(providers ? providerKey(providers.provider) : 'spotify');
  const currentName = $derived(providers?.provider.kind === 'service' ? serviceName(providers.provider.service) : (providers?.custom?.name ?? 'Spotify'));

  onMount(async () => {
    [openInApp, sidePanelOnClick, notify] = await Promise.all([settings.openInApp.getValue(), settings.sidePanelOnClick.getValue(), settings.notifyInBackground.getValue()]);
    const custom = await settings.customProvider.getValue();
    customName = custom?.name ?? '';
    customTemplate = custom?.template ?? '';
  });

  async function setProvider(key: string) {
    const provider = providerFromKey(key);
    if (!provider) return;
    if (provider.kind === 'custom') {
      const custom: CustomProvider = { name: customName, template: customTemplate };
      if (isValidCustomProvider(custom)) await chooseProvider(provider, custom);
      return;
    }
    await chooseProvider(provider);
  }

  async function saveCustom() {
    const custom: CustomProvider = { name: customName.trim(), template: customTemplate.trim() };
    if (!custom.name && !custom.template) await settings.customProvider.removeValue();
    else if (isValidCustomProvider(custom)) await settings.customProvider.setValue(custom);
  }
</script>

<h2 class="sr-only">{t('settings.general')}</h2>
<div class="group">
  <div class="frow">
    <div class="k"><label for="open-songs-in">{t('settings.openSongsIn')}</label></div>
    <div class="v">
      <select id="open-songs-in" class="popup-btn" value={currentKey} onchange={(e) => void setProvider(e.currentTarget.value)}>
        {#each ALL_PROVIDERS as service (service)}
          <option value={providerKey({ kind: 'service', service })}>{serviceName(service)}</option>
        {/each}
        {#if isValidCustomProvider({ name: customName, template: customTemplate })}
          <option value="custom">{customName.trim()}</option>
        {/if}
      </select>
    </div>
  </div>
  <SwitchRow
    label={t('settings.openInApp')}
    sub={t('settings.openInApp.sub', currentName)}
    value={openInApp}
    onchange={(v) => {
      openInApp = v;
      void settings.openInApp.setValue(v);
    }}
  />
</div>

<h3 class="ghead">{t('settings.customProvider')}</h3>
<div class="group">
  <div class="frow">
    <div class="k"><label for="custom-name">{t('provider.customName')}</label></div>
    <div class="v"><span class="field small"><input id="custom-name" bind:value={customName} onchange={saveCustom} autocomplete="off" /></span></div>
  </div>
  <div class="frow">
    <div class="k"><label for="custom-template">{t('settings.customTemplate')}</label><span class="sub">{t('settings.customTemplate.sub')}</span></div>
    <div class="v">
      <span class="field small"><input id="custom-template" bind:value={customTemplate} onchange={saveCustom} placeholder={t('provider.customPlaceholder')} spellcheck="false" autocomplete="off" /></span>
    </div>
  </div>
</div>
{#if customTemplate.trim() && !customPreview}
  <div class="gfoot problem" role="alert">{t('provider.customInvalid')}</div>
{:else if customPreview}
  <div class="gfoot preview">{t('provider.customPreviewLast', customPreview)}</div>
{:else}
  <div class="gfoot">{t('provider.customHint')}</div>
{/if}

<div class="group">
  {#if caps.panel === 'sidePanel'}
    <SwitchRow
      label={t('settings.sidePanelOnClick')}
      sub={t('settings.sidePanelOnClick.sub')}
      value={sidePanelOnClick}
      onchange={(v) => {
        sidePanelOnClick = v;
        void settings.sidePanelOnClick.setValue(v);
      }}
    />
  {/if}
  <SwitchRow
    label={t('settings.notify')}
    value={notify}
    onchange={(v) => {
      notify = v;
      void settings.notifyInBackground.setValue(v);
    }}
  />
</div>
