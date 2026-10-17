<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { browser } from 'wxt/browser';
  import type { MelodyIndexStatus } from '../../engine/types';
  import { t } from '../../i18n';
  import {
    customProviderLink,
    isValidCustomProvider,
    isValidTemplate,
    providerFromKey,
    providerKey,
    type CustomProvider,
  } from '../../providers/links';
  import { ALL_PROVIDERS, serviceName } from '../../providers/services';
  import {
    chooseProvider,
    settings,
    type LowPowerSetting,
    type MicGrant,
    type ProviderSettings,
  } from '../../settings/settings';
  import { useSurface } from '../context';
  import { SETTINGS_PANES, type SettingsPane } from '../router.svelte';

  interface Props {
    pane: SettingsPane;
    providers: ProviderSettings | null;
    onpane: (pane: SettingsPane) => void;
  }

  let { pane, providers, onpane }: Props = $props();
  const { engine, platform } = useSurface();

  const PANES: Record<SettingsPane, { icon: string; label: string }> = {
    general: { icon: 'ic-gear', label: t('settings.general') },
    listening: { icon: 'ic-mic', label: t('settings.listening') },
    melody: { icon: 'ic-hum', label: t('settings.melody') },
    privacy: { icon: 'ic-shield', label: t('settings.privacy') },
    about: { icon: 'ic-info', label: t('settings.about') },
  };

  let openInApp = $state(false);
  let sidePanelOnClick = $state(false);
  let notify = $state(true);
  let autoStop = $state(true);
  let lowPower = $state<LowPowerSetting>('auto');
  let micGrant = $state<MicGrant>('unknown');
  let melody = $state<MelodyIndexStatus | null>(null);
  let downloading = $state<number | null>(null);
  let installId: string | null = null;
  let customName = $state('');
  let customTemplate = $state('');
  let stops: (() => void)[] = [];

  const SAMPLE = { title: 'Take On Me', artist: 'a-ha', artists: ['a-ha'], isrc: 'NOA018500030' };
  const customPreview = $derived(isValidTemplate(customTemplate) ? customProviderLink(SAMPLE, customTemplate) : null);
  const currentKey = $derived(providers ? providerKey(providers.provider) : 'spotify');
  const version = browser.runtime.getManifest().version;

  onMount(async () => {
    [openInApp, sidePanelOnClick, notify, autoStop, lowPower, micGrant] = await Promise.all([
      settings.openInApp.getValue(),
      settings.sidePanelOnClick.getValue(),
      settings.notifyInBackground.getValue(),
      settings.autoStop.getValue(),
      settings.lowPower.getValue(),
      settings.micGrant.getValue(),
    ]);
    const custom = await settings.customProvider.getValue();
    customName = custom?.name ?? '';
    customTemplate = custom?.template ?? '';
    melody = await engine.melodyIndexStatus().catch(() => null);
    stops.push(
      engine.subscribe({
        sessionEvent: (id, event) => {
          if (id !== installId || typeof event !== 'object') return;
          if ('Phase' in event && event.Phase.phase === 'Downloading') downloading = event.Phase.fraction ?? 0;
          if ('Finished' in event || 'Failed' in event) {
            downloading = null;
            installId = null;
            void engine.melodyIndexStatus().then((status) => (melody = status));
          }
        },
      }),
    );
  });
  onDestroy(() => stops.forEach((stop) => stop()));

  async function setProvider(key: string) {
    const provider = providerFromKey(key);
    if (!provider) return;
    if (provider.kind === 'custom') {
      const custom: CustomProvider = { name: customName, template: customTemplate };
      if (!isValidCustomProvider(custom)) return;
      await chooseProvider(provider, custom);
    } else {
      await chooseProvider(provider);
    }
  }

  async function saveCustom() {
    const custom: CustomProvider = { name: customName.trim(), template: customTemplate.trim() };
    if (!custom.name && !custom.template) await settings.customProvider.removeValue();
    else if (isValidCustomProvider(custom)) await settings.customProvider.setValue(custom);
  }

  async function download() {
    downloading = 0;
    installId = await engine.installMelodyIndex().catch(() => null);
    if (!installId) downloading = null;
  }

  function bytes(n: number): string {
    return n >= 1e6 ? `${(n / 1e6).toFixed(1)} MB` : `${Math.round(n / 1e3)} KB`;
  }
</script>

{#snippet toggle(label: string, sub: string | null, value: boolean, onchange: (value: boolean) => void)}
  <div class="frow">
    <div class="k">{label}{#if sub}<span class="sub">{sub}</span>{/if}</div>
    <div class="v"><button class="switch" class:on={value} role="switch" aria-checked={value} aria-label={label} onclick={() => onchange(!value)}></button></div>
  </div>
{/snippet}

<div class="toolbar"><span class="wtitle">{t('nav.settings')}</span></div>
<div class="scroll-area">
  <div class="settings">
    <nav class="nav-list" aria-label={t('nav.settings')}>
      {#each SETTINGS_PANES as id (id)}
        <a class="ni" class:on={pane === id} href="#/settings/{id}" aria-current={pane === id ? 'page' : undefined} onclick={(e) => (e.preventDefault(), onpane(id))}>
          <span class="ic {PANES[id].icon}"></span>{PANES[id].label}
        </a>
      {/each}
    </nav>
    <div class="form">
      {#if pane === 'general'}
        <div class="group">
          <div class="frow">
            <div class="k">{t('settings.openSongsIn')}</div>
            <div class="v">
              <select class="popup-btn" aria-label={t('settings.openSongsIn')} value={currentKey} onchange={(e) => void setProvider(e.currentTarget.value)}>
                {#each ALL_PROVIDERS as service (service)}
                  <option value={providerKey({ kind: 'service', service })}>{serviceName(service)}</option>
                {/each}
                {#if isValidCustomProvider({ name: customName, template: customTemplate })}
                  <option value="custom">{customName.trim()}</option>
                {/if}
              </select>
            </div>
          </div>
          {@render toggle(t('settings.openInApp'), t('settings.openInApp.sub'), openInApp, (v) => {
            openInApp = v;
            void settings.openInApp.setValue(v);
          })}
        </div>

        <div class="ghead">{t('settings.customProvider')}</div>
        <div class="group">
          <div class="frow">
            <div class="k">{t('provider.customName')}</div>
            <div class="v"><label class="field small"><input bind:value={customName} onchange={saveCustom} aria-label={t('provider.customName')} /></label></div>
          </div>
          <div class="frow">
            <div class="k">{t('settings.customTemplate')}<span class="sub">{t('settings.customTemplate.sub')}</span></div>
            <div class="v">
              <label class="field small">
                <input bind:value={customTemplate} onchange={saveCustom} placeholder={t('provider.customPlaceholder')} aria-label={t('settings.customTemplate')} spellcheck="false" />
              </label>
            </div>
          </div>
        </div>
        {#if customTemplate.trim() && !customPreview}
          <div class="gfoot problem">{t('provider.customInvalid')}</div>
        {:else if customPreview}
          <div class="gfoot preview">{t('provider.customPreview', customPreview)}</div>
        {:else}
          <div class="gfoot">{t('provider.customHint')}</div>
        {/if}

        <div class="group">
          {#if platform.capabilities.panel === 'sidePanel'}
            {@render toggle(t('settings.sidePanelOnClick'), t('settings.sidePanelOnClick.sub'), sidePanelOnClick, (v) => {
              sidePanelOnClick = v;
              void settings.sidePanelOnClick.setValue(v);
            })}
          {/if}
          {@render toggle(t('settings.notify'), null, notify, (v) => {
            notify = v;
            void settings.notifyInBackground.setValue(v);
          })}
        </div>
      {:else if pane === 'listening'}
        <div class="group">
          <div class="frow">
            <div class="k">{t('settings.microphone')}<span class="sub">{micGrant === 'granted' ? t('mic.granted') : micGrant === 'denied' ? t('mic.denied') : micGrant === 'session' ? t('mic.session') : ''}</span></div>
            <div class="v"><a class="btn small" href="#/grant-mic">{t('mic.allowButton')}</a></div>
          </div>
          {@render toggle(t('settings.autoStop'), null, autoStop, (v) => {
            autoStop = v;
            void settings.autoStop.setValue(v);
          })}
          <div class="frow">
            <div class="k">{t('settings.lowPower')}<span class="sub">{t('settings.lowPower.sub')}</span></div>
            <div class="v">
              <select
                class="popup-btn"
                aria-label={t('settings.lowPower')}
                value={lowPower}
                onchange={(e) => {
                  lowPower = e.currentTarget.value as LowPowerSetting;
                  void settings.lowPower.setValue(lowPower);
                  if (lowPower !== 'auto') void engine.setLowPower(lowPower === 'on');
                }}
              >
                <option value="auto">{t('settings.lowPower.auto')}</option>
                <option value="on">{t('settings.lowPower.on')}</option>
                <option value="off">{t('settings.lowPower.off')}</option>
              </select>
            </div>
          </div>
        </div>
      {:else if pane === 'melody'}
        <div class="group">
          <div class="frow">
            <div class="k">
              {t('settings.melody')}
              <span class="sub">
                {#if downloading !== null}
                  {t('melody.status.downloading', bytes((melody?.download_bytes ?? 55_200_000) * downloading), bytes(melody?.download_bytes ?? 55_200_000))}
                {:else if melody?.installed}
                  {t('melody.status.installed', melody.song_count.toLocaleString(), bytes(melody.size_bytes))}
                {:else}
                  {t('melody.status.notInstalled', bytes(melody?.download_bytes ?? 55_200_000))}
                {/if}
              </span>
            </div>
            <div class="v">
              {#if melody?.installed}
                <button class="btn small danger" onclick={() => void engine.removeMelodyIndex().then(() => engine.melodyIndexStatus()).then((s) => (melody = s))}>{t('melody.remove')}</button>
              {:else if downloading === null}
                <button class="btn small" onclick={() => void download()}>{t('melody.download')}</button>
              {/if}
            </div>
          </div>
          {#if downloading !== null}
            <div class="frow"><div class="progress"><i style:width="{Math.round(downloading * 100)}%"></i></div></div>
          {/if}
        </div>
        <div class="gfoot">{t('melody.prompt.message.web', bytes(melody?.download_bytes ?? 55_200_000))}</div>
      {:else if pane === 'privacy'}
        <div class="group"><div class="frow"><div class="k">{t('settings.privacy.text')}</div></div></div>
      {:else}
        <div class="group"><div class="frow"><div class="k">{t('app.name')}<span class="sub">{t('settings.version', version)}</span></div></div></div>
      {/if}
    </div>
  </div>
</div>
