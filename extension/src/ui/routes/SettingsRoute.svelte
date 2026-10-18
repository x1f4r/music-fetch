<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { browser } from 'wxt/browser';
  import type { MelodyIndexStatus, StorageUsage } from '../../engine/types';
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
  import { chooseProvider, settings, type LowPowerSetting, type MicGrant, type MicPreference, type ProviderSettings } from '../../settings/settings';
  import { useSurface } from '../context';
  import { SETTINGS_PANES, type SettingsPane } from '../router.svelte';

  /**
   * Settings (extension plan §5.8, design `app-settings-*`): a pane list and grouped rows.
   * A setting lives in exactly one pane.
   */
  interface Props {
    pane: SettingsPane;
    providers: ProviderSettings | null;
    narrow: boolean;
    onpane: (pane: SettingsPane) => void;
  }

  let { pane, providers, narrow, onpane }: Props = $props();
  const { engine, platform } = useSurface();
  const caps = platform.capabilities;

  const PANES: Record<SettingsPane, { icon: string; label: string }> = {
    general: { icon: 'ic-gear', label: t('settings.general') },
    listening: { icon: 'ic-mic', label: t('settings.listening') },
    melody: { icon: 'ic-hum', label: t('settings.melody') },
    storage: { icon: 'ic-drive', label: t('settings.storage') },
    privacy: { icon: 'ic-shield', label: t('settings.privacy') },
    shortcuts: { icon: 'ic-keyboard', label: t('settings.shortcuts') },
    about: { icon: 'ic-info', label: t('settings.about') },
  };
  const COMMAND_NAMES: Record<string, Parameters<typeof t>[0]> = {
    _execute_action: 'settings.shortcuts.open',
    'identify-tab': 'settings.shortcuts.identify',
    hum: 'settings.shortcuts.hum',
  };

  let openInApp = $state(false);
  let sidePanelOnClick = $state(false);
  let notify = $state(true);
  let autoStop = $state(true);
  let lowPower = $state<LowPowerSetting>('auto');
  let micGrant = $state<MicGrant>('unknown');
  let sungLyrics = $state(false);
  let keepPinnedAudio = $state(true);
  let priority = $state<MicPreference[]>([]);
  let connected = $state<{ id: string; label: string; inUse: boolean }[]>([]);
  let melody = $state<MelodyIndexStatus | null>(null);
  let downloading = $state<number | null>(null);
  let melodyFailed = $state(false);
  let installId: string | null = null;
  let usage = $state<StorageUsage | null>(null);
  let cacheCleared = $state(false);
  let shortcuts = $state<{ name: string; shortcut: string }[]>([]);
  let coreVersion = $state<string | null>(null);
  let customName = $state('');
  let customTemplate = $state('');
  let stops: (() => void)[] = [];

  const SAMPLE = { title: 'Houdini', artist: 'Dua Lipa', artists: ['Dua Lipa'], isrc: 'GBAHT2300520' };
  const customPreview = $derived(isValidTemplate(customTemplate) ? customProviderLink(SAMPLE, customTemplate) : null);
  const currentKey = $derived(providers ? providerKey(providers.provider) : 'spotify');
  const currentName = $derived(providers?.provider.kind === 'service' ? serviceName(providers.provider.service) : (providers?.custom?.name ?? 'Spotify'));
  const version = safeVersion();
  /** The priority list, then connected microphones not in it, then "any other". */
  const devices = $derived([
    ...priority.map((p) => ({ ...p, state: connected.find((c) => c.id === p.id) ? (connected.find((c) => c.id === p.id)!.inUse ? 'inUse' : 'connected') : 'notConnected' })),
    ...connected.filter((c) => !priority.some((p) => p.id === c.id) && c.label).map((c) => ({ id: c.id, label: c.label, state: c.inUse ? 'inUse' : 'connected' })),
  ]);
  const melodyBytes = $derived(melody?.download_bytes ?? melody?.size_bytes ?? 55_200_000);

  function safeVersion(): string {
    try {
      return browser.runtime.getManifest().version;
    } catch {
      return '1.1.0';
    }
  }

  onMount(async () => {
    stops.push(
      engine.subscribe({
        sessionEvent: (id, event) => {
          if (id !== installId || typeof event !== 'object') return;
          if ('Phase' in event && event.Phase.phase === 'Downloading') downloading = event.Phase.fraction ?? 0;
          if ('Failed' in event) melodyFailed = true;
          if ('Finished' in event || 'Failed' in event) {
            downloading = null;
            installId = null;
            void engine.melodyIndexStatus().then((status) => (melody = status));
          }
        },
      }),
    );
    [openInApp, sidePanelOnClick, notify, autoStop, lowPower, micGrant, sungLyrics, keepPinnedAudio, priority] = await Promise.all([
      settings.openInApp.getValue(),
      settings.sidePanelOnClick.getValue(),
      settings.notifyInBackground.getValue(),
      settings.autoStop.getValue(),
      settings.lowPower.getValue(),
      settings.micGrant.getValue(),
      settings.searchSungLyrics.getValue(),
      settings.keepPinnedAudio.getValue(),
      settings.micPriority.getValue(),
    ]);
    const custom = await settings.customProvider.getValue();
    customName = custom?.name ?? '';
    customTemplate = custom?.template ?? '';
    if (caps.microphone === 'extension') connected = await platform.microphones();
    shortcuts = await platform.shortcuts();
    melody = await engine.melodyIndexStatus().catch(() => null);
    usage = await engine.storageUsage().catch(() => null);
    coreVersion = (await engine.hello().catch(() => null))?.coreVersion ?? null;
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
    melodyFailed = false;
    installId = await engine.installMelodyIndex().catch(() => null);
    if (!installId) {
      downloading = null;
      melodyFailed = true;
    }
  }

  async function cancelDownload() {
    if (installId) await engine.cancelSession(installId).catch(() => undefined);
    installId = null;
    downloading = null;
  }

  async function checkUpdates() {
    melody = await engine.checkMelodyIndex().catch(() => melody);
  }

  async function removeMelody() {
    await engine.removeMelodyIndex();
    melody = await engine.melodyIndexStatus();
  }

  async function clearCache() {
    await engine.clearCache();
    usage = await engine.storageUsage().catch(() => usage);
    cacheCleared = true;
  }

  async function move(index: number, step: number) {
    const list = devices.filter((d) => d.label).map((d) => ({ id: d.id, label: d.label }));
    const target = index + step;
    if (target < 0 || target >= list.length) return;
    [list[index], list[target]] = [list[target]!, list[index]!];
    priority = list;
    await settings.micPriority.setValue(list);
  }

  function mb(bytes: number): string {
    return bytes >= 1e6 ? `${Math.round(bytes / 1e6)} MB` : `${Math.max(1, Math.round(bytes / 1e3))} KB`;
  }

  function setSwitch(item: { setValue(v: boolean): Promise<void> }, value: boolean) {
    void item.setValue(value);
  }

  const micStatus = $derived(
    micGrant === 'granted' ? t('mic.status.granted') : micGrant === 'session' ? t('mic.status.session') : micGrant === 'denied' ? t('mic.status.denied') : t('mic.status.unknown'),
  );
  const shortcutRows = $derived(shortcuts.filter((s) => COMMAND_NAMES[s.name]));
</script>

{#snippet toggle(label: string, sub: string | null, value: boolean, onchange: (value: boolean) => void, disabled = false)}
  <div class="frow">
    <div class="k">{label}{#if sub}<span class="sub">{sub}</span>{/if}</div>
    <div class="v"><button class="switch" class:on={value} role="switch" aria-checked={value} aria-label={label} {disabled} onclick={() => onchange(!value)}></button></div>
  </div>
{/snippet}

<div class="toolbar settings-toolbar"><h1 class="wtitle">{t('nav.settings')}</h1></div>
<div class="settings-scroll">
  <div class="settings" class:narrow>
    <nav class="nav-list" aria-label={t('settings.panes')}>
      {#each SETTINGS_PANES as id (id)}
        {#if id !== 'shortcuts' || caps.shortcuts}
          <a class="ni" class:on={pane === id} href="#/settings/{id}" aria-current={pane === id ? 'page' : undefined} onclick={(e) => (e.preventDefault(), onpane(id))}>
            <span class="ic {PANES[id].icon}"></span>{PANES[id].label}
          </a>
        {/if}
      {/each}
    </nav>
    <div class="form">
      {#if pane === 'general'}
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
          {@render toggle(t('settings.openInApp'), t('settings.openInApp.sub', currentName), openInApp, (v) => {
            openInApp = v;
            setSwitch(settings.openInApp, v);
          })}
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
            {@render toggle(t('settings.sidePanelOnClick'), t('settings.sidePanelOnClick.sub'), sidePanelOnClick, (v) => {
              sidePanelOnClick = v;
              setSwitch(settings.sidePanelOnClick, v);
            })}
          {/if}
          {@render toggle(t('settings.notify'), null, notify, (v) => {
            notify = v;
            setSwitch(settings.notifyInBackground, v);
          })}
        </div>
      {:else if pane === 'listening'}
        <h2 class="sr-only">{t('settings.listening')}</h2>
        {#if caps.microphone === 'app'}
          <div class="group">
            <div class="frow">
              <div class="k">{t('settings.inApp')}<span class="sub">{t('settings.inApp.sub')}</span></div>
            </div>
          </div>
        {:else}
          <div class="group">
            <div class="frow">
              <div class="k">{t('settings.microphone')}<span class="sub">{micStatus}</span></div>
              <div class="v">
                {#if micGrant === 'granted' || micGrant === 'denied'}
                  <button class="btn small text" onclick={() => void platform.openSiteSettings()}>{t('mic.siteSettings')}</button>
                {/if}
                {#if micGrant !== 'granted'}
                  <a class="btn small" href="#/grant-mic">{t('mic.allowButton')}</a>
                {/if}
              </div>
            </div>
          </div>
          <h3 class="ghead">{t('settings.micPriority')}</h3>
          <div class="group" role="list" aria-label={t('settings.micPriority.label')}>
            {#each devices as device, i (device.id)}
              <div class="dev" role="listitem">
                <span class="n tnum">{i + 1}</span>
                <span class="name truncate">{device.label}</span>
                <span class="st">{device.state === 'notConnected' ? t('settings.micPriority.notConnected') : device.state === 'inUse' ? t('settings.micPriority.inUse') : ''}</span>
                <span class="reorder">
                  <button class="btn icon plain small" aria-label={t('settings.micPriority.up', device.label)} disabled={i === 0} onclick={() => void move(i, -1)}><span class="ic ic-chev-down s14 up"></span></button>
                  <button class="btn icon plain small" aria-label={t('settings.micPriority.down', device.label)} disabled={i === devices.length - 1} onclick={() => void move(i, 1)}><span class="ic ic-chev-down s14"></span></button>
                </span>
              </div>
            {/each}
            <div class="dev" role="listitem"><span class="n tnum">{devices.length + 1}</span><span class="name truncate secondary">{t('settings.micPriority.any')}</span></div>
          </div>
          <div class="gfoot">{t('settings.micPriority.footer')}</div>
        {/if}
        <div class="group">
          {@render toggle(t('settings.autoStop'), null, autoStop, (v) => {
            autoStop = v;
            setSwitch(settings.autoStop, v);
          })}
        </div>
        <div class="group">
          <div class="frow">
            <div class="k">{t('settings.lowPower')}<span class="sub">{t('settings.lowPower.sub')}</span></div>
            <div class="v">
              <div class="seg" role="radiogroup" aria-label={t('settings.lowPower')}>
                {#each ['auto', 'on', 'off'] as const as value (value)}
                  <button
                    role="radio"
                    aria-checked={lowPower === value}
                    class:on={lowPower === value}
                    onclick={() => {
                      lowPower = value;
                      void settings.lowPower.setValue(value);
                      if (value !== 'auto') void engine.setLowPower(value === 'on');
                    }}>{t(`settings.lowPower.${value}`)}</button
                  >
                {/each}
              </div>
            </div>
          </div>
        </div>
        <div class="gfoot">{t('settings.lowPower.footer')}</div>
      {:else if pane === 'melody'}
        <h2 class="sr-only">{t('settings.melody')}</h2>
        <div class="group">
          <div class="frow melody-row">
            <div class="k">
              {t('melody.title')}
              {#if downloading !== null}
                <span class="sub tnum">{t('melody.status.downloading', mb(melodyBytes * downloading), mb(melodyBytes))}</span>
              {:else if melodyFailed}
                <span class="sub problem">{t('melody.error')}</span>
              {:else if melody?.installed && melody.available_version}
                <span class="sub">{t('melody.status.updateAvailable', mb(melody.download_bytes ?? melody.size_bytes))}</span>
              {:else if melody?.installed}
                <span class="sub">{t('melody.status.installed', melody.song_count.toLocaleString(), mb(melody.size_bytes))}</span>
              {:else}
                <span class="sub">{t('melody.status.notInstalled', mb(melodyBytes))}</span>
              {/if}
            </div>
            <div class="v">
              {#if downloading !== null}
                <div class="progress melody-bar" role="progressbar" aria-label={t('melody.title')} aria-valuemin="0" aria-valuemax="100" aria-valuenow={Math.round(downloading * 100)}>
                  <i style:width="{Math.round(downloading * 100)}%"></i>
                </div>
                <button class="btn small" onclick={() => void cancelDownload()}>{t('action.cancel')}</button>
              {:else if melodyFailed}
                <button class="btn small" onclick={() => void download()}>{t('action.tryAgain')}</button>
              {:else if melody?.installed && melody.available_version}
                <button class="btn small filled" onclick={() => void download()}>{t('melody.update')}</button>
              {:else if melody?.installed}
                <button class="btn small" onclick={() => void checkUpdates()}>{t('melody.checkForUpdates')}</button>
                <button class="btn small text" onclick={() => void removeMelody()}>{t('melody.remove')}</button>
              {:else}
                <button class="btn small filled" onclick={() => void download()}>{t('melody.download')}</button>
              {/if}
            </div>
          </div>
        </div>
        <div class="gfoot">{t('melody.footer.web')}</div>
        <h3 class="ghead">{t('melody.whileHumming')}</h3>
        <div class="group">
          {@render toggle(
            t('melody.alsoLyrics'),
            caps.target === 'chromium' ? t('melody.alsoLyrics.sub') : t('melody.alsoLyrics.unavailable'),
            sungLyrics && caps.target === 'chromium',
            (v) => {
              sungLyrics = v;
              setSwitch(settings.searchSungLyrics, v);
            },
            caps.target !== 'chromium',
          )}
        </div>
      {:else if pane === 'storage'}
        <h2 class="sr-only">{t('settings.storage')}</h2>
        <div class="group">
          <div class="frow">
            <div class="k">
              {t('settings.onDisk')}
              {#if usage}<span class="sub tnum">{t('settings.onDisk.detail', mb(usage.database_bytes), mb(usage.cache_bytes), mb(usage.melody_index_bytes))}</span>{/if}
            </div>
            <div class="v">
              <span class="tnum">{usage ? mb(usage.database_bytes + usage.cache_bytes + usage.melody_index_bytes) : ''}</span>
              <button class="btn small" disabled={cacheCleared} onclick={() => void clearCache()}>{cacheCleared ? t('settings.clearCache.done') : t('settings.clearCache')}</button>
            </div>
          </div>
          {@render toggle(t('settings.keepPinnedAudio'), null, keepPinnedAudio, (v) => {
            keepPinnedAudio = v;
            setSwitch(settings.keepPinnedAudio, v);
          })}
        </div>
      {:else if pane === 'privacy'}
        <h2 class="sr-only">{t('settings.privacy')}</h2>
        <div class="group"><div class="frow prose"><p>{t('settings.privacy.text')}</p></div></div>
      {:else if pane === 'shortcuts'}
        <h2 class="sr-only">{t('settings.shortcuts')}</h2>
        <div class="group">
          {#each shortcutRows as row (row.name)}
            <div class="frow"><div class="k">{t(COMMAND_NAMES[row.name]!)}</div><div class="v"><kbd class="tnum">{row.shortcut || '—'}</kbd></div></div>
          {/each}
        </div>
        <div class="gfoot">{t('settings.shortcuts.footer')}</div>
        <div><button class="btn" onclick={() => void platform.openShortcuts()}><span class="ic ic-arrow-out s12"></span>{t('settings.shortcuts.change')}</button></div>
      {:else}
        <h2 class="sr-only">{t('settings.about')}</h2>
        <div class="group">
          <div class="frow about">
            <span class="mini-disc small" aria-hidden="true"><span class="ic ic-mark"></span></span>
            <div class="k">{t('app.name')}<span class="sub tnum">{t('settings.version', version)}{#if coreVersion} · {t('settings.engine', coreVersion)}{/if}</span></div>
          </div>
        </div>
      {/if}
    </div>
  </div>
</div>
