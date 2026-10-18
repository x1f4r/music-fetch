<script lang="ts">
  import { tick } from 'svelte';
  import { t, type MessageKey } from '../../i18n';
  import { isCompleteAcr, normalizeAcrHost, type ProviderId } from '../../platform/secrets';
  import { useSurface } from '../context';
  import type { ServicesController } from './services.svelte';

  /**
   * One provider in Settings › Services: the fields while it isn't set up (or while
   * changing it), else what is stored (host, the access key's end, "Saved" for secrets),
   * a status line that says whether the keys work, and Check / Change / Remove.
   */
  interface Props {
    id: ProviderId;
    services: ServicesController;
  }

  let { id, services }: Props = $props();
  const { platform } = useSurface();

  const NAMES: Record<ProviderId, MessageKey> = { acrHumming: 'settings.acrHum', acr: 'settings.acr', audd: 'settings.audd' };
  const FOOTERS: Record<ProviderId, MessageKey> = { acrHumming: 'settings.acrHum.footer', acr: 'settings.acr.footer', audd: 'settings.audd.footer' };
  const CONSOLES: Record<ProviderId, string> = {
    acrHumming: 'https://console.acrcloud.com/',
    acr: 'https://console.acrcloud.com/',
    audd: 'https://dashboard.audd.io/',
  };
  const HOST_EXAMPLE = 'identify-eu-west-1.acrcloud.com';

  /** Changing stored keys, or adding the first ones. */
  let editing = $state(false);
  let adding = $state(false);
  let host = $state('');
  let accessKey = $state('');
  let accessSecret = $state('');
  let token = $state('');

  const acr = $derived(id !== 'audd');
  const company = $derived(acr ? 'ACRCloud' : 'AudD');
  const summary = $derived(services.summary[id]);
  const check = $derived(services.checks[id] ?? null);
  const showFields = $derived(editing || (adding && !summary.set));
  const complete = $derived(acr ? isCompleteAcr({ host, accessKey, accessSecret }) : token.trim().length > 0);
  const hostInvalid = $derived(acr && host.trim().length > 0 && normalizeAcrHost(host) === null);
  const fieldId = (name: string) => `svc-${id}-${name}`;

  const status = $derived.by((): { text: string; tone: 'ok' | 'problem' | 'plain'; allow: boolean } | null => {
    if (!summary.set && check?.state !== 'host') return null;
    switch (check?.state) {
      case undefined:
        return { text: t('settings.services.stored'), tone: 'plain', allow: false };
      case 'checking':
        return { text: t('settings.services.checking'), tone: 'plain', allow: false };
      case 'ok':
        return { text: id === 'acrHumming' ? t('settings.services.ok.hum') : t('settings.services.ok', company), tone: 'ok', allow: false };
      case 'rejected':
        return { text: id === 'audd' ? t('settings.services.rejected.token') : t('settings.services.rejected', company), tone: 'problem', allow: false };
      case 'quota':
        return { text: t('settings.services.quota'), tone: 'problem', allow: false };
      case 'network':
        return { text: t('settings.services.network', company), tone: 'problem', allow: false };
      case 'host':
        return { text: t('settings.services.host', HOST_EXAMPLE), tone: 'problem', allow: false };
      case 'permission':
        return { text: t('settings.services.permission', summary.host ?? (acr ? company : 'api.audd.io')), tone: 'problem', allow: true };
      default:
        return { text: check?.code !== undefined ? t('settings.services.failed', company, String(check.code)) : t('settings.services.failedPlain', company), tone: 'problem', allow: false };
    }
  });

  async function startEditing(first: boolean) {
    host = summary.host ?? '';
    accessKey = '';
    accessSecret = '';
    token = '';
    if (first) adding = true;
    else editing = true;
    await tick();
    document.getElementById(fieldId(acr ? (first ? 'host' : 'key') : 'token'))?.focus();
  }

  function cancel() {
    editing = false;
    adding = false;
  }

  function save(event: SubmitEvent) {
    event.preventDefault();
    if (!complete) return;
    // No await before this call: the host permission request needs the click.
    const done = id === 'audd' ? services.save('audd', { token }) : services.save(id, { host, accessKey, accessSecret });
    void done.then(() => {
      // Typed keys leave the page as soon as they are stored.
      accessKey = '';
      accessSecret = '';
      token = '';
      if (services.summary[id].set) cancel();
    });
  }
</script>

<section class="svc" aria-labelledby="{fieldId('title')}">
  <h3 class="ghead" id={fieldId('title')}>{t(NAMES[id])}</h3>
  {#if showFields}
    <form class="group" onsubmit={save} autocomplete="off">
      {#if acr}
        <div class="frow">
          <div class="k"><label for={fieldId('host')}>{t('settings.host')}</label></div>
          <div class="v"><span class="field small" class:invalid={hostInvalid}><input id={fieldId('host')} bind:value={host} placeholder={HOST_EXAMPLE} spellcheck="false" autocomplete="off" aria-invalid={hostInvalid} /></span></div>
        </div>
        <div class="frow">
          <div class="k"><label for={fieldId('key')}>{t('settings.accessKey')}</label></div>
          <div class="v"><span class="field small"><input id={fieldId('key')} bind:value={accessKey} spellcheck="false" autocomplete="off" /></span></div>
        </div>
        <div class="frow">
          <div class="k"><label for={fieldId('secret')}>{t('settings.accessSecret')}</label></div>
          <div class="v"><span class="field small"><input id={fieldId('secret')} type="password" bind:value={accessSecret} spellcheck="false" autocomplete="new-password" /></span></div>
        </div>
      {:else}
        <div class="frow">
          <div class="k"><label for={fieldId('token')}>{t('settings.apiToken')}</label></div>
          <div class="v"><span class="field small"><input id={fieldId('token')} type="password" bind:value={token} spellcheck="false" autocomplete="new-password" /></span></div>
        </div>
      {/if}
      <div class="frow svc-actions">
        <div class="k">
          {#if status && check?.state === 'host'}
            <span class="sub svc-status problem" role="status">{status.text}</span>
          {:else}
            <button type="button" class="btn small text get-keys" onclick={() => void platform.openUrl(CONSOLES[id])}>{t('settings.services.getKeys')}<span class="ic ic-arrow-out s12"></span></button>
          {/if}
        </div>
        <div class="v">
          <button type="button" class="btn small text" onclick={cancel}>{t('action.cancel')}</button>
          <button type="submit" class="btn small filled" disabled={!complete}>{t('settings.services.save')}</button>
        </div>
      </div>
    </form>
  {:else if !summary.set}
    <div class="group">
      <div class="frow svc-actions">
        <div class="k"><span class="secondary">{t('settings.services.notSet')}</span></div>
        <div class="v">
          <button class="btn small text" onclick={() => void platform.openUrl(CONSOLES[id])}>{t('settings.services.getKeys')}<span class="ic ic-arrow-out s12"></span></button>
          <button class="btn small add-keys" onclick={() => void startEditing(true)}>{t('settings.services.add')}</button>
        </div>
      </div>
    </div>
  {:else}
    <div class="group">
      {#if acr}
        <div class="frow"><div class="k">{t('settings.host')}</div><div class="v"><span class="truncate svc-value">{summary.host}</span></div></div>
        <div class="frow"><div class="k">{t('settings.accessKey')}</div><div class="v"><span class="tnum svc-value">••••{summary.keyHint}</span></div></div>
        <div class="frow"><div class="k">{t('settings.accessSecret')}</div><div class="v"><span class="svc-value">{t('settings.services.secretSet')}</span></div></div>
      {:else}
        <div class="frow"><div class="k">{t('settings.apiToken')}</div><div class="v"><span class="svc-value">{t('settings.services.secretSet')}</span></div></div>
      {/if}
      <div class="frow svc-actions">
        <div class="k">
          {#if status}
            <span class="sub svc-status {status.tone}" role="status">
              {#if status.tone === 'ok'}<span class="ic ic-check s12" aria-hidden="true"></span>{/if}{status.text}
            </span>
          {/if}
        </div>
        <div class="v">
          {#if status?.allow}
            <button class="btn small filled" onclick={() => void services.allow(id)}>{t('settings.services.allow')}</button>
          {:else}
            <button class="btn small" disabled={check?.state === 'checking'} onclick={() => void services.check(id)}>{t('settings.services.check')}</button>
          {/if}
          <button class="btn small text" onclick={() => void startEditing(false)}>{t('settings.services.change')}</button>
          <button class="btn small text" onclick={() => void services.remove(id)}>{t('settings.services.remove')}</button>
        </div>
      </div>
    </div>
  {/if}
  <div class="gfoot">{t(FOOTERS[id])}</div>
</section>
