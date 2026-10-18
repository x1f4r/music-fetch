<script lang="ts">
  import { t } from '../../i18n';
  import { settings, type MicGrant } from '../../settings/settings';
  import ListenButton from './ListenButton.svelte';

  /**
   * The one-time microphone grant (extension plan §5.2 step 2, §5.5) and its outcomes:
   * allowed (the tab closes itself), denied (site settings), Chrome's "Allow this time".
   * It runs in an extension tab because popups lose focus to the browser's prompt, and
   * Firefox's background can never ask. The capture is stopped at once.
   */
  interface Props {
    /** Start on an outcome (deep link from the popup when the grant is known to be off). */
    initial?: 'granted' | 'denied' | 'session' | null;
    /** Step dots in the welcome flow. */
    step?: { index: number; count: number } | null;
    onclosetab?: () => void;
    onsitesettings?: () => void;
    /** Called after an outcome, "Not now" or "Keep it this way". */
    ondone?: (grant: MicGrant) => void;
  }

  let { initial = null, step = null, onclosetab, onsitesettings, ondone }: Props = $props();
  // svelte-ignore state_referenced_locally
  let state = $state<'ask' | 'asking' | 'granted' | 'denied' | 'session'>(initial ?? 'ask');

  async function persistence(): Promise<MicGrant> {
    try {
      const status = await navigator.permissions.query({ name: 'microphone' as PermissionName });
      // Chrome's "Allow this time" leaves the permission at "prompt" after the grant.
      return status.state === 'granted' ? 'granted' : 'session';
    } catch {
      return 'granted';
    }
  }

  async function allow() {
    state = 'asking';
    let grant: MicGrant;
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: { echoCancellation: false, noiseSuppression: false, autoGainControl: false },
      });
      stream.getTracks().forEach((track) => track.stop());
      grant = await persistence();
    } catch (error) {
      grant = error instanceof DOMException && error.name === 'NotAllowedError' ? 'denied' : 'unknown';
    }
    state = grant === 'unknown' ? 'ask' : grant;
    await settings.micGrant.setValue(grant);
    if (grant === 'granted') ondone?.(grant);
  }
</script>

<div class="grant" aria-live="polite">
  <div class="grant-disc">
    <ListenButton label={t('mic.allow')} decorative />
    {#if state === 'granted'}<span class="okmark" aria-hidden="true"><span class="ic ic-check s14"></span></span>{/if}
  </div>
  {#if state === 'granted'}
    <h1>{t('mic.granted')}</h1>
    <p>{t('mic.grantedMessage')}</p>
    {#if onclosetab}<div class="acts"><button class="btn" onclick={onclosetab}>{t('mic.closeTab')}</button></div>{/if}
  {:else if state === 'denied'}
    <h1>{t('mic.denied')}</h1>
    <p>{t('mic.deniedMessage')}</p>
    <div class="acts">
      <button class="btn filled" onclick={onsitesettings}><span class="ic ic-arrow-out s12"></span>{t('mic.openSiteSettings')}</button>
      <button class="btn text" onclick={() => void allow()}>{t('action.tryAgain')}</button>
    </div>
  {:else if state === 'session'}
    <h1>{t('mic.session')}</h1>
    <p>{t('mic.sessionMessage')}</p>
    <div class="acts">
      <button class="btn filled" onclick={() => void allow()}>{t('mic.askAgain')}</button>
      <button class="btn text" onclick={() => ondone?.('session')}>{t('mic.keepIt')}</button>
    </div>
  {:else}
    <h1>{t('mic.allow')}</h1>
    <p>{t('mic.allowMessage')}</p>
    <div class="acts">
      <button class="btn text" onclick={() => ondone?.('unknown')}>{t('mic.notNow')}</button>
      <button class="btn filled" disabled={state === 'asking'} onclick={() => void allow()}>{t('mic.allowButton')}</button>
    </div>
  {/if}
  {#if step}
    <div class="dots" role="img" aria-label={t('welcome.step', step.index + 1, step.count)}>
      {#each Array.from({ length: step.count }) as _, i (i)}<i class:on={i === step.index}></i>{/each}
    </div>
  {/if}
</div>
