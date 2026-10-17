<script lang="ts">
  import { t } from '../../i18n';
  import { settings, type MicGrant } from '../../settings/settings';
  import ListenButton from './ListenButton.svelte';

  /**
   * The one-time microphone grant (extension plan §5.2 step 2, §5.5). It runs in an
   * extension tab because popups lose focus to the browser's prompt, and Firefox's
   * background can never ask. The capture is stopped at once; sessions capture later.
   */
  interface Props {
    /** Called after any outcome, or "Not now". */
    ondone?: (grant: MicGrant) => void;
  }

  let { ondone }: Props = $props();
  let state = $state<'ask' | 'asking' | MicGrant>('ask');

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
    if (grant !== 'unknown') ondone?.(grant);
  }
</script>

<div class="grant">
  <ListenButton label={t('mic.allow')} onclick={() => void allow()} />
  {#if state === 'granted'}
    <h1>{t('mic.granted')}</h1>
  {:else if state === 'session'}
    <h1>{t('mic.session')}</h1>
    <div class="acts"><button class="btn filled" onclick={() => void allow()}>{t('action.tryAgain')}</button></div>
  {:else if state === 'denied'}
    <h1>{t('mic.denied')}</h1>
    <div class="acts"><button class="btn filled" onclick={() => void allow()}>{t('action.tryAgain')}</button></div>
  {:else}
    <h1>{t('mic.allow')}</h1>
    <p>{t('mic.allowMessage')}</p>
    <div class="acts">
      <button class="btn text" onclick={() => ondone?.('unknown')}>{t('mic.notNow')}</button>
      <button class="btn filled" disabled={state === 'asking'} onclick={() => void allow()}>{t('mic.allowButton')}</button>
    </div>
  {/if}
</div>
