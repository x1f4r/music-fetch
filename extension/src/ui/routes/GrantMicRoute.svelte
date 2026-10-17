<script lang="ts">
  import { browser } from 'wxt/browser';
  import MicGrant from '../components/MicGrant.svelte';

  /** `#/grant-mic`: opened from the popup or Settings; closes itself after a grant. */
  async function done(grant: string) {
    if (grant !== 'granted' && grant !== 'session') return;
    setTimeout(async () => {
      const tab = await browser.tabs.getCurrent();
      if (tab?.id !== undefined) await browser.tabs.remove(tab.id);
    }, 1200);
  }
</script>

<MicGrant ondone={(grant) => void done(grant)} />
