<script lang="ts">
  import MicGrant from '../components/MicGrant.svelte';
  import { useSurface } from '../context';

  /** `#/grant-mic` (`?state=denied` …): opened from the popup or Settings; closes itself after a grant. */
  interface Props {
    initial?: 'granted' | 'denied' | 'session' | null;
  }

  let { initial = null }: Props = $props();
  const { platform } = useSurface();

  function done(grant: string) {
    if (grant === 'granted') setTimeout(() => platform.close(), 1500);
  }
</script>

<MicGrant {initial} onclosetab={() => platform.close()} onsitesettings={() => void platform.openSiteSettings()} ondone={done} />
