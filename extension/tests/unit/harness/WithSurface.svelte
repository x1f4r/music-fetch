<script lang="ts">
  import type { Component } from 'svelte';
  import type { EngineClient } from '@/src/engine/client';
  import { provideSurface, type Platform } from '@/src/ui/context';

  /** Renders `component` inside a surface context, as the popup, panel and app tab do. */
  interface Props {
    engine: EngineClient;
    platform: Platform;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    component: Component<any>;
    props?: Record<string, unknown>;
  }

  let { engine, platform, component: Inner, props = {} }: Props = $props();
  // svelte-ignore state_referenced_locally
  provideSurface({ engine, platform });
</script>

<Inner {...props} />
