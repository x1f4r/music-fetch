<script lang="ts">
  import { DEMO_STATES, demoUrl, type DemoState } from './states';

  /**
   * Every demo state on one page (app.html?demo=gallery), scaled into cards like the
   * design gallery. Each card opens its state full size.
   */
  let theme = $state<'light' | 'dark'>(matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  const GROUPS: { title: string; surface: DemoState['surface']; android?: boolean }[] = [
    { title: 'Popup', surface: 'popup' },
    { title: 'Side panel and sidebar', surface: 'panel' },
    { title: 'App tab', surface: 'app' },
    { title: 'Firefox for Android', surface: 'popup', android: true },
  ];
  const CARD = 300;

  function scale(state: DemoState): number {
    return Math.min(CARD / state.width, 1);
  }

  $effect(() => {
    document.documentElement.dataset.theme = theme;
  });
</script>

<div class="gallery">
  <header>
    <h1>Music Fetch · demo states</h1>
    <div class="seg" role="radiogroup" aria-label="Appearance">
      <button role="radio" aria-checked={theme === 'light'} class:on={theme === 'light'} onclick={() => (theme = 'light')}>Light</button>
      <button role="radio" aria-checked={theme === 'dark'} class:on={theme === 'dark'} onclick={() => (theme = 'dark')}>Dark</button>
    </div>
  </header>
  {#each GROUPS as group (group.title)}
    {@const states = DEMO_STATES.filter((s) => s.surface === group.surface && Boolean(s.android) === Boolean(group.android))}
    <h2>{group.title}</h2>
    <div class="cards">
      {#each states as state (state.id)}
        {@const k = scale(state)}
        <a class="card" href={demoUrl(state, { theme })} target="_blank" rel="noopener">
          <span class="frame" style:width="{state.width * k}px" style:height="{state.height * k}px">
            <iframe
              title={state.title}
              src={demoUrl(state, { theme, still: true })}
              loading="lazy"
              tabindex="-1"
              style:width="{state.width}px"
              style:height="{state.height}px"
              style:transform="scale({k})"
            ></iframe>
          </span>
          <span class="callout">{state.title}</span>
          <span class="caption tertiary">{state.id}</span>
        </a>
      {/each}
    </div>
  {/each}
</div>
