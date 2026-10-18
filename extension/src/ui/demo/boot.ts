import { mount, tick, type Component } from 'svelte';
import { browserPlatform } from '../../platform/browser-platform';
import { useMemorySecrets } from '../../platform/secrets';
import { useMemorySettings } from '../../settings/settings';
import type { Platform } from '../context';
import type { EngineClient } from '../../engine/client';
import DemoGallery from './DemoGallery.svelte';
import { DemoEngine, demoPlatform, direct } from './runtime';
import { DECIDED, demoState, demoUrl, type DemoSurface } from './states';

type SurfaceProps = { engine: EngineClient; platform: Platform };

/**
 * Mount `component` in a demo state (`?demo=<id>`): demo library, in-memory settings,
 * the state's tab and browser, then its scripted clicks. Sets `html[data-demo-ready]`
 * when the state is reached (the screenshot runner waits for it).
 */
export async function bootDemo(component: Component<SurfaceProps>, surface: DemoSurface, id: string): Promise<void> {
  const params = new URLSearchParams(location.search);
  const root = document.documentElement;
  const theme = params.get('theme');
  if (theme === 'dark' || theme === 'light') root.dataset.theme = theme;
  if (params.get('still') === '1') root.classList.add('demo-still');
  const target = document.getElementById('root')!;

  if (id === 'gallery') {
    document.body.classList.add('surface-gallery');
    mount(DemoGallery, { target });
    root.dataset.demoReady = '1';
    return;
  }
  const state = demoState(id);
  if (!state) {
    target.textContent = `Unknown demo state “${id}”. All states: app.html?demo=gallery`;
    root.dataset.demoReady = 'unknown';
    return;
  }
  if (state.surface !== surface) {
    location.replace(demoUrl(state, { theme: theme === 'dark' || theme === 'light' ? theme : undefined, still: params.get('still') === '1' }));
    return;
  }
  useMemorySettings(state.settings ?? DECIDED);
  useMemorySecrets(state.secrets ?? {});
  if (state.route && location.hash !== `#${state.route}`) history.replaceState(null, '', `${location.pathname}${location.search}#${state.route}`);

  let base: Platform | null = null;
  try {
    base = browserPlatform();
  } catch {
    base = null;
  }
  const platform = demoPlatform(state, base);
  const engine = new DemoEngine(state);
  document.body.classList.add(`surface-${surface}`);
  if (platform.capabilities.android) document.body.classList.add('touch', ...(surface === 'popup' ? ['mobile-body'] : []));
  mount(component, { target, props: { engine, platform } });
  await tick();
  // Let the surface load its settings and library before the scripted clicks.
  await new Promise((resolve) => setTimeout(resolve, 120));
  const problems = await direct(state.actions ?? []);
  await new Promise((resolve) => setTimeout(resolve, 60));
  root.dataset.demoReady = problems.length ? 'problems' : '1';
  if (problems.length) root.dataset.demoProblems = problems.join('; ');
}
