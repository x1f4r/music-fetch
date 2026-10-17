import { mount, type Component } from 'svelte';
import { engineClient } from '../engine/connect';
import type { EngineClient } from '../engine/client';
import { browserPlatform } from '../platform/browser-platform';
import type { Platform } from './context';
import './theme/index.css';

type SurfaceProps = { engine: EngineClient; platform: Platform };

/** Mount a surface (popup, side panel or sidebar, app tab) with the real engine client and browser. */
export function boot(component: Component<SurfaceProps>, surface: 'popup' | 'panel' | 'app'): void {
  const platform = browserPlatform();
  const engine = engineClient();
  document.body.classList.add(`surface-${surface}`);
  if (platform.capabilities.android) document.body.classList.add('touch', ...(surface === 'popup' ? ['mobile-body'] : []));
  const target = document.getElementById('root');
  if (!target) throw new Error('The page has no #root element.');
  mount(component, { target, props: { engine, platform } });
  addEventListener('pagehide', () => engine.dispose());
}
