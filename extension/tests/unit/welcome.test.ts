import { fireEvent, render, screen } from '@testing-library/svelte';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { settings, useMemorySettings } from '@/src/settings/settings';
import { DemoEngine, demoPlatform } from '@/src/ui/demo/runtime';
import { demoState } from '@/src/ui/demo/states';
import WelcomeRoute from '@/src/ui/routes/WelcomeRoute.svelte';
import WithSurface from './harness/WithSurface.svelte';

function welcome(id: string, onfinish = vi.fn()) {
  const state = demoState(id)!;
  useMemorySettings(state.settings ?? {});
  const engine = new DemoEngine(state);
  const platform = demoPlatform(state, null);
  const providers = { decision: null, provider: { kind: 'service', service: 'Spotify' }, custom: null, needsPanel: true } as const;
  render(WithSurface, { props: { engine, platform, component: WelcomeRoute, props: { providers, onfinish } } });
  return { onfinish, platform };
}

describe('the welcome flow', () => {
  beforeEach(() => fakeBrowser.reset());

  it('has three steps on desktop, with the microphone second', async () => {
    welcome('app-welcome');
    expect(await screen.findByRole('img', { name: 'Step 1 of 3' })).toBeTruthy();
    await fireEvent.click(screen.getByRole('button', { name: 'Skip' }));
    expect(await screen.findByRole('heading', { name: 'Allow the microphone' })).toBeTruthy();
    expect(screen.getByRole('img', { name: 'Step 2 of 3' })).toBeTruthy();
  });

  it('goes on after the microphone was blocked', async () => {
    const { onfinish } = welcome('app-welcome');
    await fireEvent.click(await screen.findByRole('button', { name: 'Skip' }));
    const media = navigator.mediaDevices as MediaDevices | undefined;
    const denied = Object.assign(new DOMException('blocked', 'NotAllowedError'));
    Object.defineProperty(navigator, 'mediaDevices', { configurable: true, value: { getUserMedia: () => Promise.reject(denied) } });
    try {
      await fireEvent.click(await screen.findByRole('button', { name: 'Allow' }));
      expect(await screen.findByRole('heading', { name: 'Microphone access is off' })).toBeTruthy();
      await fireEvent.click(screen.getByRole('button', { name: 'Not now' }));
      expect(await screen.findByRole('img', { name: 'Step 3 of 3' })).toBeTruthy();
      expect(await settings.micGrant.getValue()).toBe('denied');
      expect(onfinish).not.toHaveBeenCalled();
    } finally {
      Object.defineProperty(navigator, 'mediaDevices', { configurable: true, value: media });
    }
  });

  it('skips the microphone on Firefox for Android', async () => {
    const { onfinish } = welcome('mobile-welcome');
    expect(await screen.findByRole('img', { name: 'Step 1 of 2' })).toBeTruthy();
    await fireEvent.click(screen.getByRole('button', { name: 'Skip' }));
    expect(await screen.findByText('Open Music Fetch from the menu, then Extensions.')).toBeTruthy();
    expect(screen.getByRole('img', { name: 'Step 2 of 2' })).toBeTruthy();
    expect(screen.queryByRole('heading', { name: 'Allow the microphone' })).toBeNull();
    await fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(onfinish).toHaveBeenCalledTimes(1);
    expect(await settings.welcomeStep.getValue()).toBe(3);
  });
});
