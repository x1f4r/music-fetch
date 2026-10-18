import { fireEvent, render, screen, within } from '@testing-library/svelte';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { loadProviderKeys, memoryVault, useMemorySecrets, useVault } from '@/src/platform/secrets';
import { settings, useMemorySettings } from '@/src/settings/settings';
import type { Platform } from '@/src/ui/context';
import { DemoEngine, demoPlatform } from '@/src/ui/demo/runtime';
import { demoState } from '@/src/ui/demo/states';
import type { SettingsPane } from '@/src/ui/router.svelte';
import { indexDate, melodyView } from '@/src/ui/settings/melody.svelte';
import SettingsView from '@/src/ui/settings/SettingsView.svelte';
import { storageView } from '@/src/ui/settings/storage';
import WithSurface from './harness/WithSurface.svelte';

const LICENSES = {
  notices: [{ title: 'SwiftF0', summary: 'The pitch model, MIT.', text: 'MIT License\n\nCopyright (c) 2025-2026 Lars Nieradzik' }],
  packages: [{ name: 'svelte', version: '5.57.2', license: 'MIT', text: 'Copyright (c) 2016-2025 Svelte Contributors' }],
  crates: [],
};

function open(id: string, pane: SettingsPane, override: Partial<Platform> = {}) {
  const state = demoState(id)!;
  useMemorySettings(state.settings ?? {});
  useMemorySecrets(state.secrets ?? {});
  const engine = new DemoEngine(state);
  const platform = { ...demoPlatform(state, null), ...override };
  const view = render(WithSurface, { props: { engine, platform, component: SettingsView, props: { pane, providers: null, narrow: false, onpane: vi.fn() } } });
  return { engine, platform, view };
}

describe('Settings', () => {
  beforeEach(() => {
    fakeBrowser.reset();
    vi.spyOn(globalThis, 'fetch').mockImplementation(async () => new Response(JSON.stringify(LICENSES)));
  });
  afterEach(() => useMemorySecrets(null));

  it('lists Services between Listening and Melody', () => {
    open('app-settings-general', 'general');
    const names = within(screen.getByRole('navigation', { name: 'Settings panes' }))
      .getAllByRole('link')
      .map((a) => a.textContent?.trim());
    expect(names).toEqual(['General', 'Listening', 'Services', 'Melody', 'Storage', 'Privacy', 'Shortcuts', 'About']);
  });

  describe('Services', () => {
    it('saves ACRCloud humming keys, checks them and keeps only a summary on the page', async () => {
      const requestHost = vi.fn(async () => true);
      const checkProvider = vi.fn(async () => ({ state: 'ok' as const }));
      open('app-settings-services', 'services', { requestHost, checkProvider });
      const group = (await screen.findByRole('heading', { name: 'ACRCloud humming' })).closest('section')!;
      expect(within(group).getByText('Not set up')).toBeTruthy();
      await fireEvent.click(within(group).getByRole('button', { name: 'Add keys' }));
      const save = within(group).getByRole('button', { name: 'Save and check' }) as HTMLButtonElement;
      expect(save.disabled).toBe(true);
      await fireEvent.input(within(group).getByLabelText('Host'), { target: { value: 'identify-eu-west-1.acrcloud.com' } });
      await fireEvent.input(within(group).getByLabelText('Access key'), { target: { value: 'key-0000-4f2a' } });
      await fireEvent.input(within(group).getByLabelText('Access secret'), { target: { value: 'Sx9-secret-typed-in' } });
      expect(save.disabled).toBe(false);
      await fireEvent.click(save);

      expect(await within(group).findByText('Works. Hum something to try it.')).toBeTruthy();
      expect(requestHost).toHaveBeenCalledWith('https://identify-eu-west-1.acrcloud.com/');
      expect(checkProvider).toHaveBeenCalledWith('acrHumming', { host: 'identify-eu-west-1.acrcloud.com', accessKey: 'key-0000-4f2a', accessSecret: 'Sx9-secret-typed-in' });
      expect(within(group).getByText('••••4f2a')).toBeTruthy();
      expect(group.textContent).not.toContain('Sx9-secret-typed-in');
      expect(group.querySelector('input')).toBeNull();
      expect((await loadProviderKeys()).acrHumming?.accessSecret).toBe('Sx9-secret-typed-in');
      expect(await settings.providerChecks.getValue()).toMatchObject({ acrHumming: { state: 'ok' } });
    });

    it('asks for the host inside the click, before anything is awaited', async () => {
      const order: string[] = [];
      const requestHost = vi.fn((url: string) => {
        order.push(`request ${url}`);
        return Promise.resolve(true);
      });
      open('app-settings-services', 'services', { requestHost });
      const group = (await screen.findByRole('heading', { name: 'AudD' })).closest('section')!;
      await fireEvent.click(within(group).getByRole('button', { name: 'Add keys' }));
      await fireEvent.input(within(group).getByLabelText('API token'), { target: { value: 'token-123' } });
      const click = within(group).getByRole('button', { name: 'Save and check' });
      click.click();
      order.push('click returned');
      expect(order).toEqual(['request https://api.audd.io/', 'click returned']);
    });

    it('says when the keys are rejected, and offers Allow when the browser refused the host', async () => {
      open('app-settings-services-set', 'services');
      const audd = (await screen.findByRole('heading', { name: 'AudD' })).closest('section')!;
      expect(await within(audd).findByText("AudD didn't accept this token. Check it and try again.")).toBeTruthy();
      const hum = screen.getByRole('heading', { name: 'ACRCloud humming' }).closest('section')!;
      expect(within(hum).getByText('Works. Hum something to try it.')).toBeTruthy();
      expect(within(hum).getByText('identify-eu-west-1.acrcloud.com')).toBeTruthy();
    });

    it('turns a refused host permission into Allow, which asks again and checks', async () => {
      const requestHost = vi.fn(async () => true);
      const checkProvider = vi.fn(async () => ({ state: 'ok' as const }));
      open('app-settings-services-permission', 'services', { requestHost, checkProvider });
      const hum = (await screen.findByRole('heading', { name: 'ACRCloud humming' })).closest('section')!;
      expect(await within(hum).findByText('Music Fetch needs your permission to reach identify-eu-west-1.acrcloud.com.')).toBeTruthy();
      await fireEvent.click(within(hum).getByRole('button', { name: 'Allow' }));
      expect(await within(hum).findByText('Works. Hum something to try it.')).toBeTruthy();
      expect(requestHost).toHaveBeenCalledWith('https://identify-eu-west-1.acrcloud.com/');
    });

    it('removes keys and their check', async () => {
      open('app-settings-services-set', 'services');
      const audd = (await screen.findByRole('heading', { name: 'AudD' })).closest('section')!;
      await fireEvent.click(await within(audd).findByRole('button', { name: 'Remove' }));
      expect(await within(audd).findByRole('button', { name: 'Add keys' })).toBeTruthy();
      expect((await loadProviderKeys()).audd).toBeUndefined();
      expect((await settings.providerChecks.getValue()).audd).toBeUndefined();
    });

    it('flags a host that is not one', async () => {
      const checkProvider = vi.fn(async () => ({ state: 'ok' as const }));
      open('app-settings-services', 'services', { checkProvider });
      const group = (await screen.findByRole('heading', { name: 'ACRCloud' })).closest('section')!;
      await fireEvent.click(within(group).getByRole('button', { name: 'Add keys' }));
      await fireEvent.input(within(group).getByLabelText('Host'), { target: { value: 'http://not valid' } });
      expect(within(group).getByLabelText('Host').getAttribute('aria-invalid')).toBe('true');
      expect((within(group).getByRole('button', { name: 'Save and check' }) as HTMLButtonElement).disabled).toBe(true);
    });

    it('uses the real vault outside demo states, encrypted', async () => {
      const vault = memoryVault();
      useVault(vault);
      try {
        const state = demoState('app-settings-services')!;
        useMemorySettings(state.settings ?? {});
        useMemorySecrets(null);
        const platform = { ...demoPlatform(state, null), checkProvider: async () => ({ state: 'ok' as const }) };
        render(WithSurface, { props: { engine: new DemoEngine(state), platform, component: SettingsView, props: { pane: 'services', providers: null, narrow: false, onpane: vi.fn() } } });
        const group = (await screen.findByRole('heading', { name: 'AudD' })).closest('section')!;
        await fireEvent.click(within(group).getByRole('button', { name: 'Add keys' }));
        await fireEvent.input(within(group).getByLabelText('API token'), { target: { value: 'Tk-vault-token-99' } });
        await fireEvent.click(within(group).getByRole('button', { name: 'Save and check' }));
        await within(group).findByText('Works. AudD accepted the keys.');
        const sealed = (await vault.get('providers')) as { data: ArrayBuffer };
        expect(new TextDecoder().decode(sealed.data)).not.toContain('Tk-vault-token-99');
      } finally {
        useVault(null);
      }
    });
  });

  describe('Melody', () => {
    it('shows the installed library with its date, checks for updates and removes it', async () => {
      open('app-settings-melody', 'melody');
      expect(await screen.findByText(/41,234 songs · 55 MB · updated October 1/)).toBeTruthy();
      await fireEvent.click(screen.getByRole('button', { name: 'Check for updates' }));
      expect(await screen.findByRole('button', { name: 'Up to date' })).toBeTruthy();
      await fireEvent.click(screen.getByRole('button', { name: 'Remove' }));
      expect(await screen.findByRole('button', { name: 'Download' })).toBeTruthy();
      expect(screen.getByText(/Not installed · 55 MB/)).toBeTruthy();
    });

    it('counts the encoder file into the size and says what it does', async () => {
      open('app-settings-melody-encoder', 'melody');
      expect(await screen.findByText(/41,234 songs · 69 MB/)).toBeTruthy();
      expect(screen.getByText(/Includes the hum encoder/)).toBeTruthy();
    });

    it('downloads with progress, and says when the download failed', async () => {
      open('app-settings-melody-downloading', 'melody');
      await fireEvent.click(await screen.findByRole('button', { name: 'Download' }));
      const bar = await screen.findByRole('progressbar', { name: 'Melody library' });
      expect(bar.getAttribute('aria-valuetext')).toMatch(/of 55 MB/);
      await fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
      expect(await screen.findByRole('button', { name: 'Download' })).toBeTruthy();
    });

    it('offers Try again after a failed download', async () => {
      open('app-settings-melody-failed', 'melody');
      await fireEvent.click(await screen.findByRole('button', { name: 'Download' }));
      expect(await screen.findByText("Couldn't download. Check your connection.")).toBeTruthy();
      expect(screen.getByRole('button', { name: 'Try again' })).toBeTruthy();
    });

    it('has an update state', async () => {
      open('app-settings-melody-update', 'melody');
      expect(await screen.findByText('Update available · 58 MB')).toBeTruthy();
      expect(screen.getByRole('button', { name: 'Update' })).toBeTruthy();
    });
  });

  describe('Listening', () => {
    it('applies low power at once, with arrow keys too', async () => {
      const { engine } = open('app-settings-listening', 'listening');
      const setLowPower = vi.spyOn(engine, 'setLowPower');
      const on = await screen.findByRole('radio', { name: 'On' });
      await fireEvent.click(on);
      expect(setLowPower).toHaveBeenLastCalledWith(true);
      expect(await settings.lowPower.getValue()).toBe('on');
      await fireEvent.keyDown(on, { key: 'ArrowRight' });
      expect(await settings.lowPower.getValue()).toBe('off');
      expect(setLowPower).toHaveBeenLastCalledWith(false);
      expect(screen.getByRole('radio', { name: 'Off' }).getAttribute('aria-checked')).toBe('true');
    });
  });

  describe('Storage, privacy, shortcuts and about', () => {
    it('warns when the browser has little room left', async () => {
      open('app-settings-storage-low', 'storage');
      expect(await screen.findByText(/little room left/)).toBeTruthy();
      expect(screen.getByText('1.9 GB')).toBeTruthy();
      expect(screen.getByText(/· Other 1\.9 GB/)).toBeTruthy();
    });

    it('names paid services in the privacy pane only when they have keys', async () => {
      open('app-settings-privacy', 'privacy');
      expect(await screen.findByText(/Short audio fingerprints, to Shazam/)).toBeTruthy();
      expect(screen.queryByText(/with your own keys/)).toBeNull();
      expect(screen.queryByText(/Firefox asked you/)).toBeNull();
    });

    it('lists the services with keys and Firefox’s consent', async () => {
      open('app-settings-privacy-services', 'privacy');
      expect(await screen.findByText('Short clips, to ACRCloud and AudD, with your own keys.')).toBeTruthy();
      useMemorySecrets(null);
    });

    it('mentions Firefox’s data consent on Firefox', async () => {
      open('app-settings-privacy-firefox', 'privacy');
      expect(await screen.findByText(/Firefox asked you to allow this/)).toBeTruthy();
    });

    it('shows the browser’s shortcuts', async () => {
      open('app-settings-shortcuts', 'shortcuts');
      expect(await screen.findByText('Alt+Shift+I')).toBeTruthy();
      expect(screen.getByText('Identify this tab')).toBeTruthy();
      expect(screen.getByText('Hum')).toBeTruthy();
    });

    it('lists the licenses and opens one', async () => {
      open('app-settings-about', 'about');
      expect(await screen.findByText('svelte 5.57.2')).toBeTruthy();
      expect(screen.getByText(/Version 1\.1\.0 · Engine/)).toBeTruthy();
      const buttons = screen.getAllByRole('button', { name: 'Show license' });
      await fireEvent.click(buttons[0]!);
      expect(await screen.findByText(/Lars Nieradzik/)).toBeTruthy();
      expect(screen.getByRole('button', { name: 'Hide license' }).getAttribute('aria-expanded')).toBe('true');
    });
  });
});

describe('Settings models', () => {
  it('reads index versions as dates', () => {
    expect(indexDate('2026.10.1', new Date('2026-10-18'), 'en-US')).toBe('October 1');
    expect(indexDate('2025.3.14-android', new Date('2026-10-18'), 'en-US')).toBe('March 14, 2025');
    expect(indexDate('v2', new Date('2026-10-18'))).toBeNull();
    expect(indexDate(null)).toBeNull();
  });

  it('derives the melody row', () => {
    const base = { installed: true, version: '2026.10.1', song_count: 3, size_bytes: 10, available_version: null, download_bytes: null };
    expect(melodyView(base, { downloading: null, failed: false, check: 'idle' })).toMatchObject({ phase: 'installed', bytes: 10, encoder: false });
    // The core's sizes already count the encoder file.
    expect(melodyView({ ...base, size_bytes: 15, encoder_installed: true }, { downloading: null, failed: false, check: 'idle' })).toMatchObject({ bytes: 15, encoder: true });
    expect(melodyView({ ...base, installed: false, download_bytes: 27 }, { downloading: null, failed: false, check: 'idle' })).toEqual({ phase: 'none', bytes: 27 });
    // Only the encoder is missing: the core offers the installed version again.
    expect(melodyView({ ...base, available_version: '2026.10.1', download_bytes: 7 }, { downloading: null, failed: false, check: 'idle' })).toEqual({ phase: 'update', bytes: 7 });
    expect(melodyView(base, { downloading: 0.5, failed: false, check: 'idle' })).toMatchObject({ phase: 'downloading', fraction: 0.5 });
    expect(melodyView(null, { downloading: null, failed: true, check: 'idle' })).toEqual({ phase: 'failed' });
  });

  it('combines the engine’s numbers with the browser’s estimate', () => {
    const usage = { database_bytes: 1e5, cache_bytes: 2e6, melody_index_bytes: 55e6 };
    expect(storageView(usage, { usage: 60e6, quota: 10e9 })).toMatchObject({ total: 60e6, low: false });
    expect(storageView(usage, null)).toMatchObject({ total: 57.1e6, low: false });
    expect(storageView(usage, { usage: 9.95e9, quota: 10e9 })?.low).toBe(true);
    expect(storageView(null, null)).toBeNull();
  });
});
