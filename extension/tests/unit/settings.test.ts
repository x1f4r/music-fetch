import { beforeEach, describe, expect, it } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import {
  chooseProvider,
  loadProviderSettings,
  needsProviderPanel,
  resolveProvider,
  settings,
  skipProviderPanel,
} from '@/src/settings/settings';

describe('the provider decision', () => {
  beforeEach(() => fakeBrowser.reset());

  it('is missing until the user chooses or skips', async () => {
    const loaded = await loadProviderSettings();
    expect(loaded.needsPanel).toBe(true);
    expect(loaded.provider).toEqual({ kind: 'service', service: 'Spotify' });
    expect(await fakeBrowser.storage.sync.get()).toEqual({});
  });

  it('Skip stores Spotify, marked as skipped, in storage.sync', async () => {
    await skipProviderPanel();
    expect(await fakeBrowser.storage.sync.get('providerDecision')).toEqual({ providerDecision: { provider: 'spotify', skipped: true } });
    expect((await loadProviderSettings()).needsPanel).toBe(false);
  });

  it('Choose stores the provider under the apps’ key', async () => {
    await chooseProvider({ kind: 'service', service: 'YouTubeMusic' });
    const loaded = await loadProviderSettings();
    expect(loaded.decision).toEqual({ provider: 'youTubeMusic', skipped: false });
    expect(loaded.provider).toEqual({ kind: 'service', service: 'YouTubeMusic' });
  });

  it('a custom choice needs a valid custom provider and saves it with the decision', async () => {
    await expect(chooseProvider({ kind: 'custom' }, { name: 'Plex', template: 'https://plex.example/' })).rejects.toThrow();
    expect(await settings.providerDecision.getValue()).toBeNull();
    await chooseProvider({ kind: 'custom' }, { name: ' Plex ', template: 'https://plex.example/search?q={query}' });
    const loaded = await loadProviderSettings();
    expect(loaded.custom).toEqual({ name: 'Plex', template: 'https://plex.example/search?q={query}' });
    expect(loaded.provider).toEqual({ kind: 'custom' });
  });

  it('falls back to Spotify when the decision can’t be used', () => {
    expect(needsProviderPanel({ provider: 'gone', skipped: false })).toBe(true);
    expect(resolveProvider({ provider: 'custom', skipped: false }, null)).toEqual({ kind: 'service', service: 'Spotify' });
    expect(resolveProvider({ provider: 'deezer', skipped: false }, null)).toEqual({ kind: 'service', service: 'Deezer' });
  });

  it('keeps device state in storage.local', async () => {
    await settings.micGrant.setValue('granted');
    await settings.lastSource.setValue('Hum');
    expect(await fakeBrowser.storage.local.get()).toEqual({ micGrant: 'granted', lastSource: 'Hum' });
    expect(await settings.autoStop.getValue()).toBe(true);
    expect(await settings.lowPower.getValue()).toBe('auto');
  });
});
