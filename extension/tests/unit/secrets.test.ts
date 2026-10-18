import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fakeBrowser } from 'wxt/testing/fake-browser';
import { MockEngine } from '@/src/engine/mock/engine';
import { memoryPersistence } from '@/src/engine/mock/persistence';
import type { ExportFormat } from '@/src/engine/types';
import {
  applyStoredCredentials,
  loadCredentials,
  loadProviderKeys,
  memoryVault,
  normalizeAcrHost,
  saveProviderKeys,
  summarize,
  useMemorySecrets,
  useVault,
  watchSecrets,
  type SecretVault,
} from '@/src/platform/secrets';
import { settings } from '@/src/settings/settings';
import { demoSnapshot } from '@/src/ui/demo/library';
import type { MockBackend } from '@/src/engine/host-mock-backend';
import { InProcessWorker, mockFactory } from './support/host-fakes';

/** Values that must never show up outside the encrypted vault. */
const SECRET = 'Sx9-unit-test-access-secret-7Qp';
const TOKEN = 'Tk4-unit-test-audd-token-2Vn';
const HUM = { host: 'identify-eu-west-1.acrcloud.com', accessKey: 'unit-access-key-4f2a', accessSecret: SECRET };

/** Everything a vault holds, as text (the ciphertext's bytes decoded too). */
async function vaultText(vault: SecretVault): Promise<string> {
  const sealed = (await vault.get('providers')) as { iv: Uint8Array; data: ArrayBuffer } | undefined;
  if (!sealed) return '';
  return `${JSON.stringify(sealed)} ${new TextDecoder().decode(sealed.data)} ${new TextDecoder('latin1').decode(sealed.data)}`;
}

describe('provider keys at rest', () => {
  let vault: SecretVault;
  const logged: string[] = [];

  beforeEach(() => {
    fakeBrowser.reset();
    vault = memoryVault();
    useVault(vault);
    useMemorySecrets(null);
    logged.length = 0;
    for (const level of ['log', 'info', 'warn', 'error', 'debug'] as const) {
      vi.spyOn(console, level).mockImplementation((...args: unknown[]) => void logged.push(args.map(String).join(' ')));
    }
  });

  afterEach(() => {
    useVault(null);
    // Nothing logged anywhere in these flows may carry a key.
    for (const line of logged) {
      expect(line).not.toContain(SECRET);
      expect(line).not.toContain(TOKEN);
    }
  });

  it('round-trips, encrypted under a key that cannot be exported', async () => {
    await saveProviderKeys('acrHumming', HUM);
    await saveProviderKeys('audd', { token: `  ${TOKEN} ` });
    expect(await loadProviderKeys()).toEqual({ acrHumming: HUM, audd: { token: TOKEN } });

    const text = await vaultText(vault);
    expect(text).not.toContain(SECRET);
    expect(text).not.toContain(TOKEN);
    expect(text).not.toContain(HUM.accessKey);
    const key = (await vault.get('key')) as CryptoKey;
    expect(key.extractable).toBe(false);
    expect(key.algorithm).toMatchObject({ name: 'AES-GCM', length: 256 });
    await expect(crypto.subtle.exportKey('raw', key)).rejects.toThrow();
  });

  it('never lands in storage.sync or in clear in storage.local', async () => {
    await saveProviderKeys('acrHumming', HUM);
    await saveProviderKeys('audd', { token: TOKEN });
    await settings.providerChecks.setValue({ acrHumming: { state: 'ok', at: 1 } });
    const sync = JSON.stringify(await fakeBrowser.storage.sync.get(null));
    const local = JSON.stringify(await fakeBrowser.storage.local.get(null));
    for (const area of [sync, local]) {
      expect(area).not.toContain(SECRET);
      expect(area).not.toContain(TOKEN);
      expect(area).not.toContain(HUM.accessKey);
    }
    expect(sync).not.toContain('acrHumming');
  });

  it('uses a fresh IV per write and forgets keys it can no longer decrypt', async () => {
    await saveProviderKeys('acrHumming', HUM);
    const first = (await vault.get('providers')) as { iv: Uint8Array };
    await saveProviderKeys('audd', { token: TOKEN });
    const second = (await vault.get('providers')) as { iv: Uint8Array };
    expect([...second.iv]).not.toEqual([...first.iv]);

    // Another profile's vault key (or a cleared one): nothing usable, no error.
    await vault.put('key', await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']));
    expect(await loadProviderKeys()).toEqual({});
  });

  it('removes one provider, and the whole blob with the last one', async () => {
    await saveProviderKeys('acrHumming', HUM);
    await saveProviderKeys('audd', { token: TOKEN });
    await saveProviderKeys('audd', null);
    expect(await loadProviderKeys()).toEqual({ acrHumming: HUM });
    await saveProviderKeys('acrHumming', null);
    expect(await vault.get('providers')).toBeUndefined();
    expect(await loadProviderKeys()).toEqual({});
  });

  it('summarizes without secrets', async () => {
    await saveProviderKeys('acrHumming', HUM);
    const summary = summarize(await loadProviderKeys());
    expect(summary.acrHumming).toEqual({ set: true, host: HUM.host, keyHint: '4f2a' });
    expect(summary.audd.set).toBe(false);
    expect(JSON.stringify(summary)).not.toContain(SECRET);
  });

  it('tells other contexts only that something changed', async () => {
    const posted: unknown[] = [];
    const post = vi.spyOn(BroadcastChannel.prototype, 'postMessage').mockImplementation((message: unknown) => void posted.push(message));
    const heard = vi.fn();
    const stop = watchSecrets(heard);
    await saveProviderKeys('audd', { token: TOKEN });
    stop();
    expect(heard).toHaveBeenCalled();
    expect(posted).toEqual(['changed']);
    post.mockRestore();
  });

  it('hands the engine core-shaped credentials, again after every change', async () => {
    await saveProviderKeys('acrHumming', HUM);
    const apply = vi.fn();
    const stop = applyStoredCredentials(apply);
    await vi.waitFor(() => expect(apply).toHaveBeenCalledTimes(1));
    expect(apply).toHaveBeenLastCalledWith({
      audd_token: null,
      acr_fingerprint: null,
      acr_humming: { host: HUM.host, access_key: HUM.accessKey, access_secret: SECRET },
    });
    await saveProviderKeys('audd', { token: TOKEN });
    await vi.waitFor(() => expect(apply).toHaveBeenLastCalledWith(expect.objectContaining({ audd_token: TOKEN })));
    stop();
    expect(await loadCredentials()).toMatchObject({ audd_token: TOKEN });
  });

  it('reach the engine Worker straight from the vault, at start and after a change', async () => {
    await saveProviderKeys('acrHumming', HUM);
    const worker = new InProcessWorker(mockFactory());
    const engine = new MessageChannel();
    const capture = new MessageChannel();
    worker.postMessage({ type: 'mf:worker.init', config: { target: 'chromium', android: false, lowPower: false, cpus: 8 }, engine: engine.port2, capture: capture.port2 });
    const providers = () => (worker.backend as MockBackend | null)?.providers;
    await vi.waitFor(() => expect(providers()).toEqual({ audd: false, acr: false, acrHumming: true }));
    await saveProviderKeys('audd', { token: TOKEN });
    await vi.waitFor(() => expect(providers()).toEqual({ audd: true, acr: false, acrHumming: true }));
    worker.terminate();
    engine.port1.close();
    capture.port1.close();
  });

  it('stay out of every session export', async () => {
    await saveProviderKeys('acrHumming', HUM);
    await saveProviderKeys('audd', { token: TOKEN });
    const engine = new MockEngine({ persistence: memoryPersistence(demoSnapshot()), recoverInterrupted: false, timeScale: 1 });
    const [first] = await engine.library({ kinds: [], search: null, pinned_only: false, limit: 50, offset: 0 });
    for (const format of ['Text', 'Csv', 'Cue', 'Json'] as ExportFormat[]) {
      const text = await engine.exportSession(first!.id, format);
      expect(text).not.toContain(SECRET);
      expect(text).not.toContain(TOKEN);
    }
  });

  it('demo states keep their keys in memory, away from the vault', async () => {
    useMemorySecrets({ audd: { token: 'demo-api-token' } });
    try {
      await saveProviderKeys('acrHumming', HUM);
      expect(Object.keys(await loadProviderKeys()).sort()).toEqual(['acrHumming', 'audd']);
      expect(await vault.get('providers')).toBeUndefined();
    } finally {
      useMemorySecrets(null);
    }
  });
});

describe('ACRCloud hosts', () => {
  it('normalizes like the core', () => {
    expect(normalizeAcrHost('identify-eu-west-1.acrcloud.com')).toBe('identify-eu-west-1.acrcloud.com');
    expect(normalizeAcrHost(' https://Identify-US-West-2.ACRCloud.com/ ')).toBe('identify-us-west-2.acrcloud.com');
    expect(normalizeAcrHost('127.0.0.1:8080')).toBe('127.0.0.1:8080');
    for (const bad of ['', '   ', 'http://x.acrcloud.com', 'a b.com', '-x.com', '.x.com', 'x.com/path', 'x.com?q=1']) expect(normalizeAcrHost(bad), bad).toBeNull();
  });
});
