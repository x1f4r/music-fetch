import { describe, expect, it, vi } from 'vitest';
import { acrEndpoint, acrSignature, acrState, acrStringToSign, checkAcr, checkAudd, providerOrigin, silentWav } from '@/src/ui/settings/services-check';

const SECRET = 'Example0Secret0Key0123456789abcdefABCDEF';
const KEYS = { host: 'identify-eu-west-1.acrcloud.com', accessKey: 'a1b2c3d4e5f6a7b8', accessSecret: SECRET };

function reply(body: unknown, status = 200): typeof fetch {
  return vi.fn(async () => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })) as unknown as typeof fetch;
}

describe('the ACRCloud check', () => {
  it('signs exactly as the core does (core acrcloud.rs test vector)', async () => {
    expect(acrStringToSign('a1b2c3d4e5f6a7b8', 1_791_900_000)).toBe('POST\n/v1/identify\na1b2c3d4e5f6a7b8\naudio\n1\n1791900000');
    expect(await acrSignature('a1b2c3d4e5f6a7b8', SECRET, 1_791_900_000)).toBe('mp/xuHc17uqDkJOHO74HuITLOZk=');
  });

  it('posts a second of silence with the signed fields, without cookies', async () => {
    const fetchImpl = reply({ status: { code: 1001, msg: 'No result' } });
    const result = await checkAcr(KEYS, { fetch: fetchImpl, now: () => 1_791_900_000 });
    expect(result).toEqual({ state: 'ok' });
    const [url, init] = (fetchImpl as unknown as { mock: { calls: [string, RequestInit][] } }).mock.calls[0]!;
    expect(url).toBe('https://identify-eu-west-1.acrcloud.com/v1/identify');
    expect(init.credentials).toBe('omit');
    const form = init.body as FormData;
    expect(form.get('access_key')).toBe('a1b2c3d4e5f6a7b8');
    expect(form.get('signature')).toBe('mp/xuHc17uqDkJOHO74HuITLOZk=');
    expect(form.get('timestamp')).toBe('1791900000');
    expect(form.get('data_type')).toBe('audio');
    expect(form.get('signature_version')).toBe('1');
    expect(form.get('sample_bytes')).toBe(String(silentWav().length));
    // The secret itself never goes over the wire, only its signature.
    for (const [, value] of form.entries()) expect(String(value)).not.toContain(SECRET);
  });

  it('maps ACRCloud’s answers', () => {
    expect(acrState(0).state).toBe('ok');
    expect(acrState(2004).state).toBe('ok');
    expect(acrState(3015).state).toBe('ok');
    expect(acrState(3001)).toEqual({ state: 'rejected', code: 3001 });
    expect(acrState(3014)).toEqual({ state: 'rejected', code: 3014 });
    expect(acrState(3003).state).toBe('quota');
    expect(acrState(3000).state).toBe('network');
    expect(acrState(2010)).toEqual({ state: 'failed', code: 2010 });
    expect(acrState(null)).toEqual({ state: 'failed' });
  });

  it('reports a rejected signature, an unreachable host and a bad host', async () => {
    expect(await checkAcr(KEYS, { fetch: reply({ status: { code: 3014, msg: 'invalid signature' } }) })).toEqual({ state: 'rejected', code: 3014 });
    const offline = vi.fn(async () => {
      throw new TypeError('Failed to fetch');
    }) as unknown as typeof fetch;
    expect(await checkAcr(KEYS, { fetch: offline })).toEqual({ state: 'network' });
    expect(await checkAcr({ ...KEYS, host: 'not a host' }, { fetch: offline })).toEqual({ state: 'host' });
    expect(await checkAcr(KEYS, { fetch: reply('<html>', 502) })).toEqual({ state: 'failed' });
  });

  it('reaches the local stub over http only in e2e builds', () => {
    expect(acrEndpoint('127.0.0.1:4567')).toBe('https://127.0.0.1:4567/v1/identify');
    expect(acrEndpoint('127.0.0.1:4567', true)).toBe('http://127.0.0.1:4567/v1/identify');
    expect(acrEndpoint('identify-eu-west-1.acrcloud.com', true)).toBe('https://identify-eu-west-1.acrcloud.com/v1/identify');
    expect(providerOrigin('acrHumming', KEYS)).toBe('https://identify-eu-west-1.acrcloud.com/*');
    expect(providerOrigin('audd', { token: 'x' })).toBe('https://api.audd.io/*');
  });

  it('writes a valid WAV header', () => {
    const wav = silentWav(1, 8000);
    const view = new DataView(wav.buffer);
    expect(new TextDecoder().decode(wav.subarray(0, 4))).toBe('RIFF');
    expect(view.getUint32(24, true)).toBe(8000);
    expect(view.getUint32(40, true)).toBe(16_000);
    expect(wav.length).toBe(44 + 16_000);
  });
});

describe('the AudD check', () => {
  it('maps AudD’s answers', async () => {
    expect(await checkAudd({ token: 't' }, { fetch: reply({ status: 'success', result: null }) })).toEqual({ state: 'ok' });
    expect(await checkAudd({ token: 't' }, { fetch: reply({ status: 'error', error: { error_code: 900, error_message: 'bad token' } }) })).toEqual({ state: 'rejected', code: 900 });
    expect(await checkAudd({ token: 't' }, { fetch: reply({ status: 'error', error: { error_code: 902 } }) })).toEqual({ state: 'quota', code: 902 });
    expect(await checkAudd({ token: 't' }, { fetch: reply({ status: 'error', error: { error_code: 19 } }) })).toEqual({ state: 'failed', code: 19 });
    expect(await checkAudd({ token: 't' }, { fetch: reply({}, 429) })).toEqual({ state: 'quota' });
  });
});
