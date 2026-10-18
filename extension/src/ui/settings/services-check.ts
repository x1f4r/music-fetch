import { normalizeAcrHost, type AcrKeys, type AuddKeys, type ProviderId, type ProviderKeys } from '../../platform/secrets';

/**
 * "Check" in Settings › Services: one request with a second of silence, signed exactly as
 * the engine signs its requests (core `providers::acrcloud`, `providers::audd`), so the
 * answer says whether the keys work. ACRCloud and AudD answer silence with "no result",
 * which counts as working; a rejected key or signature has its own code.
 *
 * The request leaves from the extension page with `credentials: 'omit'` (no cookies of
 * those sites), only when the user saves or presses Check, and no message here ever
 * contains a key.
 */

export type CheckState = 'ok' | 'rejected' | 'quota' | 'network' | 'host' | 'permission' | 'failed';

export interface CheckResult {
  state: CheckState;
  /** The provider's own error code, for "failed". */
  code?: number;
}

export interface CheckOptions {
  fetch?: typeof fetch;
  /** Seconds since the epoch (tests). */
  now?: () => number;
  /** e2e builds: ACRCloud on the local stub server, over http. */
  allowLoopbackHttp?: boolean;
}

const ACR_PATH = '/v1/identify';
const AUDD_ENDPOINT = 'https://api.audd.io/';

/** A WAV of `seconds` of silence (mono, 16-bit, 8 kHz): about 16 KB for one second. */
export function silentWav(seconds = 1, rate = 8000): Uint8Array<ArrayBuffer> {
  const samples = Math.round(seconds * rate);
  const bytes = new Uint8Array(44 + samples * 2);
  const view = new DataView(bytes.buffer);
  const text = (offset: number, value: string) => [...value].forEach((c, i) => view.setUint8(offset + i, c.charCodeAt(0)));
  text(0, 'RIFF');
  view.setUint32(4, 36 + samples * 2, true);
  text(8, 'WAVE');
  text(12, 'fmt ');
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, rate, true);
  view.setUint32(28, rate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  text(36, 'data');
  view.setUint32(40, samples * 2, true);
  return bytes;
}

/** `"POST\n/v1/identify\n{access_key}\naudio\n1\n{timestamp}"`, as the core signs it. */
export function acrStringToSign(accessKey: string, timestamp: number): string {
  return `POST\n${ACR_PATH}\n${accessKey}\naudio\n1\n${timestamp}`;
}

/** `base64(HMAC-SHA1(secret, string_to_sign))` */
export async function acrSignature(accessKey: string, secret: string, timestamp: number): Promise<string> {
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']);
  const mac = new Uint8Array(await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(acrStringToSign(accessKey, timestamp))));
  return btoa(String.fromCharCode(...mac));
}

/** Where the check for an ACRCloud host goes. */
export function acrEndpoint(host: string, allowLoopbackHttp = false): string | null {
  const normalized = normalizeAcrHost(host);
  if (!normalized) return null;
  if (allowLoopbackHttp && /^(127\.0\.0\.1|localhost)(:\d+)?$/.test(normalized)) return `http://${normalized}${ACR_PATH}`;
  return `https://${normalized}${ACR_PATH}`;
}

/** The origin pattern the extension needs to reach a provider (asked for when the user saves). */
export function providerOrigin<K extends ProviderId>(id: K, keys: ProviderKeys[K], allowLoopbackHttp = false): string | null {
  if (id === 'audd') return 'https://api.audd.io/*';
  const endpoint = acrEndpoint((keys as AcrKeys).host, allowLoopbackHttp);
  return endpoint ? `${new URL(endpoint).origin}/*` : null;
}

/** ACRCloud's answer codes (core `check_status`). */
export function acrState(code: number | null): CheckResult {
  switch (code) {
    case 0:
    case 1001: // no result
    case 2004: // no fingerprint (silence): the request itself was accepted
    case 3015: // too many requests per second: the keys are fine
      return { state: 'ok' };
    case 3001:
    case 3014:
      return { state: 'rejected', code };
    case 3003:
      return { state: 'quota', code };
    case 3000:
      return { state: 'network', code };
    case null:
      return { state: 'failed' };
    default:
      return { state: 'failed', code };
  }
}

async function post(url: string, form: FormData, options: CheckOptions): Promise<unknown> {
  const doFetch = options.fetch ?? fetch;
  let response: Response;
  try {
    response = await doFetch(url, { method: 'POST', body: form, credentials: 'omit', cache: 'no-store', signal: AbortSignal.timeout(15_000) });
  } catch {
    // Offline, DNS, a refused host permission: the browser says no more than that.
    throw new CheckFailure('network');
  }
  if (response.status === 401 || response.status === 403) throw new CheckFailure('rejected');
  if (response.status === 429) throw new CheckFailure('quota');
  try {
    return await response.json();
  } catch {
    throw new CheckFailure('failed');
  }
}

class CheckFailure extends Error {
  constructor(readonly state: CheckState) {
    super(state);
  }
}

export async function checkAcr(keys: AcrKeys, options: CheckOptions = {}): Promise<CheckResult> {
  const url = acrEndpoint(keys.host, options.allowLoopbackHttp);
  if (!url) return { state: 'host' };
  const accessKey = keys.accessKey.trim();
  const timestamp = Math.floor(options.now?.() ?? Date.now() / 1000);
  const sample = silentWav();
  const form = new FormData();
  form.append('access_key', accessKey);
  form.append('sample_bytes', String(sample.length));
  form.append('timestamp', String(timestamp));
  form.append('signature', await acrSignature(accessKey, keys.accessSecret.trim(), timestamp));
  form.append('data_type', 'audio');
  form.append('signature_version', '1');
  form.append('sample', new Blob([sample], { type: 'audio/wav' }), 'sample.wav');
  try {
    const body = (await post(url, form, options)) as { status?: { code?: unknown } } | null;
    const code = typeof body?.status?.code === 'number' ? body.status.code : null;
    return acrState(code);
  } catch (error) {
    return { state: error instanceof CheckFailure ? error.state : 'failed' };
  }
}

export async function checkAudd(keys: AuddKeys, options: CheckOptions = {}): Promise<CheckResult> {
  const form = new FormData();
  form.append('api_token', keys.token.trim());
  form.append('file', new Blob([silentWav()], { type: 'audio/wav' }), 'clip.wav');
  try {
    const body = (await post(AUDD_ENDPOINT, form, options)) as { status?: unknown; error?: { error_code?: unknown } } | null;
    if (body?.status === 'success') return { state: 'ok' };
    const code = typeof body?.error?.error_code === 'number' ? body.error.error_code : null;
    if (code === 900 || code === 901) return { state: 'rejected', code };
    if (code === 902) return { state: 'quota', code };
    return code === null ? { state: 'failed' } : { state: 'failed', code };
  } catch (error) {
    return { state: error instanceof CheckFailure ? error.state : 'failed' };
  }
}

export function checkProvider<K extends ProviderId>(id: K, keys: ProviderKeys[K], options: CheckOptions = {}): Promise<CheckResult> {
  return id === 'audd' ? checkAudd(keys as AuddKeys, options) : checkAcr(keys as AcrKeys, options);
}
