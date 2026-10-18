/**
 * How the engine fetches (extension plan §2.6, S4). The wasm engine's reqwest client
 * does the same through `fetch_credentials_omit()`; this is the reference for every
 * other request the extension's engine side makes.
 *
 * - `credentials: 'omit'`: the fetch default (`same-origin`) sends the user's cookies for
 *   the target site from extension contexts in both browsers (S4); the engine must never
 *   carry the user's logins.
 * - `redirect: 'follow'`: `manual` gives an opaque response without `Location`, so
 *   per-hop checks are impossible; the final URL is checked instead.
 */

/** Hosts that are never public: loopback, private ranges, link-local, `.local`/`.internal`. */
export function isPublicHost(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, '');
  if (!host || host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local') || host.endsWith('.internal') || host.endsWith('.lan') || host.endsWith('.home.arpa')) return false;
  const v4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(host);
  if (v4) {
    const [a, b] = [Number(v4[1]), Number(v4[2])];
    if (a === 10 || a === 127 || a === 0 || (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127) || a >= 224) return false;
    return true;
  }
  if (host.includes(':')) {
    // IPv6 literal: loopback, unique local, link-local, mapped v4.
    if (host === '::1' || host === '::' || /^f[cd]/.test(host) || /^fe[89ab]/.test(host) || host.startsWith('::ffff:')) return false;
    return true;
  }
  return true;
}

export class FetchPolicyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FetchPolicyError';
  }
}

/** Whether `url` may be fetched by the engine: https on a public host (http only where `allowHttp`). */
export function checkUrl(url: string, options: { allowPrivate?: boolean; allowHttp?: boolean } = {}): URL {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new FetchPolicyError(`Not a URL: ${url}`);
  }
  if (parsed.protocol !== 'https:' && !(options.allowHttp && parsed.protocol === 'http:')) throw new FetchPolicyError(`Only https links can be fetched (${parsed.protocol}).`);
  if (parsed.username || parsed.password) throw new FetchPolicyError('Links with credentials are not fetched.');
  if (!options.allowPrivate && !isPublicHost(parsed.hostname)) throw new FetchPolicyError(`${parsed.hostname} is not a public host.`);
  return parsed;
}

export interface EngineFetchOptions {
  /** Test builds: the local echo server. */
  allowPrivate?: boolean;
  allowHttp?: boolean;
  fetchImpl?: typeof fetch;
}

/** `fetch` with the engine's policy: no cookies, redirects followed, the final URL checked. */
export async function engineFetch(url: string, init: RequestInit = {}, options: EngineFetchOptions = {}): Promise<Response> {
  checkUrl(url, options);
  const response = await (options.fetchImpl ?? fetch)(url, { ...init, credentials: 'omit', redirect: 'follow' });
  // A redirect may have left the public internet (the browser follows up to 20 hops).
  if (response.url) checkUrl(response.url, options);
  return response;
}
