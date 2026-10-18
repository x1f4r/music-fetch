/**
 * Keys for the optional paid providers (Settings › Services, extension plan §3.5, X11):
 * ACRCloud humming, ACRCloud and AudD, with the user's own accounts.
 *
 * - At rest they are encrypted with AES-GCM under a non-extractable WebCrypto key. The key
 *   and the ciphertext live in this extension's IndexedDB (`mf-secrets`), never in
 *   `storage.sync` (it would copy them to every browser of the account) and never in
 *   `storage.local` in clear. The plan put the ciphertext in `storage.local`; it sits next
 *   to its key instead because the engine Worker, which has no extension APIs, reads it
 *   there (`applyStoredCredentials`), so the keys never cross the extension's messaging.
 * - Nothing here logs, and no error message carries a key. Session exports, the library
 *   and notifications never see them: only the engine's request signing does.
 * - Other contexts learn about a change through a `BroadcastChannel` message that carries
 *   no data ("changed"); they read the vault again.
 */

export type ProviderId = 'acrHumming' | 'acr' | 'audd';

/** The order of Settings › Services: humming first, the main reason to add a key. */
export const PROVIDER_IDS: readonly ProviderId[] = ['acrHumming', 'acr', 'audd'];

export interface AcrKeys {
  /** e.g. `identify-eu-west-1.acrcloud.com` */
  host: string;
  accessKey: string;
  accessSecret: string;
}

export interface AuddKeys {
  token: string;
}

export interface ProviderKeys {
  acrHumming: AcrKeys;
  acr: AcrKeys;
  audd: AuddKeys;
}

export type StoredKeys = Partial<ProviderKeys>;

/** The core's `config::Credentials` (serde shape), what the engine's `setCredentials` takes. */
export interface Credentials {
  audd_token: string | null;
  /** ACRCloud project with the fingerprint engine (recordings). */
  acr_fingerprint: CoreAcrCredentials | null;
  /** ACRCloud project with "Cover Song (humming) Identification". */
  acr_humming: CoreAcrCredentials | null;
}

export interface CoreAcrCredentials {
  host: string;
  access_key: string;
  access_secret: string;
}

/** What may be shown: whether a provider is set up, ACRCloud's host and the access key's end. */
export interface ProviderSummary {
  set: boolean;
  host: string | null;
  /** The last four characters of the ACRCloud access key (an identifier, not the secret). */
  keyHint: string | null;
}

// MARK: storage

/** Where the AES key and the ciphertext are kept. IndexedDB in the extension; memory in tests and demos. */
export interface SecretVault {
  get(name: string): Promise<unknown>;
  /** Store `value` under `name` unless something is there already; returns what is stored now. */
  add(name: string, value: unknown): Promise<unknown>;
  put(name: string, value: unknown): Promise<void>;
  delete(name: string): Promise<void>;
}

const DB_NAME = 'mf-secrets';
const STORE = 'vault';
const KEY_NAME = 'key';
const DATA_NAME = 'providers';
/** Bound to the ciphertext: a blob from another use of the key won't decrypt as ours. */
const AAD = new TextEncoder().encode('music-fetch/provider-keys/v1');

function request<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error ?? new Error('IndexedDB request failed.'));
  });
}

/** The vault in this extension's IndexedDB (works in pages, the background, offscreen documents and Workers). */
export function indexedDbVault(name = DB_NAME): SecretVault {
  let db: Promise<IDBDatabase> | null = null;
  const open = () =>
    (db ??= new Promise((resolve, reject) => {
      const req = indexedDB.open(name, 1);
      req.onupgradeneeded = () => req.result.createObjectStore(STORE);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => {
        db = null;
        reject(req.error ?? new Error('The key store could not be opened.'));
      };
    }));
  const store = async (mode: IDBTransactionMode) => (await open()).transaction(STORE, mode).objectStore(STORE);
  return {
    async get(key) {
      return request((await store('readonly')).get(key));
    },
    async add(key, value) {
      const objects = await store('readwrite');
      const existing = await request(objects.get(key));
      if (existing !== undefined) return existing;
      try {
        await request(objects.add(value, key));
        return value;
      } catch {
        // Another context added it first: use theirs.
        return request((await store('readonly')).get(key));
      }
    },
    async put(key, value) {
      await request((await store('readwrite')).put(value, key));
    },
    async delete(key) {
      await request((await store('readwrite')).delete(key));
    },
  };
}

/** A vault in memory (unit tests, demo states). Values are kept as given, like IndexedDB's structured clone keeps a CryptoKey. */
export function memoryVault(initial: Record<string, unknown> = {}): SecretVault {
  const values = new Map(Object.entries(initial));
  return {
    async get(key) {
      return values.get(key);
    },
    async add(key, value) {
      if (!values.has(key)) values.set(key, value);
      return values.get(key);
    },
    async put(key, value) {
      values.set(key, value);
    },
    async delete(key) {
      values.delete(key);
    },
  };
}

let vault: SecretVault | null = null;
/** Demo states: keys typed there stay in memory, in clear (they are never real keys). */
let plain: StoredKeys | null = null;

function currentVault(): SecretVault {
  return (vault ??= indexedDbVault());
}

/** Use another vault (tests). */
export function useVault(next: SecretVault | null): void {
  vault = next;
}

/**
 * Demo states keep provider keys in memory, like their settings, so trying a state
 * never touches the user's real keys. `null` switches back to the real vault.
 */
export function useMemorySecrets(initial: StoredKeys | null = {}): void {
  plain = initial === null ? null : structuredClone(initial);
}

// MARK: encryption

interface Sealed {
  v: 1;
  iv: Uint8Array<ArrayBuffer>;
  data: ArrayBuffer;
}

function isSealed(value: unknown): value is Sealed {
  const sealed = value as Partial<Sealed> | null;
  // By tag, not `instanceof`: a Worker's or a test realm's ArrayBuffer is another constructor.
  const tag = (v: unknown) => Object.prototype.toString.call(v);
  return Boolean(sealed && sealed.v === 1 && tag(sealed.iv) === '[object Uint8Array]' && tag(sealed.data) === '[object ArrayBuffer]');
}

function isCryptoKey(value: unknown): value is CryptoKey {
  return typeof CryptoKey === 'function' && value instanceof CryptoKey;
}

/** The vault's AES-GCM key, made once per browser profile and never extractable. */
async function vaultKey(): Promise<CryptoKey> {
  const store = currentVault();
  const existing = await store.get(KEY_NAME);
  if (isCryptoKey(existing)) return existing;
  const made = await crypto.subtle.generateKey({ name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']);
  const stored = await store.add(KEY_NAME, made);
  if (!isCryptoKey(stored)) throw new Error('The key store holds something unexpected.');
  return stored;
}

async function seal(keys: StoredKeys): Promise<Sealed> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const data = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: AAD }, await vaultKey(), new TextEncoder().encode(JSON.stringify(keys)));
  return { v: 1, iv, data };
}

async function unseal(sealed: Sealed): Promise<StoredKeys> {
  const key = await currentVault().get(KEY_NAME);
  if (!isCryptoKey(key)) return {};
  try {
    const clear = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: sealed.iv, additionalData: AAD }, key, sealed.data);
    return clean(JSON.parse(new TextDecoder().decode(clear)) as unknown);
  } catch {
    // A key from another profile or a damaged blob: nothing usable (the user enters them again).
    return {};
  }
}

/** Only well-formed, non-empty entries survive a round trip. */
function clean(value: unknown): StoredKeys {
  const out: StoredKeys = {};
  const raw = (value ?? {}) as Record<string, Record<string, unknown> | undefined>;
  for (const id of ['acrHumming', 'acr'] as const) {
    const entry = raw[id];
    if (entry && isText(entry.host) && isText(entry.accessKey) && isText(entry.accessSecret)) {
      out[id] = { host: entry.host, accessKey: entry.accessKey, accessSecret: entry.accessSecret };
    }
  }
  if (raw.audd && isText(raw.audd.token)) out.audd = { token: raw.audd.token };
  return out;
}

function isText(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

// MARK: API

/** Every provider's keys, decrypted. Only the Services pane and the engine read them. */
export async function loadProviderKeys(): Promise<StoredKeys> {
  if (plain) return structuredClone(plain);
  const sealed = await currentVault().get(DATA_NAME);
  return isSealed(sealed) ? unseal(sealed) : {};
}

/** Store one provider's keys (trimmed), or remove them with `null`. Other contexts hear about it. */
export async function saveProviderKeys<K extends ProviderId>(id: K, keys: ProviderKeys[K] | null): Promise<void> {
  const all = await loadProviderKeys();
  if (keys) all[id] = trimmed(keys);
  else delete all[id];
  const next = clean(all);
  if (plain) {
    plain = next;
  } else if (Object.keys(next).length === 0) {
    await currentVault().delete(DATA_NAME);
  } else {
    await currentVault().put(DATA_NAME, await seal(next));
  }
  announce();
}

function trimmed<T extends AcrKeys | AuddKeys>(keys: T): T {
  return Object.fromEntries(Object.entries(keys).map(([k, v]) => [k, typeof v === 'string' ? v.trim() : v])) as T;
}

/** The engine's view of the stored keys. */
export function toCredentials(keys: StoredKeys): Credentials {
  const acr = (k: AcrKeys | undefined): CoreAcrCredentials | null => (k ? { host: k.host, access_key: k.accessKey, access_secret: k.accessSecret } : null);
  return { audd_token: keys.audd?.token ?? null, acr_fingerprint: acr(keys.acr), acr_humming: acr(keys.acrHumming) };
}

export async function loadCredentials(): Promise<Credentials> {
  return toCredentials(await loadProviderKeys());
}

export function summarize(keys: StoredKeys): Record<ProviderId, ProviderSummary> {
  const acr = (k: AcrKeys | undefined): ProviderSummary => ({ set: Boolean(k), host: k?.host ?? null, keyHint: k ? k.accessKey.slice(-4) : null });
  return { acrHumming: acr(keys.acrHumming), acr: acr(keys.acr), audd: { set: Boolean(keys.audd), host: null, keyHint: null } };
}

/** Every secret value in `keys`: what must never appear in a log, export or report. */
export function secretValues(keys: StoredKeys): string[] {
  return [keys.acrHumming?.accessSecret, keys.acr?.accessSecret, keys.audd?.token].filter((v): v is string => typeof v === 'string' && v.length >= 4);
}

/** `text` with every secret of `keys` replaced by "••••" (for messages that might echo input). */
export function redactSecrets(text: string, keys: StoredKeys): string {
  return secretValues(keys).reduce((out, secret) => out.split(secret).join('••••'), text);
}

// MARK: change notices

const CHANNEL = 'mf-secrets';

function channel(): BroadcastChannel | null {
  return typeof BroadcastChannel === 'function' ? new BroadcastChannel(CHANNEL) : null;
}

const localListeners = new Set<() => void>();

function announce() {
  for (const listener of localListeners) listener();
  const out = channel();
  // The message says only that something changed; readers decrypt for themselves.
  out?.postMessage('changed');
  out?.close();
}

/** Call `callback` (without data) whenever the keys change, in this or any other extension context. */
export function watchSecrets(callback: () => void): () => void {
  localListeners.add(callback);
  const incoming = channel();
  if (incoming) {
    incoming.onmessage = (event: MessageEvent) => event.data === 'changed' && callback();
    // Node (unit tests): a listening channel must not keep the process alive.
    (incoming as BroadcastChannel & { unref?: () => void }).unref?.();
  }
  return () => {
    localListeners.delete(callback);
    incoming?.close();
  };
}

/**
 * The engine side (in the engine Worker): hand the stored keys to the engine now and after
 * every change. `apply` is the engine's `setCredentials`.
 */
export function applyStoredCredentials(apply: (credentials: Credentials) => Promise<void> | void): () => void {
  let latest = 0;
  const load = () => {
    const turn = ++latest;
    void loadCredentials()
      .then((credentials) => (turn === latest ? apply(credentials) : undefined))
      // Without the keys the engine runs with the free services only; nothing to report.
      .catch(() => undefined);
  };
  load();
  return watchSecrets(load);
}

// MARK: validation

/**
 * ACRCloud's host as the core accepts it (`providers::acrcloud::normalize_host`): a bare
 * host name, an `https://` prefix and a trailing slash tolerated. Null when invalid.
 */
export function normalizeAcrHost(host: string): string | null {
  let value = host.trim();
  if (value.startsWith('https://')) value = value.slice('https://'.length);
  value = value.replace(/\/+$/, '');
  const valid = value.length > 0 && value.length <= 253 && /^[A-Za-z0-9.:-]+$/.test(value) && !/^[.:-]/.test(value);
  return valid ? value.toLowerCase() : null;
}

export function isCompleteAcr(keys: Partial<AcrKeys>): keys is AcrKeys {
  return normalizeAcrHost(keys.host ?? '') !== null && isText(keys.accessKey) && isText(keys.accessSecret);
}
