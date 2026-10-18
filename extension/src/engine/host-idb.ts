/**
 * A tiny IndexedDB key–value store of the extension origin. It works in every context
 * the host and the engine run in (offscreen document, Firefox event page, Workers),
 * unlike `localStorage` (no Workers) and `chrome.storage` (not in offscreen documents).
 */
export interface Kv {
  get<T>(key: string): Promise<T | undefined>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
  entries<T>(): Promise<[string, T][]>;
  close(): void;
}

function request<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

export async function openKv(name: string, store = 'kv'): Promise<Kv> {
  const open = indexedDB.open(name, 1);
  open.onupgradeneeded = () => {
    if (!open.result.objectStoreNames.contains(store)) open.result.createObjectStore(store);
  };
  const db = await request(open);
  // Short transactions only: nothing here holds one across an await.
  const tx = (mode: IDBTransactionMode) => db.transaction(store, mode).objectStore(store);
  const done = (t: IDBObjectStore) =>
    new Promise<void>((resolve, reject) => {
      t.transaction.oncomplete = () => resolve();
      t.transaction.onerror = () => reject(t.transaction.error);
      t.transaction.onabort = () => reject(t.transaction.error);
    });
  return {
    get: <T>(key: string) => request(tx('readonly').get(key)) as Promise<T | undefined>,
    async put(key, value) {
      const t = tx('readwrite');
      t.put(value, key);
      await done(t);
    },
    async delete(key) {
      const t = tx('readwrite');
      t.delete(key);
      await done(t);
    },
    async entries<T>() {
      const t = tx('readonly');
      const [keys, values] = await Promise.all([request(t.getAllKeys()), request(t.getAll())]);
      return keys.map((k, i) => [String(k), values[i] as T] as [string, T]);
    },
    close: () => db.close(),
  };
}

/** An in-memory `Kv` for tests and for contexts without IndexedDB. */
export function memoryKv(): Kv & { data: Map<string, unknown> } {
  const data = new Map<string, unknown>();
  return {
    data,
    get: async <T>(key: string) => (data.has(key) ? (structuredClone(data.get(key)) as T) : undefined),
    put: async (key, value) => void data.set(key, structuredClone(value)),
    delete: async (key) => void data.delete(key),
    entries: async <T>() => [...data.entries()].map(([k, v]) => [k, structuredClone(v) as T] as [string, T]),
    close: () => undefined,
  };
}
