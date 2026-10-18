import {
  loadProviderKeys,
  normalizeAcrHost,
  PROVIDER_IDS,
  saveProviderKeys,
  summarize,
  watchSecrets,
  type ProviderId,
  type ProviderKeys,
  type ProviderSummary,
} from '../../platform/secrets';
import { settings, type ProviderCheck } from '../../settings/settings';
import { checkProvider, providerOrigin, type CheckOptions, type CheckResult } from './services-check';

/**
 * Settings › Services (extension plan §5.8, X11): the user's own ACRCloud and AudD keys.
 * Saving asks the browser for the provider's host (inside the click), stores the keys
 * encrypted (`platform/secrets.ts`) and checks them at once, so the pane says plainly
 * whether they work. The engine picks them up by itself (`applyStoredCredentials`).
 *
 * Only summaries reach the page's state (set or not, ACRCloud's host, the access key's
 * last four characters); keys typed into the fields stay in the fields.
 */
export type CheckView = ProviderCheck | { state: 'checking'; at: number };

export interface ServicesDeps {
  /** `Platform.requestHost`: must start inside the click, nothing awaited before it. */
  requestHost(url: string): Promise<boolean>;
  check?: (id: ProviderId, keys: ProviderKeys[ProviderId], options: CheckOptions) => Promise<CheckResult>;
  /** e2e builds: ACRCloud on the local stub server. */
  allowLoopbackHttp?: boolean;
  now?: () => number;
}

export class ServicesController {
  summary: Record<ProviderId, ProviderSummary> = $state(summarize({}));
  checks: Partial<Record<ProviderId, CheckView>> = $state({});
  loaded = $state(false);
  private stops: (() => void)[] = [];

  constructor(private readonly deps: ServicesDeps) {}

  async start(): Promise<void> {
    await this.reload();
    this.checks = { ...(await settings.providerChecks.getValue()) };
    this.loaded = true;
    this.stops.push(watchSecrets(() => void this.reload()));
    this.stops.push(settings.providerChecks.watch((value) => (this.checks = { ...value, ...this.running() })));
  }

  dispose(): void {
    this.stops.forEach((stop) => stop());
    this.stops = [];
  }

  /** The URL whose host the browser must allow for `keys` (null: the host is not valid). */
  permissionUrl<K extends ProviderId>(id: K, keys: ProviderKeys[K]): string | null {
    const pattern = providerOrigin(id, keys, this.deps.allowLoopbackHttp);
    return pattern ? pattern.replace(/\*$/, '') : null;
  }

  /**
   * "Save and check", called straight from the click: the permission request starts
   * before anything is awaited (both browsers need the user gesture).
   */
  save<K extends ProviderId>(id: K, keys: ProviderKeys[K]): Promise<void> {
    const url = this.permissionUrl(id, keys);
    if (!url) {
      this.setCheck(id, { state: 'host', at: this.now() });
      return Promise.resolve();
    }
    const granted = this.deps.requestHost(url);
    return this.finishSave(id, keys, granted);
  }

  private async finishSave<K extends ProviderId>(id: K, keys: ProviderKeys[K], granted: Promise<boolean>): Promise<void> {
    const normalized = id === 'audd' ? keys : ({ ...(keys as ProviderKeys['acr']), host: normalizeAcrHost((keys as ProviderKeys['acr']).host) ?? '' } as ProviderKeys[K]);
    await saveProviderKeys(id, normalized);
    await this.reload();
    if (!(await granted.catch(() => false))) {
      this.setCheck(id, { state: 'permission', at: this.now() });
      return;
    }
    await this.runCheck(id, normalized);
  }

  /** "Allow" after a refused permission: ask again inside the click, then check. */
  allow(id: ProviderId): Promise<void> {
    const summary = this.summary[id];
    const url = id === 'audd' ? 'https://api.audd.io/' : summary.host ? this.permissionUrl(id, { host: summary.host, accessKey: '-', accessSecret: '-' } as never) : null;
    if (!url) return Promise.resolve();
    const granted = this.deps.requestHost(url);
    return (async () => {
      if (!(await granted.catch(() => false))) return;
      await this.check(id);
    })();
  }

  /** "Check": one request with the stored keys. */
  async check(id: ProviderId): Promise<void> {
    const keys = (await loadProviderKeys())[id];
    if (!keys) return;
    await this.runCheck(id, keys);
  }

  async remove(id: ProviderId): Promise<void> {
    await saveProviderKeys(id, null);
    const checks = { ...(await settings.providerChecks.getValue()) };
    delete checks[id];
    await settings.providerChecks.setValue(checks);
    this.checks = { ...checks };
    await this.reload();
  }

  private async runCheck<K extends ProviderId>(id: K, keys: ProviderKeys[K]): Promise<void> {
    this.checks = { ...this.checks, [id]: { state: 'checking', at: this.now() } };
    const check = this.deps.check ?? checkProvider;
    let result: CheckResult;
    try {
      result = await check(id, keys, { allowLoopbackHttp: this.deps.allowLoopbackHttp });
    } catch {
      result = { state: 'failed' };
    }
    await this.setCheck(id, { state: result.state, ...(result.code !== undefined ? { code: result.code } : {}), at: this.now() });
  }

  private async setCheck(id: ProviderId, check: ProviderCheck): Promise<void> {
    this.checks = { ...this.checks, [id]: check };
    const stored = { ...(await settings.providerChecks.getValue()), [id]: check };
    await settings.providerChecks.setValue(stored);
  }

  private running(): Partial<Record<ProviderId, CheckView>> {
    return Object.fromEntries(PROVIDER_IDS.filter((id) => this.checks[id]?.state === 'checking').map((id) => [id, this.checks[id]!]));
  }

  private async reload(): Promise<void> {
    this.summary = summarize(await loadProviderKeys().catch(() => ({})));
  }

  private now(): number {
    return this.deps.now?.() ?? Date.now();
  }
}
