import { browser } from 'wxt/browser';
import { headerPolicy, type HeaderRule } from './policy';

/** A declarativeNetRequest rule, as both browsers take it. */
export interface DnrRule {
  id: number;
  priority: number;
  action: {
    type: 'modifyHeaders';
    requestHeaders: { header: string; operation: 'set' | 'remove'; value?: string }[];
  };
  condition: {
    urlFilter: string;
    resourceTypes: ('xmlhttprequest' | 'other')[];
    tabIds: number[];
    initiatorDomains: string[];
  };
}

/** Session rule ids this extension owns. */
const FIRST_ID = 1000;

/**
 * Compile the header policy into session rules (S4: one implementation for Chromium and
 * Firefox). Every rule is scoped to requests that belong to no tab (`tabIds: [-1]`) and
 * were made by the extension itself (`initiatorDomains: [own host]`), so the user's own
 * browsing on these sites is never touched; unscoped rules would rewrite the user's own
 * YouTube cookie (S4, variant c-none).
 */
export function compileRules(policy: HeaderRule[], ownHost: string, extraPrefixes: Partial<Record<HeaderRule['id'], string[]>> = {}): DnrRule[] {
  const rules: DnrRule[] = [];
  let id = FIRST_ID;
  for (const rule of policy) {
    const requestHeaders = [
      ...Object.entries(rule.set).map(([header, value]) => ({ header, operation: 'set' as const, value })),
      ...rule.remove.map((header) => ({ header, operation: 'remove' as const })),
    ];
    for (const prefix of [...rule.prefixes, ...(extraPrefixes[rule.id] ?? [])]) {
      rules.push({
        id: id++,
        priority: 1,
        action: { type: 'modifyHeaders', requestHeaders },
        condition: { urlFilter: `|${prefix}`, resourceTypes: ['xmlhttprequest', 'other'], tabIds: [-1], initiatorDomains: [ownHost] },
      });
    }
  }
  return rules;
}

/** The extension's own host: its id on Chromium, the install's UUID on Firefox. */
export function ownHost(): string {
  return new URL(browser.runtime.getURL('/')).host;
}

interface DnrApi {
  getSessionRules(): Promise<{ id: number }[]>;
  updateSessionRules(options: { removeRuleIds: number[]; addRules: DnrRule[] }): Promise<void>;
}

function dnr(): DnrApi | undefined {
  return (browser as unknown as { declarativeNetRequest?: DnrApi }).declarativeNetRequest;
}

/** e2e builds only: also rewrite the local echo server's paths (tests/e2e/net). */
async function testPrefixes(): Promise<Partial<Record<HeaderRule['id'], string[]>>> {
  if (import.meta.env.MODE !== 'e2e') return {};
  const { mfE2eEcho } = (await browser.storage.local.get('mfE2eEcho')) as { mfE2eEcho?: string };
  if (!mfE2eEcho) return {};
  return Object.fromEntries(headerPolicy('0').map((r) => [r.id, [`${mfE2eEcho}/echo/${r.id}`]]));
}

/**
 * Install the rules, replacing ours (idempotent). Session rules don't survive a browser
 * restart, so the background calls this on every start and the host before the engine's
 * first request. Returns the number of rules installed.
 */
export async function installHeaderRules(shazamAgent = Math.floor(Math.random() * 8)): Promise<number> {
  const api = dnr();
  if (!api) return 0;
  const rules = compileRules(headerPolicy(browser.runtime.getManifest().version, shazamAgent), ownHost(), await testPrefixes());
  const existing = await api.getSessionRules();
  await api.updateSessionRules({
    removeRuleIds: existing.map((r) => r.id).filter((id) => id >= FIRST_ID && id < FIRST_ID + 1000),
    addRules: rules,
  });
  return rules.length;
}
