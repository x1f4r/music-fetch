import { browser } from 'wxt/browser';
import { ENGINE_HOSTS } from './hosts';

/**
 * Sites outside the required hosts (a generic page, Reddit, X, an archive.org link, a
 * shortener such as t.co) are asked for one at a time, at the moment of use (extension
 * plan §2.6, §5.4 "Allow Music Fetch to read ‹host›?"). `activeTab` already covers the
 * current tab's page once the user invoked the extension on it.
 */

/** The match pattern that grants `url`'s host (`https://host/*`), or null for non-web URLs. */
export function originPattern(url: string): string | null {
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return null;
    return `${parsed.protocol}//${parsed.hostname}/*`;
  } catch {
    return null;
  }
}

function hostMatches(pattern: string, hostname: string): boolean {
  return pattern.startsWith('*.') ? hostname === pattern.slice(2) || hostname.endsWith(pattern.slice(1)) : hostname === pattern;
}

/** The engine may always reach `url` (a required host). */
export function isEngineHost(url: string): boolean {
  try {
    const { hostname, protocol } = new URL(url);
    return protocol === 'https:' && Object.values(ENGINE_HOSTS).flat().some((h) => hostMatches(h, hostname));
  } catch {
    return false;
  }
}

/** Whether the extension may read `url` already. */
export async function hasHostAccess(url: string): Promise<boolean> {
  if (isEngineHost(url)) return true;
  const origin = originPattern(url);
  if (!origin) return false;
  return browser.permissions.contains({ origins: [origin] }).catch(() => false);
}

/**
 * Ask for `url`'s host. Must run straight from a click handler (popup, panel, page) or a
 * context-menu click: both browsers require a user gesture and refuse otherwise (S4).
 * So nothing may be awaited before this call.
 */
export function requestHostAccess(url: string): Promise<boolean> {
  const origin = originPattern(url);
  if (!origin) return Promise.resolve(false);
  return browser.permissions.request({ origins: [origin] }).catch(() => false);
}
