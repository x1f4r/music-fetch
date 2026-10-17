import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import pkg from '../../package.json';
import { describe, expect, it } from 'vitest';
import { requiredHostPermissions } from '@/src/net/hosts';
import { buildManifest, CHROMIUM_PERMISSIONS, GECKO_ID } from '@/src/platform/manifest';

const chrome = buildManifest({ browser: 'chrome', mode: 'production' });
const firefox = buildManifest({ browser: 'firefox', mode: 'production' });

describe('the manifest', () => {
  it('is named Music Fetch in both browsers', () => {
    expect(chrome.name).toBe('Music Fetch');
    expect(firefox.name).toBe('Music Fetch');
  });

  it('runs no remote code', () => {
    for (const manifest of [chrome, firefox]) {
      const csp = (manifest.content_security_policy as { extension_pages: string }).extension_pages;
      expect(csp).toBe("script-src 'self' 'wasm-unsafe-eval'; object-src 'self'");
      const rest = csp.replace("'wasm-unsafe-eval'", '');
      expect(rest).not.toMatch(/unsafe|https?:|\*|data:|blob:/);
    }
  });

  it('asks for every site only as an optional permission', () => {
    for (const manifest of [chrome, firefox]) {
      const hosts = manifest.host_permissions as string[];
      expect(hosts).toEqual(requiredHostPermissions());
      expect(hosts.some((h) => h === '<all_urls>' || h.startsWith('*://') || h === 'https://*/*')).toBe(false);
      expect(manifest.optional_host_permissions).toEqual(['https://*/*', 'http://*/*']);
      expect(manifest.content_scripts).toBeUndefined();
    }
  });

  it('declares the plan’s Chromium permissions and structured-clone messaging', () => {
    expect(chrome.permissions).toEqual([...CHROMIUM_PERMISSIONS]);
    expect(chrome.permissions).not.toContain('tabs');
    expect(chrome.minimum_chrome_version).toBe('148');
    expect(chrome.message_serialization).toBe('structured_clone');
    expect(chrome.browser_specific_settings).toBeUndefined();
  });

  it('gives Firefox a UUID id, minimum versions and the data consent', () => {
    expect(GECKO_ID).toMatch(/^\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\}$/);
    const gecko = (firefox.browser_specific_settings as Record<string, Record<string, unknown>>).gecko!;
    expect(gecko.id).toBe(GECKO_ID);
    expect(gecko.strict_min_version).toBe('149.0');
    expect(gecko.data_collection_permissions).toEqual({ required: ['websiteContent', 'browsingActivity', 'searchTerms'] });
    expect(firefox.permissions).toContain('menus');
    expect(firefox.permissions).not.toContain('offscreen');
    expect(firefox.permissions).not.toContain('tabCapture');
  });

  it('carries the release version of the core', () => {
    // Vitest runs in extension/.
    const cargo = join(process.cwd(), '..', 'core', 'Cargo.toml');
    if (!existsSync(cargo)) return; // the AMO sources zip has no core/
    const core = /\[workspace\.package\][^[]*?version\s*=\s*"([^"]+)"/.exec(readFileSync(cargo, 'utf8'))?.[1];
    expect(pkg.version).toBe(core);
  });
});
