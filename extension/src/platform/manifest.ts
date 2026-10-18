/**
 * The manifest per target (extension plan §3.4). WXT adds the entrypoints (background,
 * popup, side panel or sidebar, icons); everything reviewed for permissions and privacy
 * lives here. Imported by `wxt.config.ts`, so it must stay free of browser APIs.
 */
import { OPTIONAL_HOST_PERMISSIONS, requiredHostPermissions } from '../net/hosts';

/**
 * The Firefox add-on id. Public and permanent: AMO ties signing, updates and the
 * user's data to it. UUID form, so it names no person or domain.
 */
export const GECKO_ID = '{e3457fb3-3399-4a23-ad71-6a1f8772c0a3}';

/** `wasm-unsafe-eval` lets extension pages compile the engine; no remote code anywhere. */
export const EXTENSION_PAGES_CSP = "script-src 'self' 'wasm-unsafe-eval'; object-src 'self'";

export const CHROMIUM_PERMISSIONS = [
  'storage',
  'unlimitedStorage',
  'offscreen',
  'tabCapture',
  'activeTab',
  'scripting',
  'sidePanel',
  'contextMenus',
  'notifications',
  'declarativeNetRequestWithHostAccess',
  'alarms',
] as const;

/**
 * Header rules are declarativeNetRequest session rules in Firefox too (S4: one
 * implementation for both browsers; blocking webRequest is not needed). `webRequest`
 * stays, non-blocking: the engine's own requests reaching the event page as
 * `onCompleted` events keep it alive during a job (S2).
 */
export const FIREFOX_PERMISSIONS = [
  'storage',
  'unlimitedStorage',
  'activeTab',
  'scripting',
  'menus',
  'notifications',
  'declarativeNetRequestWithHostAccess',
  'webRequest',
  'alarms',
] as const;

/**
 * Firefox's built-in data consent. What leaves the browser: audio fingerprints of the
 * tab or microphone and page URLs and titles to the services that resolve them
 * (websiteContent, browsingActivity), and typed lyrics to the lyric search
 * (searchTerms). Humming is matched on the device. No technical or usage data is sent,
 * so `technicalAndInteraction` is not declared.
 */
export const FIREFOX_DATA_COLLECTION = {
  required: ['websiteContent', 'browsingActivity', 'searchTerms'],
} as const;

/** captureStream() is unprefixed from Firefox 149 (desktop and Android). */
export const FIREFOX_MIN_VERSION = '149.0';
/** Structured-clone messaging needs Chrome 148. */
export const CHROME_MIN_VERSION = '148';

export interface ManifestInput {
  browser: string;
  mode: string;
}

// Loose on purpose: WXT validates the final manifest; keys like
// `message_serialization` and `data_collection_permissions` are newer than its types.
export type ManifestFields = Record<string, unknown>;

/**
 * `wxt build --mode e2e` only: the local test pages and echo server on the loopback
 * interface, so the end-to-end tests can inject into fixture pages without a click.
 * Release builds never carry these (scripts/privacy-check.sh can assert it).
 */
export const E2E_HOST_PERMISSIONS = ['http://127.0.0.1/*', 'http://localhost/*'];

/** WXT fills `version` from package.json, the entrypoints and the icons. */
export function buildManifest({ browser, mode }: ManifestInput): ManifestFields {
  const firefox = browser === 'firefox';
  const common: ManifestFields = {
    name: 'Music Fetch',
    short_name: 'Music Fetch',
    description: 'Identify music playing in a tab, from a link or the microphone, or by humming.',
    host_permissions: [...requiredHostPermissions(), ...(mode === 'e2e' ? E2E_HOST_PERMISSIONS : [])],
    optional_host_permissions: OPTIONAL_HOST_PERMISSIONS,
    content_security_policy: { extension_pages: EXTENSION_PAGES_CSP },
    action: { default_title: 'Music Fetch' },
    commands: {
      _execute_action: { suggested_key: { default: 'Alt+Shift+M' } },
    },
  };
  if (firefox) {
    return {
      ...common,
      permissions: [...FIREFOX_PERMISSIONS],
      browser_specific_settings: {
        gecko: {
          id: GECKO_ID,
          strict_min_version: FIREFOX_MIN_VERSION,
          data_collection_permissions: FIREFOX_DATA_COLLECTION,
        },
        gecko_android: { strict_min_version: FIREFOX_MIN_VERSION },
      },
    };
  }
  return {
    ...common,
    permissions: [...CHROMIUM_PERMISSIONS],
    minimum_chrome_version: CHROME_MIN_VERSION,
    message_serialization: 'structured_clone',
  };
}
