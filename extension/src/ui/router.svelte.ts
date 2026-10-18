/**
 * The app tab's hash routes (extension plan §5.1): `#/listen`, `#/library`,
 * `#/session/:id`, `#/settings/:pane`, `#/welcome`, `#/grant-mic`, `#/open?url=`.
 */
export type Route =
  | { name: 'listen' }
  | { name: 'library'; pinned: boolean }
  | { name: 'session'; id: string }
  | { name: 'settings'; pane: SettingsPane }
  | { name: 'welcome' }
  | { name: 'grant-mic'; state: 'granted' | 'denied' | 'session' | null }
  | { name: 'open'; url: string };

export const SETTINGS_PANES = ['general', 'listening', 'melody', 'storage', 'privacy', 'shortcuts', 'about'] as const;
export type SettingsPane = (typeof SETTINGS_PANES)[number];

export function parseRoute(hash: string): Route {
  const raw = hash.replace(/^#/, '') || '/listen';
  const [path = '', query = ''] = raw.split('?', 2);
  const parts = path.split('/').filter(Boolean);
  switch (parts[0]) {
    case 'library':
      return { name: 'library', pinned: parts[1] === 'pinned' };
    case 'session':
      return parts[1] ? { name: 'session', id: decodeURIComponent(parts[1]) } : { name: 'library', pinned: false };
    case 'settings': {
      const pane = (SETTINGS_PANES as readonly string[]).includes(parts[1] ?? '') ? (parts[1] as SettingsPane) : 'general';
      return { name: 'settings', pane };
    }
    case 'welcome':
      return { name: 'welcome' };
    case 'grant-mic': {
      const state = new URLSearchParams(query).get('state');
      return { name: 'grant-mic', state: state === 'granted' || state === 'denied' || state === 'session' ? state : null };
    }
    case 'open': {
      const url = new URLSearchParams(query).get('url');
      return url ? { name: 'open', url } : { name: 'listen' };
    }
    default:
      return { name: 'listen' };
  }
}

export function routeHash(route: Route): string {
  switch (route.name) {
    case 'library':
      return route.pinned ? '#/library/pinned' : '#/library';
    case 'session':
      return `#/session/${encodeURIComponent(route.id)}`;
    case 'settings':
      return `#/settings/${route.pane}`;
    case 'open':
      return `#/open?${new URLSearchParams({ url: route.url }).toString()}`;
    case 'grant-mic':
      return route.state ? `#/grant-mic?state=${route.state}` : '#/grant-mic';
    default:
      return `#/${route.name}`;
  }
}

/** The current route, following `hashchange`. */
export class Router {
  route: Route = $state(parseRoute(location.hash));

  constructor() {
    addEventListener('hashchange', () => {
      this.route = parseRoute(location.hash);
    });
  }

  go(route: Route, replace = false): void {
    const hash = routeHash(route);
    if (replace) history.replaceState(null, '', hash);
    else location.hash = hash;
    this.route = route;
  }
}
