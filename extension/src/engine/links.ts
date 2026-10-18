/**
 * The first link in pasted text, like the core's `links::extract_url` (`is_link`): an
 * explicit http(s) URL anywhere, else a bare domain such as `youtu.be/abc` (returned with
 * `https://`). In running text a bare domain needs a path; a lone token may be a host.
 * The wasm facade replaces this with the core function.
 */
export function extractUrl(text: string): string | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  const explicit = /https?:\/\/[^\s<>"']+/i.exec(trimmed);
  if (explicit) {
    const url = parse(trimToken(explicit[0]));
    if (url) return url;
  }
  const single = !/\s/.test(trimmed);
  for (const raw of trimmed.split(/\s+/)) {
    const token = trimToken(raw);
    const slash = token.indexOf('/');
    const host = slash >= 0 ? token.slice(0, slash) : token;
    const rest = slash >= 0 ? token.slice(slash) : '';
    const hasPath = rest !== '' && rest !== '/';
    if ((hasPath || single) && isBareDomain(host)) {
      const url = parse(`https://${token}`);
      if (url) return url;
    }
  }
  return null;
}

export function isLink(text: string): boolean {
  return extractUrl(text) !== null;
}

function trimToken(token: string): string {
  return token.replace(/^[(<["'«“]+/, '').replace(/[)>\]"'»”.,;:!?]+$/, '');
}

function isBareDomain(host: string): boolean {
  const labels = host.toLowerCase().split('.');
  if (labels.length < 2) return false;
  const tld = labels.at(-1)!;
  if (!/^[a-z]{2,24}$/.test(tld)) return false;
  return labels.every((label) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label));
}

function parse(candidate: string): string | null {
  try {
    const url = new URL(candidate);
    return (url.protocol === 'https:' || url.protocol === 'http:') && url.hostname ? url.href : null;
  } catch {
    return null;
  }
}
