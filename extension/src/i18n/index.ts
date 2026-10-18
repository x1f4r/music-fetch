import { en, type MessageKey } from './en';

export type { MessageKey } from './en';

/** A message to show: a key and its arguments, resolved with `t` at render time. */
export interface Message {
  key: MessageKey;
  args?: readonly (string | number)[];
}

const catalogs: Record<string, Partial<Record<MessageKey, string>>> = { en };

let active: Partial<Record<MessageKey, string>> = en;

/** Pick the catalog for a BCP-47 language ("de-DE" → "de"); English when none exists. */
export function setLanguage(language: string): void {
  active = catalogs[language] ?? catalogs[language.split('-')[0] ?? ''] ?? en;
}

function format(template: string, args: readonly (string | number)[]): string {
  let next = 0;
  return template.replace(/%(?:(\d+)\$)?([@d])/g, (_, position: string | undefined, type: string) => {
    const value = args[position ? Number(position) - 1 : next++];
    if (value === undefined) return '';
    return type === 'd' && typeof value === 'number' ? Math.round(value).toLocaleString() : String(value);
  });
}

/** The string for `key` with `%@` / `%d` (or `%1$@`) filled from `args`. */
export function t(key: MessageKey, ...args: (string | number)[]): string {
  return format(active[key] ?? en[key], args);
}

export function tm(message: Message): string {
  return t(message.key, ...(message.args ?? []));
}
