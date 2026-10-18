/**
 * A small ring of what the engine host did, readable over `HOST_PORT` (`log`): the e2e
 * tests print it when something doesn't happen. Release builds keep no entries.
 */
const ring: string[] = [];

export function hostLog(message: string): void {
  if (import.meta.env.MODE !== 'e2e') return;
  ring.push(`${new Date().toISOString().slice(11, 23)} ${message}`);
  if (ring.length > 200) ring.shift();
}

export function hostLogEntries(): string[] {
  return [...ring];
}
