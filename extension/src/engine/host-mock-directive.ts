/** localStorage key a developer sets to force the next session's mock outcome (README "Mock engine"). */
export const MOCK_DIRECTIVE_KEY = 'mf.mock.next';

/** The directive, consumed; null where `localStorage` is missing (Workers) or empty. */
export function takeMockDirective(): string | null {
  try {
    const value = globalThis.localStorage?.getItem(MOCK_DIRECTIVE_KEY) ?? null;
    if (value) globalThis.localStorage.removeItem(MOCK_DIRECTIVE_KEY);
    return value;
  } catch {
    return null;
  }
}
