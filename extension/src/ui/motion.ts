/**
 * Whether decorative motion should stop: demo screenshots (`html.demo-still`). Reduced
 * motion keeps the live line and trace (they are content, DESIGN §2.2) and is handled
 * by the tokens' easings.
 */
export function stillMotion(): boolean {
  return typeof document !== 'undefined' && document.documentElement.classList.contains('demo-still');
}

export function prefersReducedMotion(): boolean {
  return typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
}
