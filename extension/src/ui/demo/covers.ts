/**
 * Placeholder covers for demo states, drawn like the design prototypes' `.cv-*` tiles
 * (gradients and one simple shape). Inline SVG data URLs: nothing loads from the network.
 */

type Stop = [offset: number, color: string];

function svg(body: string, defs = ''): string {
  const markup = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" preserveAspectRatio="xMidYMid slice"><defs>${defs}</defs>${body}</svg>`;
  return `data:image/svg+xml,${encodeURIComponent(markup)}`;
}

function linear(id: string, angle: number, stops: Stop[]): string {
  const rad = ((angle - 90) * Math.PI) / 180;
  const x = Math.cos(rad) / 2;
  const y = Math.sin(rad) / 2;
  const s = stops.map(([o, c]) => `<stop offset="${o}" stop-color="${c}"/>`).join('');
  return `<linearGradient id="${id}" x1="${0.5 - x}" y1="${0.5 - y}" x2="${0.5 + x}" y2="${0.5 + y}">${s}</linearGradient>`;
}

function radial(id: string, cx: number, cy: number, r: number, stops: Stop[]): string {
  const s = stops.map(([o, c]) => `<stop offset="${o}" stop-color="${c}"/>`).join('');
  return `<radialGradient id="${id}" cx="${cx}" cy="${cy}" r="${r}">${s}</radialGradient>`;
}

const fill = (id: string) => `<rect width="100" height="100" fill="url(#${id})"/>`;

function gradient(angle: number, ...stops: Stop[]): string {
  return svg(fill('g'), linear('g', angle, stops));
}

export const COVERS = {
  houdini: svg(
    `${fill('g')}<path d="M-10 78 Q 50 52 110 66 L110 110 L-10 110 Z" fill="#fff" opacity=".85"/><path d="M-10 92 L110 80 L110 110 L-10 110 Z" fill="#0b2d6e"/>`,
    radial('g', 0.2, 0.15, 1.1, [[0, '#7fd3ff'], [0.35, '#1c6fd1'], [0.75, '#0b2d6e'], [1, '#06183d']]),
  ),
  takeonme: svg(`${fill('g')}<rect x="20" y="24" width="60" height="52" fill="none" stroke="#2a2a2a" stroke-width="3" transform="rotate(-4 50 50)"/>`, linear('g', 135, [[0, '#f1efe6'], [1, '#d9d4c5']])),
  dancingqueen: gradient(160, [0, '#f6c84c'], [0.6, '#e2612c'], [1, '#8a2b2b']),
  africa: gradient(180, [0, '#e8d9b5'], [0.55, '#b9893f'], [1, '#4e3418']),
  dontstop: svg(`${fill('g')}<circle cx="50" cy="50" r="20" fill="#f2f2f2"/>`, linear('g', 135, [[0, '#2b2b2b'], [1, '#6b6b6b']])),
  espresso: svg(`${fill('g')}<rect x="20" y="50" width="60" height="40" rx="22" fill="#fff"/>`, linear('g', 135, [[0, '#8fc9ff'], [1, '#4b7fd6']])),
  nights: gradient(180, [0, '#c9d6d1'], [1, '#5f7f7a']),
  blinding: svg(fill('g'), radial('g', 0.5, 0.7, 0.8, [[0, '#ff5c35'], [0.4, '#c0143c'], [1, '#1a0a14']])),
  shortnsweet: gradient(160, [0, '#b7d7ff'], [1, '#e6f0ff']),
  marea: gradient(160, [0, '#141a2e'], [1, '#37507e']),
  kyle: gradient(160, [0, '#e7e3dc'], [1, '#b7ada0']),
  jungle: gradient(160, [0, '#1f3d2b'], [1, '#6fa07c']),
  lightsout: gradient(160, [0, '#2b1a3d'], [1, '#a06bd6']),
  delilah: gradient(160, [0, '#f0d7a3'], [1, '#d2884d']),
  turnon: gradient(160, [0, '#0d0d0d'], [1, '#ffd54a']),
  rumble: svg(`${fill('g')}<rect x="20" y="35" width="60" height="30" fill="#ffe600"/>`, linear('g', 160, [[0, '#101010'], [1, '#5b5b5b']])),
  boiler: svg(
    `${fill('g')}${fill('h')}<rect x="10" y="40" width="80" height="20" fill="#e9e9e9" opacity=".8"/>`,
    linear('g', 160, [[0, '#0a0a0a'], [1, '#262626']]) + radial('h', 0.5, 0.28, 0.6, [[0, '#4b3a2c'], [1, 'rgba(0,0,0,0)']]),
  ),
  levitating: gradient(160, [0, '#ffd1dc'], [1, '#7a4b9c']),
  starboy: gradient(160, [0, '#d8d8d8'], [1, '#4a4a4a']),
  luft: gradient(160, [0, '#e23b3b'], [1, '#ffb3b3']),
  viva: gradient(160, [0, '#6b3b2a'], [1, '#d8b27a']),
  shape: gradient(160, [0, '#1a3a5c'], [1, '#7fb3e6']),
  mega: svg(`${fill('g')}<path d="M36 28 h28 v22 q0 18 -14 18 q-14 0 -14 -18 z" fill="#fff"/>`, linear('g', 160, [[0, '#1b1b1b'], [1, '#3a3a3a']])),
  heart: gradient(160, [0, '#0d2a4a'], [1, '#4a7fa8']),
  wakemeup: gradient(160, [0, '#e9e2d3'], [1, '#8a8070']),
} as const;

export type CoverName = keyof typeof COVERS;
