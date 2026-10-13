// Deterministic drawing helpers shared by the mockups (pitch traces, waveforms, expressive shapes).
(function (global) {
  function prng(seed) {
    let s = seed >>> 0;
    return function () { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; };
  }

  // A hummed phrase: [semitones from median, seconds]; null = unvoiced gap.
  const MELODY = [
    [0, .5], [2, .5], [4, .6], [null, .25], [4, .45], [5, .45], [7, .9], [null, .35],
    [7, .4], [9, .4], [7, .4], [5, .5], [4, .8], [null, .3], [2, .5], [4, .5], [0, .9],
    [null, .4], [-3, .5], [0, .5], [2, .5], [4, 1.0], [null, .3], [2, .4], [0, 1.1]
  ];

  function melodyAt(notes, t) {
    let acc = 0;
    for (const [semi, dur] of notes) { if (t < acc + dur) return { semi, start: acc, dur }; acc += dur; }
    return { semi: null };
  }
  function melodyLength(notes) { return notes.reduce((a, n) => a + n[1], 0); }

  // Pitch trace segments. Returns [{d, opacity}] for the time window [t1, t2] mapped onto width w.
  // range = semitones per half height. jitter/vibrato make it human; set both to 0 for a reference contour.
  function traceSegments(notes, o) {
    const { w, h, t1, t2, range = 12, jitter = .22, vibrato = .1, seed = 7, slide = .06 } = o;
    const rnd = prng(seed), segs = [];
    const px = (h / 2) / range, steps = Math.round((t2 - t1) * 60);
    let cur = null, prevSemi = null, drift = 0;
    for (let i = 0; i <= steps; i++) {
      const t = t1 + (t2 - t1) * i / steps;
      const n = melodyAt(notes, t);
      if (n.semi === null) { if (cur) { segs.push(cur); cur = null; } continue; }
      // portamento into each note, small human drift, vibrato
      let semi = n.semi;
      const into = (t - n.start);
      if (prevSemi !== null && prevSemi !== n.semi && into < slide) semi = prevSemi + (n.semi - prevSemi) * (into / slide);
      drift += (rnd() - .5) * jitter * .25; drift *= .9;
      const vib = vibrato * Math.sin(2 * Math.PI * 5.6 * t);
      const y = h / 2 - (semi + drift + vib) * px;
      const x = w * (t - t1) / (t2 - t1);
      if (!cur) cur = { d: `M${x.toFixed(1)} ${y.toFixed(1)}`, opacity: .82 + rnd() * .18 };
      else cur.d += ` L${x.toFixed(1)} ${y.toFixed(1)}`;
      if (into >= slide) prevSemi = n.semi;
    }
    if (cur) segs.push(cur);
    return segs;
  }

  function drawTrace(svg, notes, o) {
    const ns = 'http://www.w3.org/2000/svg';
    const segs = traceSegments(notes, o);
    for (const s of segs) {
      const p = document.createElementNS(ns, 'path');
      p.setAttribute('d', s.d); p.setAttribute('fill', 'none');
      p.setAttribute('stroke', o.stroke || 'currentColor'); p.setAttribute('stroke-width', o.width || 4);
      p.setAttribute('stroke-linecap', 'round'); p.setAttribute('stroke-linejoin', 'round');
      p.setAttribute('opacity', o.flat ? 1 : s.opacity.toFixed(2));
      svg.appendChild(p);
    }
    if (o.nowDot) {
      const last = segs[segs.length - 1];
      if (last) {
        const m = last.d.match(/L([\d.]+) ([\d.]+)$/) || last.d.match(/M([\d.]+) ([\d.]+)/);
        const c = document.createElementNS(ns, 'circle');
        c.setAttribute('cx', m[1]); c.setAttribute('cy', m[2]); c.setAttribute('r', o.nowDot);
        c.setAttribute('fill', o.stroke || 'currentColor'); svg.appendChild(c);
      }
    }
  }

  // Live level waveform for the listen button glyph.
  function wavePath(w, h, level, seed) {
    const rnd = prng(seed), n = 64, parts = [];
    const f = [1.0, 2.3, 3.7], a = [.6, .3, .15], ph = [rnd() * 6, rnd() * 6, rnd() * 6];
    for (let i = 0; i <= n; i++) {
      const u = i / n, x = u * w;
      const env = Math.sin(Math.PI * u);
      let y = 0;
      for (let k = 0; k < 3; k++) y += a[k] * Math.sin(2 * Math.PI * f[k] * u * 2 + ph[k]);
      y = h / 2 + y * env * level * (h / 2);
      parts.push((i ? 'L' : 'M') + x.toFixed(1) + ' ' + y.toFixed(1));
    }
    return parts.join(' ');
  }

  // Scalloped polygon (MaterialShapes cookie / sunny family), n lobes, amp as fraction of R.
  function scallopPath(cx, cy, R, n, amp, rot) {
    const pts = [], N = 240; rot = rot || 0;
    for (let i = 0; i < N; i++) {
      const th = rot + i / N * 2 * Math.PI;
      const r = R * (1 + amp * Math.cos(n * th));
      pts.push((i ? 'L' : 'M') + (cx + r * Math.cos(th)).toFixed(2) + ' ' + (cy + r * Math.sin(th)).toFixed(2));
    }
    return pts.join(' ') + ' Z';
  }

  // Wavy arc for CircularWavyProgressIndicator: fraction of a full turn starting at top.
  function wavyArc(cx, cy, R, amp, k, fraction) {
    const pts = [], N = Math.max(2, Math.round(360 * fraction));
    for (let i = 0; i <= N; i++) {
      const th = -Math.PI / 2 + (i / 360) * 2 * Math.PI;
      const r = R + amp * Math.sin(k * th);
      pts.push((i ? 'L' : 'M') + (cx + r * Math.cos(th)).toFixed(2) + ' ' + (cy + r * Math.sin(th)).toFixed(2));
    }
    return pts.join(' ');
  }

  // Linear wavy progress: wavy active part and flat track.
  function wavyLine(w, h, amp, wavelength, progress) {
    const pts = [], xEnd = w * progress, N = Math.round(xEnd / 2);
    for (let i = 0; i <= N; i++) {
      const x = xEnd * i / N;
      const y = h / 2 + amp * Math.sin(2 * Math.PI * x / wavelength);
      pts.push((i ? 'L' : 'M') + x.toFixed(1) + ' ' + y.toFixed(1));
    }
    return { active: pts.join(' '), track: `M${(xEnd + 6).toFixed(1)} ${h / 2} L${w} ${h / 2}` };
  }

  global.Mock = { prng, MELODY, melodyLength, traceSegments, drawTrace, wavePath, scallopPath, wavyArc, wavyLine };
})(window);
