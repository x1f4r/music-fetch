//! Frame labels (silence / speech / music) and the novelty curve used to place
//! boundaries. Works on 1 s feature frames.

use crate::audio::features::FrameFeatures;

/// What a second of audio sounds like.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Label {
    Silence,
    Speech,
    Music,
}

/// Silence threshold relative to full scale.
const SILENCE_DB: f32 = -50.0;
/// Frames quieter than the loud part of the media by this much count as silence.
const RELATIVE_SILENCE_DB: f32 = 30.0;

fn db(rms: f32) -> f32 {
    20.0 * rms.max(1e-6).log10()
}

fn percentile(values: &[f32], p: f32) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f32> = values.iter().copied().filter(|v| v.is_finite()).collect();
    if sorted.is_empty() {
        return 0.0;
    }
    sorted.sort_by(f32::total_cmp);
    let idx = ((sorted.len() - 1) as f32 * p.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

/// Per-frame labels with global percentile normalization and a 5-frame
/// majority filter.
///
/// Speech: little tonal focus (flat chroma), strong syllable-rate onset
/// variation, a voice-band centroid and noisy zero crossings compared with the
/// rest of the media. Everything audible that is not speech is music.
pub fn labels(frames: &[FrameFeatures]) -> Vec<Label> {
    if frames.is_empty() {
        return Vec::new();
    }
    let levels: Vec<f32> = frames.iter().map(|f| db(f.rms)).collect();
    let loud = percentile(&levels, 0.9);
    let silence = SILENCE_DB.max(loud - RELATIVE_SILENCE_DB);
    // Chroma focus: how much of the pitch-class energy sits in the top 3 classes.
    let focus: Vec<f32> = frames
        .iter()
        .map(|f| {
            let total: f32 = f.chroma.iter().sum();
            if total <= 0.0 {
                return 0.0;
            }
            let mut c = f.chroma;
            c.sort_by(|a, b| b.total_cmp(a));
            (c[0] + c[1] + c[2]) / total
        })
        .collect();
    let onset_var: Vec<f32> = (0..frames.len())
        .map(|i| {
            let lo = i.saturating_sub(2);
            let hi = (i + 3).min(frames.len());
            let window: Vec<f32> = frames[lo..hi].iter().map(|f| f.onset).collect();
            let mean = window.iter().sum::<f32>() / window.len() as f32;
            let var = window.iter().map(|o| (o - mean).powi(2)).sum::<f32>() / window.len() as f32;
            var.sqrt() / mean.max(1e-6)
        })
        .collect();
    let focus_lo = percentile(&focus, 0.3);
    let zcr_hi = percentile(&frames.iter().map(|f| f.zcr).collect::<Vec<_>>(), 0.6);
    let var_hi = percentile(&onset_var, 0.7);

    let raw: Vec<Label> = frames
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if levels[i] < silence {
                return Label::Silence;
            }
            let voice_band = (200.0..3200.0).contains(&f.centroid_hz);
            let mut votes = 0;
            votes += (focus[i] < focus_lo.min(0.42)) as u32;
            votes += (onset_var[i] > var_hi.max(0.45)) as u32;
            votes += (f.zcr > zcr_hi && f.flatness > 0.05) as u32;
            if voice_band && votes >= 2 { Label::Speech } else { Label::Music }
        })
        .collect();
    smooth(&raw, 5)
}

/// Majority filter over `width` frames.
fn smooth(labels: &[Label], width: usize) -> Vec<Label> {
    let half = width / 2;
    (0..labels.len())
        .map(|i| {
            let lo = i.saturating_sub(half);
            let hi = (i + half + 1).min(labels.len());
            let mut counts = [0usize; 3];
            for l in &labels[lo..hi] {
                counts[*l as usize] += 1;
            }
            let best = (0..3).max_by_key(|k| (counts[*k], *k == labels[i] as usize)).unwrap_or(2);
            [Label::Silence, Label::Speech, Label::Music][best]
        })
        .collect()
}

/// Foote novelty: a checkerboard kernel of `kernel_s` frames slid along the
/// diagonal of the cosine self-similarity of normalized timbre and harmony
/// features. Values are 0..=1 (scaled by the maximum).
pub fn novelty(frames: &[FrameFeatures], kernel_s: usize) -> Vec<f32> {
    let n = frames.len();
    if n < 4 {
        return vec![0.0; n];
    }
    // z-score each dimension over the media, then unit-normalize each frame.
    let dims = 13 + 12;
    let mut vecs: Vec<Vec<f32>> =
        frames.iter().map(|f| f.mfcc.iter().skip(1).chain(f.chroma.iter()).copied().collect()).collect();
    let dims = dims - 1;
    for d in 0..dims {
        let mean = vecs.iter().map(|v| v[d]).sum::<f32>() / n as f32;
        let sd = (vecs.iter().map(|v| (v[d] - mean).powi(2)).sum::<f32>() / n as f32).sqrt().max(1e-6);
        for v in vecs.iter_mut() {
            v[d] = (v[d] - mean) / sd;
        }
    }
    for v in vecs.iter_mut() {
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
        v.iter_mut().for_each(|x| *x /= norm);
    }
    let half = (kernel_s / 2).max(2);
    let sim = |a: usize, b: usize| -> f32 { vecs[a].iter().zip(&vecs[b]).map(|(x, y)| x * y).sum() };
    let mut out = vec![0.0f32; n];
    for (t, slot) in out.iter_mut().enumerate() {
        if t < 2 || t + 2 > n {
            continue;
        }
        let lo = t.saturating_sub(half);
        let hi = (t + half).min(n);
        let mut within = 0.0;
        let mut within_n = 0usize;
        let mut across = 0.0;
        let mut across_n = 0usize;
        for i in lo..hi {
            for j in (i + 1)..hi {
                // Gaussian taper keeps the kernel focused on the centre.
                let w = (-(((i as f32 - t as f32).powi(2) + (j as f32 - t as f32).powi(2))
                    / (2.0 * (half as f32 * 0.6).powi(2))))
                .exp();
                let s = sim(i, j) * w;
                if (i < t) == (j < t) {
                    within += s;
                    within_n += 1;
                } else {
                    across += s;
                    across_n += 1;
                }
            }
        }
        if within_n > 0 && across_n > 0 {
            *slot = (within / within_n as f32 - across / across_n as f32).max(0.0);
        }
    }
    let max = out.iter().copied().fold(0.0, f32::max);
    if max > 0.0 {
        out.iter_mut().for_each(|v| *v /= max);
    }
    out
}

/// Local maxima of `curve` at least `min_distance` apart, strongest first
/// among neighbours, above the `quantile` of the curve.
pub fn peaks(curve: &[f32], min_distance: usize, quantile: f32) -> Vec<usize> {
    let floor = percentile(curve, quantile);
    let mut candidates: Vec<usize> = (1..curve.len().saturating_sub(1))
        .filter(|&i| curve[i] >= curve[i - 1] && curve[i] >= curve[i + 1] && curve[i] > floor)
        .collect();
    candidates.sort_by(|a, b| curve[*b].total_cmp(&curve[*a]));
    let mut kept: Vec<usize> = Vec::new();
    for c in candidates {
        if kept.iter().all(|k| k.abs_diff(c) >= min_distance) {
            kept.push(c);
        }
    }
    kept.sort_unstable();
    kept
}

/// Runs of equal labels as (label, start second, end second).
pub fn runs(labels: &[Label]) -> Vec<(Label, usize, usize)> {
    let mut out: Vec<(Label, usize, usize)> = Vec::new();
    for (i, l) in labels.iter().enumerate() {
        match out.last_mut() {
            Some((last, _, end)) if last == l => *end = i + 1,
            _ => out.push((*l, i, i + 1)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(rms: f32, chroma_peak: bool, onset: f32) -> FrameFeatures {
        let mut chroma = [0.1f32; 12];
        if chroma_peak {
            chroma[0] = 1.0;
            chroma[4] = 0.8;
            chroma[7] = 0.9;
        }
        FrameFeatures { t_ms: 0, rms, flatness: 0.1, zcr: 0.05, centroid_hz: 1500.0, onset, chroma, mfcc: [0.0; 13] }
    }

    #[test]
    fn silence_and_music() {
        let mut frames: Vec<FrameFeatures> = (0..30).map(|_| frame(0.2, true, 1.0)).collect();
        frames.extend((0..10).map(|_| frame(0.000_01, false, 0.0)));
        frames.extend((0..30).map(|_| frame(0.2, true, 1.0)));
        let l = labels(&frames);
        assert!(l[..28].iter().all(|x| *x == Label::Music));
        assert!(l[32..38].iter().all(|x| *x == Label::Silence));
        let r = runs(&l);
        assert_eq!(r.len(), 3);
        assert_eq!(r[1].0, Label::Silence);
    }

    #[test]
    fn majority_filter_removes_blips() {
        let l = vec![Label::Music, Label::Music, Label::Speech, Label::Music, Label::Music];
        assert!(smooth(&l, 5).iter().all(|x| *x == Label::Music));
    }

    #[test]
    fn novelty_peaks_at_a_change() {
        let mut frames = Vec::new();
        for i in 0..120 {
            let mut f = frame(0.2, true, 1.0);
            f.mfcc = if i < 60 {
                [1.0, 2.0, -1.0, 0.5, 0.0, 0.3, 0.1, 0.0, 0.0, 0.2, 0.0, 0.1, 0.0]
            } else {
                [1.0, -2.0, 1.0, -0.5, 0.4, 0.0, -0.3, 0.2, 0.1, 0.0, 0.3, 0.0, 0.2]
            };
            // A little variation so z-scores are defined.
            f.mfcc[12] += (i % 7) as f32 * 0.01;
            if i >= 60 {
                f.chroma = [0.1, 0.1, 1.0, 0.1, 0.1, 0.8, 0.1, 0.1, 0.1, 0.9, 0.1, 0.1];
            }
            frames.push(f);
        }
        let curve = novelty(&frames, 24);
        let p = peaks(&curve, 20, 0.5);
        assert_eq!(p.len(), 1, "{p:?}");
        assert!(p[0].abs_diff(60) <= 2, "{p:?}");
    }
}
