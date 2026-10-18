//! 4-bit product-quantizer fast scan: distances of a query to blocks of 32 coded
//! windows through 16-entry byte lookup tables, one table per sub-quantizer.
//!
//! Block layout (`m` sub-quantizers): `m × 16` bytes. Byte `j` of sub-quantizer
//! `s` holds the code of window `j` in its low nibble and the code of window
//! `j + 16` in its high nibble. A table lookup of 16 nibbles is one `tbl` (NEON)
//! or `pshufb` (SSSE3) instruction; sums are kept in u16 lanes, which hold
//! `m ≤ 256` terms of at most 255.

/// Windows per block.
pub const BLOCK: usize = 32;
/// Centroids per sub-quantizer (4 bits).
pub const KSUB: usize = 16;

/// Which kernel [`scan_block`] uses on this machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kernel {
    Scalar,
    #[cfg(target_arch = "aarch64")]
    Neon,
    #[cfg(target_arch = "x86_64")]
    Ssse3,
}

impl Kernel {
    /// The fastest kernel this CPU supports.
    pub fn detect() -> Self {
        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                return Kernel::Neon;
            }
        }
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("ssse3") {
                return Kernel::Ssse3;
            }
        }
        Kernel::Scalar
    }
}

/// Sum of `lut[s][code(window, s)]` over sub-quantizers for the 32 windows of a
/// block. `codes` and `lut` are `m × 16` bytes each.
pub fn scan_block(kernel: Kernel, codes: &[u8], lut: &[u8], out: &mut [u16; BLOCK]) {
    debug_assert_eq!(codes.len(), lut.len());
    debug_assert!(codes.len().is_multiple_of(KSUB));
    match kernel {
        Kernel::Scalar => scan_block_scalar(codes, lut, out),
        // Safety: the kernel was detected as supported, and both slices hold whole
        // 16-byte rows (the loops read `codes.len() / 16` rows of each).
        #[cfg(target_arch = "aarch64")]
        Kernel::Neon => unsafe { scan_block_neon(codes, lut, out) },
        #[cfg(target_arch = "x86_64")]
        Kernel::Ssse3 => unsafe { scan_block_ssse3(codes, lut, out) },
    }
}

/// Portable reference kernel.
pub fn scan_block_scalar(codes: &[u8], lut: &[u8], out: &mut [u16; BLOCK]) {
    out.fill(0);
    for (row, table) in codes.as_chunks::<KSUB>().0.iter().zip(lut.as_chunks::<KSUB>().0) {
        for (j, &b) in row.iter().enumerate() {
            out[j] += table[(b & 15) as usize] as u16;
            out[j + 16] += table[(b >> 4) as usize] as u16;
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn scan_block_neon(codes: &[u8], lut: &[u8], out: &mut [u16; BLOCK]) {
    use std::arch::aarch64::*;
    let rows = codes.len().min(lut.len()) / KSUB;
    let mask = vdupq_n_u8(15);
    let (mut a0, mut a1, mut a2, mut a3) = (vdupq_n_u16(0), vdupq_n_u16(0), vdupq_n_u16(0), vdupq_n_u16(0));
    for s in 0..rows {
        // Safety: s < rows, so 16 bytes from s × 16 are in both slices.
        let (c, t) = unsafe { (vld1q_u8(codes.as_ptr().add(s * KSUB)), vld1q_u8(lut.as_ptr().add(s * KSUB))) };
        let lo = vqtbl1q_u8(t, vandq_u8(c, mask));
        let hi = vqtbl1q_u8(t, vshrq_n_u8::<4>(c));
        a0 = vaddw_u8(a0, vget_low_u8(lo));
        a1 = vaddw_high_u8(a1, lo);
        a2 = vaddw_u8(a2, vget_low_u8(hi));
        a3 = vaddw_high_u8(a3, hi);
    }
    let p = out.as_mut_ptr();
    // Safety: out holds 32 u16, written as four runs of 8.
    unsafe {
        vst1q_u16(p, a0);
        vst1q_u16(p.add(8), a1);
        vst1q_u16(p.add(16), a2);
        vst1q_u16(p.add(24), a3);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn scan_block_ssse3(codes: &[u8], lut: &[u8], out: &mut [u16; BLOCK]) {
    use std::arch::x86_64::*;
    let rows = codes.len().min(lut.len()) / KSUB;
    let mask = _mm_set1_epi8(15);
    let zero = _mm_setzero_si128();
    let (mut a0, mut a1, mut a2, mut a3) = (zero, zero, zero, zero);
    for s in 0..rows {
        // Safety: s < rows, so 16 bytes from s × 16 are in both slices (unaligned loads).
        let (c, t) = unsafe {
            (
                _mm_loadu_si128(codes.as_ptr().add(s * KSUB) as *const __m128i),
                _mm_loadu_si128(lut.as_ptr().add(s * KSUB) as *const __m128i),
            )
        };
        let lo = _mm_shuffle_epi8(t, _mm_and_si128(c, mask));
        let hi = _mm_shuffle_epi8(t, _mm_and_si128(_mm_srli_epi16::<4>(c), mask));
        a0 = _mm_add_epi16(a0, _mm_unpacklo_epi8(lo, zero));
        a1 = _mm_add_epi16(a1, _mm_unpackhi_epi8(lo, zero));
        a2 = _mm_add_epi16(a2, _mm_unpacklo_epi8(hi, zero));
        a3 = _mm_add_epi16(a3, _mm_unpackhi_epi8(hi, zero));
    }
    let p = out.as_mut_ptr() as *mut __m128i;
    // Safety: out holds 32 u16 = four 16-byte runs (unaligned stores).
    unsafe {
        _mm_storeu_si128(p, a0);
        _mm_storeu_si128(p.add(1), a1);
        _mm_storeu_si128(p.add(2), a2);
        _mm_storeu_si128(p.add(3), a3);
    }
}

/// Pack one block: `codes[w][s]` (4-bit) of up to 32 windows into `m × 16` bytes.
/// Missing windows get code 0.
pub fn pack_block(codes: &[Vec<u8>], m: usize, out: &mut [u8]) {
    out.fill(0);
    for (w, c) in codes.iter().enumerate().take(BLOCK) {
        for (s, &code) in c.iter().enumerate().take(m) {
            let byte = &mut out[s * KSUB + (w % 16)];
            if w < 16 {
                *byte |= code & 15;
            } else {
                *byte |= (code & 15) << 4;
            }
        }
    }
}

/// Code of window `w` (0..32) for sub-quantizer `s` in a packed block.
pub fn code_at(block: &[u8], w: usize, s: usize) -> u8 {
    let b = block[s * KSUB + (w % 16)];
    if w < 16 { b & 15 } else { b >> 4 }
}

/// Quantize float similarity tables (`m × 16`, larger = closer) into byte
/// distance tables (smaller = closer) with one scale for all sub-quantizers, as
/// faiss does. Returns (tables, scale, offset): similarity ≈ offset − sum / scale.
pub fn quantize_tables(sim: &[f32], out: &mut [u8]) -> (f32, f32) {
    let mut span = 0f32;
    let mut offset = 0f32;
    for row in sim.as_chunks::<KSUB>().0 {
        let hi = row.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let lo = row.iter().copied().fold(f32::INFINITY, f32::min);
        span = span.max(hi - lo);
        offset += hi;
    }
    let scale = if span > 0.0 { 255.0 / span } else { 1.0 };
    for (row, dst) in sim.as_chunks::<KSUB>().0.iter().zip(out.as_chunks_mut::<KSUB>().0) {
        let hi = row.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        for (x, d) in row.iter().zip(dst.iter_mut()) {
            *d = ((hi - x) * scale).round().clamp(0.0, 255.0) as u8;
        }
    }
    (scale, offset)
}
