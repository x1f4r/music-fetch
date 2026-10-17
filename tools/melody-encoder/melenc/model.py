"""Melody window encoder: 8 s of 20 fps pitch contour -> 128-d unit vector.

Input features (see `featurize`, mirrored in Rust): per frame the pitch relative to the window's
voiced median (held through rests, /6, clipped to +-2), the voicing flag, and the held pitch
change (/3, clipped to +-2). Key-invariant by construction; tempo- and error-tolerance is learned.

Architecture: depthwise-separable conv stem at 20 fps, stride-2 to 10 fps, more conv, stride-2 to
5 fps, a small pre-norm transformer over 40 positions, attention + mean pooling, linear head.
Only plain ops (Conv1d, MatMul, Softmax, LayerNorm, GELU) so tract can run the ONNX graph.
"""

import math

import torch
import torch.nn as nn
import torch.nn.functional as F

W = 160


def featurize(x: torch.Tensor) -> torch.Tensor:
    """x: (B, W) MIDI semitones, NaN = unvoiced -> (B, 3, W)."""
    v = torch.isfinite(x)
    n = x.shape[1]
    ar = torch.arange(n, device=x.device).expand_as(x)
    idx = torch.where(v, ar, torch.zeros_like(ar))
    idx = torch.cummax(idx, dim=1).values
    filled = torch.gather(torch.nan_to_num(x, nan=0.0), 1, idx)
    med = torch.nanmedian(x, dim=1, keepdim=True).values
    med = torch.nan_to_num(med, nan=0.0)
    any_before = torch.cummax(v.int(), dim=1).values.bool()
    filled = torch.where(any_before, filled, med.expand_as(filled))
    rel = ((filled - med) / 6.0).clamp(-2, 2)
    d = torch.zeros_like(filled)
    d[:, 1:] = filled[:, 1:] - filled[:, :-1]
    d = (d / 3.0).clamp(-2, 2)
    return torch.stack([rel, v.float(), d], dim=1)


class DS(nn.Module):
    """Depthwise-separable residual block."""

    def __init__(self, c, k=5, dil=1, expand=2):
        super().__init__()
        self.dw = nn.Conv1d(c, c, k, padding=dil * (k // 2), dilation=dil, groups=c)
        self.norm = nn.BatchNorm1d(c)
        self.pw1 = nn.Conv1d(c, c * expand, 1)
        self.pw2 = nn.Conv1d(c * expand, c, 1)

    def forward(self, x):
        return x + self.pw2(F.gelu(self.pw1(self.norm(self.dw(x)))))


class Block(nn.Module):
    """Pre-norm transformer block with explicit attention (export-friendly)."""

    def __init__(self, d, heads, ff, drop=0.1):
        super().__init__()
        self.h = heads
        self.n1 = nn.LayerNorm(d)
        self.qkv = nn.Linear(d, 3 * d)
        self.proj = nn.Linear(d, d)
        self.n2 = nn.LayerNorm(d)
        self.ff1 = nn.Linear(d, ff)
        self.ff2 = nn.Linear(ff, d)
        self.drop = nn.Dropout(drop)

    def forward(self, x):
        B, T, D = x.shape
        q, k, v = self.qkv(self.n1(x)).reshape(B, T, 3, self.h, D // self.h).permute(2, 0, 3, 1, 4)
        att = torch.softmax(q @ k.transpose(-1, -2) / math.sqrt(D // self.h), dim=-1)
        y = (att @ v).transpose(1, 2).reshape(B, T, D)
        x = x + self.drop(self.proj(y))
        return x + self.drop(self.ff2(F.gelu(self.ff1(self.n2(x)))))


class Encoder(nn.Module):
    def __init__(self, d=128, layers=3, heads=4, ff=384, dim=128, c0=64):
        super().__init__()
        self.stem = nn.Sequential(nn.Conv1d(3, c0, 5, padding=2), nn.GELU(), DS(c0, 5, 1), DS(c0, 5, 2))
        self.down1 = nn.Conv1d(c0, d, 4, stride=2, padding=1)
        self.mid = nn.Sequential(DS(d, 5, 1), DS(d, 5, 2), DS(d, 5, 4))
        self.down2 = nn.Conv1d(d, d, 2, stride=2)
        self.pos = nn.Parameter(torch.zeros(1, W // 4, d))
        nn.init.normal_(self.pos, std=0.02)
        self.blocks = nn.ModuleList([Block(d, heads, ff) for _ in range(layers)])
        self.norm = nn.LayerNorm(d)
        self.att = nn.Linear(d, 1)
        self.head = nn.Linear(2 * d, dim)

    def forward(self, feats):
        h = self.down2(self.mid(self.down1(self.stem(feats))))
        h = h.transpose(1, 2) + self.pos
        for b in self.blocks:
            h = b(h)
        h = self.norm(h)
        a = torch.softmax(self.att(h), dim=1)
        pooled = torch.cat([(a * h).sum(1), h.mean(1)], dim=-1)
        return F.normalize(self.head(pooled), dim=-1)


def params(m):
    return sum(p.numel() for p in m.parameters())
