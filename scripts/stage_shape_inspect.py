#!/usr/bin/env python3
"""
Visual inspection companion to stage_shape_post.py.

For each selected candidate generates a 3-panel PNG:
  Panel 1 — Tiny cutout (21 px): raw pixels + contour overlays at opencv thresholds.
  Panel 2 — Neighbourhood cutout (8'): inverted+bkg-subtracted image with
             target position (red ×) and flux-matched neighbour stars (blue ○).
  Panel 3 — Radial profiles: individual stars (grey), averaged (blue dashed),
             target (red). Horizontal line at the 0.1 masking threshold.

Selection modes (--mode):
  profile_diff_only  [default] — rejected by profile_diff only (not elongation/circularity)
  low_confidence     — survivors whose shape_confidence = "low" (area < 100 px²)
  survivors          — all survivors (up to --max-candidates)
  all_rejects        — all rejected rows (up to --max-candidates)

Usage:
    python scripts/stage_shape_inspect.py \\
        --flags-csv work/runs/.../stages/stage_S4S_SHAPE_flags.csv \\
        --tiles-root ./data/tiles \\
        --out-dir ./inspect_output

Parameters default to the values used by the stage run (read from the ledger JSON
that lives alongside the flags CSV).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import cv2
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits as astropy_fits
from astropy.nddata import Cutout2D
from astropy.stats import SigmaClip
from astropy.wcs import WCS

from photutils.background import Background2D, MedianBackground
from photutils.profiles import RadialProfile


# ---------------------------------------------------------------------------
# Helpers (duplicated from stage_shape_post.py to keep script self-contained)
# ---------------------------------------------------------------------------

def _parse_edge_radii(spec: str) -> np.ndarray:
    if spec.strip().lower() == "arange30/2":
        return np.arange(30) / 2.0
    return np.array([float(x) for x in spec.split(",")])


def _load_pass2(sex_csv: Path) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    try:
        with sex_csv.open(newline="", encoding="utf-8", errors="ignore") as fh:
            reader = csv.DictReader(fh)
            cols = set(reader.fieldnames or [])
            x_col    = next((c for c in ["x_fit", "X_FIT", "XWIN_IMAGE", "X_IMAGE"] if c in cols), None)
            y_col    = next((c for c in ["y_fit", "Y_FIT", "YWIN_IMAGE", "Y_IMAGE"] if c in cols), None)
            ra_col   = next((c for c in ["ALPHAWIN_J2000", "ALPHA_J2000", "ra", "RA"] if c in cols), None)
            dec_col  = next((c for c in ["DELTAWIN_J2000", "DELTA_J2000", "dec", "DEC"] if c in cols), None)
            flux_col = next((c for c in ["FLUX_MAX", "flux_max", "FLUX_AUTO", "FLUX_APER"] if c in cols), None)
            for row in reader:
                num = (row.get("NUMBER") or row.get("number") or "").strip()
                if not num:
                    continue
                entry: dict = {}
                try:
                    if x_col and y_col:
                        entry["x"] = float(row[x_col]) - 1.0
                        entry["y"] = float(row[y_col]) - 1.0
                    if ra_col and dec_col:
                        entry["ra"]  = float(row[ra_col])
                        entry["dec"] = float(row[dec_col])
                    if flux_col:
                        entry["flux_max"] = float(row[flux_col])
                    result[num] = entry
                except (ValueError, KeyError):
                    pass
    except Exception:
        pass
    return result


def _precondition(data: np.ndarray, invert_max: float = 65535.0) -> np.ndarray:
    inverted = invert_max - data.astype(float)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            bkg = Background2D(
                inverted, box_size=40, filter_size=3,
                sigma_clip=SigmaClip(sigma=3),
                bkg_estimator=MedianBackground(),
            )
        return inverted - bkg.background
    except Exception:
        return inverted


def _normalize_profile(profile: np.ndarray) -> np.ndarray:
    pr_max, pr_min = np.nanmax(profile), np.nanmin(profile)
    denom = pr_max - pr_min
    if denom == 0.0:
        return np.zeros_like(profile, dtype=float)
    return (profile - pr_min) / denom


# ---------------------------------------------------------------------------
# Per-candidate figure generation
# ---------------------------------------------------------------------------

def _make_figure(
    row: dict,
    tiles_root: Path,
    params: dict,
    out_path: Path,
) -> bool:
    """Generate a 3-panel PNG for one candidate. Returns True on success."""
    tile_id  = row["tile_id"]
    obj_id   = row["object_id"]
    src_id   = row["src_id"]

    try:
        cand_ra  = float(row["ra"])
        cand_dec = float(row["dec"])
    except (ValueError, TypeError):
        print(f"  [SKIP] {src_id}: invalid ra/dec")
        return False

    tile_dir = tiles_root / tile_id
    raw_dir  = tile_dir / "raw"
    fits_files = sorted(raw_dir.glob("*.fits")) if raw_dir.exists() else []
    if not fits_files:
        print(f"  [SKIP] {src_id}: no FITS found")
        return False

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with astropy_fits.open(fits_files[0]) as hdul:
                hdr  = hdul[0].header
                data = hdul[0].data.astype(float)
            wcs = WCS(hdr)
    except Exception as exc:
        print(f"  [SKIP] {src_id}: FITS load error: {exc}")
        return False

    sex_csv = tile_dir / "catalogs" / "sextractor_pass2.csv"
    if not sex_csv.exists():
        print(f"  [SKIP] {src_id}: pass2 missing")
        return False
    pass2 = _load_pass2(sex_csv)

    sex_entry = pass2.get(obj_id)
    if sex_entry is None or "x" not in sex_entry:
        print(f"  [SKIP] {src_id}: object not in pass2")
        return False

    x_px = sex_entry["x"]
    y_px = sex_entry["y"]
    target_flux = sex_entry.get("flux_max", float("nan"))

    edge_radii   = params["edge_radii"]
    nbhd_arcmin  = params["neighborhood_cutout_size_arcmin"]
    tiny_px      = params["tiny_cutout_size_px"]
    opencv_thr   = params["opencv_thresholds"]
    invert_max   = params["invert_max"]
    flux_range   = params["flux_range"]

    # --- Neighbourhood cutout ---
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            nbhd_cutout = Cutout2D(
                data,
                SkyCoord(ra=cand_ra * u.deg, dec=cand_dec * u.deg),
                (nbhd_arcmin * u.arcmin, nbhd_arcmin * u.arcmin),
                wcs=wcs, mode="trim",
            )
        nbhd_proc = _precondition(nbhd_cutout.data, invert_max=invert_max)
        ox, oy = nbhd_cutout.origin_original
        tx = x_px - ox
        ty = y_px - oy
    except Exception as exc:
        print(f"  [SKIP] {src_id}: neighbourhood cutout error: {exc}")
        return False

    # --- Select neighbourhood stars & build profiles ---
    neigh_pool = [
        {"object_id": oid, **s}
        for oid, s in pass2.items()
        if all(k in s for k in ("x", "y", "flux_max"))
    ]
    ch, cw = nbhd_cutout.data.shape
    flux_lo = target_flux * (1.0 - flux_range) if not math.isnan(target_flux) else float("inf")
    flux_hi = target_flux * (1.0 + flux_range) if not math.isnan(target_flux) else float("-inf")

    selected_xys: List[Tuple[float, float]] = []
    for star in neigh_pool:
        if star["object_id"] == obj_id:
            continue
        if not (flux_lo <= star["flux_max"] <= flux_hi):
            continue
        sx = star["x"] - ox
        sy = star["y"] - oy
        if 0.0 <= sx < cw and 0.0 <= sy < ch:
            selected_xys.append((sx, sy))

    # Compute individual radial profiles (for plotting)
    target_prof: Optional[np.ndarray] = None
    star_profs: List[np.ndarray] = []
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rp = RadialProfile(nbhd_proc, [tx, ty], edge_radii)
        target_prof = _normalize_profile(np.array(rp.profile, dtype=float))
    except Exception:
        pass

    for xy in selected_xys:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                rp = RadialProfile(nbhd_proc, list(xy), edge_radii)
            prof = _normalize_profile(np.array(rp.profile, dtype=float))
            if np.isfinite(prof).any():
                star_profs.append(prof)
        except Exception:
            continue

    # Bin centres for x-axis
    bin_centres = (edge_radii[:-1] + edge_radii[1:]) / 2.0

    # --- Tiny cutout ---
    try:
        tiny_cutout = Cutout2D(data, position=(x_px, y_px), size=tiny_px)
        tiny_data = tiny_cutout.data.astype(float)
    except Exception:
        tiny_data = np.zeros((tiny_px, tiny_px))

    # Normalize tiny_data to uint8 for contour display
    tiny_uint8 = np.empty(tiny_data.shape, dtype=np.uint8)
    cv2.normalize(tiny_data, tiny_uint8, 0, 255, cv2.NORM_MINMAX, cv2.CV_8U)

    # --- Build figure ---
    fig = plt.figure(figsize=(15, 4.8))
    gs  = gridspec.GridSpec(1, 3, figure=fig, width_ratios=[1, 1.6, 2.4],
                            wspace=0.35, left=0.05, right=0.97,
                            top=0.82, bottom=0.10)

    ax_tiny  = fig.add_subplot(gs[0])
    ax_nbhd  = fig.add_subplot(gs[1])
    ax_prof  = fig.add_subplot(gs[2])

    # ---- Panel 1: tiny cutout ----
    ax_tiny.imshow(tiny_uint8, origin="lower", cmap="gray",
                   interpolation="nearest", vmin=0, vmax=255)
    # Contour overlays at opencv thresholds
    for thr, col in zip(opencv_thr, ["cyan", "lime"]):
        ax_tiny.contour(tiny_uint8, levels=[thr], colors=[col],
                        linewidths=0.8, alpha=0.8)
    cx_t, cy_t = tiny_px / 2.0, tiny_px / 2.0
    ax_tiny.plot(cx_t, cy_t, "r+", markersize=10, markeredgewidth=1.5)
    ax_tiny.set_title(f"Tiny cutout ({tiny_px}px)", fontsize=9)
    ax_tiny.set_xticks([]); ax_tiny.set_yticks([])

    # ---- Panel 2: neighbourhood cutout ----
    vmin_n = np.nanpercentile(nbhd_proc, 1)
    vmax_n = np.nanpercentile(nbhd_proc, 99)
    ax_nbhd.imshow(nbhd_proc, origin="lower", cmap="gray",
                   interpolation="nearest", vmin=vmin_n, vmax=vmax_n)
    # Target
    ax_nbhd.plot(tx, ty, "r+", markersize=12, markeredgewidth=1.8,
                 label="target", zorder=5)
    # Neighbour stars
    if selected_xys:
        sx_arr = [xy[0] for xy in selected_xys]
        sy_arr = [xy[1] for xy in selected_xys]
        ax_nbhd.scatter(sx_arr, sy_arr, s=30, facecolors="none",
                        edgecolors="dodgerblue", linewidths=1.0,
                        label=f"stars ({len(selected_xys)})", zorder=4)
    ax_nbhd.legend(fontsize=7, loc="upper right", framealpha=0.6)
    ax_nbhd.set_title(f"Neighbourhood ({nbhd_arcmin}')", fontsize=9)
    ax_nbhd.set_xticks([]); ax_nbhd.set_yticks([])

    # ---- Panel 3: radial profiles ----
    ax_prof.axhline(0.1, color="orange", linewidth=0.8, linestyle="--",
                    alpha=0.7, label="mask threshold (0.1)")
    plotted_stars = 0
    for prof in star_profs:
        label = "stars" if plotted_stars == 0 else None
        ax_prof.plot(bin_centres, prof, color="lightgrey",
                     linewidth=0.7, alpha=0.8, label=label)
        plotted_stars += 1

    if star_profs:
        avg = np.nanmean(np.array(star_profs), axis=0)
        ax_prof.plot(bin_centres, avg, color="dodgerblue", linewidth=1.5,
                     linestyle="--", label="averaged")

    if target_prof is not None:
        ax_prof.plot(bin_centres, target_prof, color="red", linewidth=2.0,
                     label="target")

    pd_val = row.get("profile_diff", "")
    ax_prof.set_title(
        f"Radial profiles  profile_diff={pd_val}  stars_used={len(star_profs)}",
        fontsize=9,
    )
    ax_prof.set_xlabel("radius (px)", fontsize=8)
    ax_prof.set_ylabel("normalised flux", fontsize=8)
    ax_prof.set_ylim(-0.15, 1.15)
    ax_prof.tick_params(labelsize=7)
    ax_prof.legend(fontsize=7, loc="upper right", framealpha=0.6)

    # ---- Super-title with candidate metadata ----
    reject_reason = row.get("reject_reason", "")
    conf   = row.get("shape_confidence", "")
    circ   = row.get("circularity", "")
    area   = row.get("area", "")
    elong  = row.get("elongation", "")
    status = "KEPT" if row.get("reject_flag") == "0" else "REJECTED"
    fig.suptitle(
        f"{src_id}   [{status}]   {reject_reason or 'no rejection'}\n"
        f"circularity={circ}  area={area}  elongation={elong}  "
        f"shape_confidence={conf}",
        fontsize=8, y=0.97,
    )

    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return True


# ---------------------------------------------------------------------------
# Candidate selection
# ---------------------------------------------------------------------------

def _select_candidates(
    flags_path: Path,
    mode: str,
    max_n: int,
    seed: int,
    filter_src_ids: Optional[set] = None,
) -> List[dict]:
    rows = []
    with flags_path.open(newline="", encoding="utf-8", errors="ignore") as fh:
        rows = list(csv.DictReader(fh))

    if mode == "profile_diff_only":
        sel = [
            r for r in rows
            if r.get("rej_profile_diff") == "1"
            and r.get("rej_elongation")   == "0"
            and r.get("rej_circularity")  == "0"
        ]
    elif mode == "low_confidence":
        sel = [
            r for r in rows
            if r.get("reject_flag")       == "0"
            and r.get("shape_failed")     == "0"
            and r.get("shape_confidence") == "low"
        ]
    elif mode == "survivors":
        sel = [
            r for r in rows
            if r.get("reject_flag")   == "0"
            and r.get("shape_failed") == "0"
        ]
    elif mode == "all_rejects":
        sel = [r for r in rows if r.get("reject_flag") == "1"]
    else:
        raise ValueError(f"Unknown mode: {mode!r}")

    if filter_src_ids is not None:
        before = len(sel)
        sel = [r for r in sel if r.get("src_id") in filter_src_ids]
        print(f"[INSPECT] --filter-csv: {before} → {len(sel)} candidates after intersection")

    if len(sel) > max_n:
        rng = random.Random(seed)
        sel = rng.sample(sel, max_n)
        print(f"[INSPECT] Sampled {max_n} of {len(sel) + max_n - max_n} candidates "
              f"(mode={mode})")
    return sel


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Visual inspection companion to stage_shape_post.py."
    )
    ap.add_argument("--flags-csv", required=True,
                    help="Path to stage_*_SHAPE_flags.csv")
    ap.add_argument("--tiles-root", default="./data/tiles",
                    help="Root of tile directories. Default: ./data/tiles")
    ap.add_argument("--out-dir", default="./inspect_output",
                    help="Output directory for PNGs. Default: ./inspect_output")
    ap.add_argument("--mode",
                    choices=["profile_diff_only", "low_confidence",
                             "survivors", "all_rejects"],
                    default="profile_diff_only",
                    help="Which candidates to visualise. Default: profile_diff_only")
    ap.add_argument("--max-candidates", type=int, default=30,
                    help="Maximum number of PNGs to produce. Default: 30")
    ap.add_argument("--seed", type=int, default=42,
                    help="Random seed for sampling when N > --max-candidates. Default: 42")
    ap.add_argument("--filter-csv", default=None,
                    help="Later-stage CSV (e.g. stage_S5_VSX.csv); only candidates "
                         "whose src_id appears in this file are included. "
                         "Only meaningful with --mode survivors or low_confidence — "
                         "rejected rows (profile_diff_only, all_rejects) can never "
                         "appear in a later-stage CSV, so the intersection is always empty.")
    args = ap.parse_args()

    flags_path = Path(args.flags_csv)
    if not flags_path.exists():
        raise SystemExit(f"flags CSV not found: {flags_path}")

    tiles_root = Path(args.tiles_root)
    out_dir    = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load params from ledger (lives alongside the flags CSV)
    ledger_path = flags_path.with_name(
        flags_path.name.replace("_flags.csv", "_ledger.json")
    )
    params: dict = {}
    if ledger_path.exists():
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        p = ledger.get("parameters", {})
        params = {
            "neighborhood_cutout_size_arcmin": p.get("neighborhood_cutout_size_arcmin", 8.0),
            "edge_radii":   _parse_edge_radii(p.get("edge_radii", "arange30/2")),
            "flux_range":   p.get("flux_range", 0.1),
            "invert_max":   p.get("invert_max", 65535.0),
            "tiny_cutout_size_px": p.get("tiny_cutout_size_px", 21),
            "opencv_thresholds":   p.get("opencv_thresholds", [21, 45]),
        }
        print(f"[INSPECT] Loaded params from ledger: {ledger_path.name}")
    else:
        print("[INSPECT] Ledger not found — using default params")
        params = {
            "neighborhood_cutout_size_arcmin": 8.0,
            "edge_radii": np.arange(30) / 2.0,
            "flux_range": 0.1,
            "invert_max": 65535.0,
            "tiny_cutout_size_px": 21,
            "opencv_thresholds": [21, 45],
        }

    filter_src_ids: Optional[set] = None
    if args.filter_csv:
        filter_path = Path(args.filter_csv)
        if not filter_path.exists():
            raise SystemExit(f"--filter-csv not found: {filter_path}")
        with filter_path.open(newline="", encoding="utf-8", errors="ignore") as fh:
            filter_src_ids = {r["src_id"] for r in csv.DictReader(fh) if "src_id" in r}
        print(f"[INSPECT] --filter-csv: loaded {len(filter_src_ids)} src_ids from {filter_path.name}")

    candidates = _select_candidates(
        flags_path, args.mode, args.max_candidates, args.seed, filter_src_ids
    )
    print(f"[INSPECT] mode={args.mode}  candidates={len(candidates)}")

    n_ok = n_skip = 0
    for cand in candidates:
        # Sanitise src_id for filename
        safe_name = cand["src_id"].replace("/", "_").replace(":", "__").replace(" ", "_")
        out_path  = out_dir / f"{safe_name}.png"

        ok = _make_figure(cand, tiles_root, params, out_path)
        if ok:
            n_ok += 1
            print(f"  wrote: {out_path.name}")
        else:
            n_skip += 1

    print(f"[INSPECT] done: {n_ok} PNGs written, {n_skip} skipped → {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
