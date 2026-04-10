#!/usr/bin/env python3
"""mnras_candidates_audit.py

For every MNRAS 2022 (vanish POSSI) candidate whose sky coordinates fall inside
a downloaded tile, determine why that candidate does NOT appear as a final survivor
in our pipeline.

Outcome categories
------------------
  NO_MATCH      : no SExtractor source within 5 arcsec in sextractor_pass2.csv.
                  Note: source may have been detected but excluded by the ≤30′
                  circle cut (applied when building stage_S0.csv), or by the
                  plate-edge veto. sextractor_pass2.csv is pre-cut; if the source
                  is absent there it was simply not detected by SExtractor.
  GATE_FAIL     : detected in sextractor_pass2.csv but fails one or more MNRAS
                  2022 quality gates (FLAGS, SNR_WIN, ELONGATION, FWHM, SPREAD)
  STAGE_ELIM    : passes all gates, but eliminated by a pipeline post-process
                  stage (S0M MORPH → S4S SHAPE → S1 GSC → S2 SkyBoT → S3 SCOS
                  → S4 PTF → S5 VSX, actual order read from ledger chain)
  SURVIVOR      : made it all the way through (appears in final stage CSV)

Inputs
------
  --mnras        MNRAS candidate CSV  (RA, DEC columns)
  --registry     tiles_registry.csv  (tile_id, ra_deg, dec_deg, size_arcmin, status)
  --tiles-root   root dir containing tile_RA…_DEC… folders
  --runs-dir     directory containing run-R* folders
  --output       output CSV path  (default: work/mnras_candidates_audit.csv)

Usage
-----
  python mnras_candidates_audit.py \\
      --runs-dir ./work/runs \\
      --output   ./work/mnras_candidates_audit.csv
"""

from __future__ import annotations
import argparse
import csv
import json
import math
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def wrap_pm180(x: float) -> float:
    return (x + 180.0) % 360.0 - 180.0


def sep_arcsec(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    ra1r, dec1r = math.radians(ra1), math.radians(dec1)
    ra2r, dec2r = math.radians(ra2), math.radians(dec2)
    dra = math.radians(wrap_pm180(ra2 - ra1))
    ddec = dec2r - dec1r
    a = math.sin(ddec / 2) ** 2 + math.cos(dec1r) * math.cos(dec2r) * math.sin(dra / 2) ** 2
    c = 2 * math.asin(min(1.0, math.sqrt(a)))
    return math.degrees(c) * 3600.0


def ra_dist(ra1: float, ra2: float) -> float:
    d = abs(ra1 - ra2) % 360.0
    return d if d <= 180.0 else 360.0 - d


# ---------------------------------------------------------------------------
# MNRAS 2022 quality gates  (matching check_one_target.py)
# ---------------------------------------------------------------------------

GATE_DEFS = [
    ("FLAGS==0",      lambda r: float(r.get("FLAGS", 1)) == 0),
    ("SNR_WIN>30",    lambda r: float(r.get("SNR_WIN", float("nan"))) > 30.0),
    ("ELONG<1.3",     lambda r: float(r.get("ELONGATION", float("nan"))) < 1.3),
    ("2<FWHM<7",      lambda r: 2.0 < float(r.get("FWHM_IMAGE", float("nan"))) < 7.0),
    ("SPREAD>-0.002", lambda r: float(r.get("SPREAD_MODEL", float("nan"))) > -0.002),
]


def check_gates(row: dict) -> list[str]:
    """Return list of gate labels that FAIL. Empty = all pass."""
    failed = []
    for label, fn in GATE_DEFS:
        try:
            if not fn(row):
                failed.append(label)
        except (ValueError, TypeError):
            failed.append(label + "?")
    return failed


# ---------------------------------------------------------------------------
# SExtractor catalog — load once per tile, no global cache
# ---------------------------------------------------------------------------

RA_DEC_COLUMNS = [
    ("RA_corr", "Dec_corr"),
    ("ALPHAWIN_J2000", "DELTAWIN_J2000"),
    ("ALPHA_J2000", "DELTA_J2000"),
    ("X_WORLD", "Y_WORLD"),
]


def load_sexcat(tile_dir: Path) -> tuple[list[dict], str, str]:
    """Load sextractor_pass2.csv; return (rows, ra_col, dec_col)."""
    p = tile_dir / "catalogs" / "sextractor_pass2.csv"
    if not p.exists():
        return [], "", ""
    with open(p, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return [], "", ""
    for rc, dc in RA_DEC_COLUMNS:
        if rc in rows[0] and dc in rows[0]:
            return rows, rc, dc
    return [], "", ""


def find_nearest(rows: list[dict], ra_col: str, dec_col: str,
                 target_ra: float, target_dec: float, max_sep: float = 5.0):
    """Return (nearest_row, sep_arcsec) or (None, inf)."""
    best_row = None
    best_sep = float("inf")
    for r in rows:
        try:
            s = sep_arcsec(target_ra, target_dec, float(r[ra_col]), float(r[dec_col]))
        except (ValueError, KeyError):
            continue
        if s < best_sep:
            best_sep = s
            best_row = r
    if best_sep > max_sep:
        return None, best_sep
    return best_row, best_sep


# ---------------------------------------------------------------------------
# Stage ordering — derived from ledger input_glob chains
# ---------------------------------------------------------------------------

_STEM_META = {
    "stage_S0M_MORPH":  ("S0M", "stage_S0M_MORPH_flags"),
    "stage_S1_GSC":     ("S1",  "stage_S1_GSC_flags"),
    "stage_S2_SKYBOT":  ("S2",  "stage_S2_SKYBOT_flags"),
    "stage_S3_SCOS":    ("S3",  "stage_S3_SCOS_flags"),
    "stage_S4_PTF":     ("S4",  "stage_S4_PTF_flags"),
    "stage_S4S_SHAPE":  ("S4S", "stage_S4S_SHAPE_flags"),
    "stage_S5_VSX":     ("S5",  "stage_S5_VSX_flags"),
}


def _derive_stage_order(stages_dir: Path) -> list[tuple[str, str, str]]:
    """Return [(label, survivor_stem, flags_stem), ...] in execution order."""
    stem_input: dict[str, str] = {}
    for ledger in stages_dir.glob("*_ledger.json"):
        stem = ledger.name.replace("_ledger.json", "")
        if stem not in _STEM_META:
            continue
        try:
            d = json.loads(ledger.read_text())
            ig = d.get("input_glob", "")
            stem_input[stem] = Path(ig).stem
        except Exception:
            pass

    if not stem_input:
        return [(m[0], s, m[1]) for s, m in sorted(_STEM_META.items())]

    input_to_stems: dict[str, list[str]] = {}
    for stem, inp in stem_input.items():
        input_to_stems.setdefault(inp, []).append(stem)

    ordered: list[tuple[str, str, str]] = []
    current = "stage_S0"
    visited: set[str] = set()
    while True:
        nexts = input_to_stems.get(current, [])
        if not nexts:
            break
        nxt = nexts[0]
        if nxt in visited:
            break
        visited.add(nxt)
        meta = _STEM_META.get(nxt)
        if meta:
            ordered.append((meta[0], nxt, meta[1]))
        current = nxt

    return ordered if ordered else [(m[0], s, m[1]) for s, m in sorted(_STEM_META.items())]


# ---------------------------------------------------------------------------
# Run data — survivor id sets only; flags scanned on demand
# ---------------------------------------------------------------------------

def _load_id_set(csv_path: Path) -> set[str]:
    if not csv_path.exists():
        return set()
    with open(csv_path, newline="") as f:
        return {row["src_id"] for row in csv.DictReader(f) if "src_id" in row}


def scan_flags_for_src_id(flags_path: Path, src_id: str) -> dict | None:
    """Stream-scan a flags CSV for one src_id. Returns the row dict or None."""
    if not flags_path.exists():
        return None
    with open(flags_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("src_id") == src_id:
                return dict(row)
    return None


class RunData:
    """Holds only survivor id sets (strings). Flags are scanned on demand."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        stages_dir = run_dir / "stages"
        self.s0_ids: set[str] = _load_id_set(run_dir / "stage_S0.csv")
        stage_order = _derive_stage_order(stages_dir)
        # (label, survivor_id_set, flags_path)
        self.stages: list[tuple[str, set[str], Path]] = []
        for label, survivor_stem, flags_stem in stage_order:
            surv = _load_id_set(stages_dir / f"{survivor_stem}.csv")
            flags_path = stages_dir / f"{flags_stem}.csv"
            self.stages.append((label, surv, flags_path))

    def trace(self, src_id: str) -> tuple[str, str]:
        """Return (eliminating_stage, reason). 'SURVIVOR' if not eliminated."""
        if src_id not in self.s0_ids:
            return "PRE_S0", "not_in_stage_S0"

        prev_present = True
        for label, surv, flags_path in self.stages:
            in_this = src_id in surv
            if prev_present and not in_this:
                flag_row = scan_flags_for_src_id(flags_path, src_id) or {}
                return label, _format_reason(label, flag_row)
            prev_present = in_this

        return "SURVIVOR", ""


def _format_reason(stage: str, flag_row: dict) -> str:
    if not flag_row:
        return "no_flag_entry"
    if stage == "S0M":
        return flag_row.get("reject_reason", "morph_rejected")
    if stage == "S1":
        return f"GSC_match sep={flag_row.get('best_sep_arcsec','?')}\" id={flag_row.get('gsc2_id','?')}"
    if stage == "S2":
        return f"SkyBoT_match sep={flag_row.get('best_sep_arcsec','?')}\" wide={flag_row.get('wide_skybot_match','?')}"
    if stage == "S3":
        return "no_SCOS_match (scan_artifact)"
    if stage == "S4":
        return f"PTF_match sep={flag_row.get('best_sep_arcsec','?')}\""
    if stage == "S4S":
        return flag_row.get("reject_reason", "shape_rejected")
    if stage == "S5":
        return f"VSX_match sep={flag_row.get('best_sep_arcsec','?')}\""
    return str(flag_row)


# ---------------------------------------------------------------------------
# Run index — tile_id → RunData (only for needed tiles)
# ---------------------------------------------------------------------------

def build_tile_run_map(runs_dir: Path, needed_tiles: set[str]) -> dict[str, RunData]:
    tile_to_run_dir: dict[str, Path] = {}

    for manifest in sorted(runs_dir.glob("*/tile_manifest.csv")):
        run_dir = manifest.parent
        with open(manifest, newline="") as f:
            for row in csv.DictReader(f):
                tile_id = row.get("tile_id", "").strip()
                if tile_id not in needed_tiles:
                    continue
                if row.get("skipped_delta", "").strip() == "1":
                    continue
                tile_to_run_dir[tile_id] = run_dir

    needed_run_dirs = sorted(set(tile_to_run_dir.values()))
    print(f"[audit] {len(tile_to_run_dir)}/{len(needed_tiles)} needed tiles mapped "
          f"across {len(needed_run_dirs)} run(s)", file=sys.stderr)

    run_cache: dict[Path, RunData] = {}
    for run_dir in needed_run_dirs:
        print(f"[audit] loading run: {run_dir.name}", file=sys.stderr)
        run_cache[run_dir] = RunData(run_dir)

    return {tile_id: run_cache[rd] for tile_id, rd in tile_to_run_dir.items()}


# ---------------------------------------------------------------------------
# NO_MATCH sub-classification
# ---------------------------------------------------------------------------

def _tile_center(tile_id: str) -> tuple[float, float]:
    """Parse tile center (ra, dec) from tile_id string."""
    ra = float(tile_id.split("_RA")[1].split("_DEC")[0])
    dec_s = tile_id.split("_DEC")[1]
    dec = float(dec_s[1:]) * (1.0 if dec_s[0] == "p" else -1.0)
    return ra, dec


def _nearest_in_csv(csv_path: Path, target_ra: float, target_dec: float) -> float:
    """Return nearest sep_arcsec in a CSV, or inf if file missing/empty."""
    if not csv_path.exists():
        return float("inf")
    best = float("inf")
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            rc = row.get("ALPHAWIN_J2000") or row.get("ALPHA_J2000") or row.get("X_WORLD", "")
            dc = row.get("DELTAWIN_J2000") or row.get("DELTA_J2000") or row.get("Y_WORLD", "")
            try:
                s = sep_arcsec(target_ra, target_dec, float(rc), float(dc))
                if s < best:
                    best = s
            except (ValueError, KeyError):
                pass
    return best


def _classify_no_match(tile_dir: Path, ra: float, dec: float,
                       max_sep: float) -> str:
    """Return a specific rejection_reason for a NO_MATCH candidate.

    CIRCLE_CUT    — found in sextractor_pass2_before_circle_filter.csv (≤max_sep)
                    but removed by the ≤30′ circle filter
    NOT_DETECTED  — absent from both pre- and post-filter catalogs (SExtractor
                    did not detect it: too faint, blended, or below threshold)
    """
    tile_id = tile_dir.name
    tc_ra, tc_dec = _tile_center(tile_id)
    dist_from_center = sep_arcsec(ra, dec, tc_ra, tc_dec)

    pre_path = tile_dir / "catalogs" / "sextractor_pass2_before_circle_filter.csv"
    pre_sep = _nearest_in_csv(pre_path, ra, dec)

    if pre_sep <= max_sep:
        # Present before circle cut → removed by it (regardless of exact dist,
        # trust the catalog result over our approximate geometry check)
        return f"circle_cut (dist_from_center={dist_from_center/60:.2f}' pre_sep={pre_sep:.2f}\")"
    else:
        return f"not_detected (nearest_pre={pre_sep:.1f}\")"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mnras",      default="data/vasco-cats/vanish_possi_1765561258.csv")
    ap.add_argument("--registry",   default="data/metadata/tiles_registry.csv")
    ap.add_argument("--tiles-root", default="data/tiles")
    ap.add_argument("--runs-dir",   default="work/runs")
    ap.add_argument("--output",     default="work/mnras_candidates_audit.csv")
    ap.add_argument("--max-sep",    type=float, default=5.0,
                    help="Max arcsec separation to SExtractor source (default 5\")")
    args = ap.parse_args()

    mnras_path    = Path(args.mnras)
    registry_path = Path(args.registry)
    tiles_root    = Path(args.tiles_root)
    runs_dir      = Path(args.runs_dir)
    output_path   = Path(args.output)

    # Load MNRAS candidates
    mnras: list[tuple[int, float, float]] = []
    with open(mnras_path, newline="") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            mnras.append((i, float(row["RA"]), float(row["DEC"])))
    print(f"[audit] {len(mnras)} MNRAS candidates", file=sys.stderr)

    # Load downloaded tiles
    tiles: list[dict] = []
    with open(registry_path, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("status", "") != "ok":
                continue
            tiles.append({
                "tile_id": row["tile_id"],
                "ra":      float(row["ra_deg"]),
                "dec":     float(row["dec_deg"]),
                "half":    float(row.get("size_arcmin", 60)) / 2.0 / 60.0,
            })
    print(f"[audit] {len(tiles)} downloaded tiles", file=sys.stderr)

    # Match MNRAS candidates to tiles
    candidate_tiles: list[tuple[tuple[int, float, float], str]] = []
    seen: set[tuple[int, str]] = set()
    for cand in mnras:
        row_idx, ra, dec = cand
        for t in tiles:
            half = t["half"]
            if abs(dec - t["dec"]) > half:
                continue
            cos_dec = math.cos(math.radians(dec))
            ra_half = half / cos_dec if cos_dec > 0.01 else 180.0
            if ra_dist(ra, t["ra"]) > ra_half:
                continue
            key = (row_idx, t["tile_id"])
            if key not in seen:
                seen.add(key)
                candidate_tiles.append((cand, t["tile_id"]))

    print(f"[audit] {len(candidate_tiles)} (candidate, tile) pairs to process", file=sys.stderr)

    # Build run map — only for tiles we need
    needed_tiles = {tile_id for _, tile_id in candidate_tiles}
    tile_run_map = build_tile_run_map(runs_dir, needed_tiles)

    # Sort pairs by tile_id so we load each tile's sextractor catalog once
    candidate_tiles.sort(key=lambda x: x[1])

    fieldnames = [
        "mnras_row", "mnras_ra", "mnras_dec",
        "tile_id",
        "matched", "sep_arcsec", "src_id",
        "gate_result", "gate_failures",
        "eliminating_stage", "rejection_reason",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    n_no_match = n_gate_fail = n_stage_elim = n_survivor = n_no_run = 0

    current_tile_id: str = ""
    sex_rows: list[dict] = []
    ra_col = dec_col = ""

    with open(output_path, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        for (row_idx, ra, dec), tile_id in candidate_tiles:

            # Load sextractor catalog once per tile, then discard
            if tile_id != current_tile_id:
                current_tile_id = tile_id
                tile_dir = tiles_root / tile_id
                sex_rows, ra_col, dec_col = load_sexcat(tile_dir)

            run_data = tile_run_map.get(tile_id)
            matched_row, sep = find_nearest(sex_rows, ra_col, dec_col, ra, dec, args.max_sep)

            out: dict = {
                "mnras_row": row_idx,
                "mnras_ra":  f"{ra:.6f}",
                "mnras_dec": f"{dec:.6f}",
                "tile_id":   tile_id,
            }

            if matched_row is None:
                n_no_match += 1
                rejection_reason = _classify_no_match(
                    tile_dir, ra, dec, args.max_sep
                )
                out.update({
                    "matched":           "False",
                    "sep_arcsec":        f"{sep:.2f}" if sep != float("inf") else "",
                    "src_id":            "",
                    "gate_result":       "NO_MATCH",
                    "gate_failures":     "",
                    "eliminating_stage": "",
                    "rejection_reason":  rejection_reason,
                })
                writer.writerow(out)
                continue

            sep_str = f"{sep:.3f}"
            number = matched_row.get("NUMBER", matched_row.get("number", ""))
            src_id = f"{tile_id}:{number}" if number else ""

            failed_gates = check_gates(matched_row)
            if failed_gates:
                n_gate_fail += 1
                out.update({
                    "matched":           "True",
                    "sep_arcsec":        sep_str,
                    "src_id":            src_id,
                    "gate_result":       "GATE_FAIL",
                    "gate_failures":     ";".join(failed_gates),
                    "eliminating_stage": "",
                    "rejection_reason":  "",
                })
                writer.writerow(out)
                continue

            if run_data is None:
                n_no_run += 1
                out.update({
                    "matched":           "True",
                    "sep_arcsec":        sep_str,
                    "src_id":            src_id,
                    "gate_result":       "GATE_PASS",
                    "gate_failures":     "",
                    "eliminating_stage": "NO_RUN_DATA",
                    "rejection_reason":  "tile_not_found_in_any_run",
                })
                writer.writerow(out)
                continue

            elim_stage, reason = run_data.trace(src_id)
            if elim_stage == "SURVIVOR":
                n_survivor += 1
            else:
                n_stage_elim += 1

            out.update({
                "matched":           "True",
                "sep_arcsec":        sep_str,
                "src_id":            src_id,
                "gate_result":       "GATE_PASS",
                "gate_failures":     "",
                "eliminating_stage": elim_stage,
                "rejection_reason":  reason,
            })
            writer.writerow(out)

    total = len(candidate_tiles)
    print(f"\n[audit] Results ({total} pairs):", file=sys.stderr)
    print(f"  NO_MATCH    : {n_no_match:5d}  ({100*n_no_match/total:.1f}%)", file=sys.stderr)
    print(f"  GATE_FAIL   : {n_gate_fail:5d}  ({100*n_gate_fail/total:.1f}%)", file=sys.stderr)
    print(f"  STAGE_ELIM  : {n_stage_elim:5d}  ({100*n_stage_elim/total:.1f}%)", file=sys.stderr)
    print(f"  SURVIVOR    : {n_survivor:5d}  ({100*n_survivor/total:.1f}%)", file=sys.stderr)
    if n_no_run:
        print(f"  NO_RUN_DATA : {n_no_run:5d}  (tile in registry but not in any run manifest)",
              file=sys.stderr)
    print(f"[audit] Output: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
