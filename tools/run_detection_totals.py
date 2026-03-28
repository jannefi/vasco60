#!/usr/bin/env python3
"""
run_detection_totals.py — summarise raw detection counts for a run.

Reads tile_manifest.csv from the run folder, then for each tile reads
MNRAS_SUMMARY.json to get veto_start_rows (= pass2 unfiltered detections).
Also sums the manifest columns rows_in_tile_filtered_csv and rows_emitted_to_S0
so you can see the full funnel from raw detections → post-pipeline input.

Also reports per-plate survivor counts from the final post-pipeline stage CSV
(auto-detected as the last stage_S*.csv in stages/, or set with --final-stage).

Usage:
    python tools/run_detection_totals.py --run-dir work/runs/<run>
    python tools/run_detection_totals.py --run-dir work/runs/<run> --tiles-root data/tiles
    python tools/run_detection_totals.py --run-dir work/runs/<run> --final-stage stage_S5_VSX.csv
"""

import argparse
import collections
import csv
import json
import sys
from pathlib import Path


def _detect_final_stage(stages_dir):
    """Return the last stage_S*.csv (excluding _flags files) sorted by name."""
    candidates = sorted(
        f for f in stages_dir.glob("stage_S*.csv")
        if "_flags" not in f.name and "_ledger" not in f.name
    )
    return candidates[-1] if candidates else None


def main():
    ap = argparse.ArgumentParser(description="Summarise raw detection totals for a run")
    ap.add_argument("--run-dir", required=True, help="Run folder (contains tile_manifest.csv)")
    ap.add_argument(
        "--tiles-root",
        default="data/tiles",
        help="Root of tile folders (default: data/tiles)",
    )
    ap.add_argument(
        "--final-stage",
        default=None,
        help="Final stage CSV filename in stages/ (default: auto-detect last stage_S*.csv)",
    )
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    tiles_root = Path(args.tiles_root)

    manifest_path = run_dir / "tile_manifest.csv"
    if not manifest_path.exists():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        sys.exit(1)

    with open(manifest_path, newline="") as f:
        rows = list(csv.DictReader(f))

    def _int(val):
        """Parse int, treating blank/missing as 0."""
        return int(val) if val and val.strip() else 0

    # tile_id → plate_id lookup
    tile_to_plate = {r["tile_id"]: r.get("plate_id", "").strip() for r in rows}

    n_tiles = len(rows)
    pass2_filtered_total = sum(_int(r["rows_in_tile_filtered_csv"]) for r in rows)
    emitted_to_s0_total = sum(_int(r["rows_emitted_to_S0"]) for r in rows)

    plates = sorted({p for p in tile_to_plate.values() if p})
    n_plates = len(plates)

    pass2_unfiltered_total = 0
    missing_summary = []
    tiles_with_summary = 0

    for row in rows:
        tile_id = row["tile_id"]
        summary_path = tiles_root / tile_id / "MNRAS_SUMMARY.json"

        if not summary_path.exists():
            missing_summary.append(tile_id)
            continue

        with open(summary_path) as f:
            summary = json.load(f)

        pass2_unfiltered_total += summary.get("veto_start_rows", 0)
        tiles_with_summary += 1

    # --- Print header ---
    print(f"\nRun: {run_dir.name}")
    print(f"Tiles in manifest : {n_tiles}")
    print(f"Plates covered    : {n_plates}  ({', '.join(plates)})")
    print(f"Tiles with summary: {tiles_with_summary}")
    if missing_summary:
        print(f"  Missing MNRAS_SUMMARY.json: {len(missing_summary)} tile(s)")
        for t in missing_summary[:10]:
            print(f"    {t}")
        if len(missing_summary) > 10:
            print(f"    ... and {len(missing_summary) - 10} more")

    # --- Funnel ---
    print()
    print("Detection funnel:")
    print(f"  pass2 unfiltered (veto_start_rows)  : {pass2_unfiltered_total:>10,}")
    print(f"  pass2 filtered (post-MNRAS gates)   : {pass2_filtered_total:>10,}")
    print(f"  emitted to S0 (stage input)         : {emitted_to_s0_total:>10,}")

    if pass2_unfiltered_total > 0:
        gate_pct = 100.0 * (1 - pass2_filtered_total / pass2_unfiltered_total)
        s0_pct = 100.0 * (1 - emitted_to_s0_total / pass2_unfiltered_total)
        print()
        print(f"  MNRAS gates removed               : {gate_pct:.3f}% of pass2 unfiltered")
        print(f"  Surviving to S0                   : {100 - s0_pct:.4f}% of pass2 unfiltered")

    print()
    print("Note: pass1 detection count is not available (stored as binary .ldac, no CSV).")

    # --- Per-plate survivor breakdown from final stage CSV ---
    stages_dir = run_dir / "stages"
    if not stages_dir.exists():
        return

    if args.final_stage:
        final_csv = stages_dir / args.final_stage
    else:
        final_csv = _detect_final_stage(stages_dir)

    if final_csv is None or not final_csv.exists():
        print(f"\n(No final stage CSV found in {stages_dir})", file=sys.stderr)
        return

    # Count survivors per plate; src_id format: tile_id:object_id
    plate_survivors = collections.Counter()
    plate_tiles_seen = collections.defaultdict(set)
    total_survivors = 0

    with open(final_csv, newline="") as f:
        for src_row in csv.DictReader(f):
            src_id = src_row.get("src_id", "")
            tile_id = src_id.split(":")[0] if ":" in src_id else src_row.get("tile_id", "")
            plate_id = tile_to_plate.get(tile_id, "unknown")
            plate_survivors[plate_id] += 1
            plate_tiles_seen[plate_id].add(tile_id)
            total_survivors += 1

    print(f"\nFinal stage       : {final_csv.name}")
    print(f"Total survivors   : {total_survivors}")
    print()
    print(f"  {'Plate':<10}  {'Tiles':>6}  {'Survivors':>10}  {'Surv/tile':>10}")
    print(f"  {'-'*10}  {'-'*6}  {'-'*10}  {'-'*10}")
    for plate in sorted(plate_survivors, key=lambda p: -plate_survivors[p]):
        n_surv = plate_survivors[plate]
        n_t = len(plate_tiles_seen[plate])
        ratio = n_surv / n_t if n_t else 0.0
        print(f"  {plate:<10}  {n_t:>6}  {n_surv:>10}  {ratio:>10.2f}")


if __name__ == "__main__":
    main()
