#!/usr/bin/env python3
"""
run_detection_totals.py — summarise raw detection counts for a run.

Reads tile_manifest.csv from the run folder, then for each tile reads
MNRAS_SUMMARY.json to get veto_start_rows (= pass2 unfiltered detections).
Also sums the manifest columns rows_in_tile_filtered_csv and rows_emitted_to_S0
so you can see the full funnel from raw detections → post-pipeline input.

Usage:
    python tools/run_detection_totals.py --run-dir work/runs/<run>
    python tools/run_detection_totals.py --run-dir work/runs/<run> --tiles-root data/tiles
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description="Summarise raw detection totals for a run")
    ap.add_argument("--run-dir", required=True, help="Run folder (contains tile_manifest.csv)")
    ap.add_argument(
        "--tiles-root",
        default="data/tiles",
        help="Root of tile folders (default: data/tiles)",
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

    n_tiles = len(rows)
    pass2_filtered_total = sum(int(r["rows_in_tile_filtered_csv"]) for r in rows)
    emitted_to_s0_total = sum(int(r["rows_emitted_to_S0"]) for r in rows)

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

    print(f"\nRun: {run_dir.name}")
    print(f"Tiles in manifest : {n_tiles}")
    print(f"Tiles with summary: {tiles_with_summary}")
    if missing_summary:
        print(f"  Missing MNRAS_SUMMARY.json: {len(missing_summary)} tile(s)")
        for t in missing_summary[:10]:
            print(f"    {t}")
        if len(missing_summary) > 10:
            print(f"    ... and {len(missing_summary) - 10} more")

    print()
    print("Detection funnel:")
    print(f"  pass2 unfiltered (veto_start_rows)  : {pass2_unfiltered_total:>10,}")
    print(f"  pass2 filtered (post-MNRAS gates)   : {pass2_filtered_total:>10,}")
    print(f"  emitted to S0 (stage input)         : {emitted_to_s0_total:>10,}")

    if pass2_unfiltered_total > 0:
        gate_pct = 100.0 * (1 - pass2_filtered_total / pass2_unfiltered_total)
        s0_pct = 100.0 * (1 - emitted_to_s0_total / pass2_unfiltered_total)
        print()
        print(f"  MNRAS gates removed               : {gate_pct:.1f}% of pass2 unfiltered")
        print(f"  Surviving to S0                   : {100 - s0_pct:.2f}% of pass2 unfiltered")

    print()
    print("Note: pass1 detection count is not available (stored as binary .ldac, no CSV).")


if __name__ == "__main__":
    main()
