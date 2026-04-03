#!/usr/bin/env python3
"""
build_report.py — cross-run funnel report for vasco60 post-pipeline stages.

Reads all run folders under --runs-dir, collects counts from RUN_SUMMARY.txt
and stage ledger JSON files, and produces:

  <out-dir>/report-<timestamp>/
    funnel.txt          — ASCII funnel table with rejection %
    funnel.json         — same data, machine-readable
    survivors.csv       — deduplicated survivors: src_id,tile_id,ra,dec,plate_id,run_id
    tile_coverage.csv   — tiles seen: tile_id,plate_id,run_id,rows_to_S0
    report_index.txt    — one-page human summary

Usage:
    python scripts/build_report.py --runs-dir ./work/runs --out-dir ./work/reports
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Stage pipeline order definition
# ---------------------------------------------------------------------------
# Each entry: (ledger_glob, display_label, section)
# section: "pipeline" | "postprocess"
STAGES = [
    # These are synthesised from RUN_SUMMARY.txt, not ledgers
    ("__pass2_raw__",  "pass2 raw detections",    "pipeline"),
    ("__S0__",         "S0  post-MNRAS+dedup",    "pipeline"),
    # Post-process stages in order
    ("stage_S0M_MORPH_ledger.json",  "S0M MORPH",   "postprocess"),
    ("stage_S4S_SHAPE_ledger.json",  "S4S SHAPE",   "postprocess"),
    ("stage_S1_GSC_ledger.json",     "S1  GSC",     "postprocess"),
    ("stage_S2_SKYBOT_ledger.json",  "S2  SKYBOT",  "postprocess"),
    ("stage_S3_SCOS_ledger.json",    "S3  SCOS",    "postprocess"),
    ("stage_S4_PTF_ledger.json",     "S4  PTF",     "postprocess"),
    ("stage_S5_VSX_ledger.json",     "S5  VSX",     "postprocess"),
]

FINAL_STAGE_CSV = "stage_S5_VSX.csv"


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_run_summary(path: Path) -> dict:
    """Extract structured fields from RUN_SUMMARY.txt.
    Only reads lines up to and including the 'upload_skybot:' line.
    """
    result = {"pass2_raw": None, "S0_rows": None, "mode": None,
              "tiles_processed": None, "run_dir": None}
    if not path.exists():
        return result

    # Read the script-generated portion only; stop before the per-plate table
    # (user may have appended free text after that table).
    stop_pattern = re.compile(r"^\s+Plate\s+Tiles\s+Survivors")
    lines = []
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            if stop_pattern.match(line):
                break
            lines.append(line)

    text = "".join(lines)

    # pass2 unfiltered
    m = re.search(r"pass2 unfiltered.*?:\s+([\d,]+)", text)
    if m:
        result["pass2_raw"] = int(m.group(1).replace(",", ""))

    # dedup_rows (S0 input after dedup)
    m = re.search(r"dedup_rows:\s+(\d+)", text)
    if m:
        result["S0_rows"] = int(m.group(1))
    else:
        # fallback: final_rows_for_stage_and_uploads
        m = re.search(r"final_rows_for_stage_and_uploads:\s+(\d+)", text)
        if m:
            result["S0_rows"] = int(m.group(1))

    m = re.search(r"^mode:\s+(\S+)", text, re.MULTILINE)
    if m:
        result["mode"] = m.group(1)

    m = re.search(r"^tiles_processed:\s+(\d+)", text, re.MULTILINE)
    if m:
        result["tiles_processed"] = int(m.group(1))

    m = re.search(r"^run_dir:\s+(\S+)", text, re.MULTILINE)
    if m:
        result["run_dir"] = m.group(1)

    return result


def _load_ledger(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _kept(ledger: dict | None) -> int | None:
    if ledger is None:
        return None
    t = ledger.get("totals", {})
    for key in ("kept_rows", "kept"):
        if key in t:
            return int(t[key])
    return None


def _input_rows(ledger: dict | None) -> int | None:
    if ledger is None:
        return None
    t = ledger.get("totals", {})
    for key in ("input_rows", "total_rows"):
        if key in t:
            return int(t[key])
    return None


def _run_id(run_dir: Path) -> str:
    """Extract e.g. 'R1' from 'run-R1-20260327_165043'."""
    m = re.search(r"run-([^-]+)-", run_dir.name)
    return m.group(1) if m else run_dir.name


def _pct(new_val: int | None, old_val: int | None) -> str:
    if new_val is None or old_val is None or old_val == 0:
        return ""
    rej = old_val - new_val
    pct = 100.0 * rej / old_val
    return f"-{pct:.1f}%"


# ---------------------------------------------------------------------------
# Main collection logic
# ---------------------------------------------------------------------------

def collect_run(run_dir: Path) -> dict:
    """Return a dict with all counts for one run folder."""
    rid = _run_id(run_dir)
    summary = _parse_run_summary(run_dir / "RUN_SUMMARY.txt")

    # Load all ledgers
    ledgers = {}
    stages_dir = run_dir / "stages"
    for ledger_glob, _, _ in STAGES:
        if ledger_glob.startswith("__"):
            continue
        p = stages_dir / ledger_glob
        ledgers[ledger_glob] = _load_ledger(p)

    # Build counts list in STAGES order
    counts = []
    for ledger_glob, label, section in STAGES:
        if ledger_glob == "__pass2_raw__":
            counts.append(summary["pass2_raw"])
        elif ledger_glob == "__S0__":
            counts.append(summary["S0_rows"])
        else:
            counts.append(_kept(ledgers.get(ledger_glob)))

    # Load tile_manifest for plate coverage
    tiles = {}
    manifest = run_dir / "tile_manifest.csv"
    if manifest.exists():
        with manifest.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("rows_emitted_to_S0", "0") not in ("0", ""):
                    tiles[row["tile_id"]] = {
                        "plate_id": row.get("plate_id", ""),
                        "rows_to_S0": int(row.get("rows_emitted_to_S0", 0)),
                        "run_id": rid,
                    }

    # Load final survivors
    survivors = []
    final_csv = stages_dir / FINAL_STAGE_CSV
    if final_csv.exists():
        # Build plate_id lookup from manifest
        plate_map = {t: d["plate_id"] for t, d in tiles.items()}
        with final_csv.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                src_id = row.get("src_id", "")
                tile_id = src_id.split(":")[0] if ":" in src_id else row.get("tile_id", "")
                survivors.append({
                    "src_id":   src_id,
                    "tile_id":  tile_id,
                    "ra":       row.get("ra", ""),
                    "dec":      row.get("dec", ""),
                    "plate_id": plate_map.get(tile_id, ""),
                    "run_id":   rid,
                })

    return {
        "run_id":          rid,
        "run_dir":         str(run_dir),
        "mode":            summary.get("mode", ""),
        "tiles_processed": summary.get("tiles_processed"),
        "counts":          counts,   # parallel to STAGES list
        "tiles":           tiles,
        "survivors":       survivors,
    }


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _fmt_count(v: int | None) -> str:
    if v is None:
        return "-"
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v:,}"
    return str(v)


def build_funnel_text(runs: list[dict]) -> str:
    run_ids = [r["run_id"] for r in runs]
    col_w = max(12, max(len(rid) + 10 for rid in run_ids))

    lines = []
    lines.append("VASCO60 — Detection Funnel")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"Runs: {', '.join(run_ids)}")
    lines.append("")

    # Header
    label_w = 26
    header = f"{'Stage':<{label_w}}"
    for r in runs:
        header += f"  {r['run_id']:>{col_w}}"
    header += f"  {'TOTAL':>{col_w}}"
    lines.append(header)
    lines.append("-" * len(header))

    prev_counts = [None] * len(runs)  # track previous row for % calc

    last_section = None
    for i, (ledger_glob, label, section) in enumerate(STAGES):
        if section != last_section:
            lines.append(f"  [{section.upper()}]")
            last_section = section

        row_counts = [r["counts"][i] for r in runs]
        # Total (sum of non-None)
        total = sum(c for c in row_counts if c is not None) if any(
            c is not None for c in row_counts) else None

        # For % calculation use the ledger's own input_rows when available,
        # so stages that were run on a different input set (e.g. SKYBOT before SHAPE)
        # don't produce misleading negative rejection percentages.
        stages_dir_per_run = [Path(r["run_dir"]) / "stages" for r in runs]
        input_denominators = []
        for j, r in enumerate(runs):
            if ledger_glob.startswith("__"):
                input_denominators.append(prev_counts[j])
            else:
                ledger_path = Path(r["run_dir"]) / "stages" / ledger_glob
                led = _load_ledger(ledger_path)
                ledger_input = _input_rows(led)
                input_denominators.append(ledger_input if ledger_input is not None else prev_counts[j])

        row = f"  {label:<{label_w - 2}}"
        for j, cnt in enumerate(row_counts):
            pct = _pct(cnt, input_denominators[j])
            cell = _fmt_count(cnt)
            if pct:
                cell = f"{cell} ({pct})"
            row += f"  {cell:>{col_w}}"

        # Total column — use sum of input_denominators for total %
        t_denom = sum(d for d in input_denominators if d is not None) if any(
            d is not None for d in input_denominators) else None
        t_pct = _pct(total, t_denom) if i > 0 else ""
        tcell = _fmt_count(total)
        if t_pct:
            tcell = f"{tcell} ({t_pct})"
        row += f"  {tcell:>{col_w}}"

        lines.append(row)
        prev_counts = row_counts

    lines.append("")
    return "\n".join(lines)


def build_funnel_json(runs: list[dict]) -> dict:
    rows = []
    for i, (ledger_glob, label, section) in enumerate(STAGES):
        entry = {"stage": label, "section": section, "runs": {}}
        for r in runs:
            entry["runs"][r["run_id"]] = r["counts"][i]
        rows.append(entry)
    return {"generated": datetime.now(timezone.utc).isoformat(), "funnel": rows}


def build_survivors(runs: list[dict]) -> list[dict]:
    seen = {}
    result = []
    for r in runs:
        for s in r["survivors"]:
            sid = s["src_id"]
            if sid not in seen:
                seen[sid] = True
                result.append(s)
    return result


def build_tile_coverage(runs: list[dict]) -> list[dict]:
    rows = []
    for r in runs:
        for tile_id, td in sorted(r["tiles"].items()):
            rows.append({
                "tile_id":    tile_id,
                "plate_id":   td["plate_id"],
                "run_id":     r["run_id"],
                "rows_to_S0": td["rows_to_S0"],
            })
    return rows


def build_index(runs: list[dict], survivors: list[dict],
                tile_coverage: list[dict], report_dir: Path) -> str:
    total_tiles = len(tile_coverage)
    total_surv = len(survivors)

    plates = sorted({s["plate_id"] for s in survivors if s["plate_id"]})
    all_plates_tiles = sorted({r["plate_id"] for r in tile_coverage if r["plate_id"]})

    cross_run_dupes = sum(
        len(r["survivors"]) for r in runs
    ) - total_surv

    lines = [
        "VASCO60 — Report Index",
        f"Generated : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Report dir: {report_dir}",
        "",
        f"Runs processed : {len(runs)}  ({', '.join(r['run_id'] for r in runs)})",
        "Run modes      : " + ", ".join(r["run_id"] + "=" + r["mode"] for r in runs),
        "",
        f"Tiles in coverage : {total_tiles}",
        f"Plates in coverage: {len(all_plates_tiles)}  ({', '.join(all_plates_tiles)})",
        "",
        f"Final survivors (deduplicated) : {total_surv}",
        f"Cross-run duplicates dropped   : {cross_run_dupes}",
        f"Plates with survivors          : {len(plates)}  ({', '.join(plates)})",
        "",
        "Per-run survivor counts:",
    ]
    for r in runs:
        lines.append(f"  {r['run_id']:6s}  {len(r['survivors'])} survivors")

    lines += [
        "",
        "Output files:",
        "  funnel.txt        — stage-by-stage rejection funnel",
        "  funnel.json       — machine-readable funnel data",
        "  survivors.csv     — deduplicated final candidates with plate_id",
        "  tile_coverage.csv — all tiles processed, by run",
        "  report_index.txt  — this file",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build cross-run report for vasco60.")
    parser.add_argument("--runs-dir", default="./work/runs",
                        help="Directory containing run-R* folders")
    parser.add_argument("--out-dir", default="./work/reports",
                        help="Parent directory for report output")
    parser.add_argument("--run-glob", default="run-R*",
                        help="Glob to match run folder names (default: run-R*)")
    args = parser.parse_args()

    runs_dir = Path(args.runs_dir)
    out_parent = Path(args.out_dir)

    run_dirs = sorted(runs_dir.glob(args.run_glob))
    if not run_dirs:
        print(f"[REPORT] ERROR: no run folders found matching {runs_dir}/{args.run_glob}",
              file=sys.stderr)
        sys.exit(1)

    print(f"[REPORT] Found {len(run_dirs)} run folder(s): "
          f"{', '.join(d.name for d in run_dirs)}")

    runs = []
    for rd in run_dirs:
        print(f"[REPORT]   collecting {rd.name} ...", end=" ", flush=True)
        r = collect_run(rd)
        print(f"{len(r['survivors'])} survivors, {len(r['tiles'])} tiles")
        runs.append(r)

    # --- Build outputs ---
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_dir = out_parent / f"report-{ts}"
    report_dir.mkdir(parents=True, exist_ok=True)

    funnel_txt = build_funnel_text(runs)
    funnel_json = build_funnel_json(runs)
    survivors = build_survivors(runs)
    tile_coverage = build_tile_coverage(runs)
    index_txt = build_index(runs, survivors, tile_coverage, report_dir)

    # funnel.txt
    p = report_dir / "funnel.txt"
    p.write_text(funnel_txt, encoding="utf-8")
    print(f"[REPORT] wrote: {p}")

    # funnel.json
    p = report_dir / "funnel.json"
    p.write_text(json.dumps(funnel_json, indent=2), encoding="utf-8")
    print(f"[REPORT] wrote: {p}")

    # survivors.csv
    p = report_dir / "survivors.csv"
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["src_id", "tile_id", "ra", "dec", "plate_id", "run_id"])
        w.writeheader()
        w.writerows(survivors)
    print(f"[REPORT] wrote: {p}  ({len(survivors)} rows)")

    # tile_coverage.csv
    p = report_dir / "tile_coverage.csv"
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tile_id", "plate_id", "run_id", "rows_to_S0"])
        w.writeheader()
        w.writerows(tile_coverage)
    print(f"[REPORT] wrote: {p}  ({len(tile_coverage)} rows)")

    # report_index.txt
    p = report_dir / "report_index.txt"
    p.write_text(index_txt, encoding="utf-8")
    print(f"[REPORT] wrote: {p}")

    print(f"\n[REPORT] Done — {len(survivors)} deduplicated survivors across "
          f"{len(runs)} runs.")
    print(f"[REPORT] Report dir: {report_dir}")


if __name__ == "__main__":
    main()
