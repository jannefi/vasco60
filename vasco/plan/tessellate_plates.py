# -*- coding: utf-8 -*-
"""vasco.plan.tessellate_plates

Generate and validate a plate-driven tile plan for VASCO60 (DSS1-red / POSS-I E).

Usage
-----
Generate a full plan:
    python -m vasco.plan.tessellate_plates --tag poss1e_ps1

Single-plate smoke test:
    python -m vasco.plan.tessellate_plates --tag smoke --plate XE005

Validate an existing plan:
    python -m vasco.plan.tessellate_plates --validate plans/tiles_poss1e_ps1.csv

Optional arguments:
    --headers-dir   Path to plate header JSONs  [./data/metadata/plates/headers]
    --out-dir       Output directory for plan CSV  [./plans]
    --tag           Tag appended to output filename
    --plate         Restrict generation to one plate_id (smoke test)
    --validate      Path to an existing plan CSV to validate (skips generation)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from pathlib import Path
from typing import Iterator

from vasco.utils.tile_id import format_tile_id, parse_tile_id_center

# ---------------------------------------------------------------------------
# Constants (locked by DECISIONS.md and plan approval)
# ---------------------------------------------------------------------------
MAPS_CORE_RADIUS_DEG = 2.2      # 5.4° plate core diameter / 2  −  0.5° half-tile margin
PS1_DEC_LIMIT        = -29.5    # tile center must be >= this (full 1° tile inside PS1)
TILE_SIZE_ARCMIN     = 60
SURVEY               = "dss1-red"
GRID_RANGE           = range(-3, 4)   # integer offsets, −3 … +3 in both axes
CSV_FIELDNAMES       = [
    "plate_id", "plate_tile_idx", "tile_id",
    "ra_deg", "dec_deg", "size_arcmin", "survey",
]
_TILE_ID_RE = re.compile(
    r"^tile_RA[0-9]+\.[0-9]{3}_DEC[pm][0-9]+\.[0-9]{3}$"
)

# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def haversine_deg(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    """Angular separation in degrees between two sky positions."""
    ra1, dec1, ra2, dec2 = map(math.radians, [ra1, dec1, ra2, dec2])
    dra   = ra2 - ra1
    ddec  = dec2 - dec1
    a = math.sin(ddec / 2) ** 2 + math.cos(dec1) * math.cos(dec2) * math.sin(dra / 2) ** 2
    return 2.0 * math.degrees(math.asin(math.sqrt(min(a, 1.0))))


# ---------------------------------------------------------------------------
# Plate header loading
# ---------------------------------------------------------------------------

def load_plate_centers(headers_dir: Path) -> list[dict]:
    """Read plate center (RA, Dec, plate_id) from header JSONs.

    Returns list of dicts with keys: plate_id, plate_ra, plate_dec.
    Plates missing PLATERA or PLATEDEC are silently skipped.
    """
    plates = []
    for p in sorted(headers_dir.glob("dss1red_*.fits.header.json")):
        try:
            h = json.loads(p.read_text(encoding="utf-8"))["header"]
            ra  = h.get("PLATERA")
            dec = h.get("PLATEDEC")
            region = h.get("REGION", "")
            if ra is None or dec is None or not region:
                continue
            plates.append({"plate_id": region, "plate_ra": float(ra), "plate_dec": float(dec)})
        except Exception:
            continue
    return plates


# ---------------------------------------------------------------------------
# Tile generation for one plate
# ---------------------------------------------------------------------------

def tiles_for_plate(plate_id: str, plate_ra: float, plate_dec: float) -> list[dict]:
    """Generate tile center candidates for one plate.

    Applies MAPS-core gate (angular separation ≤ 2.2°) and PS1 gate (Dec ≥ -29.5°).
    Returns list of row dicts, sorted by (ra_deg, dec_deg), with plate_tile_idx assigned.
    """
    cos_dec = math.cos(math.radians(plate_dec))
    near_pole = abs(cos_dec) < 1e-6

    candidates = []
    for dj in GRID_RANGE:
        tile_dec_raw = plate_dec + dj
        if tile_dec_raw < -90.0 or tile_dec_raw > 90.0:
            continue
        tile_dec = round(tile_dec_raw, 3)

        for di in GRID_RANGE:
            if near_pole and di != 0:
                # RA is degenerate at poles; only the central RA column is meaningful
                continue
            tile_ra = round(((plate_ra + di / cos_dec) if not near_pole else plate_ra) % 360.0, 3)

            sep = haversine_deg(plate_ra, plate_dec, tile_ra, tile_dec)
            if sep > MAPS_CORE_RADIUS_DEG:
                continue
            if tile_dec < PS1_DEC_LIMIT:
                continue

            tile_id = format_tile_id(tile_ra, tile_dec, ndp=3)
            candidates.append({
                "plate_id":       plate_id,
                "plate_tile_idx": 0,          # assigned below
                "tile_id":        tile_id,
                "ra_deg":         f"{tile_ra:.6f}",
                "dec_deg":        f"{tile_dec:.6f}",
                "size_arcmin":    TILE_SIZE_ARCMIN,
                "survey":         SURVEY,
                # internal: for dedup and validation
                "_sep":           sep,
                "_plate_ra":      plate_ra,
                "_plate_dec":     plate_dec,
            })

    candidates.sort(key=lambda r: (float(r["ra_deg"]), float(r["dec_deg"])))
    for idx, row in enumerate(candidates):
        row["plate_tile_idx"] = idx

    return candidates


# ---------------------------------------------------------------------------
# Full plan generation
# ---------------------------------------------------------------------------

def generate_plan(
    headers_dir: Path,
    out_dir: Path,
    tag: str,
    plate_filter: str | None = None,
) -> Path:
    plates = load_plate_centers(headers_dir)
    if plate_filter:
        plates = [p for p in plates if p["plate_id"] == plate_filter]
        if not plates:
            sys.exit(f"ERROR: plate '{plate_filter}' not found in {headers_dir}")

    # Generate all tiles; dedup by tile_id (first plate wins, plates sorted alpha)
    plates.sort(key=lambda p: p["plate_id"])
    seen: dict[str, dict] = {}   # tile_id -> row
    per_plate_counts: dict[str, int] = {}
    plates_skipped = 0
    duplicates_dropped = 0

    for plate in plates:
        pid, pra, pdec = plate["plate_id"], plate["plate_ra"], plate["plate_dec"]
        if pdec < -30.0:
            plates_skipped += 1
            continue
        rows = tiles_for_plate(pid, pra, pdec)
        per_plate_counts[pid] = len(rows)
        for row in rows:
            tid = row["tile_id"]
            if tid in seen:
                duplicates_dropped += 1
            else:
                seen[tid] = row

    # Stable output order: plate_id alpha, then plate_tile_idx
    out_rows = sorted(seen.values(), key=lambda r: (r["plate_id"], r["plate_tile_idx"]))

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"tiles_{tag}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(out_rows)

    # Summary
    total_tiles = len(out_rows)
    total_plates = len(per_plate_counts)
    print(f"Plates found in headers     : {total_plates}")
    print(f"Plates skipped by Dec < -30 : {plates_skipped}  (plate center Dec < -30°)")
    print(f"Unique tiles                : {total_tiles}")
    print(f"Duplicates dropped          : {duplicates_dropped}")
    print()
    print("Tiles per plate:")
    for pid in sorted(per_plate_counts):
        print(f"  {pid:8s}  {per_plate_counts[pid]:3d}")
    print()
    print(f"Plan written to: {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_plan(csv_path: Path, headers_dir: Path) -> bool:
    """Validate an existing plan CSV. Prints violations; returns True if clean."""
    plates_by_id = {p["plate_id"]: p for p in load_plate_centers(headers_dir)}

    violations: list[str] = []
    seen_tile_ids: set[str] = set()
    prev_key = None   # for sort-order check
    row_count = 0

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Schema check
        missing_cols = [c for c in CSV_FIELDNAMES if c not in (reader.fieldnames or [])]
        if missing_cols:
            violations.append(f"SCHEMA: missing columns {missing_cols}")
            print(f"FAIL  {violations[-1]}")
            return False   # cannot continue without required columns

        for i, row in enumerate(reader, start=2):   # line 2 = first data row
            row_count += 1
            tid     = row.get("tile_id", "")
            pid     = row.get("plate_id", "")
            ra_str  = row.get("ra_deg", "")
            dec_str = row.get("dec_deg", "")
            idx_str = row.get("plate_tile_idx", "")
            size    = row.get("size_arcmin", "")
            survey  = row.get("survey", "")

            loc = f"line {i} tile_id={tid!r}"

            # --- naming contract ---
            if not _TILE_ID_RE.match(tid):
                violations.append(f"NAMING: {loc} — does not match contract")

            # --- parse ra/dec ---
            try:
                ra  = float(ra_str)
                dec = float(dec_str)
            except ValueError:
                violations.append(f"TYPE: {loc} — ra_deg/dec_deg not float")
                continue

            # --- bounds ---
            if not (0.0 <= ra < 360.0):
                violations.append(f"BOUNDS: {loc} — ra_deg={ra} out of [0, 360)")
            if not (-90.0 <= dec <= 90.0):
                violations.append(f"BOUNDS: {loc} — dec_deg={dec} out of [-90, 90]")

            # --- PS1 gate ---
            if dec < PS1_DEC_LIMIT:
                violations.append(f"PS1: {loc} — dec_deg={dec} < {PS1_DEC_LIMIT}")

            # --- static columns ---
            if size != str(TILE_SIZE_ARCMIN):
                violations.append(f"SCHEMA: {loc} — size_arcmin={size!r} expected '{TILE_SIZE_ARCMIN}'")
            if survey != SURVEY:
                violations.append(f"SCHEMA: {loc} — survey={survey!r} expected '{SURVEY}'")

            # --- tile_id encodes same ra/dec ---
            parsed = parse_tile_id_center(tid)
            if parsed is None:
                violations.append(f"NAMING: {loc} — tile_id unparseable")
            else:
                tid_ra, tid_dec = parsed
                if abs(tid_ra - ra) > 0.0005 or abs(tid_dec - dec) > 0.0005:
                    violations.append(
                        f"MISMATCH: {loc} — tile_id encodes ({tid_ra},{tid_dec}) "
                        f"but ra_deg/dec_deg=({ra},{dec})"
                    )

            # --- MAPS-core gate ---
            plate = plates_by_id.get(pid)
            if plate is None:
                violations.append(f"PLATE: {loc} — plate_id={pid!r} not found in headers")
            else:
                sep = haversine_deg(plate["plate_ra"], plate["plate_dec"], ra, dec)
                if sep > MAPS_CORE_RADIUS_DEG + 1e-6:
                    violations.append(
                        f"MAPS: {loc} — sep={sep:.4f}° from plate {pid} > {MAPS_CORE_RADIUS_DEG}°"
                    )

            # --- duplicates ---
            if tid in seen_tile_ids:
                violations.append(f"DUPLICATE: {loc} — tile_id appears more than once")
            seen_tile_ids.add(tid)

            # --- sort order ---
            key = (pid, int(idx_str) if idx_str.isdigit() else -1)
            if prev_key is not None and key < prev_key:
                violations.append(f"ORDER: {loc} — row out of sort order (plate_id, plate_tile_idx)")
            prev_key = key

    print(f"Rows validated: {row_count}")
    if violations:
        print(f"VIOLATIONS ({len(violations)}):")
        for v in violations:
            print(f"  FAIL  {v}")
        return False
    else:
        print(f"OK — 0 violations")
        return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m vasco.plan.tessellate_plates",
        description="Generate or validate a VASCO60 plate-driven tile plan.",
    )
    p.add_argument("--headers-dir", default="./data/metadata/plates/headers",
                   help="Directory containing dss1red_*.fits.header.json files")
    p.add_argument("--out-dir", default="./plans",
                   help="Output directory for generated plan CSV")
    p.add_argument("--tag", default="poss1e_ps1",
                   help="Tag appended to output filename: tiles_<tag>.csv")
    p.add_argument("--plate", metavar="REGION",
                   help="Restrict generation to a single plate_id (smoke test)")
    p.add_argument("--validate", metavar="CSV",
                   help="Validate an existing plan CSV instead of generating")
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)
    headers_dir = Path(args.headers_dir)

    if args.validate:
        ok = validate_plan(Path(args.validate), headers_dir)
        sys.exit(0 if ok else 1)

    generate_plan(
        headers_dir=headers_dir,
        out_dir=Path(args.out_dir),
        tag=args.tag,
        plate_filter=args.plate,
    )


if __name__ == "__main__":
    main()
