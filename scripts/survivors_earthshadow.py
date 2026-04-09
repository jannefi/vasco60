#!/usr/bin/env python3
"""survivors_earthshadow.py

For each final survivor in survivors.csv, compute whether its sky coordinates
were inside Earth's shadow at the time of the photographic plate observation.

Uses the earthshadow library (Guy Nir, https://pypi.org/project/earthshadow/).
Observatory is fixed to Palomar (where all DSS-1 red plates were taken).
Orbit defaults to geosynchronous (42,164 km) per earthshadow default.

Inputs
------
  survivors.csv   — produced by build_report.py; must have columns:
                    src_id, ra, dec, obs_date (ISO, may contain minute overflow)

Outputs
-------
  survivors_earthshadow.csv — all original columns plus:
    obs_date_used    ISO timestamp actually used (after overflow correction)
    time_adjusted    True if the original obs_date required overflow correction
    shadow_dist_deg  angular distance from shadow centre in degrees (GEO orbit)
    in_earth_shadow  True if shadow_dist_deg < shadow_radius_deg

Usage
-----
    python scripts/survivors_earthshadow.py \\
        --survivors ./work/reports/<report>/survivors.csv \\
        --output    ./work/reports/<report>/survivors_earthshadow.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime
import re
import sys
import warnings
from pathlib import Path

import numpy as np
from astropy.time import Time
from astropy.utils.exceptions import AstropyWarning
from erfa import ErfaWarning

warnings.simplefilter("ignore", category=AstropyWarning)
warnings.simplefilter("ignore", category=ErfaWarning)
warnings.simplefilter("ignore", category=FutureWarning)

import earthshadow

OBSERVATORY = "palomar"


def parse_obs_date(s: str) -> tuple[Time, bool]:
    """Parse an ISO obs_date string, handling minute/second/hour overflow.

    Returns (astropy.time.Time, was_adjusted: bool).

    Overflow arises from FITS headers where e.g. minutes=67 means
    the time string is not a valid ISO datetime but the value is correct
    if interpreted as raw H/M/S components added to midnight.
    """
    # Fast path: direct parse
    try:
        return Time(s), False
    except Exception:
        pass

    # Manual parse with overflow normalisation via timedelta
    m = re.match(
        r"(\d{4})-(\d{2})-(\d{2})T(\d+):(\d+):(\d+(?:\.\d+)?)$", s.strip()
    )
    if not m:
        raise ValueError(f"Cannot parse obs_date: {s!r}")

    year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hours, minutes = int(m.group(4)), int(m.group(5))
    sec_f = float(m.group(6))
    seconds = int(sec_f)
    microseconds = round((sec_f - seconds) * 1_000_000)

    base = datetime.datetime(year, month, day, 0, 0, 0, tzinfo=datetime.timezone.utc)
    delta = datetime.timedelta(
        hours=hours, minutes=minutes, seconds=seconds, microseconds=microseconds
    )
    fixed = base + delta
    return Time(fixed), True


def run(survivors_path: Path, output_path: Path) -> None:
    with open(survivors_path, newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("[earthshadow] survivors.csv is empty — nothing to do.", file=sys.stderr)
        return

    # Check required columns
    required = {"src_id", "ra", "dec", "obs_date"}
    missing = required - set(rows[0].keys())
    if missing:
        raise SystemExit(f"survivors.csv missing columns: {missing}")

    shadow_radius_deg = earthshadow.get_shadow_radius().value  # GEO default
    print(f"[earthshadow] shadow radius at GEO: {shadow_radius_deg:.3f} deg")

    out_rows = []
    n_adjusted = 0
    n_in_shadow = 0
    n_error = 0

    for row in rows:
        src_id = row["src_id"]
        try:
            ra = float(row["ra"])
            dec = float(row["dec"])
        except ValueError:
            print(f"[earthshadow] WARN: bad ra/dec for {src_id}, skipping", file=sys.stderr)
            n_error += 1
            out_rows.append({**row, "obs_date_used": "", "time_adjusted": "", "shadow_dist_deg": "", "in_earth_shadow": ""})
            continue

        try:
            t, adjusted = parse_obs_date(row["obs_date"])
        except ValueError as e:
            print(f"[earthshadow] WARN: {e} for {src_id}, skipping", file=sys.stderr)
            n_error += 1
            out_rows.append({**row, "obs_date_used": "", "time_adjusted": "", "shadow_dist_deg": "", "in_earth_shadow": ""})
            continue

        if adjusted:
            n_adjusted += 1

        dist = earthshadow.dist_from_shadow_center(
            ra=ra, dec=dec, time=t, obs=OBSERVATORY
        )
        dist_deg = float(np.atleast_1d(dist.value)[0])

        in_shadow = dist_deg < shadow_radius_deg

        if in_shadow:
            n_in_shadow += 1

        out_rows.append({
            **row,
            "obs_date_used": t.isot,
            "time_adjusted": "True" if adjusted else "False",
            "shadow_dist_deg": f"{dist_deg:.4f}",
            "in_earth_shadow": "True" if in_shadow else "False",
        })

    # Write output
    fieldnames = list(rows[0].keys()) + ["obs_date_used", "time_adjusted", "shadow_dist_deg", "in_earth_shadow"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    n_valid = len(rows) - n_error
    print(f"[earthshadow] {len(rows)} survivors processed")
    print(f"[earthshadow]   {n_adjusted} had time overflow corrected")
    print(f"[earthshadow]   {n_error} skipped (unparseable)")
    print(f"[earthshadow]   {n_in_shadow} / {n_valid} in Earth shadow ({100*n_in_shadow/n_valid:.1f}%)")
    print(f"[earthshadow] Output: {output_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--survivors", required=True, type=Path, help="Path to survivors.csv")
    ap.add_argument("--output", type=Path, default=None,
                    help="Output CSV path (default: survivors_earthshadow.csv next to input)")
    args = ap.parse_args()

    survivors = args.survivors.resolve()
    if not survivors.exists():
        raise SystemExit(f"survivors.csv not found: {survivors}")

    output = args.output or survivors.parent / "survivors_earthshadow.csv"

    run(survivors, output)


if __name__ == "__main__":
    main()
