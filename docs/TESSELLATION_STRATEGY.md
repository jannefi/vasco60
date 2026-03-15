# Tile Plan Strategy — VASCO60 (DSS1-red / POSS-I E)

## Overview

The tile plan is generated deterministically from POSS-I plate metadata.
No images are downloaded during plan generation.
The output is a CSV at `./plans/tiles_<tag>.csv` that fully drives Step 1 download.

---

## Plate source

DSS POSS-I E plates are commonly treated as covering roughly 6.5°×6.5° on sky
in the digitized survey context. Each plate has a unique identifier (`REGION`)
and a precisely measured center position (`PLATERA`, `PLATEDEC`) stored in the
per-plate FITS header sidecar at:

    ./data/metadata/plates/headers/dss1red_<REGION>.fits.header.json

932 plates are available. Plan generation reads only these JSON files.

---

## MAPS-core constraint

MAPS/APS astrometry documentation explicitly restricts reliable astrometry to
the central 5.4° diameter of each plate, and warns of strong geometric
distortions and vignetting outside this core.

To ensure every 1°×1° tile lies fully inside the core, tile centers are
constrained to within **2.2°** of the plate center:

    2.2° = (5.4° / 2) − 0.5°

where 0.5° is half the tile side length. The separation is computed using the
haversine formula (exact spherical geometry).

---

## PS1 coverage gate

Pan-STARRS 1 (PS1) covers the sky north of declination −30°. The PS1 veto is
a required pipeline stage in VASCO60 (run-scoped, post-pipeline via STILTS).
Tiles outside PS1 coverage cannot receive this veto and are therefore excluded
from the plan.

**Rule applied:** tile center Dec ≥ −29.5°

This ensures the southern edge of every 1° tile (center − 0.5°) is at or
above −30°. Tiles centered between −30° and −29.5° would have their southern
half outside PS1 and are excluded.

Plates whose center Dec is below −30° are skipped entirely (28 of 932).

---

## Tile grid algorithm

For each plate center (RA₀, Dec₀):

1. Enumerate integer offsets (di, dj) from −3 to +3 in both axes.
2. Compute tile center:
   - `tile_dec = Dec₀ + dj`
   - `tile_ra  = (RA₀ + di / cos(Dec₀)) mod 360`  — 1° arc step along RA
3. Round to 3 decimal places (consistent with `format_tile_id`).
4. Apply **MAPS-core gate**: skip if haversine(plate_center, tile_center) > 2.2°.
5. Apply **PS1 gate**: skip if tile_dec < −29.5°.
6. Assign `tile_id = tile_RA<ra>_DEC[p|m]<dec>` (locked naming convention).

Within each plate, tiles are sorted by (ra_deg, dec_deg) and assigned a
zero-based `plate_tile_idx`.

### Duplicate policy

Adjacent plates overlap. A tile_id that appears in multiple plates is kept
only once, attributed to the plate with the lowest `plate_id` (alphabetical).
The final plan contains no duplicate `tile_id` values.

### Output scale (full plan, tag `poss1e_ps1`)

| Item | Count |
|---|---|
| Plates processed | 904 |
| Plates skipped (Dec < −30°) | 28 |
| Unique tiles in plan | 11 733 |

---

## Plan CSV schema

| Column | Description |
|---|---|
| `plate_id` | REGION from FITS header (e.g. `XE005`) |
| `plate_tile_idx` | Per-plate tile index (0-based, sorted by ra/dec) |
| `tile_id` | `tile_RA<ra>_DEC[p/m]<dec>` — locked naming convention |
| `ra_deg` | Tile center RA, decimal degrees (6 d.p.) |
| `dec_deg` | Tile center Dec, decimal degrees (6 d.p.) |
| `size_arcmin` | Always `60` |
| `survey` | Always `dss1-red` |

---

## How to validate

### One-plate smoke test

```bash
# Generate plan for a single plate and print tiles/plate
python -m vasco.plan.tessellate_plates --tag smoke --plate XE005
# Expected output: 17 tiles for XE005 (polar plate at Dec ≈ +83.6°)

# Validate the generated file — must report 0 violations
python -m vasco.plan.tessellate_plates --validate plans/tiles_smoke.csv
# Expected: "Rows validated: 17   OK — 0 violations"
```

Validation checks performed:
- Schema: all required columns present
- Naming: every `tile_id` matches `tile_RA<ra>_DEC[pm]<dec>` with 3 d.p.
- Bounds: 0 ≤ ra_deg < 360, −90 ≤ dec_deg ≤ 90
- **PS1 gate**: every tile center Dec ≥ −29.5°
- **MAPS-core gate**: every tile angular separation from its plate center ≤ 2.2°
- No duplicate `tile_id` values
- Stable sort order (plate_id, plate_tile_idx)

### Full plan

```bash
python -m vasco.plan.tessellate_plates --tag poss1e_ps1
python -m vasco.plan.tessellate_plates --validate plans/tiles_poss1e_ps1.csv
# Expected: "Rows validated: 11733   OK — 0 violations"
```

---

## Generator invocation reference

```
python -m vasco.plan.tessellate_plates [options]

  --tag TAG          Output filename tag: plans/tiles_<tag>.csv  [poss1e_ps1]
  --plate REGION     Restrict to one plate (smoke test)
  --headers-dir DIR  Plate header JSON directory  [./data/metadata/plates/headers]
  --out-dir DIR      Output directory  [./plans]
  --validate CSV     Validate an existing plan CSV (no generation)
```
