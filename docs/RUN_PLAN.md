# Downloading tiles from a plan: `run_plan.py`

`scripts/run_plan.py` reads a tile plan CSV (produced by `tessellate_plates`)
and calls `step1-download` sequentially for each tile.  It is the only
intended entry point for bulk Step 1 downloads in VASCO60 — do not call
`step1-download` directly for batch runs.

---

## Prerequisites

- A plan CSV exists under `./plans/` (see `docs/TESSELLATION_STRATEGY.md`).
- `./data` symlink points to the dataset root (HDD or equivalent).
- The environment can reach the STScI DSS server (MAST).

---

## Basic usage

```bash
# Dry-run: print what would be done, no downloads
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --dry-run

# Download the first tile only
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --limit 1

# Download the first 50 tiles
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --limit 50

# Full run (all rows in the plan)
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv
```

Progress is printed to stdout and appended to `./logs/run_plan.log`.

---

## Arguments

| Argument | Default | Description |
|---|---|---|
| `plan` | (required) | Path to plan CSV |
| `--tiles-dir DIR` | `./data/tiles` | Tiles root; tile folder is computed by step1 from ra/dec |
| `--limit N` | unlimited | Stop after N successful downloads (skips do not count) |
| `--dry-run` | off | Print actions without calling step1-download |

---

## Resume after interruption

The script is safe to interrupt with **Ctrl+C** at any time.  On interrupt
it logs a summary line and exits with code 130.

To resume, simply run the same command again.  Before attempting each tile,
the script checks whether `<tiles-dir>/<tile_id>/RUN_COUNTS.json` exists.
That file is written by `step1-download` on every successful exit — including
tiles that were skipped as non-POSS.  Any tile that already has this file is
logged as `SKIP` and is not downloaded again.

```bash
# Run interrupted after 10 tiles.  Re-run the same command:
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --limit 50
# → first 10 tiles logged as SKIP, download continues from tile 11
```

`--limit` counts downloads, not rows visited, so resuming with `--limit 50`
will always result in 50 newly downloaded tiles regardless of how many were
skipped.

---

## Log file

All output is appended to `./logs/run_plan.log` (constant filename, no
timestamp suffix).  The log directory is created automatically.

Each line has the format:
```
<ISO-timestamp> <STATUS>  <tile_id>  plate=<plate_id>  [detail]
```

Status values:

| Status | Meaning |
|---|---|
| `DRY` | Dry-run: would have called step1 |
| `SKIP` | Tile already done (RUN_COUNTS.json present) |
| `OK` | step1-download exited 0 |
| `FAIL` | step1-download returned non-zero; run continues |
| `INTERRUPT` | Ctrl+C received; summary logged |
| `DONE` | End-of-run summary |

A `FAIL` does not stop the run.  Failed tiles will be retried on the next
resume (they have no `RUN_COUNTS.json`).

---

## Typical workflow

```bash
# 1. Generate the plan (one-time)
python -m vasco.plan.tessellate_plates --tag poss1e_ps1

# 2. Validate the plan
python -m vasco.plan.tessellate_plates --validate plans/tiles_poss1e_ps1.csv

# 3. Dry-run a small batch to confirm setup
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --limit 5 --dry-run

# 4. Download a small test batch
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --limit 5

# 5. Check the log
tail ./logs/run_plan.log

# 6. Full run (Ctrl+C safe)
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv
```

---

## Notes

- **Sequential only.** Downloads are intentionally not parallelised; the
  STScI MAST backend may throttle or back off under concurrent load.
- **Non-POSS tiles.** If STScI returns a non-POSS plate for a tile, step1
  logs a skip and still writes `RUN_COUNTS.json`.  The tile will not be
  retried on resume, which is correct — the sky coverage at that position is
  not POSS-I E.
- **Tile folder naming.** The orchestrator passes only `--ra`, `--dec`, and
  `--workdir` (tiles root) to step1.  The tile folder name is always computed
  internally by step1 from the coordinates using the locked naming convention
  (`tile_RA<ra>_DEC[p/m]<dec>`).  Never construct tile paths manually.
