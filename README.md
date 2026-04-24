# VASCO60

Second-generation pipeline for searching digitised POSS-I photographic plates for objects that have vanished from the sky. Based on [vasco](https://github.com/jannefi/vasco/).

This repository does not aim to reproduce the exact dataset of MNRAS 515(1):1380 (2022). The goal is to reproduce the *intent* of that workflow — a reproducible, plate-aware POSS-I processing pipeline — with improved provenance, robustness, and controls. `main` is a moving research branch and may include breaking changes.

**Public context:**
- MNRAS 515(1):1380 (2022)
- Watters et al. (2026) arXiv:2601.21946
- Villarroel et al. (2026) response arXiv:2602.15171
- Busko (2026) arXiv:2603.20407 Searching for Fast Astronomical Transients in Archival Photographic Plates
- Bruehl et al. (2026) arXiv:2604.18799 

---

## How it works

VASCO60 searches for point sources detected in 1950s POSS-I red plates that have no counterpart in modern catalogues (Gaia DR3, Pan-STARRS1, USNO-B). Each pipeline run:

1. Selects a set of 1°×1° sky tiles from a deterministic tessellation plan
2. Downloads each tile as a FITS image from STScI/MAST (DSS1-red / POSS-I E)
3. Runs SExtractor twice (pass 1 for PSF modelling, pass 2 PSF-aware) per tile
4. Cross-matches detections against Gaia, PS1, and USNO-B
5. Applies morphology and quality filters (MNRAS 2022 criteria)
6. Post-processes the run-scoped survivor set through additional veto stages (SkyBoT, SuperCOSMOS, PTF, VSX)

What remains after all veto stages is the candidate list for human review.

---

## Prerequisites

- **OS:** Linux (tested on Debian 13 Trixie)
- **Python:** 3.11 with pip (`pip install -r requirements.txt`)
- **SExtractor:** binary must be available as `sex`
- **PSFEx:** binary must be available as `psfex`
- **STILTS:** required for post-pipeline TAP queries

Verify your setup:
```bash
python -V
sex -v
psfex -v
stilts -version
```

---

## Directory layout

```
data/
  tiles/              # Per-tile working directories (tile_RA…_DEC…/)
  metadata/           # tiles_registry.csv, tile_to_plate.csv
  metadata/plates/headers/   # POSS-I plate header JSON sidecars
plans/                # Tessellation plan CSVs
work/runs/            # Post-pipeline run output folders
scripts/              # Orchestration and post-pipeline scripts
vasco/                # Pipeline Python package
logs/                 # run_plan.log and other logs
```

---

## Step 0 — Tessellation plan

The tile plan is generated deterministically from POSS-I plate metadata. It covers the sky north of Dec −29.5° (PS1 coverage limit) and restricts tiles to the reliable astrometric core of each plate.

```bash
# Generate the full plan (11 733 tiles, ~904 plates)
python -m vasco.plan.tessellate_plates --tag poss1e_ps1

# Validate
python -m vasco.plan.tessellate_plates --validate plans/tiles_poss1e_ps1.csv
```

A pre-generated plan is included in `plans/`. See [docs/TESSELLATION_STRATEGY.md](docs/TESSELLATION_STRATEGY.md) for full details.

---

## Step 1 — Download tiles

Use `scripts/run_plan.py` to drive downloads from the plan CSV. It skips already-completed tiles and supports `--plate` to restrict to one plate.

```bash
# Download all tiles for plate XE309
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --plate XE309

# Dry-run to preview
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --plate XE309 --dry-run

# Limit to N downloads (useful for testing)
python scripts/run_plan.py plans/tiles_poss1e_ps1.csv --limit 10
```

Tiles are stored under `./data/tiles/<tile_id>/`. The metadata files `tiles_registry.csv` and `tile_to_plate.csv` are updated automatically after each download.

**Tip:** Download 50–100 tiles at a time, prewarm caches, then process. Downloading thousands of tiles before processing makes the run harder to recover if something goes wrong.

---

## Steps 2–6 — Per-tile pipeline

Run these steps for each tile, in order. Parallel execution across tiles is recommended.

**Step 2 — SExtractor pass 1** (source detection for PSF modelling)
```bash
python -m vasco.cli_pipeline step2-pass1 --workdir data/tiles/<tile_id>
```

**Step 3 — PSFEx + SExtractor pass 2** (PSF-aware source extraction)
```bash
python -m vasco.cli_pipeline step3-psf-and-pass2 --workdir data/tiles/<tile_id>
```

**Step 4 — Cross-match** (Gaia, PS1, USNO-B; default 5 arcsec radius)
```bash
python -m vasco.cli_pipeline step4-xmatch \
  --workdir data/tiles/<tile_id> \
  --xmatch-radius-arcsec 5 \
  --size-arcmin 60
```
veto uses best-per-candidate semantics

**Step 5 — Filter** (apply ≤5 arcsec match filter and MNRAS morphology criteria)
```bash
python -m vasco.cli_pipeline step5-filter-within5 --workdir data/tiles/<tile_id>
```

**Step 6 — Summarise** (export final CSVs and QA artefacts)
```bash
python -m vasco.cli_pipeline step6-summarize --workdir data/tiles/<tile_id>
```

Each tile directory ends up with `tile_status.json` and `MNRAS_SUMMARY.json` tracking step outcomes and candidate counts.

### Local cache pre-warming

Steps 4–5 query PS1, Gaia, and USNO-B. Pre-warm the local caches before processing a batch:

```bash
./scripts/prewarm_ps1_neighbourhood_bg.sh start
./scripts/prewarm_gaia_neighbourhood_bg_unified.sh start
./scripts/prewarm_usnob_neighbourhood_bg.sh start
```

Cache files can consume significant disk space as your dataset grows.

---

## Parallel running (example scripts)

The `tools/` directory contains example scripts for running steps 2–5 in parallel across many tiles. They are not required — use them as a starting point or adapt to your own workflow.

**Typical flow:**

```bash
# 1. List tiles that still need step2
bash tools/list_tiles_needing_steps.sh \
    --root ./data --mode step2 --out /tmp/tiles_step2.txt

# 2. Run steps 2+3 in parallel (N workers)
python tools/run_steps_2_3_parallel.py \
    --tiles-file /tmp/tiles_step2.txt --workers 4

# 3. Run steps 4+5 in parallel (auto-discovers tiles, skips completed)
python tools/run_steps_4_5_parallel.py --workers 4 --clean --only-missing
```

`start-2-3.sh` and `start-4-5.sh` wrap these as background `nohup` jobs with `nice`/`ionice` for low-priority execution. They assume **micromamba** with a `vasco-py311` environment and the repo at `~/code/vasco60` — edit them to match your Python environment and paths before use.

---

## Post-pipeline stages

Post-pipeline veto stages operate on the run-scoped survivor set and progressively shrink it. See [docs/POSTPROCESS.md](docs/POSTPROCESS.md) for full details and commands.

| Stage | Script | Veto source |
|---|---|---|
| S0 | `scripts/build_run_stage_csvs.py` | Build initial run folder |
| S0M ⚗️ | `scripts/stage_morph_post.py` | Morphology filter — PSF consistency ([docs](docs/STAGE_MORPH.md)) |
| S0S ⚗️ | `scripts/stage_shape_post.py` | Shape filter — ellipticity & elongation ([docs](docs/STAGE_SHAPE.md)) |
| GSC ⚗️ | `scripts/stage_gsc_post.py` | GSC 2.4.2 cross-match (VizieR I/353/gsc242) — experimental positional reduction stage |
| S1 | `scripts/run_skybot_stage_bg.sh` | SkyBoT (solar system objects) |
| S2 | `scripts/stage_supercosmos_post.py` | SuperCOSMOS (keep matches) |
| S3 | `scripts/stage_ptf_post.py` | PTF catalogue |
| S4 | `scripts/stage_vsx_post.py` | VSX variable stars |

⚗️ = experimental stage; opt-in only. Each stage outputs a carry-forward CSV, a flags CSV, and a ledger JSON.

---

## Key outputs

| Artefact | Location | Description |
|---|---|---|
| `tile_status.json` | `data/tiles/<tile_id>/` | Step completion and status per tile |
| `MNRAS_SUMMARY.json` | `data/tiles/<tile_id>/` | Filter counts and veto statistics |
| `tiles_registry.csv` | `data/metadata/` | All downloaded tiles with plate provenance |
| `tile_to_plate.csv` | `data/metadata/` | Tile → FITS REGION mapping |
| `stage_SN_*.csv` | `work/runs/<run>/stages/` | Per-stage survivor sets |

---

## Public releases

- [Release 6-Apr-2026](releases/release_2026_04_06/README.md)
- [MNRAS 2022 Audit report 10-Apr-2026](releases/mnras_2022_candidate_audit/mnras_audit_report.md)
- [Plate forensics library](docs/PLATE_FORENSICS_LIBRARY.md)

---
## Acknowledgements

Special thanks to [Beatriz Villarroel](https://orcid.org/0000-0002-4101-237X) and
[Alina Streblyanska](https://orcid.org/0000-0001-8876-9102), for guidance and support.

Special thanks to Ivo Busko for his [plateanalysis](https://github.com/cuernodegazpacho/plateanalysis)
software and the related [arXiv:2603.20407](https://arxiv.org/abs/2603.20407) publication,
and for his help with that approach.

Many thanks to Mick West (https://www.metabunk.org/members/mick-west.1/) for finding, fixing and reporting several bugs, and improving the pipeline. Fork: https://github.com/MickWest/vasco60 

---

## Changes from vasco (v1)

- Tile size fixed at 60×60 arcmin
- Deterministic tessellation plan replaces random tile selection
- Tiles are treated as circular regions of 30 arcmin radius after download
- Flat tile storage under `./data/tiles/`
- Automatic metadata maintenance (`tiles_registry.csv`, `tile_to_plate.csv`)
- Per-tile `MNRAS_SUMMARY.json` and `tile_status.json`
- No infrared-based veto
- Post-pipeline stages unified and simplified
