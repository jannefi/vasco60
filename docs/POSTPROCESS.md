## Post-pipeline stages (run-scoped shrinking set)

**Goal:** Produce a run-scoped folder containing the current shrinking survivor set (S1 → S2 → S3 …), plus provenance and artifacts for each stage.  
**Canonical run folder:** `./work/runs/run-S1-<date>/`

### Overview (what this produces)
- A **master audit CSV** for the run
- A **science-grade dedup CSV**
- A **current survivor set** (“edge-core”) that shrinks forward at each stage
- Per-stage artifacts:
  - `stage_SX_*.csv` (carry-forward survivors)
  - `stage_SX_*_flags.csv` (per-row flags for auditing)
  - `stage_SX_*_ledger.json` (counts + parameters + stats)

> **Invariant:** Every stage outputs a smaller “current survivors” CSV used as the input for the next stage. 

---

### 1) Build run-scoped stage CSVs (S1)
**Script:** `scripts/build_run_stage_csvs.py`  
**Purpose:** Create the initial run folder with master CSVs, the current survivor set and upload chunks.

**Run (typical):**
```sh
RUN=./work/runs/run-S1-$(date +%Y%m%d_%H%M%S)
python scripts/build_run_stage_csvs.py --run-tag "$(basename "$RUN")"
```

**Key outputs (under $RUN/):**

- source_extractor_final_filtered.csv (master, audit)
- source_extractor_final_filtered__dedup.csv (science-grade dedup)
- stage_S1.csv (minimal stage view)
- upload_positional.csv and upload_positional_chunk_*.csv (S1 upload view for next fetcher)
- tile_manifest.csv, RUN_SUMMARY.txt, allow/exclude list copies


### 2) SkyBoT stage (run once, keep artifacts, shrink forward)

SkyBoT is typically a small cutter. Run it once, keep artifacts, then shrink forward without requerying.

Background run (preferred):
```sh
RUN=./work/runs/run-S1... \
STAGE=S1 \
INPUT='stage_S0.csv' \
bash scripts/run_skybot_stage_bg.sh start
```

Foreground run
```sh
RUN=./work/runs/run-S1... \
STAGE=S1 \
INPUT='stage_S0.csv' \
bash scripts/run_skybot_stage_bg.sh start
```

**Expected outputs (under $RUN/stages/):**

- stage_S1_SKYBOT.csv (carry forward)
- stage_S1_SKYBOT_flags.csv
- stage_S1_SKYBOT_ledger.json


### 3) SuperCOSMOS stage (shrink to S2)

Script: `scripts/stage_supercosmos_post.py`

Run
```sh
python scripts/stage_supercosmos_post.py \
  --run-dir "$RUN" \
  --input-glob 'stages/stage_S1_SKYBOT.csv' \
  --stage S2 \
  --radius-arcsec 5 \
  --chunk-size 5000 \
  --mode keep_matches
```

**Expected outputs (under $RUN/stages/):**

- stage_S2_SCOS.csv (carry forward)
- stage_S2_SCOS_flags.csv
- stage_S2_SCOS_ledger.json

### 4) PTF stage (shrink to S3)

Script: `scripts/stage_ptf_post.py`
Run:
```sh
python scripts/stage_ptf_post.py \
  --run-dir "$RUN" \
  --input-glob 'stages/stage_S2_SCOS.csv' \
  --stage S3 \
  --radius-arcsec 5 \
  --ptf-table ptf_objects
```

**Expected outputs (under $RUN/stages/):**

- stage_S3_PTF.csv
- stage_S3_PTF_flags.csv
- stage_S3_PTF_ledger.json

### 5) VSX stage (local mirror; shrink forward)

Script: `scripts/stage_vsx_post.py`
Run:
```sh
python scripts/stage_vsx_post.py \
  --run-dir "$RUN" \
  --input-glob 'stages/stage_S3_PTF.csv' \
  --stage S4 \
  --radius-arcsec 5
```

**Expected outputs (under $RUN/stages/):**

- stage_S4_VSX.csv
- stage_S3_VSX_flags.csv
- stage_S3_VSX_ledger.json

### Consolidated reporting (all runs)

After delta runs exist, maintain an all-up report that spans all run folders (initial big run + later deltas).The consolidated report should also materialize a single current-survivors view across the union of runs so downstream fetchers always consume one canonical shrinking set.

**Recommended inputs:**

- $RUN/STAGE_LEDGER.csv (rows_in / rows_flagged / rows_out per stage)
- Per-run manifest: run tag, tile selection/range, created timestamp, schema/version

**Recommended outputs:**

- All-up counts per stage across runs
- Current survivors (union view) exported as CSV + upload_positional chunks for the next fetcher




