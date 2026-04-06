# Example run - stage artefacts (`./stages`)

This folder contains the *post-process stage outputs* for a single example run.
Each stage is represented by three files:

- `stage_<ID>_<NAME>.csv`	  **carry-forward survivors** (rows kept after the stage)
- `stage_<ID>_<NAME>_flags.csv`	  **audit table** for *all* input rows (match flags, best separation, IDs, etc.)
- `stage_<ID>_<NAME>_ledger.json` **stage ledger** (counts + parameters used)

The goal is to make the example run reproducible and auditable without requiring any external context.

## File naming (important)

Stage filenames use historical prefixes (e.g. `S4S_SHAPE`). These prefixes are **identifiers**, not a promise of the current logical ordering. They were created during iterative development and are kept stable to avoid renaming large numbers of run-folder artefacts.

When reading the run, focus on the **stage name** (`MORPH`, `SHAPE`, `GSC`, `SKYBOT`, `SCOS`, `PTF`, `VSX`) and the **ledger counts**, not the numeric prefix.

## Stages in this example

This example includes the following post-process stages:

- **MORPH**  - experimental morphology-based reduction stage
- **SHAPE**  - experimental shape-based reduction stage
- **GSC**    - experimental cross-match reduction stage against Guide Star Catalog (GSC) 2.4.2 (VizieR `I/353/gsc242`)
- **SKYBOT** - Solar System object identification / veto step
- **SCOS**   - SuperCOSMOS cross-match step
- **PTF**    - PTF cross-match veto step
- **VSX**    - variable star catalogue veto step

## What each file type contains

### 1) `stage_*.csv` (survivors)
A small CSV containing only the rows that remain after the stage. Typically includes:
- `src_id` (stable join key)
- `ra`, `dec` (coordinates used downstream)

### 2) `stage_*_flags.csv` (audit)
A row-by-row audit of the stage decision for every input row, typically including:
- `has_<catalog>_match` (0/1)
- `best_sep_arcsec` (best match separation)
- catalogue identifier (e.g. `gsc2_id`)
- `source_chunk` (which upstream stage file this row came from)

This is the file to use when you want to answer: *“Why did this row get removed?”*

### 3) `stage_*_ledger.json` (ledger)
A small JSON summary of:
- stage parameters (e.g., match radius, catalog/table ID, blocksize)
- detected input columns (`src_id`, `ra`, `dec`)
- counts (`input_rows`, `matched_rows`, `kept_rows`)
- output paths

Ledgers are meant to be stable provenance records.

## Notes on the GSC stage

The GSC stage is implemented as an **experimental post-process reduction stage** that cross-matches against GSC 2.4.2 (VizieR table `I/353/gsc242`) within a configurable radius (default 5 arcsec) and produces:
- survivors CSV (kept rows without a GSC match)
- full flags CSV
- ledger JSON

See the script documentation in `/scripts/stage_gsc_post.py` for details.

## Quick sanity check workflow 
1. Open `stage_<last>_VSX.csv` to see the final remainder for this run.
2. If you want to understand removals, open the corresponding `*_flags.csv` for the stage.
3. Use `*_ledger.json` to see exactly what parameters were used (radius, table IDs, etc.).
