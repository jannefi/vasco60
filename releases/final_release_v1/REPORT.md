# VASCO60 – Final Release v1 (Data-first)

## 1. What this release contains
This release is a data-first snapshot of a completed VASCO60 multi-run campaign.
It includes:
- Final candidate list (deduplicated): `report/survivors.csv`
- Stage outputs and intermediate shrinking sets: `run/` and `run/stages/`
- Funnel / stage counts for transparency and reproducibility

## 2. Pipeline goal and scope
VASCO60 aims to reproduce the *intent* of the published POSS-I “vanishing source” workflow:
a reproducible, plate-aware pipeline that reduces POSS-I detections against modern catalogues
(Gaia, PS1, USNO-B) and then applies post-pipeline veto stages (e.g. SkyBoT, SuperCOSMOS, PTF, VSX).
This repository does not claim to reproduce the exact dataset reported in MNRAS 515(1):1380 (2022).

## 3. Reproducibility notes
- The stable join key is `src_id = tile_id:object_id`.
- Each post-pipeline stage outputs:
  - `stage_SX_*.csv` (carry-forward survivors)
  - `stage_SX_*_flags.csv` (audit flags)
  - `stage_SX_*_ledger.json` (parameters + counts)

## 4. Summary counts (funnel)
See `report/funnel.json` (machine-readable) and/or `report/funnel.txt` (human readable).

## 5. Interpretation boundaries
- No visual vetting is included in v1.
- Remaining candidates may be plate defects/artefacts (e.g., ghost images, halation, internal reflections),
or real astrophysical transients.
- Stages marked “experimental” are opt-in reduction steps and are documented as such.

## 6. Discussion: survivor-count discrepancies vs prior literature
With the criteria implemented here, the survivor counts are substantially below six figures.
This suggests differences in coverage, definitions, selection criteria, or unpublished inputs.
If additional reference lists become available publicly, direct cross-checks can quantify the gap.

## 7. Next steps
- TBD
