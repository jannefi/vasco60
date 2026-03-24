# 02 — Decisions (Locked)

This document contains the "Locked" technical decisions for VASCO60. Code must implement these decisions and never silently deviate from them.

---
### PS1/Gaia/USNO veto (2026-03-24)
* **Veto accuracy**: Gaia/PS1/USNO elimination uses tskymatch2 find=best1 (best match per candidate row)

### Pipeline Orchestration & State (2026-03-05)
* **Repo Mainline**: VASCO60 is the current production branch; legacy repo is isolated.
* **Data Root Contract**: `./data` is a symlink to HDD and is gitignored.
* **Deterministic Coverage**: Execution is driven by a tile plan CSV; no random downloads.
* **Naming Convention**: Use `tile_RA<ra>_DECp/m<dec>` (avoid +/- characters).
* **State Tracking**: Each tile must contain a `tile_status.json` to enable delta-runs and audit visibility.

### Veto & Stage Logic (2026-03-04)
* **PS1 Veto Correctness**: Retired tile-local PS1 caches (catalogs/ps1_neighbourhood.csv) due to truncation leaks. (fixed)
* **Canonical PS1 Veto**: Moved to a run-scoped postprocess stage using STILTS `cdsskymatch` (find=best, ≤5″). 
* **Stage Ordering (Cost)**: Run high-impact, cheap cutters (like SuperCOSMOS) before SkyBoT to reduce workload.
* **SCOS Semantics**: Keep candidates with a SuperCOSMOS counterpart within 5″; treat DSS-only (no SCOS) as scan artifacts (per Watters et al. 2026).
* **USNO Status**: `stage_usno_post.py` remains an optional/experimental artifact.

### Science Baselines (Parity Requirements)
* **Geometry**: Default downloads are 60×60 arcmin squares; apply ≤30′ catalogue-level cuts for parity checks.
* **Spike Check (Safety)**: Use **90 arcsec** (90″) for USNO-B1.0. Ignore the 90' typo in the MNRAS 2022 text.
* **SkyBoT Radius**: Fixed at 60′. Tuning must focus on call-efficiency, not radius reduction.
* **Gate Thresholds**: 
    * FLAGS = 0
    * SNR_WIN > 30
    * ELONGATION < 1.3
    * SPREAD_MODEL > -0.002 (absolute threshold)
* **Deduplication**: Use 0.25″ tolerance per plate under WCS-fixed coordinates.

---

### Ledger & Audit Standards
* Every stage must produce a `.json` ledger recording `in_rows`, `out_rows`, and failure reasons.
* The "MNRAS_SUMMARY.json" must be fixed to correctly report `total_after_filters`.