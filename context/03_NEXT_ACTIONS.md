# 03 — Next Actions
Active coordination point for VASCO60.
Tasks are derived from the Blocker Checklist in (runbook) [./context/10_VASCO60_RUNBOOK.md].

---

## Phase 3: Operational Hardening (Blocker C)

[ ] Optional - SkyBoT Resumability: Improve the SkyBoT stage to allow resuming from cached results without re-querying. This requires larger dataset.

[ ] Move parallel running examples to repo ./tools. They currently assume micromamba is in use. Number of parallel workers is hardcoded. At minimun, document the scripts as examples.

---

## Sanity & Explainability (not MNRAS-R parity)

Goal: ensure the pipeline is reproducible, auditable, and internally explainable.
We do NOT target parity with the published MNRAS “R remainder” list.

[ ] Deterministic subset run (tens-to-hundreds tiles)
    - Use 60×60 square download → ≤30′ circle cut policy when required.
    - Purpose: validate geometry + gating + veto ordering + ledgers (not external remainder parity).

[ ] Funnel explainability report (run-scoped)
    - Produce a small “what removed what” summary per stage (counts + reasons).

[ ] Gate sanity check on subset
    - Summarize SPREAD_MODEL distribution vs the hard baseline (> -0.002) and record any shifts.

---

## Log of Recent Completions

[x] Post-pipeline steps docs: move from online-only documentation into repo docs index

[x] Established vasco60 repo reset and HDD symlink structure.

[x] Locked the 60×60 arcmin tile geometry policy.

[x] Transitioned PS1 veto to run-scoped post-processing via STILTS.

[x] Drop support for sharded tile folder layouts in vasco60 (flat only for now)
    - Remove dual “flat + sharded” discovery/globbing where it adds complexity.
    - Standardize on: `./data/tiles/<tile-id>/...`

[x] Purge old tile-id format assumptions
    - Find scripts that still parse/expect the old tile naming format (this was seen during cache prewarmers work).
    - Update parsers/globs to the vasco60 tile naming contract.

[x] Enforce tile folder naming contract in Step1-download
    - Do not accept user-supplied tile folder names in download phase.
    - Step1 must compute tile folder name from (ra, dec) using the locked naming convention, so downstream scripts don’t break.

[x] Remove “CDS backend” legacy branching from vasco60
    - Vasco60 is “local-backend” oriented; older CDS backend code paths are already behind and increase maintenance burden.
    - Simplify code paths by removing or isolating vasco30-style `xmatch_backend == local/cds` branches that are no longer used in vasco60.

[x] Pixel scale parameter cleanup
    - If --pixel-scale-arcsec (or similar) is not used meaningfully, either remove it or hard-pin to a project constant (1.7″/px) and keep the CLI flag only as a no-op / compatibility shim.

[x] Default tile size policy: ensure “60×60 arcmin” is the default everywhere (no lingering 30′ defaults)
    - Vasco60 posture is 60×60 squares; ≤30′ circle cut is applied only when needed.  
    - Sweep code/config defaults that still assume 30 arcmin. (CLI may still accept overrides, but defaults must be 60.)

[x] State Implementation: Integrate tile_status.json updates into all 6 pipeline steps (step1–step6 + post stages as applicable).

[x] Improve first post-process step ./scripts/build_run_stage_csvs.py: remove plate edge veto if it's no longer needed due to the tessellation plan. Check and fix dedupe and PS1 inclusion/exlusion features. Implement support for delta-runs.

[x] Plate download option: allow user to download a full plate of their choice if that tile is included in the tile_plan.csv. Implement as a new command-line option in ./scripts/run_plan.py 

[x] Pre-warm check: ensure cache prewarmers (PS1/Gaia) collect enough data for xmatch purposes. See docs/PREWARM.md. Default radius must be sufficient for xmatch with 60x60 square tiles, after ≤30′ circle cut.

[x] Registry Automation: Step1 updates tile/plate registries automatically (no separate post-step scripts).

[x] Wiring Step 1: Ensure the tile_plan.csv fully drives Step1-download (no implicit/random coverage).

[x] Bug Fix: Resolve the issue where total_after_filters in MNRAS_SUMMARY.json is always 0.

[x] Env Tracking: Add ps1_veto_enabled and usnob_veto_enabled flags to the per-tile summary artifacts.

[x] PTF query check ./scripts/stage_ptf_post.py

[x] Root Documentation (CSV-first): Define and document the single consumer read root for run-scoped artifacts. README rewritten with full workflow, directory layout, all pipeline steps, post-pipeline stage table, and key outputs table.