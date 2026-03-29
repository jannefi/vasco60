# 03 — Next Actions
Active coordination point for VASCO60.
Tasks are derived from the Blocker Checklist in (runbook) [./context/10_VASCO60_RUNBOOK.md].

---

## Experimental feature: morphology-based filtering

[x] scripts/stage_morph_post.py — implemented and tested (2026-03-27)
    docs/STAGE_MORPH.md — feature documentation

Approach: per-tile PSF model from Gaia-matched SExtractor sources (sextractor_pass2.csv).
Two rejection criteria (OR): fwhm_ratio > 1.5 OR spread_snr > 5.0.
CLASS_STAR dropped — unreliable on photographic plates (PSF stars score ~0.015).
Stage label: S0M (runs before SKYBOT to shrink R early).

Calibration on 181-tile run (713 candidates):
- fwhm_ratio only:        17.8%
- spread_snr only:        49.1% (dominant; SPREAD_MODEL is PSFEx-derived)
- Combined default:       50.5%  → 713 → 353 kept
- All 181 tiles had sufficient PSF sample (0 psf_insufficient)

Usage:
    python scripts/stage_morph_post.py \
        --run-dir ./work/runs/run-S1-... \
        --input-glob 'stage_S0.csv' \
        --stage S0M \
        --tiles-root ./data/tiles

[x] Validate: ~20 rejected candidates sampled manually across low/mid/high RA tiles.
    All appeared to be plate artifacts. Result: looks promising.
    Note: 50.5% measured post-MNRAS-gates; true reduction (pre-gates) likely 60-80%.

[ ] Scale up: download more tiles and re-run to confirm rejection rate at larger N.
[ ] Future: pixel-level radial profile analysis (Busko 2026) as enhancement once
    the current approach is validated at full scale.

---


## Catalog cache truncation — RESOLVED (2026-03-29)

Observed in R3 run (2026-03-29): tiles in dense stellar fields (galactic plane,
high-dec crowded regions) hit hard row limits in neighbourhood cache fetches:
- Gaia: 200K cap — 4 tiles affected, all 0 survivors (USNOB backstop adequate)
- PS1:  50K cap — 73 tiles affected, 70 had PS1 actively eliminating candidates

**Fix applied**: PS1 cap raised 50K → 200K in external_fetch_online.py (VizieR honours
-out.max above the server default of 50K, confirmed by Gaia returning 200K rows).
All 73 truncated tiles re-fetched and re-run through step4-5.

**Delta validation run D1-20260329_115645** (2026-03-29):
- 73 tiles processed, 674 delta-skipped
- S0 input: 125 rows from the 73 re-fetched tiles
- Final survivors: 26 across 6 plates (XE028, XE029, XE030, XE111, XE603, XE695)
- S0M rejection: 58.4% (northern tiles, 3K–5K PSF stars — healthy)
- All survivors are in the PS1-covered sky (dec > -30°), consistent with re-fetch region

Prior S6 live-CDS test (matched=0 across 793 survivors) confirmed truncation had not
caused missed vetos in the earlier runs. D1 confirms the fix is applied and pipeline
runs cleanly with the higher cap going forward.

[ ] Add truncation flag to MNRAS_SUMMARY: detect when len(gaia_neighbourhood.csv) == max_rows
    or len(ps1_neighbourhood.csv) == max_records and write `gaia_cache_truncated: true` /
    `ps1_cache_truncated: true`. Allows future cross-run reports to flag affected tiles
    (lower priority now that cap is raised).

---

## Phase 3: Operational Hardening (Blocker C)

[ ] Optional - SkyBoT Resumability: Improve the SkyBoT stage to allow resuming from cached results without re-querying. This requires larger dataset.


---

## Sanity & Explainability (not MNRAS-R parity)

Goal: ensure the pipeline is reproducible, auditable, and internally explainable.
We do NOT target parity with the published MNRAS “R remainder” list.

[ ] Funnel explainability report (run-scoped)
    - Produce a small “what removed what” summary per stage (counts + reasons).



---

## Log of Recent Completions

[x] Post-pipeline steps docs: move from online-only documentation into repo docs index

[x] Gate sanity check on subset
    - Summarize SPREAD_MODEL distribution vs the hard baseline (> -0.002) and record any shifts.

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

[x] Move parallel running examples to repo ./tools. Documented in README; start-*.sh note micromamba/path assumptions.

[x] Root Documentation (CSV-first): Define and document the single consumer read root for run-scoped artifacts. README rewritten with full workflow, directory layout, all pipeline steps, post-pipeline stage table, and key outputs table.

[x] Deterministic subset run (tens-to-hundreds tiles)
    - Use 60×60 square download → ≤30′ circle cut policy when required.
    - Purpose: validate geometry + gating + veto ordering + ledgers (not external remainder parity).

[x] Bug: Gaia veto uses tskymatch2 find=best (two-way one-to-one) instead of find=best1
    - With find=best, if two SExtractor sources are within 5" of the same Gaia star, only
      the closer one gets vetoed. The farther one (confirmed: source 3747, sep=4.843",
      tile_RA74.712_DECp84.144) slips through the Gaia veto unmatched even though the
      Gaia star is in the local cache. Fix: pass find=best1 to stilts_xmatch in _veto()
      so each SExtractor source independently finds its best Gaia match.
    - Also add a small margin (~3 arcmin) to the Gaia neighbourhood fetch radius
      (currently exact circumscribed circle) to prevent edge leakage (source 2319,
      X=2031 near tile edge, confirmed missed due to zero margin).
    - Both bugs allow real stars to leak through Gaia veto; at scale (~11K tiles)
      this could be significant. Usually caught by PS1/USNO but not guaranteed.

