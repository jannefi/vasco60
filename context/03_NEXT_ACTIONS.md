# 03 — Next Actions
Active coordination point for VASCO60.
Tasks are derived from the Blocker Checklist in (runbook) [./context/10_VASCO60_RUNBOOK.md].

---

## Experimental feature: morphology-based filtering

[x] New feature to scripts/stage_shape_inspect.py: optionally exclude later stage CSV reductions e.g. if row is not present in stage_S5_VSX.csv, exclude it from inspection output

[x] build_report.py: add correct observation date/time from plate headers to survivor.csv 

[x] build_report.py: fix possible rounding bug in percentages. Example:
S0  post-MNRAS+dedup      974 (-99.9%)  930 (-99.9%)  524 (-99.9%)  524 (-100.0%) 
Run R4 shows -100% at S0 which is incorrect. 

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

[x] Scale up: download more tiles and re-run to confirm rejection rate at larger N.
[X] pixel-level radial profile analysis (Busko 2026) as enhancement once
    the current approach is validated at full scale.
[X] Implement reporting tool across delta runs

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

Cross-check D1 vs R1/R2/R3 (src_id comparison against final stage CSVs):
- D1 ∩ R1: 2, D1 ∩ R2: 16, D1 ∩ R3: 8 → all 26 were already present in a prior run
- Zero truly new candidates from the re-fetch
- Confirms PS1 truncation introduced no false positives and no missed discoveries
- R3 (+ predecessors) is the complete, correct survivor set; D1 is a validation artifact

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

[X] Funnel explainability report (run-scoped)
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

[x] No action — SExtractor config items 11–13 (2026-05-12)
    - All three items challenge SExtractor configuration choices for the
      pass1/pass2 catalogs (configs/sex_pass1.sex, configs/sex_pass2.sex).
      Operator has empirically tested these and the reporter is incorrect or
      practically moot. Recording the rationale so future readers don't
      re-open the same triage.

    [x] 11 — DETECT_TYPE CCD vs PHOTO
        Reporter argues POSS-I is photographic emulsion and the configs
        should use DETECT_TYPE PHOTO. Empirically wrong for STScI scans:
        DETECT_TYPE PHOTO assumes direct photographic intensity with
        logarithmic scaling — by the time SExtractor sees these files,
        they are already 16-bit digital scans. PHOTO mode does not work
        on this input (operator verified); CCD is the correct choice.

    [x] 12 — hardcoded CCD detector values
        GAIN=1.0, SATUR_LEVEL=50000, SEEING_FWHM=1.2, MAG_ZEROPOINT=21.1
        are CCD-style defaults. Operator has tested various values; no
        practical impact on end results. SATUR_LEVEL≈50000 in 16-bit
        space tracks the practical bright-star saturation point; GAIN
        affects formal flux uncertainty but not detection FLAGS/positions;
        SEEING_FWHM seeds the convolution kernel; MAG_ZEROPOINT only
        shifts MAG_AUTO without affecting gates.

    [x] 13 — FILTER N (pass1) vs FILTER Y (pass2) mismatch
        Reporter framing ("PSF built from unfiltered detections then applied
        to filtered measurements") misunderstands SExtractor FILTER
        semantics. FILTER controls convolution of the *detection* image for
        source identification only — it does not propagate into the
        measurement image. PSF model (PSFEx-built from pass1 candidates) and
        SPREAD_MODEL measurement (pass2) both operate on the same raw pixel
        data; the filter only changes which sources get detected at each
        pass. The pass1-N / pass2-Y pattern is standard SExtractor+PSFEx
        practice: sharp detection of PSF candidates first, smoother
        detection statistics for the measurement pass. Operator-confirmed
        no practical impact across tested filter configurations.

    Decision: without the reference MNRAS 2022 configurations from
    E. Solano et al. (which the operator has attempted to obtain), further
    tuning is
    speculative. Operator-led config sensitivity testing has bounded the
    practical impact, and the gates the audit relies on are robust to these
    choices. No further config-tuning work without an upstream reference.

[ ] Morphology methodology triage — reporter items 18–21 (2026-05-12)
    - All four items concern stage_morph_post.py methodology. The stage is
      documented EXPERIMENTAL / "Not an official veto stage"; the calibration
      doc (docs/STAGE_MORPH.md) and rejections have been visually validated
      on a sample. Dispositions reflect that framing.

    [x] 18 (no action) — "circular reference definition"
        Reporter claims PSF reference is selected by ELONGATION<1.3, then
        candidates are evaluated against that round subset, making the test
        circular. Misframed: the ELONG cut at stage_morph_post.py:249 selects
        the *reference population* (standard PSF-selection practice, exclude
        blends/galaxies); candidates are gated at lines 370/373 on FWHM_IMAGE
        and SPREAD_MODEL — *not* on ELONGATION. A round-but-extended real
        source isn't rejected for being elongated; it's rejected (correctly)
        for being extended. Standard PSF-photometry methodology.

    [ ] 19 (doc clarification) — S0M and S0S use different reference pops
        Factually correct: S0M uses Gaia-matched mag-window references;
        S0S uses ±10% FLUX_MAX neighbourhood references. They measure
        different things (S0M = global PSF size/concentration; S0S = local
        radial profile shape) and are complementary independent tests, not
        cross-checks. Worth a one-paragraph note in either doc explaining
        that S0M and S0S verdicts shouldn't be naively combined as
        replicates. Low priority.

    [ ] 20 (methodology polish) — spread_snr denominator
        stage_morph_post.py:365 uses per-source SPREADERR_MODEL as the
        denominator of spread_snr. Reporter is correct that this is the
        single-source 1σ measurement uncertainty, not the population scatter
        of the PSF reference. Calling the resulting ratio "snr" and gating
        at >5 suggests a 5σ population test, which it isn't — it's
        ">5× the candidate's own measurement uncertainty above the reference
        median." Aggressive gate. Practical mitigation: visual inspection of
        rejects (already logged) suggests it behaves as a plate-artifact
        filter, not a precise statistical test. Recorded options:
          (a) rename metric to avoid "snr" connotation (e.g. `spread_dev`)
          (b) change denominator to sqrt(SPREADERR² + pop_std²) or pop MAD
          (c) document the choice explicitly in STAGE_MORPH.md
        Will need a quantitative comparison run before changing the gate
        behaviour. Worth doing when the experimental gate is promoted.

    [ ] 21 (doc note + check) — Gaia G vs POSS-I E saturation
        Reporter's concern about color-term and saturation has partial
        mitigation already: stage_morph_post.py:245 excludes any source with
        SExtractor FLAGS != "0", which removes SEx-flagged saturated stars
        from the PSF reference. The Gaia 12<G<18 window selects in Gaia
        bandpass; POSS-I E mapping shifts by ~0.5–1.5 mag depending on
        spectral type. Marginally saturated cores that SExtractor doesn't
        flag could still leak in. Polish:
          (a) note in STAGE_MORPH.md that the window is Gaia-band and that
              saturation is handled via SExtractor FLAGS filter
          (b) optionally add an explicit MAG_AUTO bright-limit guard for
              residual marginal saturation
        Low priority unless the FLAGS filter is found to miss saturated
        cores in audit.

[ ] Shape/morph stage triage — reporter items 22–27 (2026-05-12)
    - Both `scripts/stage_shape_post.py` and `scripts/stage_morph_post.py` are
      documented EXPERIMENTAL / "Not an official veto stage" and several
      choices are explicit "reference implementation parity" with Busko (2026)
      / cuernodegazpacho/plateanalysis. Disposition reflects that framing.

    [ ] 22 (polish) — no min-stars guard on profile_diff
        `stage_shape_post.py:574` records `stars_used` in the flags output but
        doesn't act on it. With N=1 the "averaged-star" profile is a 1-vs-1
        comparison. Add `--min-profile-diff-stars N` that forces
        shape_confidence='low' (or sets profile_diff=NaN) when N < min.
        Low priority; stars_used is already audit-visible.

    [x] 23 (no action) — per-cutout MIN/MAX normalization "uncalibrated"
        Per-cutout normalization at stage_shape_post.py:352 is the Busko
        reference's deliberate choice: shape, not photometry. Circularity
        0.80 floor is a unitless shape metric (4πA/P², max 1.0) — no
        photometric reference to calibrate against. Reporter category error.

    [ ] 24 (polish) — last-valid-contour wins is OpenCV-order-dependent
        stage_shape_post.py:370 comment acknowledges "reference implementation
        parity: last valid overwrites". Cross-version stability risk only.
        Could sort contours deterministically (e.g. by area desc) without
        meaningful semantic change. Low priority; matters only on OpenCV
        major-version bumps.

    [x] 25 (no action) — `invert-max=65535` allegedly hardcoded
        It's a CLI flag default (`--invert-max`, stage_shape_post.py:672), not
        a hardcoded constant. 65535 is correct for canonical 16-bit unsigned
        STScI DSS/POSS-I scans. Auto-detection from FITS BITPIX/BSCALE could
        be added but isn't required for the POSS-I canonical product.

    [ ] 26 (polish) — Cutout2D mode="trim" biases edge candidates
        stage_shape_post.py:545. Practical impact limited because the ≤30′
        circle cut from tile tessellation usually removes edge candidates
        upstream. Could switch to mode="partial" (NaN-pad + handle in the
        profile aggregator) or mode="strict" + skip-with-reason. Low priority.

    [x] 27 (no action on code; consider doc refresh) — calibration sample size
        `docs/STAGE_MORPH.md` calibrates fwhm_ratio/spread_snr thresholds on
        684 candidates from 181 tiles; reporter notes the 25k-pair audit
        population is ~37× larger. Methodologically fair critique for a
        published gate, but stage_morph_post.py is documented EXPERIMENTAL
        and thresholds are operator-tunable CLI flags. The 181-tile scale-up
        validation is already in the log above. Could add a clarifying note
        in STAGE_MORPH.md that the calibration numbers are starting points,
        not certified for hard rejection.

[x] No action — _enforce_possi_e_or_skip alleged to silently delete legitimate POSS-I plates (2026-05-12)
    - Reporter item 32: `vasco/cli_pipeline.py:88-101` deletes FITS whose SURVEY
      header is not exactly "POSSI-E"; claim was that STScI metadata variance
      destroys legitimate POSS-I E plates and produces silent coverage gaps.
    - Investigation: claim is incorrect on multiple counts.
      * Not silent: pipeline gate logs `[STEP1][FILTER] Non-POSS plate;
        SURVEY=... — file will be discarded` and raises RuntimeError; the
        upstream downloader gate (vasco/downloader.py:209-221) also writes a
        REJECT_NON_POSS error artifact via _write_error_artifacts before
        deletion, so downstream coverage audits see explicit reject records,
        not phantom gaps.
      * Asymmetric-but-intentional: the downloader uses a tolerant substring
        match ('POSS' in survey_name) to absorb metadata variance; the
        pipeline gate is the strict belt-and-suspenders second pass.
    - Operator confirmation: STScI dss1-red endpoint (with the undocumented
      parameters this downloader uses) consistently returns canonical
      `SURVEY = 'POSSI-E'`. Full POSS-I red set downloaded with this pipeline
      verified clean. Strict gate has never fired on legitimate data.
    - No action.

[x] Cleanup — removed broken-but-unused vasco/mnras/xmatch.py (2026-05-12)
    - Reporter item 31: `vasco/mnras/xmatch.py:44` picked "nearest" Gaia source
      via Manhattan distance `|Δra| + |Δdec|` with no cos(dec) correction —
      not even a valid angular metric, fails worst at high dec and near the
      0°/360° wrap.
    - Confirmed not on the production path: zero importers across the repo;
      all real xmatch flows through `vasco.mnras.xmatch_stilts`. Package
      `__init__.py` exports `__all__ = []`, so nothing was committed public
      API — but the module path looked like one, which was the reporter's
      concern.
    - Action taken: deleted the file. Verified `vasco.mnras` package and
      `vasco.mnras.xmatch_stilts` still import cleanly.

[x] Cleanup — removed dead within5arcsec validators (2026-05-12)
    - Reporter flagged `vasco/cli_pipeline.py:168` heuristic
      `d_arcsec = d if d > 0.1 else d * 3600.0` as silently rejecting
      sub-arcsecond catalog matches (mistaken as degrees, multiplied by 3600,
      then failing the 5″ cutoff). Their conclusion: "biases NO_MATCH counts
      upward."
    - The heuristic is genuinely faulty in principle. BUT the bug-impact claim
      is incorrect — the function `_validate_within5_arcsec_unit_tolerant`
      (and its sibling `_validate_within_5_arcsec` in vasco/pipeline.py) had
      zero call sites in the repo. The Step 5 handler that historically used
      them (`cmd_step5_filter_within5`) is already a no-op cleanup that just
      deletes stale `_within5arcsec.csv` files; its own comment confirms
      "nothing downstream reads within5arcsec files." No NO_MATCH counter is
      fed by this code path.
    - The sibling validator in vasco/pipeline.py had a *symmetric but opposite*
      bug (reporter item 29): "try angDist<=5 first; only fall back to degrees
      if zero rows" — meaning if angDist were in degrees, the arcsec test would
      match everything within 5° and the degree fallback would never trigger.
      Two divergent validators failing in opposite directions; both dead.
    - Reporter item 30 (dead-write at vasco/pipeline.py:120-121, where
      `out` was assigned in an if/else then unconditionally overwritten on
      the following line) was also inside the same removed function. All
      three findings (items 28/29/30) flushed by this single cleanup.
    - Action taken: deleted both dead functions (~140 lines across two files).
      The buggy heuristics are gone by virtue of removing the dead code that
      contained them, not because the claimed impact was real. Step 5 no-op
      smoke-tested on prod.

[x] No action — WCSFIX bootstrap 5″ radius alleged to miss HPM stars (2026-05-12)
    - Reporter flagged `vasco/wcsfix_early.py:19` (bootstrap_radius_arcsec=5.0)
      as silently excluding HPM stars from the tie-point set, biasing the
      polynomial WCS correction.
    - Investigation: claim doesn't hold. `_post_xmatch_tile` picks gaia_csv via
      `_prefer_plate` at cli_pipeline.py:1306 BEFORE invoking WCSFIX
      (cli_pipeline.py:1336-1339). The bootstrap matches POSS-epoch SExtractor
      positions against plate-epoch propagated Gaia, so HPM stars land within
      sub-arcsec of their POSS detection and are captured by the 5″ bootstrap.
    - Secondary concern (polynomial fit "applied uniformly") is misconceived:
      a polynomial WCS correction is *meant* to be uniform; it models plate-
      scale distortion, not per-source motion. A few HPM rejections among
      thousands of tie points have no effect on the polynomial coefficients.
    - Edge case: combined failure of Bug #6 propagation + 5″ bootstrap + 15″
      fallback bootstrap (cli_pipeline.py:1330). Pathological; wcsfix_status.json
      makes it auditable when it occurs.
    - No action.

[ ] Polish: `_filter_hpm_gaia` hardcodes target_epoch=1950.0
    - `vasco/cli_pipeline.py:672` passes target_epoch=1950.0 to backprop_gaia_row,
      while `_propagate_catalog_epoch` (cli_pipeline.py:1710) correctly reads
      per-plate DATE-OBS from the FITS header via `_plate_epoch_year_from_fits`.
    - POSS-I plates span 1949–1958; the observed test tile is 1955.81 → 5.81 yr
      offset. For a 500 mas/yr genuine HPM star this is a ~2.9″ position error
      in the back-projection (large enough to flip the 5″ flag near threshold);
      sub-arcsec for typical PMs.
    - Audit-only impact (hpm_objects counter in MNRAS_SUMMARY.json and contents
      of sex_gaia_hpm_flagged.csv). Candidate funnel unaffected — the
      load-bearing HPM defense is `_propagate_catalog_epoch` which already uses
      the per-plate epoch.
    - Low priority; fix opportunistically next time `_post_xmatch_tile` is
      edited. Thread plate_epoch into `_filter_hpm_gaia` and on to
      `backprop_gaia_row(row, target_epoch=plate_epoch)`.

[x] Bug: S0M morphology stage bypassed Bug #6 epoch propagation (2026-05-12)
    - `scripts/stage_morph_post.py:204` read `catalogs/gaia_neighbourhood.csv`
      (raw J2016) instead of preferring `gaia_neighbourhood_at_plate.csv`,
      which Step 4 produces and `vasco/cli_pipeline.py:1306` correctly prefers.
    - 3″ PSF-reference match against un-propagated Gaia silently excluded HPM
      stars whose POSS-epoch positions had drifted from J2016. Second-order
      effect (slightly biased PSF reference sample composition), not a funnel
      defect, but inconsistent with the Bug #6 architecture established in
      commit c9f7f05 (fix/pm-leakage, 2026-04-11).
    - Fix: prefer `_at_plate.csv` with conservative fall-through to raw catalog,
      mirroring `_prefer_plate` semantics in cli_pipeline.py.
    - Verified on prod: behaviour identical when `_at_plate.csv` absent/empty
      (existing tiles unchanged); 4099-star PSF reference reproduced exactly
      when fed a copy of the raw catalog via the new path.

[x] Bug: HPM filter was a no-op (2026-05-12)
    - `vasco/mnras/hpm.py` looked up `pmra`/`pmdec` (lowercase), but the Gaia
      neighbourhood CSV (both VizieR fetcher and local cache) writes `pmRA`/`pmDE`.
      Case-sensitive dict.get() returned the 0.0 default every time, so
      apply_space_motion was always invoked with zero proper motion.
    - Effect: `_filter_hpm_gaia` was structurally a "static Gaia↔POSS sep > 5″"
      filter (i.e. loose xmatch radius), not an HPM filter. Real high-PM stars
      whose static sep happened to be <5″ slipped through; rows with tiny PMs
      whose static sep happened to be >5″ were mis-flagged as HPM.
    - Verified on prod (tile_RA85.667_DECp29.061): all 3 rows in
      sex_gaia_hpm_flagged.csv had recorded hpm_sep_arcsec equal to their
      static catalog separation (rows with pmRA≈1–2 mas/yr flagged at 7–9″,
      impossible under real back-propagation).
    - Fix: lookup tries pmRA/pmDE first, falls back to pmra/pmdec, and tolerates
      empty-string (CSV NaN encoding).
    - [ ] Regenerate sex_gaia_hpm_flagged.csv and re-compute hpm_objects counts
          on any tiles where the existing flagged file is used for downstream
          accounting (current contents are stale under the new semantics).

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

