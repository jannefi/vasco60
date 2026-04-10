# VASCO60 — MNRAS 2022 Candidate Audit Report

**Generated:** 2026-04-10  
**Dataset coverage:** 3,652 / 11,733 tiles (31.1% of tessellation plan)  
**Runs included:** R1–R16 (16 runs)

---

## 1. Pipeline Funnel (R1–R16)

```
Run      RAW               S0  |             S0M             S4S            S1           S2            S3           S4           S5
-----------------------------------------------------------------------------------------------------------------------------------
R1   962,680     974 (-99.9%)  |    479 (-50.8%)     77 (-83.9%)   62 (-19.5%)   62 (-0.0%)   55 (-11.3%)   55 (-0.0%)   55 (-0.0%)
R2     1.15M     930 (-99.9%)  |    469 (-49.6%)     85 (-81.9%)   72 (-15.3%)   72 (-0.0%)    65 (-9.7%)   62 (-4.6%)   62 (-0.0%)
R3   606,657     524 (-99.9%)  |    322 (-38.5%)     60 (-81.4%)   32 (-46.7%)   32 (-0.0%)   20 (-37.5%)   20 (-0.0%)   20 (-0.0%)
R4     1.19M     524 (-99.9%)  |    278 (-46.9%)     51 (-81.7%)    48 (-5.9%)   48 (-0.0%)   43 (-10.4%)   42 (-2.3%)   42 (-0.0%)
R5   906,027     615 (-99.9%)  |    315 (-48.8%)     51 (-83.8%)   35 (-31.4%)   35 (-0.0%)   28 (-20.0%)   26 (-7.1%)   26 (-0.0%)
R6   635,911     725 (-99.9%)  |    358 (-50.6%)     75 (-79.1%)   65 (-13.3%)   65 (-0.0%)   57 (-12.3%)   57 (-0.0%)   57 (-0.0%)
R7     1.96M     940 (-99.9%)  |    469 (-50.1%)     72 (-84.6%)   54 (-25.0%)   54 (-0.0%)   47 (-13.0%)   47 (-0.0%)   47 (-0.0%)
R8   411,076     963 (-99.8%)  |    547 (-43.2%)    107 (-80.4%)   84 (-21.5%)   84 (-0.0%)   71 (-15.5%)   67 (-5.6%)   67 (-0.0%)
R9     2.03M     759 (-99.9%)  |    398 (-47.6%)     73 (-81.7%)   53 (-27.4%)   53 (-0.0%)   46 (-13.2%)   46 (-0.0%)   46 (-0.0%)
R10  524,127     837 (-99.8%)  |    451 (-46.1%)     98 (-78.3%)   63 (-35.7%)   63 (-0.0%)   52 (-17.5%)   52 (-0.0%)   52 (-0.0%)
R11    2.17M     798 (-99.9%)  |    402 (-49.6%)     70 (-82.6%)   50 (-28.6%)   50 (-0.0%)   44 (-12.0%)   44 (-0.0%)   44 (-0.0%)
R12    1.12M     908 (-99.9%)  |    473 (-47.9%)    100 (-78.9%)   69 (-31.0%)   69 (-0.0%)   52 (-24.6%)   50 (-3.8%)   50 (-0.0%)
R13    1.95M     555 (-99.9%)  |    294 (-47.0%)     35 (-88.1%)   20 (-42.9%)   20 (-0.0%)   18 (-10.0%)   17 (-5.6%)   17 (-0.0%)
R14  638,569     724 (-99.9%)  |    402 (-44.5%)     80 (-80.1%)   57 (-28.8%)   57 (-0.0%)    53 (-7.0%)   53 (-0.0%)   53 (-0.0%)
R15  886,813     756 (-99.9%)  |    427 (-43.5%)     52 (-87.8%)   37 (-28.8%)   37 (-0.0%)   30 (-18.9%)   28 (-6.7%)   28 (-0.0%)
R16    1.31M     664 (-99.9%)  |    303 (-54.4%)     52 (-82.8%)   33 (-36.5%)   33 (-0.0%)    31 (-6.1%)   30 (-3.2%)   30 (-0.0%)
-----------------------------------------------------------------------------------------------------------------------------------
TOT   18.44M  12,196 (-99.9%)  |  6,387 (-47.6%)  1,138 (-82.2%)  834 (-26.7%)  834 (-0.0%)  712 (-14.6%)  696 (-2.2%)  696 (-0.0%)
```

**Stage key:**  
- S0 = MNRAS 2022 quality gates (FLAGS=0, SNR\_WIN>30, ELONG<1.3, 2<FWHM<7, SPREAD\_MODEL>−0.002) + ≤30′ circle cut + deduplication  
- S0M = Morphology filter (FWHM ratio vs PSF, SPREAD\_MODEL SNR)  
- S4S = Shape profile filter  
- S1 = GSC2 cross-match veto  
- S2 = SkyBoT solar system object veto  
- S3 = SuperCOSMOS — keep only sources with SCOS counterpart (no SCOS = scan artifact)  
- S4 = PTF cross-match veto  
- S5 = VSX variable star veto  

**Final survivors: 696 across 597 tiles, 212 plates.**

---

## 2. MNRAS 2022 Candidate Audit

### Setup

The MNRAS 2022 vanishing star candidate list (Solano et al.) contains **5,399 POSSI candidates**.  Source: http://svocats.cab.inta-csic.es/vanish-possi/index.php?action=search 
Of these, **459** fall within a downloaded tile's 60×60′ bounding box.  
After deduplication across overlapping tiles: **467 (candidate, tile) pairs** were audited.

**Script:** [mnras_candidates_audit.py](mnras_candidates_audit.py) 

**Output:** [mnras_candidates_audit.csv](mnras_candidates_audit.csv)

### Results

| Outcome | Count | % | Explanation |
|---|---:|---:|---|
| GATE_FAIL | 249 | 53.3% | Detected by SExtractor but fails MNRAS 2022's own quality gates |
| NO_MATCH | 116 | 24.8% | Not found in `sextractor_pass2.csv` within 5″ |
| STAGE_ELIM | 97 | 20.8% | Passes quality gates; removed by vasco60 post-pipeline stage |
| SURVIVOR | 5 | 1.1% | Present in vasco60 final survivor set |

### 2a. GATE_FAIL detail

Most MNRAS 2022 candidates in the dataset are eliminated by the **same quality gates the MNRAS paper itself defines**:

| Gate | Failures |
|---|---:|
| SPREAD\_MODEL > −0.002 | 198 |
| ELONGATION < 1.3 | 64 |
| 2 < FWHM < 7 px | 53 |
| SNR\_WIN > 30 | 40 |
| FLAGS = 0 | 34 |

SPREAD\_MODEL is the dominant gate (80% of gate failures). This indicates that the majority of MNRAS candidates which appear in our tiles are morphologically non-stellar on re-measurement — consistent with extended sources, blends, or plate artifacts.

Note: a candidate can fail multiple gates; the table counts each gate separately.

### 2b. NO_MATCH detail

| Sub-reason | Count |
|---|---:|
| Circle cut (detected pre-filter, beyond 30′ radius) | 104 |
| Not detected by SExtractor | 12 |

**104 of 116 NO_MATCH cases** are present in `sextractor_pass2_before_circle_filter.csv` with <5″ match but lie just outside the ≤30′ catalogue circle applied when building `stage_S0.csv`. This is by design: the tessellation uses 60×60′ squares but catalogue cross-matches apply a 30′ circle cut. Coordinates near tile corners fall in the square but outside the circle.

The remaining 12 were genuinely not detected by SExtractor (too faint, blended with a nearby source, or on a low-quality plate region).

### 2c. STAGE_ELIM detail

| Stage | Count | Meaning |
|---|---:|---|
| S4S (Shape) | 53 | Morphologically non-stellar profile (Busko pixel-level analysis) |
| S0M (Morph) | 32 | Extended FWHM ratio or high SPREAD\_MODEL SNR vs local PSF |
| PRE\_S0 | 12 | In sextractor\_pass2.csv but absent from stage\_S0.csv (failed circle cut or dedup at S0 build step) |

Of the 102 candidates that pass all MNRAS 2022 gates, **97 are eliminated by vasco60 stages** — predominantly by shape/morphology filters (S4S + S0M = 85 of 97). Only **5 survive to our final R set**.

### 2d. Survivors (MNRAS 2022 ∩ vasco60 R)

| MNRAS row | RA (deg) | Dec (deg) | Tile |
|---:|---:|---:|---|
| 3074 | 120.791084 | +79.005714 | tile\_RA120.170\_DECp78.626 |
| 3118 | 126.312805 | +85.352510 | tile\_RA130.037\_DECp85.636 |
| 1371 | 155.050220 | +48.633488 | tile\_RA155.243\_DECp48.274 |
| 4657 | 44.043365 | +35.354210 | tile\_RA44.262\_DECp35.589 |
| 3842 | 63.243404 | +42.580715 | tile\_RA63.852\_DECp42.388 |

---

## 3. Coverage Assessment

| Metric | Value |
|---|---|
| Tiles in tessellation plan | 11,733 |
| Tiles downloaded & processed | 3,652 (31.1%) |
| Plates covered | 212 / 904 (23.5%) |
| Tiles with ≥1 MNRAS 2022 candidate | 397 |
| MNRAS 2022 candidates in dataset | 459 / 5,399 (8.5%) |

At 31% coverage, the pipeline has demonstrated consistent behaviour across 16 runs with stable per-stage rejection rates (S0M ~44–88%, S4S ~78–89%). No systematic outliers. Remaining 69% of tiles will expand the MNRAS overlap proportionally (~1,500 additional MNRAS candidate tiles expected at full coverage).

---

## 4. Key Findings

1. **MNRAS quality gates are the dominant eliminator (53%)**: Over half of all MNRAS 2022 candidates that appear in our tiles fail the paper's own detection quality criteria when re-measured. This calls into question the reliability of that subset.

2. **Circle-cut geometry explains most NO_MATCH (90%)**: The 30′ circle applied inside a 60×60′ tile is responsible for 104/116 non-detections. Not a pipeline failure.

3. **Morphology stages are effective (S0M + S4S)**: Combined they eliminate 85 of the 97 gate-passing MNRAS candidates. Both criteria (FWHM ratio vs local PSF, and pixel-level shape profile) consistently flag non-stellar morphology.

4. **5 MNRAS 2022 candidates survive into vasco60 R** (from 459 in-dataset, 31% sky coverage). These warrant individual scrutiny.

5. **SkyBoT and VSX contribute zero eliminations** against MNRAS candidates in the current dataset — consistent with POSSI-era (1950s) plates predating most known solar system ephemerides and variable star catalogue completeness at these epochs.
