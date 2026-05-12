# Observations on the Third-Party POSS-I Candidate List: Spatial Coverage and Plate-Edge Extent

**Reference dataset:** Third-party POSS-I candidate list, 107,875 sources; Solano et al. (2022), MNRAS 515(1):1380  
**Date:** 2026-04-30

---

## Main Finding

> **The third-party POSS-I candidate list does not appear to have been derived from a dataset restricted to the astrometrically reliable central region of each plate. Candidates are distributed across the full plate area, including the plate-edge zone where geometric distortions and vignetting are strongest. No evidence of a circular inclusion boundary was detected in the spatial distribution.**

This finding is supported by spatial analysis of the candidate distribution relative to plate centres, and is independently corroborated by the spatial feature importance reported in a subsequent machine-learning analysis of the same dataset (Bruehl et al. 2026, arXiv:2604.18799).

---

## 1. Stated Method vs. Observed Spatial Coverage

### 1.1 What the paper describes

Solano et al. (2022) describe a tessellation of the sky using overlapping circular search regions (radius 30 arcmin). This description could be interpreted as implying that candidates are restricted to within 30 arcmin of each search region centre, or alternatively that the circles are purely a computational tiling device and impose no spatial filter.

### 1.2 What the spatial distribution shows

Angular separation analysis of candidates in a representative POSS-I plate was performed by projecting candidates against the known plate centre coordinates.

Key results:
- Candidates are present at separations **well beyond 30 arcmin** from the plate centre, extending to approximately **238 arcmin (3.97°)** — placing a substantial fraction at or beyond the plate corners.
- The **plate-edge reliable zone** of a POSS-I plate is defined by a ~5.4° diameter (162 arcmin radius) centred on the plate. Approximately **39% of candidates in the analysed plate fall outside this reliable zone**, in the astrometrically degraded outer ring.
- The angular distribution is **smooth and monotonically increasing** with separation from the plate centre (as expected for a uniform surface distribution over an extended area), with no step, gap, or depletion at 30 arcmin that would indicate a circular cut was applied.

If a 30 arcmin circular inclusion boundary had been applied at any stage of the candidate generation, the distribution would show a sharp depletion or zero density beyond that radius relative to each search circle centre. No such feature is present.

### 1.3 Interpretation

The circular regions described in the paper most plausibly represent the computational tiling grid used for the search, not a spatial filter applied to the resulting candidates. The candidate list appears to represent the full plate extent, including the outer zone where POSS-I geometric distortions are largest.

This is directly relevant to any footprint comparison: the VASCO60 pipeline restricts tile centres to within 2.2° of each plate centre (132 arcmin) to avoid the astrometrically unreliable outer ring. The third-party list, by contrast, appears to contain candidates from the full plate area out to ~238 arcmin from the plate centre. The two datasets are spatially non-equivalent by construction, not as an oversight.

---

## 2. Independent Corroboration: Machine-Learning Analysis of the Same Dataset

A subsequent machine-learning paper (Bruehl et al. 2026, arXiv:2604.18799) trained an ensemble classifier on the same 107,875-source dataset (XGBoost, Random Forest, Gradient Boosting, LightGBM). Two findings from that work are directly relevant here.

### 2.1 High predicted defect fraction

The ML ensemble assigns low candidate probability (below 0.66) to approximately **80% of the 107,875 candidates**. The authors find that the majority of the dataset is more consistent with plate artefacts, blended sources, or extended objects than with genuine point-source transients. This quantitative outcome is consistent with the SPREAD_MODEL gate failure rate observed in the VASCO60 pipeline check (32.8% of in-footprint pairs fail the morphology gate), and with the 70.6% elimination rate at the shape and morphology pipeline stages (S0S, S0M).

### 2.2 Plate-edge distance as a predictive feature

Among the features with the highest SHAP importance in the ML model, **`red_dist_to_edge_px`** (the candidate's pixel distance from the plate or image boundary in the red-band image) ranks in the top twelve predictors of candidate probability. A feature that encodes distance to the plate edge would not carry predictive power if all candidates were drawn from a region safely interior to the plate boundary. Its inclusion among the highest-importance predictors is direct empirical evidence that **plate-edge proximity is a systematic quality signal in this dataset** — i.e., a measurable fraction of candidates sit near or at the plate boundary where image quality degrades.

The fact that the ML model, trained entirely on the third-party dataset's own features, identifies plate-edge distance as a top predictor independently supports the spatial analysis in Section 1: the dataset contains candidates drawn from the full plate extent, and their quality correlates with proximity to the plate boundary.

---

## 3. Implications for Footprint Comparison

The footprint difference described in *VASCO60 Pipeline Check: Third-party POSS-I Candidate List Crossmatch* (77% of the third-party candidates fall outside the VASCO60 tile footprint) has two sources:

1. **Intentional plate-edge exclusion (VASCO60 design):** VASCO60 tiles are constrained to the 2.2° central zone of each plate. Candidates in the outer ring are excluded by design, not by pipeline failure.

2. **Full-plate sourcing (third-party list):** The third-party list appears to include candidates from the full plate extent. Approximately 61% of the uncovered candidates fall in the plate-edge exclusion zone (2.2°–3.25° from plate centre), indicating that a large fraction of the out-of-footprint population originates from the astrometrically degraded outer ring.

This asymmetry means that a low recovery rate in the VASCO60 pipeline check is structurally expected and does not reflect a limitation of the VASCO60 pipeline's sensitivity within its own footprint. Within the VASCO60 tile footprint, 1.4% of in-footprint candidate pairs reach the final output — consistent with the pipeline's gate and stage design.

Conversely, the 32.8% SPREAD_MODEL failure rate and 70.6% shape/morphology elimination rate are measured **entirely within the VASCO60 pipeline's own reliable-zone footprint**, where geometric distortions are minimal. These rates therefore reflect intrinsic properties of the third-party candidate list — not a consequence of which region of sky was searched.

---

## Notes

- No source coordinates from the third-party dataset are reproduced here. All statistics are aggregate counts or angular separations relative to publicly available POSS-I plate centre coordinates.
- Plate-centre coordinates used in the spatial analysis are from the public STScI POSS-I plate registry.
- The ML analysis cited (Bruehl et al. 2026) is a preprint; the underlying candidate scores have not been publicly released as of the date of this document.
