# VASCO60 — Final release v1 

This page is a short “how to read the release” guide for people who want the results **without running the pipeline**.

**Release folder:**  
https://github.com/jannefi/vasco60/tree/main/releases/final_release_v1

---

## 1) Start here 

### Quick entry point 
- `releases/final_release_v1/report/survivors.csv`  
  Final deduplicated candidate list (≈440 rows in this release). No visual vetting is included.

### For full traceability / reproducibility
- `releases/final_release_v1/run/stages/`  
  Stage-by-stage outputs for the run-scoped “shrinking set”, including audit artefacts (flags + ledgers).

---

## 2) What VASCO60 is 

VASCO60 is a reproducible, plate-aware pipeline for scanning digitised POSS-I red plates for sources detected on POSS-I that have no counterpart in modern catalogues (Gaia / Pan-STARRS1 / USNO-B), followed by a run-scoped sequence of post-pipeline veto stages. The repository goal is reproducibility, provenance, and controls (not reproducing any single external unpublished list).  
Project overview is in the root README:  
https://github.com/jannefi/vasco60 [1](https://github.com/jannefi/vasco60)

---

## 3) How the post-pipeline stages work

VASCO60 uses a run-scoped **“shrinking set”** model:
each stage takes the current survivor CSV as input, applies one veto/filter, and outputs:

- `stage_SX_*.csv` (carry-forward survivors for the next stage)
- `stage_SX_*_flags.csv` (per-row audit flags)
- `stage_SX_*_ledger.json` (parameters + totals + stats)

The canonical stage model, folder structure, and stage semantics are documented here:
- `docs/POSTPROCESS.md`  
  https://github.com/jannefi/vasco60/blob/main/docs/POSTPROCESS.md [2](https://github.com/jannefi/vasco60/blob/main/docs/POSTPROCESS.md)

---

## 4) Footprint matters: tessellation plan 

All results depend on the sky footprint that was actually downloaded and processed.
VASCO60 uses a deterministic tessellation plan to decide which tiles exist and which tiles were downloaded.  
Tessellation strategy:
- `docs/TESSELLATION_STRATEGY.md`  
  https://github.com/jannefi/vasco60/blob/main/docs/TESSELLATION_STRATEGY.md

Concrete plan files live under:
- `plans/`  
  https://github.com/jannefi/vasco60/tree/main/plans

*(Any comparison of candidate counts across pipelines or papers should use compatible footprint definitions.)*

---

## 5) Known deviation: 6 tiles not returned by the download service

There is a small coverage deviation where the download service returned nothing for six tiles.
These tiles are documented here:
- `plans/TILES_REJECTED.md`  
  https://github.com/jannefi/vasco60/blob/main/plans/TILES_REJECTED.md


---

## 6) Interpretation boundaries (please read before over-interpreting survivors)

- **No visual vetting** is included in final_release_v1.
- Remaining candidates may be photographic/digitisation artefacts (ghosts, halation, internal reflections, etc.) or real astrophysical transients.

For practical “plate literacy” and suggested minimum viable controls for vetting, see:
- `docs/PLATE_FORENSICS_LIBRARY.md`  
  https://github.com/jannefi/vasco60/blob/main/docs/PLATE_FORENSICS_LIBRARY.md [3](https://github.com/jannefi/vasco60/blob/main/docs/PLATE_FORENSICS_LIBRARY.md)

---

## 7) Context: the 107K dataset and the 2026 preprint

Recent literature continues to analyse a ~107k candidate dataset. The preprint below reports applying machine learning to assign each of **107,875** previously identified transients a probability of being real, and then reporting statistical results after controlling for ML-identified artefacts:

- arXiv:2604.18799  
  https://arxiv.org/abs/2604.18799 

VASCO60 final_release_v1 provides an open, fully auditable dataset and pipeline outputs (including stage-by-stage audit artefacts) that others can inspect and reproduce end-to-end.


