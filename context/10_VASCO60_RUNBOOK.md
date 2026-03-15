
# 10 — VASCO60 Runbook (reference; large)

## Token guard: This file is intentionally large. Do NOT read it fully.
Use it by searching headings/keywords and reading only the relevant section.

How to cite this runbook in a plan:
- Name the section heading(s) you used.
- Quote only the minimal bullet(s) needed to justify a change.

Primary purpose:
- Evidence, risks, and decision gates before “production mode”.
- Controlled experiment notes.
- Detailed audit checklist and funnel ledger expectations.

## Purpose
Document the current evidence, risks, and decision gates before starting vasco60 “production mode”. This section captures what we verified in small controlled experiments and what remains unresolved regarding R-set parity vs. MNRAS 2022. 

MNRAS 2022: https://doi.org/10.1093/mnras/stac1552 
Critique (2026): arXiv:2601.21946
Response (2026): arXiv:2602.15171

Current posture (vasco60 reset)

New repo + dataset root: ~/code/vasco60, with ./data as a symlink to the HDD dataset root; ./data and ./work are gitignored.

Default tile size: vasco60 will use 60×60 arcmin tiles by default. Tile folder naming is tile_RA<ra>_DECp/m<dec>.

Footprint policy for parity: “30arcmin” STScI cutouts are typically ~30′ wide squares, not 30′ radius coverage; parity work should use 60×60 tiles and apply a ≤30′ catalogue-level cut when needed.

Southern Hemisphere policy: in vasco30 the Southern Hemisphere cut was applied post‑pipeline; in vasco60 it will be enforced by the tessellation/plan (do not download/process tiles from the excluded region), so coverage is controlled upstream and is auditable in the plan.

Plan-driven execution: a deterministic plan (CSV/JSON) drives what is downloaded/processed (no random downloads).

Duplicates posture: vasco60 should have fewer duplicates than vasco30 because it follows the plan and avoids overlap-prone random downloads. When dedupe is needed, use true angular separation under canonical WCS‑fixed coordinates; internal default is 0.25″ per plate, with an optional audit band up to 0.30″ for overlap cleanup, and the dedupe ledger must record the tolerance used.

Local vs CDS runs: Vasco60 will by by default local-backend using 60x60 tiles. Old code still contains support for CDS: that code can be removed in Vasco60. Old code also supports and often assumes sharded tile folders i.e. ./data/tiles_by_sky/<ra-bin><dec-bin>/<tile-id>. Vasco60 will use flat folder structure i.e. ./data/tiles/<tile-id>. Old code assumes <tile-id> is in old format. All code dealing with sharded folders and old <tild-id> format should be reviewed and refactored. 

Important update 7-March:“Verified overlap: 90 (≤5″), 97 (≤60″) between MNRAS R (5399) and stage_S5_VSX on run-S1-20260302_185423.”

Our pipeline overlaps with the published 5,399 catalogue at ~100 objects; later stages don’t change this. Therefore, differences are largely due to upstream candidate definition and/or the published set’s aggressive all-catalog filtering purpose.

Current stage-ledger results already point to a sane conclusion: our pipeline is behaving consistently, and the MNRAS 2022 5,399 catalogue is not a completeness parity target. It’s a deliberately "over-filtered" list.

Cross-checking against other known MNRAS 2022 catalogs: we got 2913 matches within 5″ (and 3421 within 60″). Given our Stage 0 pool is ~10K rows, this means a very large fraction of the optical survivors are spatially consistent with objects that the MNRAS 2022 pipeline classed as “IR-identified (NEOWISE)” and published as a separate catalogue

Reminder: fix the bug in MNRAS_SUMMARY.json per tile: total_after_filters is currently always 0.Also add new info on important env/settings like:ps1_veto_enabled: true/false 
usnob_veto_enabled: true/false

Situation update 7-March-2026 (start)
What was clarified (and is no longer a blocker)
1) “Zero overlap with MNRAS R” was a false alarm
We proved overlap exists once you compare against the run‑scoped stage CSVs, not a single debug tile. 

2) Tile footprint mismatch is real — and you’ve already adopted the correct fix for vasco60
Your internal snapshot makes it explicit: “30arcmin” STScI cutouts are ~30′ wide squares, not 30′ radius circles, which can cause “covered by circle but not in pixels” confusion. For vasco60 the adopted posture is: download 60×60 and apply ≤30′ selection at catalogue level when MNRAS-style circle semantics are needed. 
That means vasco60 no longer inherits the geometric mismatch that made early parity checks misleading.
3) PS1 veto correctness: tile-local PS1 is known-bad; run-scoped PS1+SH is now canonical
You have a locked decision to retire tile-local ps1_neighbourhood.csv as veto input (truncation/leaks) and move PS1 veto + Southern Hemisphere cut to a run‑scoped postprocess stage using STILTS cdsskymatch (find=best, ≤5″). 
Your runbook example also anchors that S0 stage is operational and produces a stable downstream input (stage_S0_PS1SH.csv). 
4) Ordering and semantics are locked and consistent
You’ve locked the Step4 split:

Step4a: optical veto chain (Gaia→PS1→USNO), elimination semantics find=best within 5″ 
Step4b: late filters (morphology/spike/HPM)
This is stated explicitly in your decisions. 
So the “which stage eats what?” confusion is no longer conceptual — it’s implementational reporting.
5) IR is no longer a production veto dependency
Your stored policy note (and the linked preprint mention in decisions) supports keeping IR as annotation/classification stream rather than hard veto in the core reduction ledger. 
And practically, vasco60 production isn’t blocked on NEOWISE-scale matching anymore. 
6) MAPS experiment gave a concrete, interpretable result (and it does not create a new blocker)
You successfully matched your USNO-remainder against MAPS and found a large fraction has MAPS counterparts within 5″, and you also showed MNRAS 5,399 has only ~O(100) MAPS matches within 60″. This supports the idea that MAPS can function as an “old-epoch persistent object” reference, but it doesn’t need to be integrated into production unless you want it. 


What remains unclear / still a blocker for vasco60 production
Below are the blockers that actually affect “can we run vasco60 end-to-end reliably and reproducibly?”
Blocker A — Deterministic execution + state/ledger completeness
Your March 5 next-actions explicitly call out production posture changes that are not yet fully implemented everywhere:

Tile plan CSV drives execution (deterministic, not random) 
Bake tile→plate_id and tiles registry into Step1-download 
Introduce per-tile stage state (tile_status.json) for delta runs
These are listed as next actions, meaning they’re still “to-do” for production hardening. 
Why this is a production blocker: without fully reliable state/ledger, you’ll keep having “was this stage skipped because done vs because broken?” ambiguity during long runs and reruns.
Blocker B — Per‑tile summary correctness (MNRAS_SUMMARY) and invariant totals
You found (and confirmed across tiles) that total_after_filters is always 0 even though the rest of the counters are sane. That means the per‑tile accounting still has at least one broken field and could mislead rollups. The pipeline module overview explicitly warns that some categories are placeholders; that’s consistent with this bug.
Why this is a production blocker: it undermines automated “what ate what?” reporting and makes it harder to trust stage counts when scaling.
Blocker C — Long-running external stages (SkyBoT) still operationally heavy
Your changelog notes SkyBoT remains long-running even after reduction, and your locked decisions say SkyBoT radius is fixed at 60′ (parity requirement) and tuning is about reducing field calls, not radius. 
Why this is a production blocker: not correctness, but operational stability (cost/time/retry behavior) for full runs. It’s explicitly recorded as long-running.
Blocker D — USNO posture is still “experimental” and conflicts with PS1+SH in non-trivial ways
Your runbook/notes explicitly state: “PS1+SH is canonical; USNO remains experimental” and that USNO and PS1+SH disagree on non-trivial sets even if final counts converge.

Why this is a production blocker: you need a final decision: is USNO a required production veto, or an optional strictness experiment? Right now it’s explicitly not canonical.

Blocker E — Parquet/derived artifacts vs “tile tree is source of truth”
Your changelog says the tile tree is the master optical source of truth and Parquet masters are derived artifacts; that’s good — but it also implies production needs consistent rules for rebuilding derived artifacts and where consumers read from. Your next actions include “decide and document the consumer read root.” 

Why this is a production blocker: if downstream tools read from different roots, you can get mismatched catalogs/flags without noticing.


What is “ready enough” for vasco60 production right now
Based on your records:

Repo + dataset root + symlink + gitignore conventions are already established for vasco60. 
60×60 tiles + tile naming conventions are adopted. 
Veto-first semantics and SkyBoT radius are locked. 
Run-scoped stage system exists and is operational (S0 PS1+SH example; run-scoped stages are the intended production pattern). 
So production is not blocked by science logic anymore; it’s blocked by workflow hardening and ledger correctness.


 The short “blocker checklist” for vasco60 production

Finish deterministic “tile plan CSV drives execution” wiring end‑to‑end (Step1→Step6 + post-stages). 
Implement tile_status.json + ensure every stage updates it (and reruns respect it).
Fix MNRAS_SUMMARY.json total_after_filters (and/or add explicit enabled/disabled flags and invariants). 
Decide USNO’s role: canonical veto vs optional experiment. Right now it’s explicitly “experimental.” 
Operationalize SkyBoT: keep 60′ radius, but reduce field calls / improve resumability (already acknowledged as long-running). 
Document one consumer read root for derived artifacts vs tile tree (so nothing silently diverges). 


About mysterious VOSA services mentioned in MNRAS 2022
The “VOSA” link path leads to a SED analyzer tool requiring accounts, designed for SED fitting — not an optical veto list.
The MNRAS 2022 published catalogues live on a separate SVO archive endpoint: svocats…/vanish/, which explicitly lists the 5,399 and IR companion sets
Therefore: VOSA mention is not a Vasco60 production blocker; treat it as a paper/ecosystem reference (and at most an optional classification path). 

One-line “status”
Clarified: parity confusion was mostly geometry + reporting; run-scoped stages and 60×60 tile strategy are the right foundation.
Remaining blockers: deterministic orchestration (tile plan + status), correct ledger/summaries, and operational hardening for long stages (SkyBoT) plus one decision on USNO’s canonicality.

Situation update 7-March-2026 (ends) 


Baseline pipeline steps (as-implemented)
Quick reference for the 6-step tile pipeline: 1) Step1-download → FITS + header sidecar JSON + bookkeeping 2) Step2-pass1 → SExtractor pass1 (LDAC) 3) Step3-psf-and-pass2 → PSFEx + SExtractor pass2 (LDAC) 4) Step4-xmatch → ensure pass2 CSV exists; apply extract+MNRAS-like filters + spikes; xmatch vetoes; HPM check; write summaries 5) Step5-filter-within5 → within-5" artifacts from xmatch outputs 6) Step6-summarize → exports + summary
Evidence from controlled parity experiments (local cutouts)
A) “R survives but hard gates reject local matches” (initial small sample)
In a controlled test on a single 60×60 tile (RA 130.013°, Dec +33.081°), 6/7 MNRAS-R coordinates inside the ≤30′ circle had a nearest pass2 detection within 5″, but 0/6 passed the full hard-gate set (FLAGS=0; SNR_WIN>30; ELONGATION<1.3; SPREAD_MODEL>-0.002; and the FWHM gate).
B) Centered 30×30 cutouts for three targets (eliminates footprint confounder)
Three 30×30 tiles were downloaded and processed, each centered exactly on an MNRAS coordinate (1551, 1554, 1558). In all three cases a within-5″ pass2 detection exists, but each fails the MNRAS-style gates:

1551: fails SNR_WIN>30, ELONGATION<1.3, SPREAD_MODEL>-0.002
1554: fails 2<FWHM_IMAGE<7 (pixel units) and SPREAD_MODEL>-0.002
1558: fails 2<FWHM_IMAGE<7 (pixel units) and SPREAD_MODEL>-0.002
(These tests demonstrate that the mismatch is not explained solely by “point outside pixels”.)
C) Pixel scale sanity
Per-tile pixel scale derived from WCS (CD matrix) for the centered cutouts is stable at ~1.6997–1.6998 arcsec/px, so the above results are not explained by a 1.7→2.0 arcsec/px swing in these tiles.
D) SPREAD_MODEL distribution sanity
Tile-level SPREAD_MODEL summaries in the centered cutouts show negative medians (≈ -0.005) and p05 around ≈ -0.035, indicating that the absolute hard threshold SPREAD_MODEL > -0.002 can reject a large fraction of detections in this measurement regime.
Known unresolved issue: R-set parity vs MNRAS 2022 (production blocker)
(note: this is not yet reconciled in a single, fully-audited funnel):

A prior 30×30 pipeline run over ~11.7k tiles produced an R-like set around ~10k, while MNRAS 2022 reports a smaller vetted remainder (order of a few thousand).
The user observed that some tiles overlap (e.g., ~170 tiles containing at least one MNRAS-R coordinate), yet the user’s R-like set contains no positional matches to the published MNRAS R (even at coarse radii).
The user’s full processed population scale (post-pass2 filtered summary) was ~18.38M rows; most reductions were from Gaia + PS1 vetoes; later steps tended to be small cutters (thousands at most).

Why this blocks vasco60 production:

The centered-cutout experiments explain why some published R positions can be eliminated by the MNRAS-style gates in our measurement regime, but they do not explain why our overall remainder is larger than MNRAS and yet has zero overlap. That points to a systematic mismatch in the end-to-end selection funnel and/or a parity gap in how vetoes and late-stage reductions were applied.
Working hypotheses to test (ranked)

Gate semantics mismatch (units/definitions):

FWHM gate currently applies to FWHM_IMAGE in pixels; if a paper threshold is described in physical units (arcsec), portability is not guaranteed.
SPREAD threshold is absolute (>-0.002) while the observed tile distributions can have negative medians; threshold portability is questionable without confirming measurement context.2) Veto implementation parity (Gaia/PS1):
PS1 tile-local neighbourhood caches have been observed to truncate ("gapped at 20K lines"), so PS1 veto should be run in a way that guarantees completeness (run-scoped or chunked) and produces an auditable ledger of eliminated rows.3) Late-stage reduction parity (small cutters but still required for parity):
SuperCOSMOS (SCOS) and other post-pipeline stages should be run in a controlled, ledgered way even if their cut-rate is expected to be small, because they affect overlap claims.
Note: internal decisions explicitly define SCOS semantics for “cross-scan artifact removal parity” as keep matches within 5″ (DSS-only treated as artifacts) and position SCOS before expensive SkyBoT when shrinking is desired.4) Coordinate policy / WCSFIX propagation:
Ensure downstream steps consistently use the canonical coordinate columns intended for matching and filtering (e.g., WCS-fixed coordinates where applicable), and that any comparisons to MNRAS use the same coordinate frame/provenance.5) Region policy (square vs circle):
For parity runs, standardize “30′ radius circular region” as: download 60×60 then apply ≤30′ at catalogue level.
Paper text bug to track (must not carry into vasco60)
MNRAS 2022 describes diffraction-spike removal using USNO-B1.0 with:
“look for counterparts … in a circular region of 90-arcmin radius” -> it should be 90 arcsec

Decisions for vasco60 (what we will and won’t change yet)
Keep (parity baseline)

Keep the current MNRAS-style gate implementation in filters_mnras.py as the parity baseline (FLAGS, SNR_WIN, ELONGATION, FWHM_IMAGE, SPREAD_MODEL + optional robust clipping).
Keep the default 60×60 tile posture and apply a ≤30′ catalogue-level cut when parity requires a circular region.
Do not commit speculative changes yet

Do not commit PSFEx/SExtractor config tweaks as “production defaults” until they are shown to move the parity needle (they did not resolve the centered-cutout gate failures).
Concrete actions to record (before starting production-scale runs)
1) Make the investigation reproducible

Commit the diagnostic scripts used during this investigation (e.g., compare_sextractor_runs.py, check_one_target.py).
Store per-tile “diagnostic snapshots” for parity tiles (small JSON/CSV) capturing:pixel scale (arcsec/px) from header WCS
SPREAD_MODEL distribution summary (median/p05/p95)
per-gate pass/fail counts at each stage (extract → morphology → spikes → xmatch vetoes)
2) Implement deterministic parity subset runs

Create a small parity subset plan (tens–hundreds of tiles) with 60×60 downloads and the ≤30′ circle cut post-extraction.
3) Ensure veto parity is complete and auditable (Gaia + PS1)

Use a ledgered veto stage that records, for each input row, whether it was eliminated by Gaia and/or PS1 and with what nearest separation.
Avoid tile-local PS1 veto inputs if they are known/trusted to truncate; prefer run-scoped/chunked PS1 veto workflows (e.g., VizieR/II/389) with reproducible artifacts and row-count sanity checks.
4) Add explicit stage-state and funnel accounting

Add per-tile tile_status.json (or run-scoped ledger equivalents) to track step completion and enable delta runs and post-stage visibility.
5) Run post-pipeline steps in “shrink-forward” mode (even if small cutters)

For each post-pipeline stage (PS1 run-scoped veto, SCOS, SkyBoT, PTF, VSX, etc.), produce:stage_SX_*.csv (kept rows)
stage_SX_*_flags.csv (per-row flags)
stage_SX_*_ledger.json (counts + match stats)
Internal runbooks already outline this stage-based approach and ordering rationale.
Audit checklist: per-tile and full-population funnel report (vasco60)
Goal: For any parity subset or production run, produce a single “funnel ledger” that makes it obvious where rows were removed and why, and supports overlap analysis against external catalogues (MNRAS R/S/W lists) without ambiguity.
A) Inputs and geometry (per tile)

Tile ID, plate ID/REGION, survey (e.g., poss1_red)
Cutout size (arcmin) and pixel dimensions
Pixel scale (arcsec/px) from WCS (CD matrix)
Region policy applied: square only vs. ≤30′ circle cut (and resulting row counts)
B) Extraction products and QC (per tile)

SExtractor pass1 counts (detections) and LDAC row counts
PSFEx status summary: accepted/total samples, reported PSF FWHM/ellipticity
SExtractor pass2 counts (detections) and LDAC row counts
pass2 CSV row count and coordinate columns used (ALPHAWIN vs WCSFIX RA_corr, etc.)
C) In-tile filtering ledger (per tile)
For each of the following, record in_rows, out_rows, and failure breakdown by reason:

Extract filters (FLAGS/SNR gate)
Morphology hard gates (SPREAD/FWHM/ELONG)
Robust clipping on FWHM/elongation (median+MAD 2σ) if enabled
Spike removal (diffraction rules): in/out + rule hit counts
HPM filter (Gaia proper motion back-prop) if enabled
D) Veto ledger (per tile)

Gaia veto: matched count, eliminated count, match-radius stats (min/p50/p95/max), and the coordinate pair used
PS1 veto: matched/eliminated counts and match stats; explicitly state the backend (tile-local cache vs run-scoped VizieR) and any row-limit behavior; store artifacts used
USNO-B usage: whether used as veto vs spike helper, and what radius was used (ensure 90″ if used for spike neighbourhood per bug note)
E) Post-pipeline stage ledger (run-scoped)
For each stage (e.g., PS1 run-scoped sweep, SCOS, SkyBoT, PTF, VSX, etc.):

Input stage CSV checksum/row count
Query/match radius and backend
Rows matched / kept / eliminated
Output artifacts (kept CSV, flags CSV, ledger JSON)
F) Overlap audits (run-scoped)

Tile coverage audit: how many MNRAS points fall within any processed tile footprint (square) and within parity footprint (≤30′ circle) if applicable
Positional overlap audit against published MNRAS lists at multiple radii (5″, 30″, 1′), using the canonical coordinate policy
“Explainers”: sample a small number of MNRAS points that are covered by tiles but absent from our remainder; for each, report the first stage that removed it (e.g., morphology gate vs PS1 veto vs spikes)
G) GO / NO-GO decision gates

NO-GO if we cannot reproduce an auditable stage ledger on a parity subset that explains why our remainder size and overlap differ materially from MNRAS.
GO when the parity subset provides a clear, stage-attributed explanation for the remainder size and overlap behavior, and the PS1/Gaia veto path is complete and reproducible.
Go / No-Go criteria for “vasco60 production mode”
NO-GO if any of the following remain true:

We cannot explain the mismatch between the user’s R-like set and the published MNRAS R after running an audited, parity-intent funnel on a controlled subset.
The audited funnel does not demonstrate where (which stage) the mismatch is introduced (e.g., morphology gates vs. veto logic vs. later-stage reductions).
GO when:

A controlled parity subset run produces a fully-audited stage ledger showing how counts evolve and which stages dominate reductions (especially Gaia+PS1), and the resulting remainder is interpretable relative to MNRAS expectations.
The pipeline’s coordinate policy (raw vs WCS-fixed) and region policy (square vs circle) are explicitly documented and enforced for parity runs.

Appendix: Key references in internal notes

00 - RESUME CARD  (vasco60 reset + initial parity experiment outcome)
03 - Next Actions (tile plan CSV, circle cut policy, registry automation; PS1 posture notes)
02 - Decisions (Locked) (PS1 veto correctness; SCOS semantics; stage ordering)


