
# CSV Contract

Post‑pipeline “Shrinking Set” Fetchers (S1→S2→S3…)

## 0) Goals

Provide one stable join key across all post‑pipeline stages (S1 → SkyBoT → S2 → ZTF/PTF → S3 → VSX → …).
Avoid service failures caused by reserved keywords (notably NUMBER/number in TAP/ADQL contexts).
Keep inputs small: single CSV if ≤2000 rows; else chunk in ~2000 rows.
Scope note: This contract is pre‑dedup and pre‑plate‑edge filtering.

Pre‑dedupe: overlapping tiles may yield duplicate survivors; dedupe is a separate stage and must preserve src_id.
Per‑plate edge filtering: plate-edge cuts are a separate stage; deferred to the next discussion.

## 1) Canonical identifier (MUST exist in all stage CSVs)

src_id (string, required)

Definition: src_id = tile_id + ":" + object_id
Example: tile-RA0.998-DEC+1.558:1234
Rationale: mirrors the proven join‑back pattern previously used as row_id = tile_id:NUMBER, but with the numeric part renamed safely.


## 2) Canonical stage CSV schema (service‑agnostic)

Every stage dataset (S1, S2, S3, …) MUST be representable as CSV with columns:

src_id (string) — primary join key
tile_id (string)
object_id (int) — renamed from internal NUMBER
ra (float, degrees) — prefer WCS‑fixed coordinates when present
dec (float, degrees) — prefer WCS‑fixed coordinates when present
Forbidden column names in upload CSVs: NUMBER, number (and any alias that is exactly NUMBER/number).

## 3) Service upload CSV “views” (minimal columns)

Upload CSVs are thin views of the canonical stage CSV:

VSX upload: src_id, ra, dec
PTF/ZTF upload (position‑only screen): src_id, ra, dec
SkyBoT upload (epoch‑aware): src_id, ra, dec, epoch_mjd (do not run SkyBoT without epoch context).

## 4) Fetcher output contract (MUST always allow re‑join)

All fetchers MUST emit outputs keyed by src_id:

Always include: src_id
Always include: one boolean flag column (e.g., has_skybot_match, has_ztf_match, is_known_variable)
Optional diagnostics: best_sep_arcsec, matched_count, epoch_used, source_chunk, provenance timestamps
Re‑join rule (shrinking set):

stage_next = stage_prev LEFT ANTI JOIN flags_on_src_id

## 5) Chunking policy

If N_rows ≤ 2000: write one CSV (e.g., stage_S1.csv)
Else: write chunk CSVs chunk_0000001.csv, … with ~2000 rows each
Each chunk file must contain the canonical columns (or at minimum the upload‑view subset including **src_id).

## 6) Migration note: PTF → ZTF

Keep the same input/output contract so the backend can be swapped later:

Input: src_id, ra, dec
Output: src_id, has_match, best_sep_arcsec, provenance

## 7) Standard mapping (the only mapping you ever do)

When building canonical stage CSVs from tile outputs:

Read per‑tile survivors: catalogs/sextractor_pass2.filtered.csv
Map internal NUMBER → object_id
Construct src_id = f"{tile_id}:{object_id}"
Emit upload CSVs by selecting/renaming columns — never output NUMBER/number to services.

## 8) Validation checks (MUST run at each stage)

Uniqueness: src_id must be unique within a stage CSV
No forbidden columns: upload CSV must not contain NUMBER/number
Re‑join works: join fetcher outputs back to the stage CSV on src_id and verify counts match (no loss/dup explosions)

## 9) Run outputs & builder script (plan — next)

Create a run-scoped output folder: ./work/runs/run-<date>/.
Produce a master, run-specific survivor CSV (PS1+Gaia-only policy applied) named:source_extractor_final_filtered.csv
Current header (fixed): src_id,tile_id,object_id,ra,dec,plate_id,ps1_eligible,edge_class_px,edge_class_arcsec
Store all run reports and artifacts alongside it (tile allow/exclude lists, stage CSVs/chunks, per-stage flag outputs, summaries).
Reuse parts of the legacy parquet merger script <File>merge_tile_catalogs.py</File> for:tile discovery under ./data/tiles_by_sky (and flat roots), and tile → plate_id (REGION) mapping via ./data/metadata/tile_to_dss1red.csv (REGION is the frozen plate_id contract).

Edge cut (easy at CSV stage; keep visibility):

Keep edge_class_px and edge_class_arcsec in the master CSV for audit.
Define core using the proven permissive rule:is_core = (edge_class_px == 'core') OR (edge_class_arcsec == 'core')

Write a derived file alongside the master (for statistics + fetcher inputs):source_extractor_final_filtered__edge_core.csv (rows where is_core is true)
Optional audit: ...__edge_noncore.csv
Stage accounting (“what ate what”): keep a run-local stage_ledger.csv where each fetcher stage appends (rows_in, rows_flagged, rows_out); each stage shrinks via anti-join on src_id (S1→S2→S3…).
Plate-edge sensitivity work (buffer curves, Expected maps, etc.) remains deferred and will be discussed next; the existing per-tile edge report stays the authoritative input for that future cut policy.
Related internal context (why this exists)

Past chunk files already used a join‑back ID shaped like tile_id:NUMBER (previously called row_id).
A recorded incident showed NUMBER as a problematic token in at least one TAP/ADQL environment, driving the “never emit NUMBER/number” rule.
Current SkyBoT planning is chunk‑based and join‑back oriented, matching this contract.
