# VASCO60 public release -  2026-04-06

This folder contains a public snapshot of one VASCO60 multi-run campaign:
11 runs (R1 full + R2-R11 delta) covering 2318 tiles and 188 POSS-I plates.

## Files
- report_index.txt - campaign overview (runs, tiles, plates, survivor totals)
- funnel.json      - machine-readable stage counts per run
- survivors.csv    - final deduplicated candidates (src_id, ra, dec, plate_id, obs_date, run_id)
- example_run/     - one example run folder with post-process stage artefacts (CSV + flags + ledgers)

## Funnel (all runs combined)

| Logical stage | Total remaining |
|---|---:|
| pass2 raw detections | 12.53M |
| S0 post-MNRAS + dedup | 8,589 |
| MORPH (experimental) | 4,488 |
| SHAPE (experimental) | 819 |
| GSC (experimental) | 618 |
| SKYBOT | 618 |
| SCOS | 528 |
| PTF | 518 |
| VSX | 518 |

Stage names and per-run counts are in funnel.json.


## No images included (regeneratable)
This release does not include inspection images. You can generate the SHAPE inspection
images locally from the example run flags file using the provided script:

    python scripts/stage_shape_inspect.py \
	--flags-csv releases/release_2026_04_06/example_run/stages/stage_S4S_SHAPE_flags.csv \
	--tiles-root ./data/tiles \
	--out-dir ./inspect_output

This works as-is with the provided example run CSVs, assuming you have the corresponding tile FITS data under ./data/tiles.
		
## Notes
- src_id is the stable join key: tile_id:object_id
- Stages marked "experimental" are opt-in candidate-reduction steps

