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

## Post-release pipeline corrections (2026-04)

Three step4 correctness bugs were fixed after this release was cut:

1. **Spike-veto radius** (Bug #5, Mick West): PS1 bright-star fetch radius was 3′ instead
   of 45′. Candidates near bright stars at tile edges could leak through the spike veto.
2. **USNO-B epoch propagation**: Column name mismatch (`RAJ2000` vs actual `ra`) caused
   propagation to silently fail for all tiles; high-PM USNO-B neighbours were matched at
   J2000.0 instead of the plate epoch (~1950s).
3. **DSS DATE-OBS overflow**: Tiles with malformed timestamps (e.g. `T11:77:00`) had no
   plate epoch and skipped all epoch propagation.

All three fixes are monotone-reducing: they can only remove survivors, never add them.
**`survivors.csv` in this release may therefore contain a small number of false positives**
that would be rejected by a corrected rerun. This release cannot be regenerated (it combined
R1 full + R2–R11 delta runs in a one-time campaign). The figures in this README and
`funnel.json` reflect the pre-fix pipeline. See commits `0b5279a`, `5b48711`, `353dc44`.

