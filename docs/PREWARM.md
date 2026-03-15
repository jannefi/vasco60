
# VASCO60 Prewarmers (Unified)

This bundle provides a **unified neighbourhood prewarmer** and thin **background wrappers** for PS1 and USNO-B.

## Files

- `scripts/prewarm_neighbourhood_cache.py` — single runner for **Gaia/PS1/USNO-B**.
- `scripts/prewarm_ps1_neighbourhood_bg.sh` — nohup/PID/STOP wrapper calling the unified runner with `--catalog ps1`.
- `scripts/prewarm_usnob_neighbourhood_bg.sh` — nohup/PID/STOP wrapper calling the unified runner with `--catalog usnob`.

> Gaia already has an existing background wrapper in your repo; keep it or adapt its call to the unified runner.

## Tile naming contract
Matches directory names of the form `tile_RA<ra>_DECp<dec>` or `tile_RA<ra>_DECm<dec>` (e.g., `tile_RA130.013_DECp33.081`). The regex in the runner is:

```python
_TILE_RE = re.compile(r"^tile_RA(?P<ra>\d+(?:\.\d+)?)_DEC(?P<hem>[pm])(?P<dec>\d+(?:\.\d+)?)$")
```

## Radius policy (≤30′ tile circle)
- **Gaia / USNO‑B:** use **31′** (30′ + 5″ safety) for local ≤5″ matches inside the circle.
- **PS1 (spike rule):** use **35′** best‑effort. For strict spike coverage at the 30′ edge, request ~**65′** or use multi‑cone tiling; the PS1 fetcher may cap to 0.5° by default — raise via `VASCO_PS1_RADIUS_DEG` if supported by your fetcher.

## Examples
```bash
# Gaia (31′)
python scripts/prewarm_neighbourhood_cache.py --catalog gaia  --tiles-root ./data/tiles --radius-arcmin 31 --workers 6

# PS1 (35′ best‑effort; allow larger if your fetcher supports it)
export VASCO_PS1_RADIUS_DEG=1.08   # optional; ~65′
python scripts/prewarm_neighbourhood_cache.py --catalog ps1   --tiles-root ./data/tiles --radius-arcmin 35 --workers 4

# USNO‑B (31′)
python scripts/prewarm_neighbourhood_cache.py --catalog usnob --tiles-root ./data/tiles --radius-arcmin 31 --workers 4
```

## Notes
- The unified runner writes per‑tile CSVs under `<tile>/catalogs/` with catalog‑specific names.
- USNO‑B requires `fetch_usnob_neighbourhood()` to be present in `vasco/external_fetch_online.py`. If not, the runner will raise a helpful error.
- All scripts are **resumable** and write a progress JSON into `./logs/`.
