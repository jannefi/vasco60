
#!/usr/bin/env python3
"""
Run step2 and step3 per tile concurrently across tiles.

Usage:
  python scripts/run_steps_2_3_parallel.py --tiles-file /tmp/tiles.txt --workers 6

Tips:
- Increase --workers if CPU and disk allow; decrease if the disk becomes the bottleneck.
- This script prints concise per-tile status and a final summary.
"""
import argparse, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

def run(cmd: list[str]) -> int:
    # Stream minimal output; rely on tile logs for detailed info
    try:
        return subprocess.run(cmd, check=False).returncode
    except Exception:
        return 1

def process_tile(tile: str) -> tuple[str, bool, str]:
    t0 = time.time()
    tile = str(Path(tile).resolve())
    # Step 2
    rc2 = run(["python","-u","-m","vasco.cli_pipeline","step2-pass1","--workdir",tile])
    if rc2 not in (0,):  # 2 can be "missing raw" or similar; treat non-zero as soft fail
        # Still attempt step 3; gating may skip it
        pass
    # Step 3
    rc3 = run(["python","-u","-m","vasco.cli_pipeline","step3-psf-and-pass2","--workdir",tile])
    ok = (rc3 == 0)
    dt = time.time() - t0
    msg = f"{Path(tile).name}: step2 rc={rc2}, step3 rc={rc3}, {dt:.1f}s"
    return tile, ok, msg

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiles-file", required=True)
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    tiles = [line.strip() for line in Path(args.tiles_file).read_text().splitlines() if line.strip()]
    print(f"[2+3] Tiles: {len(tiles)}, workers={args.workers}")
    ok_n = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process_tile, t): t for t in tiles}
        for i, fut in enumerate(as_completed(futs), 1):
            tile, ok, msg = fut.result()
            print(f"[{i:>5}/{len(tiles)}] {msg}")
            if ok: ok_n += 1
    print(f"[2+3] Done. OK tiles ~{ok_n}/{len(tiles)}. See per-tile logs for details.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
