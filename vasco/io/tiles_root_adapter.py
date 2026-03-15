
# -*- coding: utf-8 -*-
"""
tiles_root_adapter.py — Flat tile tree discovery for VASCO60.

Layout:  ./data/tiles/<tileid>/

Usage in scripts:
  from vasco.io.tiles_root_adapter import TilesAdapter
  ta = TilesAdapter(base_dir="./data", env_var="VASCO_TILES_ROOT")

  for tile in ta.iter_tiles():          # iterates ./data/tiles/
      tile_id   = tile.tile_id
      tile_path = tile.path
      ra, dec   = ta.read_ra_dec(tile)  # normalized [0,360), [-90,+90]
      # ... per-tile processing ...
"""

import os, re, json
from dataclasses import dataclass
from typing import Iterator, Optional, Tuple, List
from vasco.utils.tile_id import parse_tile_id_center

RA_KEYS  = ["RA_DEG", "CRVAL1", "RA", "OBJ_RA"]
DEC_KEYS = ["DEC_DEG", "CRVAL2", "DEC", "OBJ_DEC"]

# FITS like "dss1-red_110.135_-24.656_60arcmin.fits"
PAT_FITS_NAME = re.compile(r"^.+?_([0-9]+(?:\.[0-9]+)?)_([+\-]?[0-9]+(?:\.[0-9]+)?)_[0-9]+(?:arcmin)?\.fits$", re.I)

@dataclass
class Tile:
    tile_id: str
    path: str         # absolute path to the tile directory
    layout: str       # "flat"
    ra: Optional[float] = None
    dec: Optional[float] = None

class TilesAdapter:
    def __init__(self, base_dir: str = "./data", env_var: Optional[str] = "VASCO_TILES_ROOT"):
        if env_var and os.getenv(env_var):
            base_dir = os.getenv(env_var)
        self.base_dir  = os.path.abspath(base_dir)
        self.tiles_root = os.path.join(self.base_dir, "tiles")

    # -------- public API --------
    def iter_tiles(self) -> Iterator[Tile]:
        """Iterate tiles under ./data/tiles/."""
        if os.path.isdir(self.tiles_root):
            yield from self._iter_flat()

    def read_ra_dec(self, tile: Tile) -> Tuple[Optional[float], Optional[float]]:
        if tile.ra is not None and tile.dec is not None:
            return tile.ra, tile.dec
        ra_dec = self._read_header_ra_dec(tile.path, tile.tile_id) \
                 or self._parse_tileid_ra_dec(tile.tile_id) \
                 or self._parse_fitsname_ra_dec(tile.path)
        tile.ra, tile.dec = ra_dec if ra_dec else (None, None)
        return tile.ra, tile.dec

    # -------- private helpers --------
    def _iter_flat(self) -> Iterator[Tile]:
        for e in sorted(os.scandir(self.tiles_root), key=lambda x: x.name):
            if e.is_dir():
                yield Tile(tile_id=e.name, path=e.path, layout="flat")

    def _read_header_ra_dec(self, tile_path: str, tile_id: str):
        raw_dir = os.path.join(tile_path, "raw")
        if not os.path.isdir(raw_dir): return None
        preferred = os.path.join(raw_dir, f"{tile_id}.fits.header.json")
        candidates: List[str] = []
        if os.path.isfile(preferred): candidates.append(preferred)
        for e in os.scandir(raw_dir):
            if e.is_file() and e.name.endswith(".fits.header.json"):
                if e.path not in candidates: candidates.append(e.path)
        for path in candidates:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    hdr = json.load(f)
                ra, dec = None, None
                for k in RA_KEYS:
                    if k in hdr: ra = float(hdr[k]); break
                for k in DEC_KEYS:
                    if k in hdr: dec = float(hdr[k]); break
                if ra is not None and dec is not None:
                    return self._norm_ra_dec(ra, dec)
            except Exception:
                continue
        return None

    def _parse_tileid_ra_dec(self, tile_id: str):
        parsed = parse_tile_id_center(tile_id)
        if parsed:
            return parsed
        return None

    def _parse_fitsname_ra_dec(self, tile_path: str):
        raw_dir = os.path.join(tile_path, "raw")
        if not os.path.isdir(raw_dir): return None
        for e in os.scandir(raw_dir):
            if e.is_file() and e.name.lower().endswith(".fits"):
                m = PAT_FITS_NAME.match(e.name)
                if m: return self._norm_ra_dec(float(m.group(1)), float(m.group(2)))
        return None

    def _norm_ra_dec(self, ra: float, dec: float):
        ra  = ra % 360.0
        dec = max(-90.0, min(90.0, dec))
        return ra, dec
