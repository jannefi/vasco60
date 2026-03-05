# vasco/utils/tile_id.py
from __future__ import annotations
import re
from typing import Optional, Tuple

# New: tile_RA130.013_DECp33.081  or  tile_RA130.013_DECm33.081
_RE_NEW = re.compile(r"^tile_RA([0-9]+(?:\.[0-9]+)?)_DEC([pm])([0-9]+(?:\.[0-9]+)?)$")

# Old: tile-RA130.013-DEC+33.081  or  tile-RA130.013-DEC-33.081
_RE_OLD = re.compile(r"^tile-RA([0-9]+(?:\.[0-9]+)?)-DEC([+\-])([0-9]+(?:\.[0-9]+)?)$")

def parse_tile_id_center(tile_id: str) -> Optional[Tuple[float, float]]:
    tile_id = (tile_id or "").strip()
    m = _RE_NEW.match(tile_id)
    if m:
        ra = float(m.group(1))
        sign = m.group(2)
        dec_abs = float(m.group(3))
        dec = dec_abs if sign == "p" else -dec_abs
        return ra % 360.0, max(-90.0, min(90.0, dec))

    m = _RE_OLD.match(tile_id)
    if m:
        ra = float(m.group(1))
        sign = m.group(2)
        dec_abs = float(m.group(3))
        dec = dec_abs if sign == "+" else -dec_abs
        return ra % 360.0, max(-90.0, min(90.0, dec))

    return None
