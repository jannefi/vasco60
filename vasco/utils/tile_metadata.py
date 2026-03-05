# -*- coding: utf-8 -*-
"""vasco.utils.tile_metadata

Utilities to keep dataset metadata up to date *during* Step1-download.

Goal: retire separate post-download scripts that were previously required to:
  A) maintain data/metadata/tile_to_plate.csv (tile -> plate_id/REGION mapping)
  B) write per-tile raw/dss1red_title.txt sidecar
  C) maintain data/metadata/tiles_registry.csv

This module is called from the pipeline Step1-download success path.

Notes
-----
- plate_id is frozen to FITS header REGION.
- The FITS header JSON sidecar written by Step1 includes a full header dump.
  We prefer reading REGION/PLTLABEL/PLATEID/DATE-OBS from that JSON to avoid
  reopening FITS files.
"""

from __future__ import annotations

import csv
import json
import os
import platform
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None


@dataclass
class TilePlateRow:
    tile_id: str
    plate_id: str  # == REGION
    irsa_region: str = ''
    irsa_platelabel: str = ''
    irsa_plateid: str = ''
    irsa_date_obs: str = ''
    tile_survey: str = ''
    tile_date_obs: str = ''
    tile_fits: str = ''
    irsa_filename: str = ''
    irsa_center_sep_deg: str = ''


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lock_exclusive(fp):
    if fcntl is None:
        return
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
    except Exception:
        return


def _unlock(fp):
    if fcntl is None:
        return
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
    except Exception:
        return


def _read_header_sidecar(fits_path: Path) -> Tuple[dict, Optional[Path]]:
    """Read the FITS header JSON sidecar written next to the FITS.

    The sidecar format in cli_pipeline.py is:
      { 'fits_file': ..., 'selected': {...}, 'header': {...} }
    We return the merged header dict and the sidecar path.
    """
    sidecar = fits_path.with_suffix(fits_path.suffix + '.header.json')
    if not sidecar.exists() or sidecar.stat().st_size == 0:
        return {}, None
    try:
        payload = json.loads(sidecar.read_text(encoding='utf-8'))
        if isinstance(payload, dict):
            hdr = payload.get('header', payload)
            if isinstance(hdr, dict):
                return hdr, sidecar
    except Exception:
        pass
    return {}, sidecar


def ensure_metadata_dirs(data_root: Path) -> Path:
    meta = data_root / 'metadata'
    meta.mkdir(parents=True, exist_ok=True)
    return meta


def update_tile_to_plate_csv(meta_dir: Path, row: TilePlateRow, filename: str = 'tile_to_plate.csv') -> Path:
    """Upsert a row into tile_to_plate.csv keyed by tile_id."""
    out = meta_dir / filename
    fieldnames = [
        'tile_id', 'plate_id', 'tile_region', 'tile_survey', 'tile_date_obs', 'tile_fits',
        'irsa_region', 'irsa_filename', 'irsa_survey', 'irsa_platelabel', 'irsa_plateid',
        'irsa_date_obs', 'irsa_center_sep_deg'
    ]

    rows: Dict[str, dict] = {}
    if out.exists() and out.stat().st_size > 0:
        try:
            with out.open('r', newline='', encoding='utf-8') as f:
                rdr = csv.DictReader(f)
                for r in rdr:
                    tid = (r.get('tile_id') or '').strip()
                    if tid:
                        rows[tid] = r
        except Exception:
            rows = {}

    rows[row.tile_id] = {
        'tile_id': row.tile_id,
        'plate_id': row.plate_id,
        'tile_region': row.plate_id,
        'tile_survey': row.tile_survey,
        'tile_date_obs': row.tile_date_obs,
        'tile_fits': row.tile_fits,
        'irsa_region': row.irsa_region or row.plate_id,
        'irsa_filename': row.irsa_filename or row.tile_fits,
        'irsa_survey': row.tile_survey,
        'irsa_platelabel': row.irsa_platelabel,
        'irsa_plateid': row.irsa_plateid,
        'irsa_date_obs': row.irsa_date_obs or row.tile_date_obs,
        'irsa_center_sep_deg': row.irsa_center_sep_deg,
    }

    # Write atomically-ish (rewrite whole file under lock)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + '.tmp')
    with tmp.open('w', newline='', encoding='utf-8') as f:
        _lock_exclusive(f)
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for tid in sorted(rows.keys()):
            w.writerow({k: rows[tid].get(k, '') for k in fieldnames})
        _unlock(f)
    tmp.replace(out)
    return out


def update_tiles_registry(meta_dir: Path, *, tile_id: str, ra_deg: float, dec_deg: float,
                          survey: str, size_arcmin: float, pixel_scale_arcsec: float,
                          status: str = 'ok', source: str = 'step1-download', notes: str = '') -> Path:
    """Upsert into tiles_registry.csv keyed by tile_id."""
    out = meta_dir / 'tiles_registry.csv'
    fieldnames = [
        'tile_id','ra_deg','dec_deg','survey','size_arcmin','pixel_scale_arcsec',
        'status','downloaded_utc','source','notes'
    ]

    rows: Dict[str, dict] = {}
    if out.exists() and out.stat().st_size > 0:
        try:
            with out.open('r', newline='', encoding='utf-8') as f:
                rdr = csv.DictReader(f)
                for r in rdr:
                    tid = (r.get('tile_id') or '').strip()
                    if tid:
                        rows[tid] = r
        except Exception:
            rows = {}

    rows[tile_id] = {
        'tile_id': tile_id,
        'ra_deg': f'{ra_deg:.6f}',
        'dec_deg': f'{dec_deg:.6f}',
        'survey': survey,
        'size_arcmin': f'{float(size_arcmin):.3f}',
        'pixel_scale_arcsec': f'{float(pixel_scale_arcsec):.3f}',
        'status': status,
        'downloaded_utc': _utc_now_iso(),
        'source': source,
        'notes': notes,
    }

    tmp = out.with_suffix(out.suffix + '.tmp')
    with tmp.open('w', newline='', encoding='utf-8') as f:
        _lock_exclusive(f)
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for tid in sorted(rows.keys()):
            w.writerow({k: rows[tid].get(k, '') for k in fieldnames})
        _unlock(f)
    tmp.replace(out)
    return out


def write_dss1red_title(tile_dir: Path, row: TilePlateRow, *, prefer_local_header: bool = True) -> Path:
    """Write <tile>/raw/dss1red_title.txt (best-effort).

    We keep the same key lines as the legacy helper:
      PLTLABEL, PLATEID, REGION, DATE-OBS, FITS, SOURCE, SEP_DEG

    SOURCE preference:
      1) local <tile>/raw/<tile_fits>.header.json (if prefer_local_header)
      2) fallback to FITS basename

    We intentionally avoid pointing SOURCE to repo-internal header paths because in vasco60
    headers now live under data/metadata (gitignored) and absolute paths are undesirable.
    """
    raw = tile_dir / 'raw'
    raw.mkdir(parents=True, exist_ok=True)
    title_path = raw / 'dss1red_title.txt'

    src_rel = ''
    if prefer_local_header and row.tile_fits:
        local_json = raw / f'{row.tile_fits}.header.json'
        if local_json.exists():
            try:
                src_rel = os.path.relpath(local_json, raw)
            except Exception:
                src_rel = local_json.name
    if not src_rel:
        src_rel = row.irsa_filename or row.tile_fits or ''

    content_lines = [
        f'PLTLABEL: {row.irsa_platelabel}',
        f'PLATEID: {row.irsa_plateid}',
        f'REGION: {row.plate_id}',
        f'DATE-OBS: {row.irsa_date_obs or row.tile_date_obs}',
        f'FITS: {row.irsa_filename or row.tile_fits}',
        f'SOURCE: {src_rel}',
        f'SEP_DEG: {row.irsa_center_sep_deg}',
    ]
    title_path.write_text('
'.join(content_lines) + '
', encoding='utf-8')
    return title_path


def update_all_after_download(*, tile_dir: Path, fits_path: Path, tile_id: str,
                              ra_deg: float, dec_deg: float, survey: str,
                              size_arcmin: float, pixel_scale_arcsec: float,
                              data_root: Path, prefer_local_header: bool = True) -> dict:
    """Main entry point: update registry + mapping + title after a successful download."""
    hdr, sidecar = _read_header_sidecar(fits_path)

    region = str(hdr.get('REGION','') or '').strip()
    platelabel = str(hdr.get('PLTLABEL','') or '').strip()
    plateid = str(hdr.get('PLATEID','') or '').strip()
    date_obs = str(hdr.get('DATE-OBS','') or '').strip()

    meta_dir = ensure_metadata_dirs(data_root)

    row = TilePlateRow(
        tile_id=tile_id,
        plate_id=region,
        irsa_region=region,
        irsa_platelabel=platelabel,
        irsa_plateid=plateid,
        irsa_date_obs=date_obs,
        tile_survey=survey,
        tile_date_obs=date_obs,
        tile_fits=fits_path.name,
        irsa_filename=fits_path.name,
        irsa_center_sep_deg='',
    )

    out_map = update_tile_to_plate_csv(meta_dir, row)
    out_reg = update_tiles_registry(meta_dir,
                                    tile_id=tile_id,
                                    ra_deg=ra_deg,
                                    dec_deg=dec_deg,
                                    survey=survey,
                                    size_arcmin=size_arcmin,
                                    pixel_scale_arcsec=pixel_scale_arcsec,
                                    status='ok',
                                    source='step1-download')
    out_title = write_dss1red_title(tile_dir, row, prefer_local_header=prefer_local_header)

    return {
        'tile_to_plate_csv': str(out_map),
        'tiles_registry_csv': str(out_reg),
        'dss1red_title_txt': str(out_title),
        'plate_id': region,
        'header_sidecar': str(sidecar) if sidecar else '',
    }
