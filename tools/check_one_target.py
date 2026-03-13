#!/usr/bin/env python3
import sys, math
from pathlib import Path
import pandas as pd

def wrap_pm180(x): return (x + 180.0) % 360.0 - 180.0

def sep_arcsec(ra1, dec1, ra2, dec2):
    ra1r, dec1r = math.radians(ra1), math.radians(dec1)
    ra2r, dec2r = math.radians(ra2), math.radians(dec2)
    dra = math.radians(wrap_pm180(ra2 - ra1))
    ddec = dec2r - dec1r
    a = math.sin(ddec/2)**2 + math.cos(dec1r)*math.cos(dec2r)*math.sin(dra/2)**2
    c = 2*math.asin(min(1.0, math.sqrt(a)))
    return math.degrees(c) * 3600.0

def load_catalog(tile_dir: Path):
    p = tile_dir / "catalogs" / "sextractor_pass2.csv"
    df = pd.read_csv(p)
    for ra_col, dec_col in [
        ("RA_corr","Dec_corr"),
        ("ALPHAWIN_J2000","DELTAWIN_J2000"),
        ("ALPHA_J2000","DELTA_J2000"),
        ("X_WORLD","Y_WORLD"),
    ]:
        if ra_col in df.columns and dec_col in df.columns:
            return df, ra_col, dec_col, p
    raise RuntimeError("No RA/Dec columns found")

def gates(row):
    g = {}
    g["FLAGS==0"] = (float(row.get("FLAGS", 1)) == 0)
    g["SNR_WIN>30"] = (float(row.get("SNR_WIN", float("nan"))) > 30.0)
    g["ELONG<1.3"] = (float(row.get("ELONGATION", float("nan"))) < 1.3)
    fwhm = float(row.get("FWHM_IMAGE", float("nan")))
    g["2<FWHM<7"] = (fwhm > 2.0 and fwhm < 7.0)
    g["SPREAD>-0.002"] = (float(row.get("SPREAD_MODEL", float("nan"))) > -0.002)
    return g

def main():
    if len(sys.argv) != 4:
        print("Usage: check_one_target.py <TILE_DIR> <RA_DEG> <DEC_DEG>")
        return 2
    tile = Path(sys.argv[1])
    ra = float(sys.argv[2]); dec = float(sys.argv[3])

    df, ra_col, dec_col, path = load_catalog(tile)
    seps = df.apply(lambda r: sep_arcsec(ra, dec, float(r[ra_col]), float(r[dec_col])), axis=1)
    idx = seps.idxmin()
    sep = float(seps.loc[idx])
    row = df.loc[idx]

    print(f"\nTile: {tile}")
    print(f"Catalog: {path.name}  coords={ra_col}/{dec_col}")
    print(f"Target: RA={ra} Dec={dec}")
    print(f"Nearest: sep_arcsec={sep:.6f}  NUMBER={row.get('NUMBER', 'NA')}")

    if sep > 5.0:
        print('RESULT: NO within-5" match')
        return 0

    fields = ["FLAGS","SNR_WIN","ELONGATION","FWHM_IMAGE","SPREAD_MODEL"]
    print("Values:", {k: row.get(k, None) for k in fields})

    pxscale = 1.69978  # or compute from raw/*.fits.header.json CD matrix
    print("FWHM_ARCSEC =", float(row.get("FWHM_IMAGE")) * pxscale)

    g = gates(row)
    failed = [k for k,v in g.items() if not v]
    print("Gates:", g)
    print("RESULT:", "PASS" if not failed else "FAIL  (" + ", ".join(failed) + ")")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
