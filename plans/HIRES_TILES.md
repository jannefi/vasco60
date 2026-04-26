# HiRes plates note


STScI DSS scan resolution varies by declination

STScI silently returns finer (15µm, ~1.0 arcsec/pix) scans for some Dec-minus POSS-I E plates vs standard 25µm (~1.7 arcsec/pix) for northern plates

For Dec-minus plates STScI's DSS archive sometimes returns POSS-I E plates scanned at 15 µm (≈1.0 arcsec/pix) instead of the standard 25 µm (≈1.7 arcsec/pix) used for northern plates. Files are ~25 MB instead of ~9 MB.

Confirmed on 2026-04-26: e.g. tile_RA280.325_DECm28.830 — SURVEY=POSSI-E, TELESCOP=Palomar Schmidt

**Why:** Unknown. STScI does not document this

**How to apply:**
- Pipeline is unaffected: SExtractor reads pixel scale from WCS header; PSFEx adapts per-image; all gates are pixel-scale-independent.
- `tiles_registry.csv` records `pixel_scale_arcsec=1.700` for these tiles. This is cosmetic inaccuracy only, not used in computation.
- SExtractor runtime is longer on these tiles (more pixels) as expected
- No action needed when encountering large Dec-minus FITS files
