#!/usr/bin/env python3
"""Regression test: a failed catalog extraction must never look like success.

Guards the fix in `vasco/cli_pipeline.py::_ensure_sextractor_csv`.

Two defects, one symptom. The LDAC->CSV extraction sweeps a list of HDU
selectors and used to try that sweep exactly once; under concurrent
(parallel-tile) load `stilts tcopy` fails intermittently even when the LDAC is
perfectly valid, so the whole sweep could fail for a tile that had real
detections. Worse, the fall-through then wrote an *empty* CSV and returned it
as if nothing had gone wrong -- so the tile was recorded as step "ok" with a
0-row catalog, indistinguishable from a tile that legitimately had no
detections. Whole tiles could drop out of a run silently.

The fix retries the sweep, and raises rather than writing a placeholder when
every attempt fails, leaving the tile un-ok so a resume picks it up again.
"""
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vasco import cli_pipeline as cp  # noqa: E402

HEADER = ("ALPHA_J2000,DELTA_J2000,FLAGS,SNR_WIN,FWHM_IMAGE,ELONGATION,"
          "SPREAD_MODEL\n")
ROW = "10.0,40.0,0,25.0,3.1,1.2,0.001\n"

# One full sweep of HDU selectors, per _ensure_sextractor_csv.
SWEEP = 11


def _stub_run(fail_first: int, calls: list):
    """stilts stand-in: fail the first `fail_first` calls, then write a catalog."""
    def run(argv, *a, **kw):
        calls.append(argv)
        if len(calls) <= fail_first:
            raise subprocess.CalledProcessError(1, argv)
        out = next(x for x in argv if str(x).startswith("out="))
        Path(str(out)[4:]).write_text(HEADER + ROW, encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0)
    return run


def _tile(tmp: Path) -> tuple[Path, Path]:
    tile = tmp / "tile_RA10.000_DECp40.000"
    (tile / "catalogs").mkdir(parents=True)
    ldac = tile / "pass2.ldac"
    ldac.write_bytes(b"not really an LDAC, the stub never reads it")
    return tile, ldac


def test_transient_extraction_failure_is_retried():
    """A whole sweep failing must not lose the tile -- the retry recovers it."""
    with tempfile.TemporaryDirectory() as td:
        tile, ldac = _tile(Path(td))
        calls: list = []
        with mock.patch.object(cp, "_ensure_tool_cli", lambda tool: None), \
             mock.patch.object(cp.time, "sleep", lambda s: None), \
             mock.patch.object(cp.subprocess, "run", _stub_run(SWEEP, calls)):
            out = cp._ensure_sextractor_csv(tile, ldac)

        assert out.exists() and out.stat().st_size > 0, "no catalog written"
        assert "ALPHA_J2000" in out.read_text(), "catalog missing required columns"
        assert len(calls) > SWEEP, (
            f"gave up after one sweep ({len(calls)} calls) -- not retried")


def test_permanent_failure_raises_and_writes_no_placeholder():
    """When every attempt fails, fail loudly -- never leave an empty catalog."""
    with tempfile.TemporaryDirectory() as td:
        tile, ldac = _tile(Path(td))
        calls: list = []
        raised = None
        with mock.patch.object(cp, "_ensure_tool_cli", lambda tool: None), \
             mock.patch.object(cp.time, "sleep", lambda s: None), \
             mock.patch.object(cp.subprocess, "run", _stub_run(10 ** 6, calls)):
            try:
                cp._ensure_sextractor_csv(tile, ldac)
            except RuntimeError as e:
                raised = e

        assert raised is not None, (
            "returned successfully after total extraction failure")
        sex_csv = tile / "catalogs" / "sextractor_pass2.csv"
        assert not sex_csv.exists(), (
            "wrote an empty placeholder -- a failed tile still looks like an "
            "empty-but-successful one")
        assert len(calls) >= 3 * SWEEP, f"fewer than 3 sweeps attempted ({len(calls)})"


if __name__ == "__main__":
    test_transient_extraction_failure_is_retried()
    print("PASS  transient extraction failure is retried")
    test_permanent_failure_raises_and_writes_no_placeholder()
    print("PASS  permanent failure raises, writes no placeholder")
