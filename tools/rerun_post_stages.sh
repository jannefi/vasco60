#!/usr/bin/env bash
# tools/rerun_post_stages.sh
#
# Re-run all post-pipeline stages for one or more run folders.
# Designed for use after a step4 rerun that changes the survivor set
# (e.g. spike-veto radius fix, veto-chain correctness fix).
#
# Usage:
#   # single run
#   bash tools/rerun_post_stages.sh ./work/runs/run-R1-20260327_165043
#
#   # all runs
#   for r in ./work/runs/run-R*; do
#     bash tools/rerun_post_stages.sh "$r" || break
#   done
#
# Stage chain (matches existing run-folder layout):
#   S0        build_run_stage_csvs.py          → stage_S0.csv
#   S0M_MORPH stage_morph_post.py              → stages/stage_S0M_MORPH.csv
#   S1_GSC    stage_gsc_post.py                → stages/stage_S1_GSC.csv
#   S2_SKYBOT stage_skybot_post.py             → stages/stage_S2_SKYBOT.csv
#   S3_SCOS   stage_supercosmos_post.py        → stages/stage_S3_SCOS.csv
#   S4S_SHAPE stage_shape_post.py              → stages/stage_S4S_SHAPE.csv
#   S4_PTF    stage_ptf_post.py                → stages/stage_S4_PTF.csv
#   S5_VSX    stage_vsx_post.py                → stages/stage_S5_VSX.csv
#
# Environment overrides:
#   SKIP_MORPH=1    skip S0M_MORPH stage
#   SKIP_GSC=1      skip S1_GSC stage
#   SKIP_SKYBOT=1   skip S2_SKYBOT stage
#   SKIP_SCOS=1     skip S3_SCOS stage
#   SKIP_SHAPE=1    skip S4S_SHAPE stage
#   SKIP_PTF=1      skip S4_PTF stage
#   SKIP_VSX=1      skip S5_VSX stage
#   PY=python       Python binary to use (default: python)

set -euo pipefail

RUN_DIR="${1:-}"
if [[ -z "$RUN_DIR" || ! -d "$RUN_DIR" ]]; then
    echo "[ERROR] Usage: $0 <run-dir>" >&2
    echo "        Example: $0 ./work/runs/run-R1-20260327_165043" >&2
    exit 2
fi
RUN_DIR="$(cd "$RUN_DIR" && pwd)"
RUN_TAG="$(basename "$RUN_DIR")"

PY="${PY:-python}"
SKIP_MORPH="${SKIP_MORPH:-0}"
SKIP_GSC="${SKIP_GSC:-0}"
SKIP_SKYBOT="${SKIP_SKYBOT:-0}"
SKIP_SCOS="${SKIP_SCOS:-0}"
SKIP_SHAPE="${SKIP_SHAPE:-0}"
SKIP_PTF="${SKIP_PTF:-0}"
SKIP_VSX="${SKIP_VSX:-0}"

# Resolve repo root (one level above tools/)
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "[rerun_post_stages] run=$RUN_TAG"
echo "[rerun_post_stages] repo=$REPO_ROOT"
echo ""

# --------------------------------------------------------------------------
# Helper: run a command and print timing
# --------------------------------------------------------------------------
_run() {
    local label="$1"; shift
    echo "--- $label ---"
    local t0=$SECONDS
    "$@"
    local rc=$?
    local elapsed=$(( SECONDS - t0 ))
    echo "--- $label done (${elapsed}s) ---"
    echo ""
    return $rc
}

# --------------------------------------------------------------------------
# S0: rebuild stage_S0.csv from fresh tile xmatch outputs
# --------------------------------------------------------------------------
_run "S0 build_run_stage_csvs" \
    "$PY" scripts/build_run_stage_csvs.py \
        --run-tag "$RUN_TAG" \
        --accept-empty-file-as-valid-empty

# --------------------------------------------------------------------------
# S0M_MORPH: morphology filter
# --------------------------------------------------------------------------
if [[ "$SKIP_MORPH" != "1" ]]; then
    _run "S0M_MORPH morph" \
        "$PY" scripts/stage_morph_post.py \
            --run-dir "$RUN_DIR" \
            --input-glob "stage_S0.csv" \
            --stage "S0M_MORPH" \
            --tiles-root "./data/tiles"
fi

# --------------------------------------------------------------------------
# S1_GSC: GSC cross-match
# --------------------------------------------------------------------------
if [[ "$SKIP_GSC" != "1" ]]; then
    _run "S1_GSC gsc" \
        "$PY" scripts/stage_gsc_post.py \
            --run-dir "$RUN_DIR" \
            --input-glob "stages/stage_S0M_MORPH.csv" \
            --stage "S1_GSC"
fi

# --------------------------------------------------------------------------
# S2_SKYBOT: asteroid screening (the slow one)
# --------------------------------------------------------------------------
if [[ "$SKIP_SKYBOT" != "1" ]]; then
    _run "S2_SKYBOT skybot" \
        "$PY" scripts/stage_skybot_post.py \
            --run-dir "$RUN_DIR" \
            --input-glob "stages/stage_S1_GSC.csv" \
            --stage "S2_SKYBOT"
fi

# --------------------------------------------------------------------------
# S3_SCOS: SuperCOSMOS (keep matches)
# --------------------------------------------------------------------------
if [[ "$SKIP_SCOS" != "1" ]]; then
    _run "S3_SCOS supercosmos" \
        "$PY" scripts/stage_supercosmos_post.py \
            --run-dir "$RUN_DIR" \
            --input-glob "stages/stage_S2_SKYBOT.csv" \
            --stage "S3_SCOS" \
            --radius-arcsec 5 \
            --mode keep_matches
fi

# --------------------------------------------------------------------------
# S4S_SHAPE: shape filter (experimental)
# --------------------------------------------------------------------------
if [[ "$SKIP_SHAPE" != "1" ]]; then
    _run "S4S_SHAPE shape" \
        "$PY" scripts/stage_shape_post.py \
            --run-dir "$RUN_DIR" \
            --input-glob "stages/stage_S3_SCOS.csv" \
            --stage "S4S_SHAPE" \
            --tiles-root "./data/tiles"
fi

# --------------------------------------------------------------------------
# S4_PTF: PTF catalogue
# --------------------------------------------------------------------------
if [[ "$SKIP_PTF" != "1" ]]; then
    _run "S4_PTF ptf" \
        "$PY" scripts/stage_ptf_post.py \
            --run-dir "$RUN_DIR" \
            --input-glob "stages/stage_S4S_SHAPE.csv" \
            --stage "S4_PTF" \
            --radius-arcsec 5
fi

# --------------------------------------------------------------------------
# S5_VSX: VSX variable stars
# --------------------------------------------------------------------------
if [[ "$SKIP_VSX" != "1" ]]; then
    _run "S5_VSX vsx" \
        "$PY" scripts/stage_vsx_post.py \
            --run-dir "$RUN_DIR" \
            --input-glob "stages/stage_S4_PTF.csv" \
            --stage "S5_VSX" \
            --radius-arcsec 5
fi

echo "[rerun_post_stages] DONE: $RUN_TAG"
