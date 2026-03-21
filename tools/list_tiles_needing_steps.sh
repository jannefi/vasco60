
#!/usr/bin/env bash
# List tile folders (absolute paths) that still need step2 and/or step3.
# Looks only under ./data/tiles (flat layout — tiles_by_sky is retired).
#
# Usage examples:
#   bash tools/list_tiles_needing_steps.sh \
#       --root /home/janne/code/vasco60/data \
#       --mode step2 \
#       --out /tmp/tiles_step2.txt
#
#   bash tools/list_tiles_needing_steps.sh \
#       --root /home/janne/code/vasco60/data \
#       --mode both \
#       --out2 /tmp/tiles_step2.txt \
#       --out3 /tmp/tiles_step3.txt
set -euo pipefail

ROOT=""
MODE="step2"           # step2 | step3 | both
OUT_STEP2=""           # required if MODE=step2 or both
OUT_STEP3=""           # required if MODE=step3 or both

# --- parse args ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)       ROOT="$2"; shift 2 ;;
    --mode)       MODE="$2"; shift 2 ;;
    --out)        OUT_STEP2="$2"; shift 2 ;;    # alias for OUT_STEP2
    --out2)       OUT_STEP2="$2"; shift 2 ;;
    --out3)       OUT_STEP3="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${ROOT}" ]]; then
  echo "ERROR: --root is required (e.g., /home/janne/code/vasco60/data)" >&2
  exit 2
fi

case "${MODE}" in
  step2)
    [[ -n "${OUT_STEP2}" ]] || { echo "ERROR: --out/--out2 required for mode=step2" >&2; exit 2; }
    ;;
  step3)
    [[ -n "${OUT_STEP3}" ]] || { echo "ERROR: --out3 required for mode=step3" >&2; exit 2; }
    ;;
  both)
    [[ -n "${OUT_STEP2}" && -n "${OUT_STEP3}" ]] || { echo "ERROR: --out2 and --out3 required for mode=both" >&2; exit 2; }
    ;;
  *)
    echo "ERROR: invalid --mode (use step2|step3|both)" >&2; exit 2 ;;
esac

ROOT="$(realpath "${ROOT}")"
TILES_ROOT="${ROOT}/tiles"
if [[ ! -d "${TILES_ROOT}" ]]; then
  echo "ERROR: No tiles root found at ${TILES_ROOT}" >&2
  exit 2
fi

# gather all tile directories (tile_RA*_DEC*) — flat layout only
mapfile -t TILE_DIRS < <( \
  find "${TILES_ROOT}" -type d -name 'tile_RA*_DEC*' -print | sort -V \
)

# classify tiles by presence of required artifacts
needs_step2=()  # raw/*.fits exists AND pass1.ldac missing or empty
needs_step3=()  # raw/*.fits & pass1.ldac exist (non-empty) AND pass2.ldac missing or empty

for tile in "${TILE_DIRS[@]}"; do
  raw="${tile}/raw"
  fits_file=""
  # pick first FITS in raw (if any)
  if [[ -d "${raw}" ]]; then
    fits_file="$(find "${raw}" -maxdepth 1 -type f -name '*.fits' -print -quit || true)"
  fi

  pass1="${tile}/pass1.ldac"
  pass2="${tile}/pass2.ldac"

  has_fits=false; [[ -n "${fits_file}" && -s "${fits_file}" ]] && has_fits=true
  has_pass1=false; [[ -s "${pass1}" ]] && has_pass1=true
  has_pass2=false; [[ -s "${pass2}" ]] && has_pass2=true

  # Need step2: FITS present, but pass1 missing/empty
  if ${has_fits} && ! ${has_pass1}; then
    needs_step2+=("$(realpath "${tile}")")
  fi

  # Need step3: FITS & pass1 present, but pass2 missing/empty
  if ${has_fits} && ${has_pass1} && ! ${has_pass2}; then
    needs_step3+=("$(realpath "${tile}")")
  fi
done

# write outputs
case "${MODE}" in
  step2)
    printf "%s\n" "${needs_step2[@]}" > "${OUT_STEP2}"
    echo "[list] step2 candidates: ${#needs_step2[@]} -> ${OUT_STEP2}"
    ;;
  step3)
    printf "%s\n" "${needs_step3[@]}" > "${OUT_STEP3}"
    echo "[list] step3 candidates: ${#needs_step3[@]} -> ${OUT_STEP3}"
    ;;
  both)
    printf "%s\n" "${needs_step2[@]}" > "${OUT_STEP2}"
    printf "%s\n" "${needs_step3[@]}" > "${OUT_STEP3}"
    echo "[list] step2=${#needs_step2[@]} -> ${OUT_STEP2}; step3=${#needs_step3[@]} -> ${OUT_STEP3}"
    ;;
esac
