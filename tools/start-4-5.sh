#!/usr/bin/env bash
set -euo pipefail

mkdir -p ~/code/vasco60/logs
LOG=~/code/vasco60/logs/steps_4_5_parallel_$(date +%F_%H%M%S).log

MM="$HOME/.local/bin/micromamba"
export MAMBA_ROOT_PREFIX="$HOME/.micromamba"

nohup bash -lc "
  set -euo pipefail
  cd \"$HOME/code/vasco60\"

  if command -v ionice >/dev/null 2>&1; then
    exec ionice -c2 -n4 nice -n5 \
      \"$MM\" run -n vasco-py311 python \
      tools/run_steps_4_5_parallel.py --workers 4 --clean --only-missing \
      >> \"$LOG\" 2>&1
  else
    echo \"[WARN] ionice not found; continuing with nice only\" >> \"$LOG\"
    exec nice -n5 \
      \"$MM\" run -n vasco-py311 python \
      tools/run_steps_4_5_parallel.py --workers 4 --clean --only-missing \
      >> \"$LOG\" 2>&1
  fi
" >/dev/null 2>&1 & echo $! > /tmp/steps_4_5.pid
