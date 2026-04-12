#!/usr/bin/env bash
# tools/start-4-5-rerun.sh
#
# Force-rerun steps 4+5 across all tiles in the background.
# Safe to disconnect from SSH — logs to ~/code/vasco60/logs/.
#
# Use this after pipeline logic changes that affect the veto chain or
# spike cuts, when tiles are already processed and you need to redo them
# without re-fetching the neighbourhood catalogues (Gaia/PS1/USNO-B).
#
# What it does differently from start-4-5.sh:
#   --force   : ignore tile_status.json "already done" guard
#   --clean   : remove stale xmatch/veto CSVs before each tile
#   VASCO_STEP4_NO_FETCH=1 : skip Gaia/PS1/USNO-B neighbourhood re-fetches
#               (existing caches are valid; only spike-PS1 caches re-fetch
#               automatically on cache-miss by filename change)
#
# Usage:
#   bash tools/start-4-5-rerun.sh           # start background job
#   bash tools/start-4-5-rerun.sh status    # tail log + check if running
#   bash tools/start-4-5-rerun.sh stop      # graceful stop (place .STOP file)
#   bash tools/start-4-5-rerun.sh kill      # SIGTERM the process
#
# Environment overrides (set before calling):
#   WORKERS=4          number of parallel tile workers (default: 4)
#   TILES_ROOT=./...   tile root directory (default: ./data/tiles)
#   LOGDIR=./logs      log directory (default: ~/code/vasco60/logs)

set -euo pipefail

WORKERS="${WORKERS:-4}"
TILES_ROOT="${TILES_ROOT:-./data/tiles}"
LOGDIR="${LOGDIR:-$HOME/code/vasco60/logs}"
PIDFILE="$LOGDIR/rerun_step4_5.pid"
STOPFILE="$LOGDIR/RERUN_STEP4_5.STOP"  # also checked by run_steps_4_5_parallel.py as .STOP
LOG="$LOGDIR/rerun_step4_5_$(date +%F_%H%M%S).log"

MM="$HOME/.local/bin/micromamba"
export MAMBA_ROOT_PREFIX="$HOME/.micromamba"

_is_running() {
    [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null
}

cmd_start() {
    mkdir -p "$LOGDIR"

    if _is_running; then
        echo "[start] already running: pid=$(cat "$PIDFILE")"
        echo "[start] tail: tail -f $(ls -t "$LOGDIR"/rerun_step4_5_*.log 2>/dev/null | head -1)"
        exit 0
    fi

    rm -f "$STOPFILE"

    echo "[start] launching step4+5 force-rerun in background"
    echo "[start] workers=$WORKERS  tiles_root=$TILES_ROOT"
    echo "[start] VASCO_STEP4_NO_FETCH=1 (neighbourhood caches reused)"
    echo "[start] log: $LOG"

    nohup bash -lc "
      set -euo pipefail
      cd \"$HOME/code/vasco60\"

      if command -v ionice >/dev/null 2>&1; then
        exec ionice -c2 -n4 nice -n5 \
          env VASCO_STEP4_NO_FETCH=1 \
          \"$MM\" run -n vasco-py311 python \
          tools/run_steps_4_5_parallel.py \
          --workers $WORKERS \
          --tiles-root \"$TILES_ROOT\" \
          --force \
          --clean
      else
        echo \"[WARN] ionice not found; continuing with nice only\"
        exec nice -n5 \
          env VASCO_STEP4_NO_FETCH=1 \
          \"$MM\" run -n vasco-py311 python \
          tools/run_steps_4_5_parallel.py \
          --workers $WORKERS \
          --tiles-root \"$TILES_ROOT\" \
          --force \
          --clean
      fi
    " > "$LOG" 2>&1 &

    echo $! > "$PIDFILE"
    echo "[start] pid=$(cat "$PIDFILE")"
    echo "[start] monitor: bash tools/start-4-5-rerun.sh status"
}

cmd_status() {
    local latest_log
    latest_log="$(ls -t "$LOGDIR"/rerun_step4_5_*.log 2>/dev/null | head -1 || true)"

    if _is_running; then
        echo "[status] RUNNING — pid=$(cat "$PIDFILE")"
    else
        echo "[status] NOT RUNNING"
    fi

    if [[ -n "$latest_log" ]]; then
        echo "[status] log: $latest_log"
        echo "---- progress (last 40 lines) ----"
        tail -n 40 "$latest_log"
        echo "---- counts ----"
        echo "  OK:   $(grep -c ' OK ' "$latest_log" 2>/dev/null || echo 0)"
        echo "  SKIP: $(grep -c ' SKIP ' "$latest_log" 2>/dev/null || echo 0)"
        echo "  FAIL: $(grep -c 'FAIL\|ERROR' "$latest_log" 2>/dev/null || echo 0)"
    else
        echo "[status] no log file found in $LOGDIR"
    fi
}

cmd_stop() {
    echo "[stop] placing .STOP file — worker will stop after current tile"
    mkdir -p "$LOGDIR"
    touch "$STOPFILE"
    # run_steps_4_5_parallel.py checks for .STOP in the working directory
    touch .STOP
    if _is_running; then
        echo "[stop] pid=$(cat "$PIDFILE") — will exit cleanly at next tile boundary"
    else
        echo "[stop] process not running; .STOP placed for safety"
    fi
}

cmd_kill() {
    if _is_running; then
        local pid
        pid="$(cat "$PIDFILE")"
        echo "[kill] SIGTERM pid=$pid"
        kill "$pid" || true
        rm -f "$PIDFILE"
    else
        echo "[kill] not running"
    fi
}

case "${1:-start}" in
    start)  cmd_start  ;;
    status) cmd_status ;;
    stop)   cmd_stop   ;;
    kill)   cmd_kill   ;;
    *)      echo "usage: $0 {start|status|stop|kill}"; exit 2 ;;
esac
