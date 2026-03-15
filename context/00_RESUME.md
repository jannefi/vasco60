# 00 — RESUME CARD (VASCO60)

Owner: Janne Ahlberg
Purpose: current posture only (keep SMALL). Historical notes are below.

## Current Snapshot (authoritative)
- Pipeline posture: Step4a = optical veto first (Gaia → PS1 → USNO, best ≤5″), then Step4b = late filters (morphology/spike/HPM).  
- Tile geometry: download 60×60 arcmin squares; when parity needs “30′ radius”, apply a ≤30′ catalogue-level circular cut downstream.
- Coordinates: adopt early WCS‑fixed coordinates; carry canonical RA/Dec downstream; joins must use composite keys (tile_id, object id) or stable src_id.
- PS1 local cache warning: tile-local PS1 neighbourhood files can truncate (e.g., 50,001 cap) → do not rely on them for gating; prefer auditable run-scoped workflows.
- SkyBoT: match policy is strict 5″ (with labeled wider fallback if used). Radius is fixed at 60′; optimize call-efficiency/resumability, not radius.
- IR gating: not a production veto dependency; treat as annotation/classification stream.
- Cache radius posture:
  - Gaia/USNO caches: default center radius ~31′ (covers 30′ circle + margin).
  - PS1 spike prewarm: best-effort unless radius strategy is expanded (needs much larger effective coverage).

## Working principle
Prefer auditability: each stage writes counts + ledgers; avoid silent heuristics.

---

## Historical snapshots (reference only — do not ingest unless needed)
(Keep older dated notes here as-needed; treat as “forensics”, not “current rules”.)