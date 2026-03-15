# CLAUDE.md — VASCO60 (repo root)

Claude is a planning + implementation partner for VASCO60. Not autocomplete.

## 0) Context Loading Protocol (minimize tokens)
Default: start SMALL and expand only as needed.

**Always read first (in this order):**
1) ./context/02_DECISIONS.md (locked invariants)
2) ./context/03_NEXT_ACTIONS.md (current task board)

**Read only if needed:**
- ./context/00_RESUME.md (current posture summary / background)
- ./context/30_CSV_CONTRACT.md (post-pipeline “shrinking set” stages)

**Never read fully unless explicitly required:**
- ./context/10_VASCO60_RUNBOOK.md  (large; use headings/keyword search and read only the relevant section)

If you think you need more context, ask which file/section to open rather than ingesting everything.

## 1) Mandatory Planning Protocol (“Plan Mode”)
**No code is written until a plan is explicitly approved.**

Plan Mode is REQUIRED when:
- Any change touches more than one file, OR
- Any change affects pipeline semantics (veto/gates/geometry/order), OR
- Any change introduces new artifacts (ledgers/summaries/state files)

A valid plan MUST include:
1) Goal & Scope (what changes, what does NOT change)
2) Files & Artifacts (exact files to edit/create; outputs/ledgers)
3) Invariants preserved (thresholds, ordering, radius/geometry rules)
4) Risks & Verification (how we prove correctness; what to test; what counters/ledgers must change)

## 2) Non‑Negotiables (must obey)
### 2.1 Locked Decisions are law
Do not reinterpret or silently deviate from ./context/02_DECISIONS.md.
If something seems inconsistent, propose a plan and call it out.

### 2.2 Privacy / publication / safety
- Never introduce internal/private URLs, emails, or private correspondence into repo files.
- If you see private notes, summarize into neutral technical conclusions or move to a non-committed scratch area.
- Avoid names of private individuals in committed docs unless already public and intentionally included.

### 2.3 Output style
- Prefer full-file replacements over diffs/patches.
- Keep code/mermaid fenced and flush-left.
- When writing code, include a short “how to validate” section.

## 3) Working Practices (how to collaborate effectively)
- Start with the smallest change that makes progress.
- Always preserve auditability: per-stage ledgers and explicit row counts.
- When editing logic: state before/after behavior in one paragraph.
- When unsure: propose 2–3 options with tradeoffs, then wait for selection.

## 4) Fast task entry (recommended prompt shape)
When asked to change something:
1) Restate the goal in one sentence.
2) List the 2–5 files you need (by path).
3) Produce a plan.
4) Wait for approval.
5) Then output full file contents.

End.