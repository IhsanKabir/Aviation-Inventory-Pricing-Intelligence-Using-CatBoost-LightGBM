# Evidence Folder Policy

Purpose:

- Store a small, curated set of publishable evidence artifacts that support
  thesis claims, validation statements, and operational decisions.
- Keep `output/` as transient runtime output and avoid committing bulk generated
  files to Git.

Principle:

- `output/` = working/runtime generation area (ignored)
- `evidence/` = curated, citation-ready subset (committed intentionally)

What to keep in `evidence/`

- Artifacts explicitly cited in documentation (`PROJECT_DECISIONS.md`,
  validation guides, thesis text)
- Comparative-study outputs used for route-selection policy decisions
- Probe panel summaries used to justify passenger-size sensitivity claims
- Model summary artifacts used to justify route viability decisions
- Ops/storage evidence only when it supports a documented decision

What not to keep in `evidence/`

- Full `run_*` report packs unless specifically cited
- Repeated intermediate exports that can be regenerated
- `*_latest.*` convenience files (ignored by policy)
- Heartbeat/status files
- Temporary lock files (`~$*.xlsx`) and staging files

Directory layout

- `evidence/reports/` : selected report workbooks/CSVs used as references
- `evidence/comparative_studies/` : route-policy and route-priority comparisons
- `evidence/probe_summaries/` : probe panel and probe-group summarized outputs
- `evidence/model_summaries/` : baseline/route trainer JSON/MD/CSV evidence
- `evidence/ops/` : storage health, compaction, operations evidence (if cited)
- `evidence/manifests/` : optional manifest/checklist files for what was kept
- `evidence/staging/` : temporary copy area before curation (ignored)
- `evidence/tmp/` : temporary work area (ignored)

Naming policy (recommended)

- Prefer timestamped artifacts over `latest` pointers
- Include route/panel identifiers when relevant
- Keep original filenames when practical for traceability

Examples:

- `evidence/model_summaries/inventory_state_baseline_20260224_091027_793121.json`
- `evidence/comparative_studies/route_priority_policy_comparative_study_20260223_181951.md`
- `evidence/probe_summaries/probe_panel_run_summary_vq_routes_20260311.csv`

Operational curation workflow

1. Generate artifacts in `output/reports/`
2. Review and select only what supports a documented claim/decision
3. Copy selected files into the appropriate `evidence/` subfolder
4. Prefer timestamped versions (not `*_latest.*`)
5. Update docs to cite the curated evidence path if needed

Publishing note

- `evidence/` may contain large binary files (e.g., `.xlsx`). Keep this folder
  curated and small.
- If the repository must remain code-only, keep the same folder structure but do
  not commit the files (commit only this policy + manifests).
