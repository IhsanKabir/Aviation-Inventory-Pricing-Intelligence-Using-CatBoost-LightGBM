# Airline Fare & Inventory Intelligence via Inventory-State Modeling, Probe Analysis, and Two-Stage Route-Gated ML Forecasting (Thesis Track)

_Current implemented intelligence stack: Inventory-State Modeling + Passenger-Size Probe Analysis + Two-Stage Route-Gated ML Forecasting (rolling-viability-gated route selection)._  
_Planned DL path (exact methods): TCN (first), TFT (later), survival/hazard models for event timing, and network-aware models in later phases._

Last updated: 2026-02-22

## 1) Program Vision

Build a multi-airline intelligence platform that progresses through these outcomes:

1. Monitoring
2. Pricing intelligence
3. Revenue prediction
4. Competitor benchmarking
5. Automation (later semi-automated actions)

Target: implement as much as possible in parallel, but execute in phases when needed.

## 2) Confirmed Product Decisions

### Users

- Analysts
- Revenue Management (RM) team
- Internal stakeholders
- Public users who want current and expected price movement

### Airline Expansion Priority

1. Biman (current)
2. Novo Air
3. US Bangla
4. Air Astra
5. Indigo
6. Emirates
7. Qatar
8. Saudia
9. Singapore Airlines
10. Malaysia Airlines
11. Maldivian Air
12. Others later

### Market Scope

- All markets

### Route Scope

- Dynamic route + airline configuration (already in use)

### Accumulation Frequency

- Target every 3-4 hours (adaptive by actual runtime/load)

### Required Data Fields

Mandatory for analysis (minimum set):

- Fare components: fare, tax, total, currency
- Inventory: seats available, sold-out state
- Fare structure: fare basis, booking class (RBD), brand
- Product/cabin: cabin class and fare basis-to-cabin mapping
- Flight/ops: airline, flight number, origin, destination, departure, arrival, aircraft/equipment, duration, stops
- Passenger mix dimensions: ADT/CHD/INF
- Other available fields from source responses should be preserved in raw metadata

### Change Definition

- Any column difference from last valid snapshot is a change event

### Sold-Out Logic

- If all RBDs of a flight/cabin/departure are sold out => flight instance is sold out
- If flight exists but seats are unavailable => treat as sold out (unless API explicitly labels temporary technical unavailability)

### Search Scope

- All possible search combinations over time (routes/cabins/passenger mixes/date windows)

### Cabin & Fare Mapping

- Cabin-specific monitoring is required
- Track which fare basis belongs to which cabin over time

### Reporting

- Dynamic/on-demand report generation
- Alert types and thresholds must be configurable at runtime

### Forecast Priorities

1. Price-change prediction
2. Availability prediction

### Forecast Horizon

- Flexible horizon (user-defined "next X" time)

### Decision Mode

- Phase 1: human decision support
- Phase 2: semi-automated actions

### Accuracy Evaluation Intent

- Compare prediction for future date/time with actual observed data when that time arrives

### External Enrichment

Include over time:

- Holidays
- Weekday effects
- Country-wise vacation calendars
- Macro/market context and future condition adaptors

### Infrastructure Constraints

- Current environment: local laptop
- Database: PostgreSQL
- Budget: zero (prefer free/open-source stack)

### Timeline

- As early as possible

### Phase-1 Must-Have

- Reports working for all target airlines first
- Then move to prediction layer

## 3) Thesis-Grade Upgrade Decisions (Added)

### Research-Quality Evaluation Pack

Use multiple evaluation families (not just one):

- Directional: up/down accuracy, F1 for rise/fall classes
- Magnitude: MAE, RMSE, MAPE/sMAPE (for fare deltas)
- Event quality: precision/recall for alerts (spikes, sell-out, schedule shock)
- Calibration: reliability plots / Brier score (if probabilistic outputs)
- Operational value: lead-time gain, false-alarm cost, missed-event cost

### Benchmarking Baselines

Always compare models against:

- Naive persistence (next = last)
- Seasonal naive (same weekday/time bucket)
- Moving-average / EWMA baseline

### Reproducibility Standard

- Versioned datasets/snapshots
- Versioned features and model configs
- Backtest windows with fixed train/validation/test splits
- Logged experiment metadata

### Explainability

- Feature importance tracking
- "Why forecast changed" summary per route/flight/cabin
- Store model confidence + uncertainty bands

## 4) Suggested System Architecture (Zero-Budget Compatible)

- Ingestion: airline-specific connectors (modular)
- Standardization: canonical schema + raw payload archive
- Storage:
  - PostgreSQL for normalized facts/events
  - Compressed JSON archive for raw payload lineage
- Processing layers:
  - Snapshot builder
  - Column-level diff engine
  - Event/alert engine
  - Forecast engine
- Delivery:
  - On-demand report generator (Excel/CSV/JSON)
  - Optional lightweight API/dashboard later

## 5) Data Governance and Risk Note

Use only legally permitted collection. Respect airline terms, robots/policies where
applicable, and avoid methods that violate law or contractual restrictions. Build
throttling, retry policy, and source-specific compliance controls into each connector.

## 6) Immediate Build Sequence

1. Stabilize canonical schema across airlines
2. Implement connector contract for each airline (same output contract)
3. Enable multi-airline accumulation orchestration + quality checks
4. Make dynamic report pack stable (hourly/daily/on-demand)
5. Add baseline forecasting pipeline (price then availability)
6. Add benchmarking and thesis evaluation framework

## 7) Open Questions (Need Answers)

1. What is the single canonical key for a "flight product": `(airline, flight_no, departure_dt, origin, destination, cabin, fare_basis)` or include brand too?
2. When fare basis is missing but brand exists, should brand become fallback identity?
3. For multi-leg itineraries, do you want segment-level tracking, itinerary-level tracking, or both?
4. Should all timestamps be stored in UTC + local airport timezone offset?
5. Do you want one global currency (e.g., BDT/USD) for all analytics plus original currency retained?
6. What maximum acceptable accumulation latency per full cycle (all airlines/routes) do you want?
7. For public users, which outputs are exposed: current cheapest fare only, trend chart, or predictions too?
8. What confidence threshold should gate alerts/predictions shown to public users?
9. Which report templates are mandatory for thesis submission (chapter-ready figures/tables)?
10. Do you want a formal backtesting cadence (daily retrain, weekly retrain, monthly retrain)?

## 8) Definition of Done (Phase 1)

Phase 1 is done when:

- Multi-airline scrapes run reliably on schedule
- All mandatory fields are populated or explicitly null-coded
- Column-level change events are persisted and queryable
- On-demand reports are generated correctly for all onboarded airlines
- Data quality checks and failure logs are in place

## 9) Final Clarifications (2026-02-20)

- Data access is confirmed by project owner as authorized.
- Backtesting method selected: Rolling Window (final).
- Time standard selected: store canonical timestamps in UTC; store local timezone fields for display/ops context.
- Identity key finalized as:
  - airline, day, time, origin, destination, flight number, fare basis, brand, cabin
- Fare basis policy:
  - expected always present; if missing, mark row as invalid/incomplete (fail-safe), do not fallback identity substitution.
- Change policy:
  - any column difference is a change event (no minimum threshold filter).
- Output for public users:
  - show current fare + trend + prediction.
- Model refresh cadence:
  - daily.

## 10) Compliance Statement

All collection and usage must remain within authorized, lawful, and policy-compliant boundaries for each source.

## 11) Completion Plan (Execution Checklist)

Use this section as the single source of truth for "what remains" and "what is done".

### Phase 1 Closure (Must Complete First)

- [x] **P1-A: Final Data Quality Closure**
  - Target:
    - `adt/chd/inf` nulls = 0 for new scrapes
    - `inventory_confidence` nulls = 0 for new scrapes
    - `source_endpoint` nulls = 0 for new scrapes
    - `departure_utc` nulls = 0 for all rows where airport TZ mapping exists
  - Notes:
    - Remaining `arrival_utc` nulls are acceptable only when source arrival local timestamp is missing.
  - Verify:
    - Run data quality report and archive result under `output/reports/`.
  - Evidence (2026-02-21):
    - `output/reports/run_20260221_160815_332731_UTCp0600/data_quality_report_20260221_160815_332731_UTCp0600.csv`
    - `output/reports/run_20260221_160815_339148_UTCp0600/data_quality_report_20260221_160815_339148_UTCp0600.csv`

- [x] **P1-B: Unknown Airport TZ Reduction**
  - Target:
    - Keep `config/airport_timezones.json` aligned with active routes.
  - Action:
    - Monthly query: identify new airport codes with UTC null patterns, update timezone map, run backfill.
    - Repro command:
      - `.\\.venv\\Scripts\\python.exe tools/audit_airport_timezones.py --output-dir output/reports --timestamp-tz local`
  - Verify:
    - Backfill output shows `departure_utc` null trend not increasing for known airports.
  - Evidence (2026-02-21):
    - `config/airport_timezones.json` (`YYZ` added with `-300`)
    - `tools/audit_airport_timezones.py`
    - `output/reports/timezone_coverage_gaps_20260221_234252.csv` (pre-fix: `YYZ` gap)
    - `output/reports/timezone_backfill_verification_clean_20260221_234742.txt` (`null_departure_utc` remains `0`, `YYZ` arrival UTC null resolved to `0`)
    - `output/reports/timezone_coverage_audit_20260221_234622.csv` (post-fix audit)
    - `output/reports/timezone_coverage_gaps_20260221_234622.csv` (post-fix gaps empty)

- [x] **P1-C: Connector Stability Gates**
  - Target:
    - BG + VQ targeted checks succeed for `DAC->CXB` and at least one international BG route.
  - Verify commands:
    - `run_all.py --quick --airline BG --origin DAC --destination CXB --date <date> --cabin Economy`
    - `run_all.py --quick --airline VQ --origin DAC --destination CXB --date <date> --cabin Economy`

- [x] **P1-D: Reporting Reliability**
  - Target:
    - `price_changes_daily`, `availability_changes_daily`, `route_airline_summary`, `data_quality_report` generated every run.
    - `raw_meta_coverage_pct` = 100 for active run scope.
  - Verify:
    - `generate_reports.py` creates all report files without manual fixes.

- [x] **P1-E: Ops Hardening**
  - Target:
    - Scheduler runs every 3-4h with no crash loops.
    - Failures logged with actionable reason.
  - Verify:
    - Review `logs/` for one full day cycle.
  - Close-out status (2026-02-21):
    - `output/reports/ops_health_latest.md` is `PASS` with no non-zero pipeline runs.
    - Window currently captured: `2026-02-21 05:12:08` to `2026-02-21 17:32:23` (~12.34h).
    - Closed per project owner directive; continue scheduler logging to accumulate full-day and multi-day evidence.

### Phase 2 (Thesis/Prediction Enablement)

- [x] **P2-A: Baseline Forecast Pack**
  - Implement and compare:
    - Naive persistence
    - Seasonal naive
    - EWMA baseline
  - Metrics:
    - MAE, RMSE, MAPE/sMAPE, directional F1.
  - Evidence (2026-02-21):
    - `output/reports/prediction_eval_total_change_events_20260221_164627.csv`
    - `output/reports/prediction_eval_price_events_20260221_164625.csv`
    - `output/reports/prediction_eval_availability_events_20260221_164626.csv`

- [x] **P2-B: Backtesting Framework**
  - Rolling-window backtest with fixed splits and saved experiment metadata.
  - Evidence (2026-02-21):
    - `output/reports/prediction_backtest_eval_total_change_events_20260221_165325.csv`
    - `output/reports/prediction_backtest_splits_total_change_events_20260221_165325.csv`
    - `output/reports/prediction_backtest_meta_total_change_events_20260221_165325.json`
    - `output/reports/prediction_backtest_eval_price_events_20260221_165330.csv`
    - `output/reports/prediction_backtest_splits_price_events_20260221_165330.csv`
    - `output/reports/prediction_backtest_meta_price_events_20260221_165330.json`
    - `output/reports/prediction_backtest_eval_availability_events_20260221_165331.csv`
    - `output/reports/prediction_backtest_splits_availability_events_20260221_165331.csv`
    - `output/reports/prediction_backtest_meta_availability_events_20260221_165331.json`

- [x] **P2-C: Alert Quality Evaluation**
  - Precision/Recall for spike/sell-out alerts.
  - False alarm and missed event cost tracking.
  - Evidence (2026-02-21):
    - `output/reports/alert_quality_daily_20260221_173346.csv`
    - `output/reports/alert_quality_overall_20260221_173346.csv`
    - `output/reports/alert_quality_by_route_20260221_173346.csv`
  - Notes:
    - Spike alert metrics are computed against `total_change_events` thresholding with rolling baseline prediction.
    - Sellout alert pipeline is implemented, but current source window has zero
      positive sellout events (`support=0`), so precision/recall are pending
      future positives.

- [x] **P2-D: Thesis-Ready Output Pack**
  - Reproducible figures/tables from report + model outputs.
  - Chapter-ready summary for methodology + results.
  - Evidence (2026-02-21):
    - `tools/build_thesis_pack.py`
    - `output/reports/thesis_pack_20260221_174107/thesis_summary.md`
    - `output/reports/thesis_pack_20260221_174107/tables/table_prediction_best_models.csv`
    - `output/reports/thesis_pack_20260221_174107/tables/table_backtest_test_summary.csv`
    - `output/reports/thesis_pack_20260221_174107/tables/table_alert_quality_overall.csv`
    - `output/reports/thesis_pack_20260221_174107/tables/table_data_quality_snapshot.csv`
    - `output/reports/thesis_pack_20260221_174107/manifest.json`
    - `output/reports/thesis_pack_20260221_174107.zip`

### Weekly Completion Ritual

Every week, run and record:

1. `run_all.py` targeted checks (BG + VQ)
2. `tools/backfill_raw_meta_fields.py`
3. `generate_reports.py --format both`
4. `scheduler/maintenance_tasks.py --task both` (ensures weekly pack + restore validation + smoke snapshots)
5. Update this file:
   - Tick completed items
   - Add blockers under "Open Questions" if any new dependency appears

### Progress Snapshot (2026-02-22)

- `P1-B` unknown airport timezone reduction:
  - Added `tools/audit_airport_timezones.py` for recurring monthly timezone-gap detection.
  - Updated `config/airport_timezones.json` with `YYZ: -300` based on active data audit.
  - Backfill validation confirms `null_departure_utc` remains `0` and `YYZ` arrival UTC nulls are cleared.
- `P1-C` connector stability gates:
  - Verified via one-cycle scheduler runs for BG and VQ (`--once`) with `rc=0`.
- `P1-D` reporting reliability:
  - All core report artifacts generated successfully in current cycle.
  - `route_flight_fare_monitor` is now soft-skipped when no rows exist (no pipeline failure).
- `P1-E` ops hardening:
  - Added `tools/ops_health_check.py`.
  - Latest baseline: `output/reports/ops_health_latest.md` shows `PASS` with no non-zero pipeline runs in analyzed window.
  - Checklist item marked complete per project owner directive; scheduler remains active for additional evidence accumulation.
- Ops automation hardening extensions:
  - Added health notifier: `tools/notify_ops_health.py` (WARN/FAIL alert logic, webhook-capable, local audit log).
  - Added forced alert test controls: `--force-status` and `--test-mode` in `tools/notify_ops_health.py`.
  - Added retention cleanup: `tools/retention_cleanup.py` (default keep windows: logs 30d, reports 60d).
  - Added unified status dashboard: `tools/system_status_snapshot.py` (`system_status_latest.md/json` + timestamped snapshots).
  - Added DB backup automation: `tools/db_backup.py` (writes `.dump` + `db_backup_latest.json`).
  - Added DB restore validation: `tools/db_restore_test.py` (non-destructive `pg_restore --list` / schema render checks).
  - Added smoke gate: `tools/smoke_check.py` (deps, DB connectivity, heartbeat freshness, ops/report artifact freshness).
  - Wired into daily/weekly maintenance flow via `scheduler/maintenance_tasks.py`.
  - Added no-admin always-on fallback daemon: `scheduler/always_on_maintenance.py` and startup/pulse launchers.
  - Added setup reproducibility:
    - `requirements-lock.txt`
    - `setup_env.ps1`
    - `SETUP_QUICKSTART.md`
  - Evidence:
    - `output/reports/ops_notifications.log`
    - `output/reports/retention_cleanup_latest.json`
    - `output/reports/system_status_latest.md`
    - `output/reports/system_status_latest.json`
    - `output/reports/smoke_check_latest.md`
    - `output/reports/smoke_check_latest.json`
    - `output/backups/db_backup_latest.json`
    - `output/backups/db_restore_test_latest.json`
    - `output/reports/ops_health_20260222_002728.md`
    - `output/reports/smoke_check_20260222_002735.md`
    - `output/reports/thesis_pack_20260222_002736.zip`
    - `scheduler/install_always_on_autorun.ps1`
    - `scheduler/always_on_maintenance.py`
  - Current environment note:
    - Backup/restore tools now auto-discover PostgreSQL client binaries from common
      Windows install paths (`C:\\Program Files\\PostgreSQL\\*\\bin`) even when PATH
      is not preconfigured.
- Operational excellence upgrade pack (2026-02-22):
  - CI + commit quality gates:
    - `tools/ci_checks.py` (compile + tests + smoke + report dry run)
    - `.github/workflows/ci.yml`
    - `.githooks/pre-commit`
    - `tools/install_git_hooks.ps1`
  - DB resilience and verification:
    - `tools/db_backup.py` captures table metrics at backup time.
    - `tools/db_restore_test.py` validates dump readability.
    - `tools/db_restore_drill.py` performs full temporary-DB restore and row-count/checksum comparison.
  - SLA + drift + operator visibility:
    - `tools/data_sla_dashboard.py`
    - `tools/model_drift_monitor.py`
    - `tools/build_operator_dashboard.py`
  - Recovery + performance:
    - `tools/recover_missed_windows.py` (dry-run scan and active recovery mode)
    - `run_all.py --profile-runtime --profile-output-dir <dir>`
    - `run_pipeline.py --parallel-airlines <N>` via `tools/parallel_airline_runner.py`
  - Retention tiers:
    - `tools/retention_cleanup.py` now supports raw/aggregate/thesis retention windows.
  - Secrets hardening:
    - Removed embedded DB credentials from code/config defaults.
    - Added env-driven DB resolution helper: `core/runtime_config.py`.
    - Added `.env.example`.
  - Evidence (latest):
    - `output/reports/ci_checks_latest.json`
    - `output/reports/data_sla_latest.md`
    - `output/reports/model_drift_latest.md`
    - `output/reports/recover_missed_windows_latest.json`
    - `output/reports/operator_dashboard_latest.md`
    - `output/backups/db_restore_drill_latest.json`
    - `output/reports/scrape_parallel_latest.json`
    - `output/reports/runtime_profile_latest.json`
- `P2-A` baseline forecasting:
  - Upgraded `predict_next_day.py` with seasonal naive + EWMA baselines.
  - Added RMSE and directional metrics (directional accuracy + up/down/macro F1).
  - Implemented fallback history source from `flight_offers` when route summary view has insufficient history.
- `P2-B` backtesting framework:
  - Added fixed rolling train/validation/test split execution in `predict_next_day.py`.
  - Added saved backtest artifacts: `prediction_backtest_eval_*`, `prediction_backtest_splits_*`, and `prediction_backtest_meta_*.json`.
  - Added auto-window fallback when requested split lengths exceed available history range.
- `P2-C` alert evaluation:
  - Added `tools/evaluate_alert_quality.py`.
  - Added precision/recall/F1/accuracy + false-alarm/missed-event cost outputs (overall and by route).
  - Added `run_pipeline.py --run-alert-eval` integration with configurable thresholds and cost weights.
- `P2-D` thesis-ready output pack:
  - Added `tools/build_thesis_pack.py` to auto-discover latest artifacts and assemble a reproducible thesis bundle.
  - Pack output includes copied raw evidence, consolidated thesis tables, chapter-ready markdown summary, and SHA-256 manifest.
  - Latest evidence:
    - `output/reports/thesis_pack_20260221_174107/`
    - `output/reports/thesis_pack_20260221_174107.zip`
- Dynamic search-horizon prediction/trend enhancement:
  - `run_all.py` now accepts dynamic date windows:
    - `--dates`, `--date-offsets`, `--dates-file`
  - `run_pipeline.py` now forwards dynamic accumulation date-window args and dynamic
    prediction args (`--prediction-series-mode`, departure bounds, optional
    backtest disable).
  - `predict_next_day.py` now supports `--series-mode search_dynamic` for search-day to search-day forecasting by `departure_day`.
  - Added trend outputs per route/cabin/departure-day:
    - `prediction_trend_<target>_<timestamp>.csv`
  - Example evidence:
    - `output/reports/prediction_next_day_min_price_bdt_20260221_170752.csv`
    - `output/reports/prediction_trend_min_price_bdt_20260221_170752.csv`
    - `output/reports/prediction_backtest_splits_min_price_bdt_20260221_170752.csv`
- Legacy historical data migration:
  - Added `tools/migrate_legacy_history.py` to import legacy archive/sqlite snapshots into current Postgres schema.
  - Apply run imported historical records into current DB:
    - `flight_offers`: +95
    - `flight_offer_raw_meta`: +95
  - Dry-run evidence:
    - `output/reports/legacy_migration_dry_run_20260221_171733.txt`

- Latest inclusions (2026-02-22, v2 integration + validation):
  - Route scope segmentation + market-country domestic logic:
    - Added shared route-scope utility: `engines/route_scope.py`
    - Added airport-country mapping config: `config/airport_countries.json`
    - `run_all.py` now supports:
      - `--route-scope all|domestic|international`
      - `--market-country <ISO2 or country-name>` (e.g., `BD`, `IN`, `Bangladesh`, `India`)
    - `generate_reports.py` and `generate_route_flight_fare_monitor.py` now support the same route-scope filters.
    - Multi-airline filters now accepted as comma-separated values in accumulation/report flows (e.g., `--airline BG,VQ`).
  - Dynamic date range selection (search horizon):
    - `run_all.py` now supports explicit departure-date range search:
      - `--date-start YYYY-MM-DD --date-end YYYY-MM-DD`
    - `config/dates.json` date config now supports explicit ranges in addition to lists and offsets.
    - `run_pipeline.py` forwards `--date-start/--date-end` and route-scope flags into accumulation + report steps.
  - Route monitor visual refinement:
    - Route blocks remain boxed top-to-bottom with thicker bottom boundary.
    - Data cells are no longer globally bold; emphasis is now kept primarily on arrows and subscript annotations for cleaner readability.
  - Unified intelligence output layer:
    - Added `tools/build_intelligence_hub.py` (forecast + competitive intelligence + ops status in one pack).
    - Added `run_pipeline.py --run-intelligence-hub` with controls:
      - `--intel-lookback-days`
      - `--intel-forecast-target`
    - Evidence:
      - `output/reports/intelligence_hub_latest.xlsx`
      - `output/reports/intelligence_overview_latest.md`
      - `output/reports/intelligence_competitive_latest.csv`
      - `output/reports/intelligence_route_summary_latest.csv`
  - Prediction ML v2 (pluggable with fallback):
    - `predict_next_day.py` now supports optional ML backends:
      - `--ml-models catboost,lightgbm`
      - `--ml-quantiles 0.1,0.5,0.9`
      - `--ml-min-history`
      - `--ml-random-seed`
    - Baseline models remain default and active fallback when ML libs are missing.
    - `run_pipeline.py` forwards ML options via:
      - `--prediction-ml-models`
      - `--prediction-ml-quantiles`
      - `--prediction-ml-min-history`
      - `--prediction-ml-random-seed`
  - Route report clarification for non-operating flights:
    - In `engines/output_writer.py`, blank cells for non-operating
      flight/date intersections are now rendered as `N/O` (plus `—` in other
      metric cells), avoiding confusion with missing data.
  - CXB-DAC 22-Feb validation note:
    - Validation against latest full accumulation pair confirms fares exist on `2026-02-22` for:
      - `VQ-928` and `VQ-936` (min fare observed `4,999`).
    - If `VQ-922` appears blank on `2026-02-22`, it is treated as non-operating for that date (not missing-route data).
  - run_all runtime optimization:
    - Removed per-row DB lookup for `flight_offer_id` during raw-meta linking.
    - Replaced with one bulk ID map load per search block (`scrape_id + airline + route + cabin`) and in-memory key matching.
    - Added matched/unmatched diagnostics in logs:
      - `Persisted X core rows + Y raw-meta rows (matched=M unmatched=U)`
    - Added comparison prefetch cache per route+cabin:
      - Preloads latest prior snapshots for all selected dates in one DB query and reuses in-memory map during loop.
      - Excludes current accumulation run from baseline snapshots to avoid self-comparison drift.
    - Normalized departure identity key between current parser rows and DB snapshots to improve match hit-rate in change comparison.
  - DB storage sustainability upgrades (no-delete / no-new-storage compliant):
    - Added read-only storage monitor: `tools/db_storage_health_check.py`
      - Reports DB size, top tables, disk free space, raw-meta growth runway estimate, and bloat heuristic.
    - Added lossless raw payload fingerprint + dedupe store:
      - New table: `raw_offer_payload_store` (fingerprint-keyed payload storage).
      - `flight_offer_raw_meta` now records `raw_offer_fingerprint` and `raw_offer_storage`.
      - Future duplicate payloads can be externalized while preserving one observation row per snapshot (time-series integrity retained).
    - Added raw-meta compaction tool + scheduler maintenance hook:
      - `tools/db_compact_raw_meta.py`
      - Optional weekly maintenance-window execution via `scheduler/maintenance_tasks.py --enable-db-compact-raw-meta`
    - Compatibility note:
      - `tools/backfill_raw_meta_fields.py` now supports deduped/externalized payload lookup via fingerprint reference.

### Final Project Completion Sign-Off

Mark project complete when all are true:

1. All Phase 1 checklist items are checked, or explicitly marked as closed by project owner directive with date, rationale, and evidence-gap note.
2. Any owner-directed manual closure keeps background evidence accumulation
   active until the original evidence target is met, with latest evidence file
   paths recorded.
3. At least 2 full weeks of stable scheduled execution logs exist for final operational sign-off.
4. Data quality report shows no critical nulls in mandatory fields for active scopes.
5. Baseline forecasting + backtest evidence is generated and archived.
6. Thesis-ready report package is reproducible from repository scripts.

### Storage Sustainability Decision (2026-02-22, No-Delete / No-New-Storage Constraint)

Project-owner constraint (confirmed):

- Raw historical data will not be deleted or archived away from active use
  because it is required for ML/DL training, backtesting, and future decision
  optimization (including "which fare at which time is optimal" analyses).
- Additional paid storage is not currently an option.

Therefore, storage strategy must focus on in-place efficiency and lossless preservation:

1. In-place compaction first (no data loss)
   - Treat table/index bloat reclamation as a required maintenance task (e.g., `VACUUM FULL` during maintenance window or `pg_repack` when available).
   - Priority target: `public.flight_offer_raw_meta` (dominant storage consumer).

2. Lossless raw-payload deduplication (preserve full training fidelity)
   - Store repeated identical raw payloads once (content-hash keyed), and reference them from observation rows.
   - This keeps reconstructability while reducing duplicate storage.

3. Lossless compression of raw payload fields
   - Compress large/volatile raw payload content (e.g., JSON payloads) before persistence or in a dedicated compressed column/table design.
   - Requirement: reversible (lossless) for audit and model reproducibility.

4. Partitioning for manageability (not retention deletion)
   - Partition large fact/raw tables by accumulation date/time to improve maintenance operations, targeted reindexing, and future scalability.
   - Partitioning is adopted for operational control, not for deleting training history.

5. Ingestion efficiency over unnecessary duplication
   - Reduce duplicate/raw writes caused by repeated unchanged snapshots where
     possible (e.g., snapshot fingerprinting, idempotent raw-write logic), while
     preserving time-series evidence required for forecasting validation.
   - Maintain a stable validation panel of departure dates for longitudinal comparison, and use dynamic windows as additive intelligence coverage.

6. DB observability + capacity forecasting must remain active
   - Track database size, per-table growth, bloat indicators, WAL usage, and disk free space.
   - Add pre-run / daily health checks and threshold alerts before storage becomes a blocking issue.

7. Non-DB cleanup remains allowed
   - Logs/reports/temp artifacts may still use retention cleanup policies, because they do not replace the canonical training history stored in PostgreSQL.

Implementation direction (next upgrades):

- Add DB storage health monitor (size + runway estimate + bloat heuristic).
- Add raw payload fingerprinting / dedupe design.
- Add partitioning plan for `flight_offer_raw_meta` and other high-growth tables.
- Add maintenance runbook step for compaction/reindex windows.

## 12) Enhancement Flexibility Note

The project remains intentionally open to modifications required for future enhancements.

Guiding rule:

- If a change improves data quality, reliability, coverage, research quality,
  or operational usability, it is allowed and should be integrated through
  controlled updates.

Change handling expectation:

1. Document the change intent in this file (or linked implementation note).
2. Apply schema/code/report updates as needed.
3. Re-run regression checks and data-quality validation.
4. Update completion checklist items if scope/timeline shifts.

## 13) Passenger-Mix Search Basis Decision (2026-02-23)

Finding (validated):

- Search results (fares and visible inventory) can change when passenger count
  changes (for example `ADT=1` vs `ADT=2`), including NOVOAIR and potentially
  other carriers.
- Therefore, search output reflects commercial inventory state for the requested party size, not a universal single-seat truth.

Decision:

1. Keep `ADT=1, CHD=0, INF=0` as the baseline time-series for continuity.
   - This preserves comparability with existing historical data and current forecasting/backtest evidence.

2. Treat passenger mix (`ADT/CHD/INF`) as a first-class search dimension.
   - Comparisons must use the same passenger mix basis.
   - Route-monitor comparisons across different passenger mixes are considered non-like-for-like and must be flagged.

3. Support optional probe searches (additive to baseline, not replacement).
   - Recommended probes: `ADT=2` for priority routes; `ADT=4` only for selective benchmark runs.
   - Purpose: detect fare-bucket release/closure behavior and party-size sensitivity.

4. Preserve passenger mix metadata in persisted raw-meta observations.
   - `flight_offer_raw_meta.adt_count/chd_count/inf_count` are stored and used for comparison-basis checks.

5. Reporting and ops visibility must show basis.
   - Runtime heartbeat and watcher include `pax=ADT/CHD/INF`.
   - Workbook methodology note warns to compare only runs with the same passenger mix.

Implementation status (2026-02-23):

- `run_all.py`, `run_pipeline.py`, `modules/biman.py`, `modules/novoair.py`, scheduler wrappers, and parallel runner support `--adt/--chd/--inf`.
- `tools/watch_run_status.py` displays passenger mix from heartbeat.
- `generate_route_flight_fare_monitor.py` warns when compared scrapes have mismatched passenger mix basis.

## 14) Route-Specific Modeling Focus Decision (2026-02-23)

Finding (validated on VQ route-specific two-stage baselines with `inventory_state_v2`):

- `DAC-SPD` is currently the only tested VQ route where the two-stage model
  beats the zero-delta baseline on RMSE after threshold tuning (under
  `min_move_delta=200`, `min_stage_b_moves=5`).
- `SPD-DAC` is sparse for Stage B at the production viability floor
  (`min_stage_b_moves=5`), and remains non-viable even when explored at a lower
  floor (`min_stage_b_moves=2`).
- `DAC-CXB` and `CXB-DAC` have enough rows to evaluate, but current two-stage
  modeling only ties (does not beat) the zero baseline.

Decision:

1. Focus near-term model development on `DAC-SPD` first.
   - Treat `DAC-SPD` as the primary route for model iteration, feature tuning,
     threshold tuning, and baseline improvement experiments.

2. Keep probe collection active on other priority routes.
   - Continue `ADT=1,2` (and selective `ADT=3/4`) probe collection for:
     `SPD-DAC`, `DAC-CXB`, `CXB-DAC` and other priority routes.
   - Purpose: accumulate more move events, improve route-level priors, and
     re-test viability later.

3. Preserve a production viability gate for route selection.
   - Default modeling gate remains `min_stage_b_moves=5` (production-like).
   - Lower floors (for example `min_stage_b_moves=2`) are allowed only for
     exploratory diagnostics and should not be used to declare production
     viability.

4. Use route-level viability flags for automatic route triage.
   - `beats_zero_rmse`
   - `beats_zero_mae`
   - `sparse_stage_b`

Operational consequence:

- Modeling work should proceed in two tracks:
  - Primary modeling track: `DAC-SPD`
  - Data accumulation / probe track: other priority routes until viability improves

## 15) Route Selection Policy Comparative Study Decision (2026-02-24)

Scope:

- Airline: `VQ`
- Routes: `DAC-SPD`, `SPD-DAC`, `DAC-CXB`, `CXB-DAC`
- Dataset/model settings: `ADT=1`, `min_move_delta=200`, `min_test_moves=1`,
  `min_stage_b_moves=5`, `route_rolling_folds=4`, Stage A=`RF (uncalibrated)`,
  Stage B=`Ridge`

Compared rolling viability policies:

1. `beats_zero_folds` (consistency gate)
2. `mean_rmse` (average-improvement gate)

Findings:

- `DAC-SPD` passes under both policies and remains the strongest route.
- `DAC-CXB` is promoted by `mean_rmse` but not by `beats_zero_folds`.
  - Interpretation: improvement exists on average, but fold consistency is not
    strong enough yet for production promotion.
- `SPD-DAC` remains blocked by `sparse_stage_b` at the production Stage B floor.
- `CXB-DAC` remains non-viable under both policies.

Decision (futureproof):

1. Use `beats_zero_folds` as the production route-selection gate.
   - Rationale: it requires repeatable fold-level evidence and avoids route
     promotion from a single favorable fold.

2. Use `mean_rmse` only as a watchlist signal.
   - Routes promoted only by `mean_rmse` (for example `DAC-CXB`) should remain
     in probe collection and be re-tested later.

3. Keep `route_model_priority` as the consolidated route triage label.
   - Production focus remains `DAC-SPD` (`candidate` under the production gate).

Artifacts:

- `output/reports/route_priority_policy_comparative_study_latest.md`
- `output/reports/route_priority_policy_comparative_study_latest.csv`
- `output/reports/route_priority_policy_comparative_study_latest.json`

Rerun note (2026-02-24, trigger-based review):

- Trigger met: `SPD-DAC` was no longer flagged `sparse_stage_b` at the
  production Stage B floor, so the comparative policy study was re-run using
  the same 4-route batch and the same model settings.
- New rerun artifacts:
  - `output/reports/route_priority_policy_comparative_study_20260224_134151.md`
  - `output/reports/route_priority_policy_comparative_study_20260224_134151.csv`
  - `output/reports/route_priority_policy_comparative_study_20260224_134151.json`
- Rerun outcome: policy difference remained consistent with the prior study.
  - Only `DAC-CXB` changed across policies (`watch` under
    `beats_zero_folds` vs `high` under `mean_rmse`).
  - `DAC-SPD`, `SPD-DAC`, and `CXB-DAC` were unchanged across policies.
- Decision unchanged: keep `beats_zero_folds` as the production gate and use
  `mean_rmse` as a watchlist-only signal.

## 16) DL Strategy Notes + Market Intelligence Expansion Hypotheses (2026-02-24)

Context:

- Current system intelligence is primarily built from point-to-point data
  accumulation and route-level inventory-state observations.
- This is sufficient for route-level fare/inventory behavior learning, but not
  sufficient for broader market-level intelligence without explicit market and
  passenger-behavior modeling layers.

Distribution channel reality (important):

- Airlines distribute inventory through multiple channels, including:
  - airline-owned direct platform (current accumulation source in this system)
  - GDS (Global Distribution Systems)
  - NDC channels
  - OTA / agency channels (typically consuming inventory through GDS and/or NDC)
- Therefore, observations collected from airline-owned platforms represent an
  important but partial view of commercial inventory behavior.
- Market intelligence expansion must explicitly consider channel effects:
  - channel-specific availability differences
  - channel-specific fare/distribution rules
  - channel mix effects on observed inventory state changes

Observed constraints / realities (important):

1. Inventory-state observations do not guarantee exact seat-sale interpretation.
   - A large change in visible seat availability may reflect:
     - actual sales
     - airline bucket release/closure strategy
     - inventory control changes for yield management
   - Therefore, seat/inventory changes must be modeled as observable state
     transitions, not direct sales truth.

2. Market behavior is not uniform across destinations/regions.
   - Bangladesh labor-market routes (especially Middle East) likely behave
     differently from travel/business markets (for example Thailand, Canada).
   - Travel intent and booking horizon materially affect fare/inventory dynamics.

3. Route/airport/market yield is heterogeneous.
   - Each airline likely has high-yield and low-yield markets based on:
     - airport
     - route
     - market/region
     - route combinations (hub-driven or feeder patterns)

4. Point-to-point only is not the final intelligence layer.
   - To gain stronger intelligence, the system must also model:
     - market-to-market variation
     - airline-to-airline demand variation
     - hub-and-spoke vs point-to-point operational behavior
     - route network dependency and spillover behavior (later phase)

Working market-behavior hypotheses (to validate with accumulated evidence):

1. Bangladesh labor-market outbound ticket demand may cluster near visa issuance
   windows.
   - Working hypothesis: demand concentration often occurs within approximately
     7 days, and in some cases within 10-14 days from visa day.
   - This is a hypothesis and requires evidence-backed validation.

2. Bangladesh labor-market return travel (Middle East -> Bangladesh) may have
   much longer booking horizons.
   - Working hypothesis: return tickets may be booked 3-8 months from arrival /
     travel planning context.
   - This must be studied as a distinct market pattern, not merged with leisure
     route assumptions.

3. Thailand/Canada routes from Bangladesh likely follow different demand curves
   from Middle East labor routes.
   - Likely drivers:
     - tourism
     - business travel
     - mixed-purpose discretionary demand

4. A general airline-behavior layer can still be studied from 30-45 day
   observation windows.
   - Use 30-45 day alteration/transition patterns as a broad behavior lens, but
     do not assume this fully captures all labor-market long-horizon effects.

Decision: intelligence modeling must expand beyond route-only forecasting

1. Keep route-level inventory-state modeling as the base layer.
   - Continue route-specific route-gated forecasting and probe-based learning.

2. Add market-level segmentation as a formal modeling dimension.
   - Route and airline behavior should be analyzed by:
     - market/region class
     - travel purpose proxy (labor / leisure / business / mixed)
     - route directionality (outbound vs inbound)

3. Treat inventory availability changes as state signals, not sales truth.
   - Large seat changes should be interpreted as possible inventory policy moves
     unless corroborated by additional evidence.

DL strategy roadmap (exact method names, future-facing but grounded):

Current production research baseline (not DL):

- Inventory-State Modeling
- Passenger-Size Probe Analysis
- Two-Stage Route-Gated Forecasting

Planned DL path (ordered):

1. Temporal Convolutional Network (TCN) for route-level sequence modeling
   (recommended first DL model)
   - Input: ordered inventory-state snapshots (`inventory_state_v2`) with
     passenger-size probe priors and temporal covariates.
   - Targets:
     - next-move probability (move vs no-move)
     - next fare delta / quantile estimates
     - bucket/pressure transition risk
   - Reason:
     - strong for structured temporal sequences
     - lower operational/training complexity than transformer models at current
       dataset scale

2. Temporal Fusion Transformer (TFT) for multi-horizon forecasting
   (later, after more data accumulation)
   - Use when:
     - route-level and market-level covariates are richer
     - multi-horizon forecasts become a priority
     - interpretability across covariates is needed

3. Survival / hazard modeling (DL or hybrid) for event timing
   - Focus:
     - time-to-next-fare-jump
     - time-to-bucket-closure
     - time-to-high-pressure regime
   - This is highly aligned with timing decisions and revenue-management style
     intelligence.

4. Network-aware / graph-based modeling (later research phase)
   - Use only after market/route dependency evidence is sufficient.
   - Candidate use cases:
     - hub-spoke effects
     - competitor spillover
     - multi-route market interactions

Required data/feature expansion for DL (future steps, not immediate patching):

1. Market classification layer
   - route -> market/region label
   - outbound/inbound directionality
   - demand-purpose proxy class

2. Horizon-pattern features
   - labor-like long-horizon return behavior vs short-horizon outbound behavior
   - 30-45 day transition summaries + longer-horizon route windows where needed

3. Yield segmentation features
   - high-yield / low-yield route and airport combinations by airline
   - route-level and market-level behavior priors

4. Inventory-policy uncertainty flags
   - mark sudden availability changes as potential release/closure events
   - do not automatically treat them as sold-seat events

Decision rule (important for future work):

- Do not label titles/docs as "DL-powered" until a concrete DL model
  (for example TCN) is implemented, trained, and evaluated on the current route
  selection framework.
- Until then, describe the system as:
  - Inventory-State Modeling + Passenger-Size Probe Analysis + Route-Gated ML
    Forecasting

## 17) Prediction Execution Plan (Current -> DL)

Purpose:

- Define the explicit prediction roadmap (not just tooling/components) with
  stage gates, acceptance criteria, and estimated timelines.

Current stage (as of 2026-02-24):

- Stage `P2/P3` (active):
  - `P2`: route-level ML prediction baseline iteration on selected routes
  - `P3`: continued probe evidence accumulation + route-promotion gating for
    watch routes
- Current production route gate (fixed): `beats_zero_folds`
- Current primary model route focus: `VQ DAC-SPD` (route priority may move based
  on fresh weekly evidence; gate remains fixed)

Prediction objective hierarchy (exact, current-first):

1. Near-term route-level prediction (current implementation target)
   - Predict next state transitions from `inventory_state_v2`:
     - move vs no-move
     - next fare delta
     - inventory pressure movement
   - Use route-gated acceptance (rolling viability vs zero baseline).

2. Route-selection and route-promotion intelligence (current operational target)
   - Decide which routes are model-ready vs watch/hold using rolling-fold
     evidence and sparse-route gating.

3. Market-level forecasting and intelligence (future expansion)
   - Add market segmentation, directionality, yield class, and route-family
     behavior priors.

4. DL sequence forecasting (future, after stable route-level ML evidence)
   - Start with TCN on inventory-state sequences, then TFT if justified by data
     scale and covariate richness.

Stage plan (with gates and estimated timeline):

### P1. Stable Evidence Accumulation and Probe Discipline (foundation)

Goal:

- Keep accumulation/probe semantics fixed long enough to build comparable
  evidence.

Status:

- Mostly complete / operationalized

Required outputs:

- Stable `ADT=1` baseline accumulation
- Probe-group runs (`ADT=1,2,3`) with `probe_group_id`
- `inventory_state_v2` rebuild cadence
- Weekly route batch evaluation using fixed gate

Exit gate:

- Weekly cadence runs cleanly for at least 1-2 cycles without policy churn
- Route viability artifacts are reproducible from the same settings

Estimated duration:

- 1-2 weeks (already in progress / partially complete)

### P2. Route-Level ML Baseline Prediction (route-gated)

Goal:

- Establish at least one route with repeatable route-gated ML prediction
  evidence (rolling-fold viability) under the fixed gate.

Current approach:

- Two-stage route-gated ML baseline:
  - Stage A: move/no-move classifier
  - Stage B: delta regressor on moved rows
  - Threshold sweep with zero-baseline comparison

Acceptance criteria (primary):

- `rolling_viable_rmse = True` under fixed gate (`beats_zero_folds`)
- Route is not sparse at production floor (`min_stage_b_moves=5`) or sparse
  behavior is explicitly accepted for exploratory mode only
- Route receives `route_model_priority in {candidate, high}`

Secondary checks:

- `rolling_viable_mae` (diagnostic/quality guardrail)
- Feature impact visibility confirms model is using plausible signals

Estimated duration:

- 2-4 weeks (depends on move density and route evidence accumulation)

### P3. Route Promotion Loop (watch routes -> candidate/high)

Goal:

- Promote additional routes from `watch/hold` to `candidate/high` using the
  same fixed route-selection policy.

Current watch routes:

- `VQ SPD-DAC`
- `VQ DAC-CXB`
- `VQ CXB-DAC`

Operating rule:

- Continue probe collection; do not change the route gate to force promotion.

Acceptance criteria:

- Route clears sparse constraints consistently
- Route beats zero baseline on rolling RMSE under fixed gate
- Route priority upgrades via `route_model_priority`

Estimated duration:

- 3-8 weeks (route-specific, evidence-dependent)

### P4. Market-Level Intelligence and Forecasting Expansion

Goal:

- Move beyond point-to-point route-only inference into market-level behavior and
  route-family/airline comparisons.

Required additions:

- Market classification layer
- Directionality / purpose proxies (labor / leisure / business / mixed)
- Yield segmentation priors (high-yield / low-yield route+airport combinations)
- Channel-awareness roadmap (direct platform vs GDS/NDC/OTA differences)

Prediction outputs (future):

- market-level fare pressure tendencies
- route family behavior priors
- airline strategy differences by market segment

Estimated duration:

- 4-10 weeks after at least one route-level ML path is stable

### P5. DL Sequence Forecasting (TCN first)

Goal:

- Add a concrete DL model that improves or complements the route-level ML
  baseline on sequence prediction tasks.

First DL implementation target:

- Temporal Convolutional Network (TCN)

Candidate tasks:

- next-move probability
- next fare delta / quantile
- pressure regime transition risk

Required gate before DL build:

- At least one route shows stable route-gated ML evidence across repeated
  weekly cycles
- Sufficient sequence depth and move-event density exist for the target route

DL success criteria (initial):

- Beats current ML baseline on at least one primary metric for the chosen route
  and horizon, or
- Provides materially better calibration / transition-risk prediction while
  maintaining operational interpretability

Estimated duration:

- 4-8 weeks after DL start (depending on data volume and sequence quality)

Overall estimated completion windows (pragmatic):

1. Operational/ML thesis MVP (route-gated ML + evidence discipline)
   - ~3-6 weeks

2. Thesis-ready ML baseline package (multi-route evidence + route-selection
   methodology)
   - ~6-10 weeks

3. First DL prototype (TCN, route-level)
   - ~10-16 weeks total timeline (including evidence buildup and ML baseline
     stabilization)

Decision (prediction roadmap):

- Do not add more feature families by default until the weekly cadence confirms
  stable route evidence progression or a route promotion event occurs.
- Prioritize evidence quality, gate stability, and route selection discipline
  over feature expansion.


