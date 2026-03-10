# BigQuery Warehouse Plan

## Purpose

BigQuery is the hosted analytics and read layer for this platform. Local PostgreSQL stays operational for ingestion, comparisons, and ML/DL training; BigQuery holds curated fact tables for the website, Looker Studio, thesis analysis, and longer-range query workloads.

## Chosen BI Layer

- BigQuery sandbox for storage and SQL analytics
- Looker Studio for dashboards

This is the strongest free-to-start combination for:

- query management
- portfolio visibility
- dashboard delivery
- thesis-friendly analytics outputs

## Curated Tables

- `dim_airline`
- `dim_route`
- `fact_cycle_run`
- `fact_offer_snapshot`
- `fact_change_event`
- `fact_penalty_snapshot`
- `fact_tax_snapshot`
- `fact_forecast_bundle`
- `fact_forecast_model_eval`
- `fact_forecast_route_eval`
- `fact_forecast_route_winner`
- `fact_forecast_next_day`
- `fact_backtest_eval`
- `fact_backtest_route_winner`
- `fact_backtest_split`

`fact_offer_snapshot` now includes round-trip route-monitor fields:

- `search_trip_type`
- `trip_request_id`
- `requested_outbound_date`
- `requested_return_date`
- `trip_duration_days`
- `trip_origin`
- `trip_destination`
- `trip_pair_key`
- `leg_direction`
- `leg_sequence`
- `itinerary_leg_count`

## Export Contract

Source of truth for export layout:

- [sql/bigquery/create_analytics_tables.sql](../../sql/bigquery/create_analytics_tables.sql)
- [sql/bigquery/create_analytics_views.sql](../../sql/bigquery/create_analytics_views.sql)
- [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql)
- [sql/bigquery/create_aviation_intel_dataset.sql](../../sql/bigquery/create_aviation_intel_dataset.sql)
- [sql/bigquery/create_aviation_intel_tables.sql](../../sql/bigquery/create_aviation_intel_tables.sql)
- [sql/bigquery/create_aviation_intel_looker_views.sql](../../sql/bigquery/create_aviation_intel_looker_views.sql)
- [tools/export_bigquery_stage.py](../../tools/export_bigquery_stage.py)
- [warehouse/bigquery/BOOTSTRAP_CHECKLIST.md](BOOTSTRAP_CHECKLIST.md)

## Step-by-Step Setup

1. Create a Google Cloud project.
2. Enable BigQuery API.
3. Create dataset `aviation_intel` for this platform.
4. Create a service account with BigQuery Data Editor access for that dataset.
5. Point `GOOGLE_APPLICATION_CREDENTIALS` to the service account JSON locally.
6. Run the concrete dataset bootstrap SQL.
7. Run the local export staging command.
8. Load staged parquet files into BigQuery.
9. Create Looker-facing views.
10. Point hosted API reads to BigQuery-backed tables/views.
11. Connect Looker Studio to the curated dataset views.

## Local Export Example

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07
```

## Optional Direct Load Example

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07 --load-bigquery --project-id your-gcp-project --dataset aviation_intel
```

## Schema Note

If `fact_offer_snapshot` already exists in BigQuery, add the new round-trip columns before the next append load or rerun the bootstrap SQL against a fresh table set. The canonical schema is in [sql/bigquery/create_aviation_intel_tables.sql](../../sql/bigquery/create_aviation_intel_tables.sql).

For a live additive patch, run [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql) first. It covers:

- round-trip route-monitor columns on `fact_offer_snapshot`
- `via_airports` on `fact_offer_snapshot`
- forecast bundle flags that older live tables may still be missing

## Live Reload Path

For the current production dataset:

1. Open BigQuery SQL workspace and run [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql)
2. Reload the recent window with the loader helper:

```powershell
.\tools\load_bigquery_latest.ps1 -CredentialsJson "C:\path\to\aero-pulse-bq-loader.json" -StartDate 2026-03-03 -EndDate 2026-03-10
```

3. Validate the new column has data:

```sql
SELECT
  COUNTIF(via_airports IS NOT NULL AND via_airports != '') AS rows_with_via_airports,
  COUNTIF(search_trip_type = 'RT') AS round_trip_rows
FROM `aeropulseintelligence.aviation_intel.fact_offer_snapshot`;
```

4. Validate hosted operations reads:

```sql
SELECT
  route_key,
  airline,
  via_airports,
  stops,
  departure_date
FROM `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
WHERE via_airports IS NOT NULL
ORDER BY captured_at_utc DESC
LIMIT 50;
```
