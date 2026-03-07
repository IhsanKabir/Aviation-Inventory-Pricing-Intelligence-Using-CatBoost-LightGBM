-- BigQuery curated analytics tables for Aero Pulse Intelligence Platform
-- Concrete BigQuery table bootstrap for project aeropulseintelligence and dataset aviation_intel.

CREATE SCHEMA IF NOT EXISTS `aeropulseintelligence.aviation_intel`;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.dim_airline` (
  airline STRING NOT NULL,
  first_seen_at_utc TIMESTAMP,
  last_seen_at_utc TIMESTAMP,
  offer_rows INT64,
  latest_cycle_id STRING
)
CLUSTER BY airline;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.dim_route` (
  route_key STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  first_seen_at_utc TIMESTAMP,
  last_seen_at_utc TIMESTAMP,
  offer_rows INT64,
  airlines_present INT64
)
CLUSTER BY origin, destination;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_cycle_run` (
  cycle_id STRING NOT NULL,
  cycle_started_at_utc TIMESTAMP,
  cycle_completed_at_utc TIMESTAMP,
  offer_rows INT64,
  airline_count INT64,
  route_count INT64
)
PARTITION BY DATE(cycle_completed_at_utc)
CLUSTER BY cycle_id;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_offer_snapshot` (
  cycle_id STRING NOT NULL,
  captured_at_utc TIMESTAMP NOT NULL,
  airline STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  route_key STRING NOT NULL,
  flight_number STRING NOT NULL,
  departure_utc TIMESTAMP NOT NULL,
  departure_date DATE NOT NULL,
  cabin STRING,
  brand STRING,
  fare_basis STRING,
  total_price_bdt NUMERIC,
  base_fare_amount NUMERIC,
  tax_amount NUMERIC,
  currency STRING,
  seat_available INT64,
  seat_capacity INT64,
  load_factor_pct NUMERIC,
  booking_class STRING,
  baggage STRING,
  aircraft STRING,
  duration_min INT64,
  stops INT64,
  soldout BOOL,
  penalty_source STRING
)
PARTITION BY DATE(captured_at_utc)
CLUSTER BY airline, origin, destination, departure_date;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_change_event` (
  cycle_id STRING,
  previous_cycle_id STRING,
  detected_at_utc TIMESTAMP NOT NULL,
  report_day DATE NOT NULL,
  airline STRING NOT NULL,
  origin STRING,
  destination STRING,
  route_key STRING,
  flight_number STRING,
  departure_day DATE,
  departure_time TIME,
  cabin STRING,
  fare_basis STRING,
  brand STRING,
  domain STRING,
  change_type STRING,
  direction STRING,
  field_name STRING,
  old_value STRING,
  new_value STRING,
  magnitude NUMERIC,
  percent_change NUMERIC,
  event_meta STRING
)
PARTITION BY report_day
CLUSTER BY airline, route_key, domain, field_name;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_penalty_snapshot` (
  cycle_id STRING NOT NULL,
  captured_at_utc TIMESTAMP NOT NULL,
  airline STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  route_key STRING NOT NULL,
  flight_number STRING NOT NULL,
  departure_utc TIMESTAMP NOT NULL,
  cabin STRING,
  fare_basis STRING,
  penalty_source STRING,
  penalty_currency STRING,
  fare_change_fee_before_24h NUMERIC,
  fare_change_fee_within_24h NUMERIC,
  fare_change_fee_no_show NUMERIC,
  fare_cancel_fee_before_24h NUMERIC,
  fare_cancel_fee_within_24h NUMERIC,
  fare_cancel_fee_no_show NUMERIC,
  fare_changeable BOOL,
  fare_refundable BOOL,
  penalty_rule_text STRING
)
PARTITION BY DATE(captured_at_utc)
CLUSTER BY airline, origin, destination;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_tax_snapshot` (
  cycle_id STRING NOT NULL,
  captured_at_utc TIMESTAMP NOT NULL,
  airline STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  route_key STRING NOT NULL,
  flight_number STRING NOT NULL,
  departure_utc TIMESTAMP NOT NULL,
  cabin STRING,
  fare_basis STRING,
  tax_amount NUMERIC,
  currency STRING
)
PARTITION BY DATE(captured_at_utc)
CLUSTER BY airline, origin, destination;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_forecast_bundle` (
  bundle_id STRING NOT NULL,
  bundle_name STRING NOT NULL,
  bundle_dir STRING NOT NULL,
  target STRING NOT NULL,
  stamp STRING NOT NULL,
  bundle_created_at_utc TIMESTAMP,
  has_overall_eval BOOL,
  has_route_eval BOOL,
  has_next_day BOOL,
  has_backtest_eval BOOL,
  has_backtest_splits BOOL,
  has_backtest_meta BOOL,
  target_column STRING,
  backtest_status STRING,
  backtest_split_count INT64,
  backtest_selection_metric STRING
)
PARTITION BY DATE(bundle_created_at_utc)
CLUSTER BY target, bundle_name;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_forecast_model_eval` (
  bundle_id STRING NOT NULL,
  bundle_name STRING NOT NULL,
  target STRING NOT NULL,
  stamp STRING NOT NULL,
  bundle_created_at_utc TIMESTAMP,
  model STRING NOT NULL,
  n INT64,
  mae NUMERIC,
  rmse NUMERIC,
  mape_pct NUMERIC,
  smape_pct NUMERIC,
  n_directional INT64,
  directional_accuracy_pct NUMERIC,
  f1_up NUMERIC,
  f1_down NUMERIC,
  f1_macro NUMERIC
)
PARTITION BY DATE(bundle_created_at_utc)
CLUSTER BY target, model;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_forecast_route_eval` (
  bundle_id STRING NOT NULL,
  bundle_name STRING NOT NULL,
  target STRING NOT NULL,
  stamp STRING NOT NULL,
  bundle_created_at_utc TIMESTAMP,
  airline STRING,
  origin STRING,
  destination STRING,
  route_key STRING,
  cabin STRING,
  model STRING NOT NULL,
  n INT64,
  mae NUMERIC,
  rmse NUMERIC,
  mape_pct NUMERIC,
  smape_pct NUMERIC,
  n_directional INT64,
  directional_accuracy_pct NUMERIC,
  f1_up NUMERIC,
  f1_down NUMERIC,
  f1_macro NUMERIC
)
PARTITION BY DATE(bundle_created_at_utc)
CLUSTER BY target, airline, route_key, model;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_forecast_next_day` (
  bundle_id STRING NOT NULL,
  bundle_name STRING NOT NULL,
  target STRING NOT NULL,
  stamp STRING NOT NULL,
  bundle_created_at_utc TIMESTAMP,
  latest_report_day DATE,
  predicted_for_day DATE,
  history_days INT64,
  airline STRING,
  origin STRING,
  destination STRING,
  route_key STRING,
  cabin STRING,
  latest_actual_value NUMERIC,
  pred_last_value NUMERIC,
  pred_rolling_mean_3 NUMERIC,
  pred_rolling_mean_7 NUMERIC,
  pred_seasonal_naive_7 NUMERIC,
  pred_ewm_alpha_0_30 NUMERIC,
  pred_dl_mlp_q10 NUMERIC,
  pred_dl_mlp_q50 NUMERIC,
  pred_dl_mlp_q90 NUMERIC,
  pred_ml_catboost_q10 NUMERIC,
  pred_ml_catboost_q50 NUMERIC,
  pred_ml_catboost_q90 NUMERIC,
  pred_ml_lightgbm_q10 NUMERIC,
  pred_ml_lightgbm_q50 NUMERIC,
  pred_ml_lightgbm_q90 NUMERIC
)
PARTITION BY predicted_for_day
CLUSTER BY target, airline, route_key;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_backtest_eval` (
  bundle_id STRING NOT NULL,
  bundle_name STRING NOT NULL,
  target STRING NOT NULL,
  stamp STRING NOT NULL,
  bundle_created_at_utc TIMESTAMP,
  split_id INT64,
  dataset STRING,
  model STRING NOT NULL,
  selected_on_val BOOL,
  n INT64,
  mae NUMERIC,
  rmse NUMERIC,
  mape_pct NUMERIC,
  smape_pct NUMERIC,
  n_directional INT64,
  directional_accuracy_pct NUMERIC,
  f1_up NUMERIC,
  f1_down NUMERIC,
  f1_macro NUMERIC,
  train_start DATE,
  train_end DATE,
  val_start DATE,
  val_end DATE,
  test_start DATE,
  test_end DATE
)
PARTITION BY DATE(bundle_created_at_utc)
CLUSTER BY target, dataset, model;

CREATE TABLE IF NOT EXISTS `aeropulseintelligence.aviation_intel.fact_backtest_split` (
  bundle_id STRING NOT NULL,
  bundle_name STRING NOT NULL,
  target STRING NOT NULL,
  stamp STRING NOT NULL,
  bundle_created_at_utc TIMESTAMP,
  split_id INT64,
  train_start DATE,
  train_end DATE,
  val_start DATE,
  val_end DATE,
  test_start DATE,
  test_end DATE,
  train_rows INT64,
  val_rows INT64,
  test_rows INT64,
  selected_model STRING
)
PARTITION BY DATE(bundle_created_at_utc)
CLUSTER BY target, split_id;


