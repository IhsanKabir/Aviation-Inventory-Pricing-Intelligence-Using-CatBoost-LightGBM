-- Generic Looker-ready BigQuery views for Aero Pulse Intelligence Platform.

CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.__DATASET__`;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_cycle_health` AS
SELECT
  cycle_id,
  cycle_started_at_utc,
  cycle_completed_at_utc,
  offer_rows,
  airline_count,
  route_count,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), cycle_completed_at_utc, MINUTE) AS cycle_age_minutes
FROM `__PROJECT_ID__.__DATASET__.fact_cycle_run`;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_route_daily_fare` AS
SELECT
  DATE(captured_at_utc) AS report_day,
  airline,
  origin,
  destination,
  route_key,
  cabin,
  COUNT(*) AS offer_rows,
  MIN(total_price_bdt) AS min_total_price_bdt,
  AVG(total_price_bdt) AS avg_total_price_bdt,
  MAX(total_price_bdt) AS max_total_price_bdt,
  AVG(tax_amount) AS avg_tax_amount,
  AVG(load_factor_pct) AS avg_load_factor_pct,
  SUM(CASE WHEN soldout THEN 1 ELSE 0 END) AS soldout_rows
FROM `__PROJECT_ID__.__DATASET__.fact_offer_snapshot`
GROUP BY report_day, airline, origin, destination, route_key, cabin;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_change_activity_daily` AS
SELECT
  report_day,
  airline,
  origin,
  destination,
  route_key,
  domain,
  change_type,
  direction,
  field_name,
  COUNT(*) AS event_count,
  AVG(magnitude) AS avg_magnitude,
  AVG(percent_change) AS avg_percent_change
FROM `__PROJECT_ID__.__DATASET__.fact_change_event`
GROUP BY report_day, airline, origin, destination, route_key, domain, change_type, direction, field_name;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_penalty_reference` AS
SELECT
  cycle_id,
  DATE(captured_at_utc) AS report_day,
  airline,
  origin,
  destination,
  route_key,
  flight_number,
  departure_utc,
  cabin,
  fare_basis,
  penalty_source,
  penalty_currency,
  fare_change_fee_before_24h,
  fare_change_fee_within_24h,
  fare_change_fee_no_show,
  fare_cancel_fee_before_24h,
  fare_cancel_fee_within_24h,
  fare_cancel_fee_no_show,
  fare_changeable,
  fare_refundable,
  penalty_rule_text
FROM `__PROJECT_ID__.__DATASET__.fact_penalty_snapshot`;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_tax_reference` AS
SELECT
  cycle_id,
  DATE(captured_at_utc) AS report_day,
  airline,
  origin,
  destination,
  route_key,
  flight_number,
  departure_utc,
  cabin,
  fare_basis,
  tax_amount,
  currency
FROM `__PROJECT_ID__.__DATASET__.fact_tax_snapshot`;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_forecast_model_latest` AS
WITH latest_bundle AS (
  SELECT bundle_id
  FROM `__PROJECT_ID__.__DATASET__.fact_forecast_bundle`
  QUALIFY ROW_NUMBER() OVER (ORDER BY bundle_created_at_utc DESC, stamp DESC, bundle_name DESC) = 1
)
SELECT
  b.bundle_name,
  b.target,
  b.stamp,
  b.bundle_created_at_utc,
  e.model,
  e.n,
  e.mae,
  e.rmse,
  e.mape_pct,
  e.smape_pct,
  e.directional_accuracy_pct,
  e.f1_macro
FROM `__PROJECT_ID__.__DATASET__.fact_forecast_model_eval` e
JOIN `__PROJECT_ID__.__DATASET__.fact_forecast_bundle` b
  ON b.bundle_id = e.bundle_id
JOIN latest_bundle lb
  ON lb.bundle_id = e.bundle_id;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_forecast_route_latest` AS
WITH latest_bundle AS (
  SELECT bundle_id
  FROM `__PROJECT_ID__.__DATASET__.fact_forecast_bundle`
  QUALIFY ROW_NUMBER() OVER (ORDER BY bundle_created_at_utc DESC, stamp DESC, bundle_name DESC) = 1
)
SELECT
  b.bundle_name,
  b.target,
  b.stamp,
  b.bundle_created_at_utc,
  r.airline,
  r.origin,
  r.destination,
  r.route_key,
  r.cabin,
  r.model,
  r.n,
  r.mae,
  r.rmse,
  r.mape_pct,
  r.smape_pct,
  r.directional_accuracy_pct,
  r.f1_macro
FROM `__PROJECT_ID__.__DATASET__.fact_forecast_route_eval` r
JOIN `__PROJECT_ID__.__DATASET__.fact_forecast_bundle` b
  ON b.bundle_id = r.bundle_id
JOIN latest_bundle lb
  ON lb.bundle_id = r.bundle_id;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_forecast_next_day_latest` AS
WITH latest_bundle AS (
  SELECT bundle_id
  FROM `__PROJECT_ID__.__DATASET__.fact_forecast_bundle`
  QUALIFY ROW_NUMBER() OVER (ORDER BY bundle_created_at_utc DESC, stamp DESC, bundle_name DESC) = 1
)
SELECT
  b.bundle_name,
  b.target,
  b.stamp,
  b.bundle_created_at_utc,
  n.latest_report_day,
  n.predicted_for_day,
  n.history_days,
  n.airline,
  n.origin,
  n.destination,
  n.route_key,
  n.cabin,
  n.latest_actual_value,
  n.pred_last_value,
  n.pred_rolling_mean_3,
  n.pred_rolling_mean_7,
  n.pred_seasonal_naive_7,
  n.pred_ewm_alpha_0_30,
  n.pred_dl_mlp_q10,
  n.pred_dl_mlp_q50,
  n.pred_dl_mlp_q90,
  n.pred_ml_catboost_q10,
  n.pred_ml_catboost_q50,
  n.pred_ml_catboost_q90,
  n.pred_ml_lightgbm_q10,
  n.pred_ml_lightgbm_q50,
  n.pred_ml_lightgbm_q90
FROM `__PROJECT_ID__.__DATASET__.fact_forecast_next_day` n
JOIN `__PROJECT_ID__.__DATASET__.fact_forecast_bundle` b
  ON b.bundle_id = n.bundle_id
JOIN latest_bundle lb
  ON lb.bundle_id = n.bundle_id;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_backtest_eval_latest` AS
WITH latest_backtest AS (
  SELECT bundle_id
  FROM `__PROJECT_ID__.__DATASET__.fact_forecast_bundle`
  WHERE has_backtest_eval
  QUALIFY ROW_NUMBER() OVER (ORDER BY bundle_created_at_utc DESC, stamp DESC, bundle_name DESC) = 1
)
SELECT
  b.bundle_name,
  b.target,
  b.stamp,
  b.bundle_created_at_utc,
  b.backtest_status,
  b.backtest_split_count,
  e.split_id,
  e.dataset,
  e.model,
  e.selected_on_val,
  e.n,
  e.mae,
  e.rmse,
  e.mape_pct,
  e.smape_pct,
  e.directional_accuracy_pct,
  e.f1_macro,
  e.train_start,
  e.train_end,
  e.val_start,
  e.val_end,
  e.test_start,
  e.test_end
FROM `__PROJECT_ID__.__DATASET__.fact_backtest_eval` e
JOIN `__PROJECT_ID__.__DATASET__.fact_forecast_bundle` b
  ON b.bundle_id = e.bundle_id
JOIN latest_backtest lb
  ON lb.bundle_id = e.bundle_id;
