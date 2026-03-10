-- Additive schema patch for an existing aviation_intel dataset.
-- Safe to run on live BigQuery tables before append loads.

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS via_airports STRING;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS search_trip_type STRING;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS trip_request_id STRING;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS requested_outbound_date DATE;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS requested_return_date DATE;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS trip_duration_days INT64;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS trip_origin STRING;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS trip_destination STRING;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS trip_pair_key STRING;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS leg_direction STRING;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS leg_sequence INT64;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
ADD COLUMN IF NOT EXISTS itinerary_leg_count INT64;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_forecast_bundle`
ADD COLUMN IF NOT EXISTS has_route_winner BOOL;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_forecast_bundle`
ADD COLUMN IF NOT EXISTS has_backtest_route_eval BOOL;

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_forecast_bundle`
ADD COLUMN IF NOT EXISTS has_backtest_route_winner BOOL;

-- Quick post-patch checks.
SELECT
  table_name,
  column_name,
  data_type
FROM `aeropulseintelligence.aviation_intel.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN ("fact_offer_snapshot", "fact_forecast_bundle")
  AND column_name IN (
    "via_airports",
    "search_trip_type",
    "trip_request_id",
    "requested_outbound_date",
    "requested_return_date",
    "trip_duration_days",
    "trip_origin",
    "trip_destination",
    "trip_pair_key",
    "leg_direction",
    "leg_sequence",
    "itinerary_leg_count",
    "has_route_winner",
    "has_backtest_route_eval",
    "has_backtest_route_winner"
  )
ORDER BY table_name, column_name;
