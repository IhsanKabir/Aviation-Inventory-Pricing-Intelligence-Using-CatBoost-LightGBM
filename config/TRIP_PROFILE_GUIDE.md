# Trip Profile Guide

This guide explains where trip-search behavior is defined and where it is activated.

## File Roles

`dates.json`
- Global default outbound date engine.
- Use this for default day offsets, default date ranges, and other broad outbound-date seeds.

`market_priors.json`
- Reusable business templates.
- This file defines profile behavior, but does not activate routes by itself.

`route_trip_windows.json`
- Actual route-by-route execution control.
- This is the file you edit to turn profiles on or off for an airline and route.

## Activation Model

For each route, these keys matter:

`market_trip_profiles`
- All candidate profiles available for that route.
- Think of this as the menu of possible behaviors.

`active_market_trip_profiles`
- Profiles that are ON for `operational` runs.
- This is the main on/off switch for live collection behavior.

`training_market_trip_profiles`
- Extra profiles used only in `training` mode.
- These do not affect normal operational collection unless also included in `active_market_trip_profiles`.

## Common Profile Meanings

`default_one_way_monitoring`
- Normal one-way monitoring.
- Current default outbound offsets: `0, 3, 5, 7, 15, 30`

`bangladesh_domestic_round_trip_short`
- Normal Bangladesh domestic round-trip behavior.
- Current return offset: `+2 days`

`bangladesh_domestic_eid_round_trip_2026`
- Eid-focused domestic round-trip window.
- Uses exact outbound and return date ranges around Eid.

`bangladesh_domestic_eid_capital_outbound_one_way_2026`
- One-way Dhaka to domestic flows before Eid.

`bangladesh_domestic_eid_capital_return_one_way_2026`
- One-way domestic to Dhaka flows after Eid.

`regional_round_trip_flexible`
- Short and medium regional return windows.

`worker_visa_outbound_to_middle_east_one_way`
- One-way worker/visa travel from South Asia to the Middle East.

`worker_return_from_middle_east_long_window`
- Long-window return behavior from the Middle East back to South Asia.

`hub_spoke_or_longhaul_return_window`
- Wider return windows for hub-spoke and long-haul routes.

`tourism_bkk_can_round_trip`
- Tourism-oriented return behavior for Bangkok and Guangzhou style routes.

`inventory_anchor_departure_tracking_default`
- Training-only inventory anchor profile.
- Used to repeatedly observe the same departure horizon for inventory movement analysis.

## Practical Examples

### Turn on normal one-way only

In `route_trip_windows.json`:

```json
"active_market_trip_profiles": [
  "default_one_way_monitoring"
]
```

### Turn on normal one-way and domestic round-trip

```json
"active_market_trip_profiles": [
  "default_one_way_monitoring",
  "bangladesh_domestic_round_trip_short"
]
```

### Keep operational small, but make training richer

```json
"active_market_trip_profiles": [
  "default_one_way_monitoring"
],
"training_market_trip_profiles": [
  "bangladesh_domestic_round_trip_short",
  "inventory_anchor_departure_tracking_default"
]
```

## Rule of Thumb

If you want to change what actually runs for a route:
- edit `route_trip_windows.json`

If you want to change what a profile means:
- edit `market_priors.json`

If you want to change the default outbound date universe:
- edit `dates.json`

## Current Operational Pattern

The current design separates:
- `operational` for comparison-safe live cycles
- `training` for enrichment, holiday overlays, and inventory-anchor behavior

Use `active_market_trip_profiles` conservatively if runtime is important.

At the moment, the intended common operational baseline is:
- `default_one_way_monitoring` for one-way coverage across routes
- route-specific round-trip profiles layered on top where applicable
