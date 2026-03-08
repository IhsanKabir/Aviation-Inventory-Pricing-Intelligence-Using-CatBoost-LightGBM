export type CycleSummary = {
  cycle_id: string | null;
  cycle_started_at_utc?: string;
  cycle_completed_at_utc?: string;
  offer_rows?: number;
  airline_count?: number;
  route_count?: number;
};

export type HealthPayload = {
  database_ok: boolean;
  latest_cycle_id: string | null;
  latest_cycle_completed_at_utc?: string | null;
};

export type CycleHealthPayload = {
  database_ok: boolean;
  cycle_id: string | null;
  cycle_started_at_utc?: string | null;
  cycle_completed_at_utc?: string | null;
  cycle_age_minutes?: number | null;
  stale: boolean;
  offer_rows?: number | null;
  airline_count?: number | null;
  route_count?: number | null;
  configured_route_pair_count: number;
  observed_route_pair_count: number;
  route_pair_coverage_pct: number;
  missing_route_pairs: string[];
  latest_run_status?: {
    state?: string | null;
    phase?: string | null;
    overall_query_total?: number | null;
    overall_query_completed?: number | null;
    total_rows_accumulated?: number | null;
    completed_at_utc?: string | null;
    selected_dates?: string[] | null;
  } | null;
};

export type AirlineItem = {
  airline: string;
  first_seen_at_utc?: string;
  last_seen_at_utc?: string;
  offer_rows?: number;
};

export type RouteItem = {
  route_key: string;
  origin: string;
  destination: string;
  offer_rows?: number;
  airlines_present?: number;
};

export type SnapshotRow = {
  cycle_id: string;
  captured_at_utc?: string;
  airline: string;
  origin: string;
  destination: string;
  route_key: string;
  flight_number: string;
  departure_utc?: string;
  cabin?: string | null;
  brand?: string | null;
  fare_basis?: string | null;
  total_price_bdt?: number | null;
  base_fare_amount?: number | null;
  tax_amount?: number | null;
  currency?: string | null;
  seat_available?: number | null;
  seat_capacity?: number | null;
  load_factor_pct?: number | null;
  booking_class?: string | null;
  baggage?: string | null;
  aircraft?: string | null;
  duration_min?: number | null;
  stops?: number | null;
  soldout?: boolean | null;
  penalty_source?: string | null;
};

export type SnapshotPayload = {
  cycle_id: string | null;
  rows: SnapshotRow[];
};

export type RouteMonitorMatrixCell = {
  flight_group_id: string;
  airline: string;
  min_total_price_bdt?: number | null;
  max_total_price_bdt?: number | null;
  tax_amount?: number | null;
  seat_available?: number | null;
  seat_capacity?: number | null;
  load_factor_pct?: number | null;
  soldout?: boolean | null;
  signal: "increase" | "decrease" | "new" | "sold_out" | "unknown";
};

export type RouteMonitorMatrixCapture = {
  captured_at_utc: string;
  is_latest: boolean;
  cells: RouteMonitorMatrixCell[];
};

export type RouteMonitorFlightGroup = {
  flight_group_id: string;
  airline: string;
  flight_number: string;
  departure_time?: string | null;
  cabin?: string | null;
  aircraft?: string | null;
};

export type RouteMonitorMatrixDateGroup = {
  departure_date: string;
  day_label: string;
  captures: RouteMonitorMatrixCapture[];
};

export type RouteMonitorMatrixRoute = {
  route_key: string;
  origin: string;
  destination: string;
  flight_groups: RouteMonitorFlightGroup[];
  date_groups: RouteMonitorMatrixDateGroup[];
};

export type RouteMonitorMatrixPayload = {
  cycle_id: string | null;
  routes: RouteMonitorMatrixRoute[];
  signal_counts?: Record<string, number>;
};

export type PenaltyRow = {
  cycle_id: string;
  captured_at_utc?: string;
  airline: string;
  origin: string;
  destination: string;
  route_key: string;
  flight_number: string;
  departure_utc?: string;
  cabin?: string | null;
  fare_basis?: string | null;
  penalty_source?: string | null;
  penalty_currency?: string | null;
  fare_change_fee_before_24h?: number | null;
  fare_change_fee_within_24h?: number | null;
  fare_change_fee_no_show?: number | null;
  fare_cancel_fee_before_24h?: number | null;
  fare_cancel_fee_within_24h?: number | null;
  fare_cancel_fee_no_show?: number | null;
  fare_changeable?: boolean | null;
  fare_refundable?: boolean | null;
  penalty_rule_text?: string | null;
};

export type PenaltyPayload = {
  cycle_id: string | null;
  rows: PenaltyRow[];
};

export type TaxRow = {
  cycle_id: string;
  captured_at_utc?: string;
  airline: string;
  origin: string;
  destination: string;
  route_key: string;
  flight_number: string;
  departure_utc?: string;
  cabin?: string | null;
  fare_basis?: string | null;
  tax_amount?: number | null;
  currency?: string | null;
};

export type TaxPayload = {
  cycle_id: string | null;
  rows: TaxRow[];
};

export type ChangeEventRow = {
  id: number;
  cycle_id?: string | null;
  previous_cycle_id?: string | null;
  detected_at_utc?: string;
  airline: string;
  origin?: string | null;
  destination?: string | null;
  route_key?: string | null;
  flight_number?: string | null;
  departure_day?: string | null;
  departure_time?: string | null;
  cabin?: string | null;
  fare_basis?: string | null;
  brand?: string | null;
  domain?: string | null;
  change_type?: string | null;
  direction?: string | null;
  field_name?: string | null;
  old_value?: unknown;
  new_value?: unknown;
  magnitude?: number | null;
  percent_change?: number | null;
  event_meta?: unknown;
};

export type ChangeEventsPayload = {
  items: ChangeEventRow[];
};

export type ForecastMetricRow = {
  model?: string | null;
  n?: number | null;
  mae?: number | null;
  rmse?: number | null;
  mape_pct?: number | null;
  smape_pct?: number | null;
  directional_accuracy_pct?: number | null;
  f1_macro?: number | null;
  [key: string]: unknown;
};

export type ForecastRouteWinnerRow = {
  airline?: string | null;
  origin?: string | null;
  destination?: string | null;
  route_key?: string | null;
  cabin?: string | null;
  winner_model?: string | null;
  winner_metric?: string | null;
  winner_n?: number | null;
  winner_mae?: number | null;
  winner_rmse?: number | null;
  winner_directional_accuracy_pct?: number | null;
  winner_f1_macro?: number | null;
  max_candidate_n?: number | null;
  coverage_threshold_n?: number | null;
  candidate_models?: number | null;
  split_id?: number | null;
  dataset?: string | null;
  selected_on_val?: boolean | null;
  [key: string]: unknown;
};

export type ForecastBundle = {
  bundle_dir: string;
  bundle_name: string;
  target: string;
  stamp: string;
  modified_at_utc?: string | null;
  overall_eval: ForecastMetricRow[];
  route_eval: ForecastMetricRow[];
  route_winners: ForecastRouteWinnerRow[];
  next_day: Array<Record<string, unknown>>;
  backtest_eval: ForecastMetricRow[];
  backtest_route_winners: ForecastRouteWinnerRow[];
  backtest_meta?: Record<string, unknown> | null;
};

export type ForecastingPayload = {
  latest_prediction_bundle: ForecastBundle | null;
  latest_backtest_bundle: ForecastBundle | null;
  bundle_count: number;
  source?: string | null;
  warning?: string | null;
};

export type SnapshotQuery = {
  cycleId?: string;
  airlines?: string[];
  origins?: string[];
  destinations?: string[];
  cabins?: string[];
  limit?: number;
};

type FetchResult<T> = {
  ok: boolean;
  data: T | null;
  error?: string;
};

type QueryValue =
  | string
  | number
  | null
  | undefined
  | Array<string | number | null | undefined>;

export function getApiBaseUrl(): string {
  const candidate =
    process.env.API_BASE_URL ||
    process.env.NEXT_PUBLIC_API_BASE_URL ||
    "http://127.0.0.1:8000";

  const normalized = candidate.trim().replace(/\s+/g, "");
  return normalized.replace(/\/+$/, "");
}

async function fetchJson<T>(path: string): Promise<FetchResult<T>> {
  return fetchJsonWithRevalidate<T>(path, 60);
}

async function fetchJsonWithRevalidate<T>(path: string, revalidateSeconds: number): Promise<FetchResult<T>> {
  try {
    const response = await fetch(`${getApiBaseUrl()}${path}`, {
      next: { revalidate: revalidateSeconds }
    });

    if (!response.ok) {
      return {
        ok: false,
        data: null,
        error: `${response.status} ${response.statusText}`
      };
    }

    const data = (await response.json()) as T;
    return { ok: true, data };
  } catch (error) {
    return {
      ok: false,
      data: null,
      error: error instanceof Error ? error.message : "Unknown API error"
    };
  }
}

function buildPath(path: string, params?: Record<string, QueryValue>): string {
  if (!params) {
    return path;
  }

  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        if (item !== null && item !== undefined && String(item).trim()) {
          query.append(key, String(item));
        }
      }
      continue;
    }

    if (value !== null && value !== undefined && String(value).trim()) {
      query.set(key, String(value));
    }
  }

  const queryString = query.toString();
  return queryString ? `${path}?${queryString}` : path;
}

export async function getLatestCycle() {
  return fetchJsonWithRevalidate<CycleSummary>("/api/v1/reporting/cycles/latest", 30);
}

export async function getAirlines() {
  return fetchJsonWithRevalidate<{ items: AirlineItem[] }>("/api/v1/meta/airlines", 300);
}

export async function getRoutes() {
  return fetchJsonWithRevalidate<{ items: RouteItem[] }>("/api/v1/meta/routes", 300);
}

export async function getDashboardPayload() {
  const [health, latestCycle, airlines, routes, cycleHealth] = await Promise.all([
    fetchJson<HealthPayload>("/health"),
    getLatestCycle(),
    getAirlines(),
    getRoutes(),
    getCycleHealth()
  ]);

  return { health, latestCycle, airlines, routes, cycleHealth };
}

export async function getCycleHealth() {
  return fetchJsonWithRevalidate<CycleHealthPayload>("/api/v1/reporting/cycle-health", 30);
}

export async function getCurrentSnapshotPayload(query: SnapshotQuery) {
  return fetchJsonWithRevalidate<SnapshotPayload>(
    buildPath("/api/v1/reporting/current-snapshot", {
      cycle_id: query.cycleId,
      airline: query.airlines,
      origin: query.origins,
      destination: query.destinations,
      cabin: query.cabins,
      limit: query.limit
    }),
    60
  );
}

export async function getRouteMonitorMatrixPayload(query: SnapshotQuery & { routeLimit?: number; historyLimit?: number }) {
  return fetchJsonWithRevalidate<RouteMonitorMatrixPayload>(
    buildPath("/api/v1/reporting/route-monitor-matrix", {
      cycle_id: query.cycleId,
      airline: query.airlines,
      origin: query.origins,
      destination: query.destinations,
      cabin: query.cabins,
      route_limit: query.routeLimit,
      history_limit: query.historyLimit
    }),
    60
  );
}

export async function getPenaltyPayload(query: SnapshotQuery) {
  return fetchJsonWithRevalidate<PenaltyPayload>(
    buildPath("/api/v1/reporting/penalties", {
      cycle_id: query.cycleId,
      airline: query.airlines,
      origin: query.origins,
      destination: query.destinations,
      limit: query.limit
    }),
    60
  );
}

export async function getTaxPayload(query: SnapshotQuery) {
  return fetchJsonWithRevalidate<TaxPayload>(
    buildPath("/api/v1/reporting/taxes", {
      cycle_id: query.cycleId,
      airline: query.airlines,
      origin: query.origins,
      destination: query.destinations,
      limit: query.limit
    }),
    60
  );
}

export async function getChangeEventsPayload(query: {
  airlines?: string[];
  origins?: string[];
  destinations?: string[];
  domains?: string[];
  changeTypes?: string[];
  directions?: string[];
  startDate?: string;
  endDate?: string;
  limit?: number;
}) {
  return fetchJsonWithRevalidate<ChangeEventsPayload>(
    buildPath("/api/v1/reporting/change-events", {
      airline: query.airlines,
      origin: query.origins,
      destination: query.destinations,
      domain: query.domains,
      change_type: query.changeTypes,
      direction: query.directions,
      start_date: query.startDate,
      end_date: query.endDate,
      limit: query.limit
    }),
    60
  );
}

export async function getForecastingPayload() {
  return fetchJsonWithRevalidate<ForecastingPayload>("/api/v1/reporting/forecasting/latest", 300);
}
