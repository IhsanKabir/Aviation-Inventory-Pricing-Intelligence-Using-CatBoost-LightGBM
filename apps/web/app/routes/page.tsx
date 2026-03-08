import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { RouteMonitorMatrix } from "@/components/route-monitor-matrix";
import {
  getLatestCycle,
  getRouteMonitorMatrixPayload,
  getRoutes
} from "@/lib/api";
import { formatDhakaDateTime, shortCycle } from "@/lib/format";
import { firstParam, manyParams, parseLimit, type RawSearchParams } from "@/lib/query";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

export default async function RoutesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const cabin = firstParam(params, "cabin");
  const routeLimit = parseLimit(firstParam(params, "route_limit"), 5);
  const historyLimit = parseLimit(firstParam(params, "history_limit"), 6);

  const [latestCycle, routes] = await Promise.all([
    getLatestCycle(),
    getRoutes()
  ]);

  const cycleId = firstParam(params, "cycle_id") ?? latestCycle.data?.cycle_id ?? undefined;
  const matrix = await getRouteMonitorMatrixPayload({
    cycleId,
    origins: origin ? [origin] : undefined,
    destinations: destination ? [destination] : undefined,
    cabins: cabin ? [cabin] : undefined,
    routeLimit,
    historyLimit
  });

  const routeBlocks = matrix.data?.routes ?? [];
  const routeOptions = [...(routes.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key))
    .slice(0, 16)
    .map((item) => ({ routeKey: item.route_key, origin: item.origin, destination: item.destination }));

  const availableAirlineCount = new Set(
    routeBlocks.flatMap((route) => route.flight_groups.map((flight) => flight.airline))
  ).size;
  const flightGroupCount = routeBlocks.reduce((sum, route) => sum + route.flight_groups.length, 0);
  const datedRowCount = routeBlocks.reduce((sum, route) => sum + route.date_groups.length, 0);

  return (
    <>
      <h1 className="page-title">Route Monitor</h1>
      <p className="page-copy">
        Report-style route matrix against the reporting API. Hosted reads now prefer the
        BigQuery warehouse path; airline, signal, and capture-history interaction stay in
        the page for workbook-like review without Excel.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Cycle"
          value={shortCycle(matrix.data?.cycle_id ?? cycleId ?? null)}
          footnote={latestCycle.data?.cycle_completed_at_utc ? formatDhakaDateTime(latestCycle.data.cycle_completed_at_utc) : "No cycle loaded"}
        />
        <MetricCard label="Route blocks" value={routeBlocks.length.toLocaleString()} footnote={`Limit ${routeLimit.toLocaleString()}`} />
        <MetricCard
          label="Flight groups"
          value={flightGroupCount.toLocaleString()}
          footnote={`${availableAirlineCount.toLocaleString()} airlines in scope`}
        />
        <MetricCard
          label="Departure rows"
          value={datedRowCount.toLocaleString()}
          footnote={`History depth ${historyLimit.toLocaleString()}`}
        />
      </div>

      <div className="stack">
        <DataPanel
          title="Matrix scope"
          copy="Use route scope controls to load a tighter matrix from the API. Inside the matrix itself, airline and signal toggles behave like the workbook."
        >
          <form className="filter-form" action="/routes">
            <div className="field-grid route-scope-grid">
              <label className="field">
                <span>Origin</span>
                <input defaultValue={origin ?? ""} name="origin" placeholder="DAC" type="text" />
              </label>
              <label className="field">
                <span>Destination</span>
                <input defaultValue={destination ?? ""} name="destination" placeholder="CXB" type="text" />
              </label>
              <label className="field">
                <span>Cabin</span>
                <input defaultValue={cabin ?? ""} name="cabin" placeholder="Economy" type="text" />
              </label>
              <label className="field">
                <span>Route blocks</span>
                <input defaultValue={String(routeLimit)} inputMode="numeric" name="route_limit" pattern="[0-9]*" type="text" />
              </label>
              <label className="field">
                <span>History depth</span>
                <input defaultValue={String(historyLimit)} inputMode="numeric" name="history_limit" pattern="[0-9]*" type="text" />
              </label>
            </div>
            <div className="button-row">
              <button className="button-link" type="submit">
                Reload matrix
              </button>
              <a className="button-link ghost" href="/routes">
                Reset scope
              </a>
            </div>
            {routeOptions.length ? (
              <div className="route-hint-row">
                {routeOptions.map((item) => (
                  <a
                    className="route-hint-chip"
                    href={`/routes?origin=${encodeURIComponent(item.origin)}&destination=${encodeURIComponent(item.destination)}&route_limit=${routeLimit}&history_limit=${historyLimit}${cabin ? `&cabin=${encodeURIComponent(cabin)}` : ""}`}
                    key={item.routeKey}
                  >
                    {item.routeKey}
                  </a>
                ))}
              </div>
            ) : null}
          </form>
        </DataPanel>

        <DataPanel
          title="Route flight fare monitor"
          copy="Latest captures are shown first. Use the capture column to expand older observations for the same departure date."
        >
          {!matrix.ok ? (
            <div className="empty-state error-state">API error: {matrix.error ?? "Unable to load route monitor matrix."}</div>
          ) : routeBlocks.length === 0 ? (
            <div className="empty-state">No route blocks matched the current scope.</div>
          ) : (
            <RouteMonitorMatrix initialAirlines={selectedAirlines} payload={matrix.data!} />
          )}
        </DataPanel>
      </div>
    </>
  );
}
