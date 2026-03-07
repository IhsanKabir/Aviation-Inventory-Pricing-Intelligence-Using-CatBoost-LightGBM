"use client";

import type { CSSProperties } from "react";
import { Fragment, useMemo, useState } from "react";

import type {
  RouteMonitorFlightGroup,
  RouteMonitorMatrixCell,
  RouteMonitorMatrixPayload,
  RouteMonitorMatrixRoute
} from "@/lib/api";
import { formatDhakaDateTime, formatMoney, formatPercent } from "@/lib/format";

type ViewMode = "context" | "strict";
type SignalKey = "increase" | "decrease" | "new" | "sold_out" | "unknown";

const SIGNAL_LABELS: Record<SignalKey, string> = {
  increase: "Increase",
  decrease: "Decrease",
  new: "New",
  sold_out: "Sold out",
  unknown: "Unknown"
};

const AIRLINE_THEME: Record<string, { header: string; sub: string; cell: string; text: string; headerText: string }> = {
  BG: { header: "#c8102e", sub: "#fff1f4", cell: "#f9f0f2", text: "#5f1020", headerText: "#ffffff" },
  VQ: { header: "#003a70", sub: "#ffe7d0", cell: "#fff4ea", text: "#123a6e", headerText: "#ffffff" },
  BS: { header: "#00557f", sub: "#d8ebf7", cell: "#eef7fc", text: "#11384d", headerText: "#ffffff" },
  "2A": { header: "#b78700", sub: "#fdefc7", cell: "#fff9ea", text: "#6b4e00", headerText: "#1e1e1e" },
  G9: { header: "#c6282b", sub: "#f9e1e2", cell: "#fdf2f3", text: "#7a1d20", headerText: "#ffffff" },
  "3L": { header: "#c6282b", sub: "#f9e1e2", cell: "#fdf2f3", text: "#7a1d20", headerText: "#ffffff" },
  "6E": { header: "#2b2f86", sub: "#e5e6fb", cell: "#f4f4ff", text: "#1b1f69", headerText: "#ffffff" },
  EK: { header: "#d71920", sub: "#fde7e8", cell: "#fff4f4", text: "#7f1116", headerText: "#ffffff" },
  FZ: { header: "#005b96", sub: "#daeefe", cell: "#f1f8ff", text: "#0f3b66", headerText: "#ffffff" },
  CZ: { header: "#2a9fd8", sub: "#ddf3fd", cell: "#f2fbff", text: "#0d4c69", headerText: "#ffffff" }
};

function themeForAirline(code: string) {
  return (
    AIRLINE_THEME[code] ?? {
      header: "#194866",
      sub: "#dcecf6",
      cell: "#f4f8fb",
      text: "#163449",
      headerText: "#ffffff"
    }
  );
}

function signalArrow(signal: SignalKey) {
  if (signal === "increase") return "\u2191";
  if (signal === "decrease") return "\u2193";
  return "";
}

function summarizeCell(cell: RouteMonitorMatrixCell | undefined) {
  if (!cell) {
    return {
      minFare: "N/O",
      maxFare: "\u2014",
      tax: "\u2014",
      seats: "\u2014 / \u2014",
      load: "\u2014"
    };
  }

  return {
    minFare: cell.min_total_price_bdt != null ? formatMoney(cell.min_total_price_bdt, "BDT").replace("BDT ", "") : "N/O",
    maxFare: cell.max_total_price_bdt != null ? formatMoney(cell.max_total_price_bdt, "BDT").replace("BDT ", "") : "\u2014",
    tax: cell.tax_amount != null ? formatMoney(cell.tax_amount, "BDT").replace("BDT ", "") : "\u2014",
    seats:
      cell.seat_available != null || cell.seat_capacity != null
        ? `${cell.seat_available ?? "\u2014"} / ${cell.seat_capacity ?? "\u2014"}`
        : "\u2014 / \u2014",
    load: formatPercent(cell.load_factor_pct)
  };
}

function routeLeader(route: RouteMonitorMatrixRoute, visibleFlights: RouteMonitorFlightGroup[]) {
  const visibleSet = new Set(visibleFlights.map((item) => item.flight_group_id));
  let best:
    | {
        airline: string;
        flightNumber: string;
        amount: number;
        dates: string[];
      }
    | undefined;

  for (const dateGroup of route.date_groups) {
    const latestCapture = dateGroup.captures[0];
    if (!latestCapture) continue;
    for (const cell of latestCapture.cells) {
      if (!visibleSet.has(cell.flight_group_id) || cell.min_total_price_bdt == null) {
        continue;
      }
      const flight = route.flight_groups.find((item) => item.flight_group_id === cell.flight_group_id);
      if (!flight) continue;
      if (!best || cell.min_total_price_bdt < best.amount) {
        best = {
          airline: flight.airline,
          flightNumber: flight.flight_number,
          amount: Number(cell.min_total_price_bdt),
          dates: [dateGroup.departure_date]
        };
      } else if (cell.min_total_price_bdt === best.amount && !best.dates.includes(dateGroup.departure_date)) {
        best.dates.push(dateGroup.departure_date);
      }
    }
  }

  return best;
}

export function RouteMonitorMatrix({
  payload,
  initialAirlines = []
}: {
  payload: RouteMonitorMatrixPayload;
  initialAirlines?: string[];
}) {
  const [selectedAirlines, setSelectedAirlines] = useState<string[]>(initialAirlines);
  const [selectedSignals, setSelectedSignals] = useState<SignalKey[]>([]);
  const [viewMode, setViewMode] = useState<ViewMode>("context");
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});

  const availableAirlines = useMemo(() => {
    const codes = new Set<string>();
    for (const route of payload.routes) {
      for (const flight of route.flight_groups) {
        codes.add(flight.airline);
      }
    }
    return Array.from(codes).sort();
  }, [payload.routes]);

  const visibleRoutes = useMemo(() => {
    return payload.routes
      .map((route) => {
        const flightGroups =
          selectedAirlines.length === 0 || viewMode === "context"
            ? route.flight_groups
            : route.flight_groups.filter((item) => selectedAirlines.includes(item.airline));

        const routeHasSelectedAirline =
          selectedAirlines.length === 0 || route.flight_groups.some((item) => selectedAirlines.includes(item.airline));
        if (!routeHasSelectedAirline || flightGroups.length === 0) {
          return null;
        }

        const visibleFlightSet = new Set(flightGroups.map((item) => item.flight_group_id));
        const dateGroups = route.date_groups
          .map((dateGroup) => {
            const captures = dateGroup.captures
              .map((capture) => ({
                ...capture,
                cells: capture.cells.filter((cell) => visibleFlightSet.has(cell.flight_group_id))
              }))
              .filter((capture) => capture.cells.length > 0);

            if (captures.length === 0) {
              return null;
            }

            const latestSignals = new Set(captures[0].cells.map((cell) => cell.signal));
            if (selectedSignals.length > 0 && !selectedSignals.some((signal) => latestSignals.has(signal))) {
              return null;
            }

            return { ...dateGroup, captures };
          })
          .filter(Boolean) as RouteMonitorMatrixRoute["date_groups"];

        if (dateGroups.length === 0) {
          return null;
        }

        return {
          ...route,
          flight_groups: flightGroups,
          date_groups: dateGroups
        };
      })
      .filter(Boolean) as RouteMonitorMatrixRoute[];
  }, [payload.routes, selectedAirlines, selectedSignals, viewMode]);

  const signalCounts = useMemo(() => {
    const counts: Record<SignalKey, number> = {
      increase: 0,
      decrease: 0,
      new: 0,
      sold_out: 0,
      unknown: 0
    };
    for (const route of visibleRoutes) {
      for (const dateGroup of route.date_groups) {
        const latestCapture = dateGroup.captures[0];
        if (!latestCapture) continue;
        for (const cell of latestCapture.cells) {
          counts[cell.signal] += 1;
        }
      }
    }
    return counts;
  }, [visibleRoutes]);

  function toggleAirline(code: string) {
    setSelectedAirlines((current) =>
      current.includes(code) ? current.filter((item) => item !== code) : [...current, code]
    );
  }

  function toggleSignal(signal: SignalKey) {
    setSelectedSignals((current) =>
      current.includes(signal) ? current.filter((item) => item !== signal) : [...current, signal]
    );
  }

  function toggleRow(key: string) {
    setExpandedRows((current) => ({ ...current, [key]: !current[key] }));
  }

  function clearInteractiveFilters() {
    setSelectedAirlines([]);
    setSelectedSignals([]);
    setViewMode("context");
    setExpandedRows({});
  }

  return (
    <div className="report-monitor">
      <div className="report-toolbar card">
        <div className="report-toolbar-row">
          <div className="report-label">Airlines</div>
          <div className="report-chip-row">
            {availableAirlines.map((code) => {
              const theme = themeForAirline(code);
              return (
                <button
                  key={code}
                  className="report-airline-chip"
                  data-active={selectedAirlines.includes(code)}
                  onClick={() => toggleAirline(code)}
                  style={
                    {
                      "--chip-bg": theme.header,
                      "--chip-fg": theme.headerText
                    } as CSSProperties
                  }
                  type="button"
                >
                  {code}
                </button>
              );
            })}
          </div>
        </div>

        <div className="report-toolbar-row">
          <div className="report-label">Signals</div>
          <div className="report-chip-row">
            {(["increase", "decrease", "new", "sold_out", "unknown"] as SignalKey[]).map((signal) => (
              <button
                key={signal}
                className="report-signal-chip"
                data-active={selectedSignals.includes(signal)}
                onClick={() => toggleSignal(signal)}
                type="button"
              >
                {signal === "increase" ? "\u2191 " : signal === "decrease" ? "\u2193 " : ""}
                {SIGNAL_LABELS[signal]}
                {signal !== "unknown" ? ` (${signalCounts[signal]})` : ""}
              </button>
            ))}
          </div>
        </div>

        <div className="report-toolbar-row report-toolbar-meta">
          <div className="report-mode-switch">
            <button
              className="button-link ghost"
              data-active={viewMode === "context"}
              onClick={() => setViewMode("context")}
              type="button"
            >
              Context
            </button>
            <button
              className="button-link ghost"
              data-active={viewMode === "strict"}
              onClick={() => setViewMode("strict")}
              type="button"
            >
              Strict
            </button>
          </div>
          <button className="button-link ghost" onClick={clearInteractiveFilters} type="button">
            Clear interactive filters
          </button>
        </div>
      </div>

      <div className="route-report-stack">
        {visibleRoutes.length === 0 ? (
          <div className="empty-state">No route blocks match the current airline/signal selection.</div>
        ) : (
          visibleRoutes.map((route) => {
            const leader = routeLeader(route, route.flight_groups);
            return (
              <section className="route-report-block" key={route.route_key}>
                <div className="route-report-title-row">
                  <div className="route-report-title">{route.route_key}</div>
                  <div className="route-report-leader">
                    Route Price Leader (Lowest Fare):{" "}
                    {leader ? (
                      <>
                        {leader.airline}
                        {leader.flightNumber} — {leader.amount.toLocaleString()} (Dates:{" "}
                        {leader.dates.map((item) => item.slice(5).replace("-", " ")).join(", ")})
                      </>
                    ) : (
                      "No visible fare leader"
                    )}
                  </div>
                </div>

                <div className="route-report-scroll">
                  <table className="route-report-table">
                    <thead>
                      <tr>
                        <th className="sticky-col sticky-route-meta" rowSpan={3}>
                          Date
                        </th>
                        <th className="sticky-col sticky-route-meta second" rowSpan={3}>
                          Day
                        </th>
                        <th className="sticky-col sticky-route-meta third" rowSpan={3}>
                          Capture Date/Time
                        </th>
                        {route.flight_groups.map((flight) => {
                          const theme = themeForAirline(flight.airline);
                          return (
                            <th
                              className="flight-band"
                              colSpan={5}
                              key={flight.flight_group_id}
                              style={{ background: theme.header, color: theme.headerText }}
                            >
                              {flight.airline}
                              {flight.flight_number} | {flight.aircraft || "Flight"}
                            </th>
                          );
                        })}
                      </tr>
                      <tr>
                        {route.flight_groups.map((flight) => {
                          const theme = themeForAirline(flight.airline);
                          return (
                            <th
                              className="flight-subband"
                              colSpan={5}
                              key={`sub-${flight.flight_group_id}`}
                              style={{ background: theme.header, color: theme.headerText }}
                            >
                              {flight.departure_time || "\u2014"}
                            </th>
                          );
                        })}
                      </tr>
                      <tr>
                        {route.flight_groups.map((flight) => {
                          const theme = themeForAirline(flight.airline);
                          return (
                            <Fragment key={`metrics-${flight.flight_group_id}`}>
                              <th className="metric-head" key={`${flight.flight_group_id}-min`} style={{ background: theme.sub, color: theme.text }}>
                                Min Fare
                              </th>
                              <th className="metric-head" key={`${flight.flight_group_id}-max`} style={{ background: theme.sub, color: theme.text }}>
                                Max Fare
                              </th>
                              <th className="metric-head" key={`${flight.flight_group_id}-tax`} style={{ background: theme.sub, color: theme.text }}>
                                Tax Amount
                              </th>
                              <th className="metric-head" key={`${flight.flight_group_id}-seat`} style={{ background: theme.sub, color: theme.text }}>
                                Open/Cap
                              </th>
                              <th className="metric-head" key={`${flight.flight_group_id}-load`} style={{ background: theme.sub, color: theme.text }}>
                                Inv Press
                              </th>
                            </Fragment>
                          );
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {route.date_groups.map((dateGroup) => {
                        const rowKey = `${route.route_key}-${dateGroup.departure_date}`;
                        const expanded = Boolean(expandedRows[rowKey]);
                        const visibleCaptures = expanded ? dateGroup.captures : dateGroup.captures.slice(0, 1);

                        return visibleCaptures.map((capture, captureIndex) => {
                          const showDateMeta = captureIndex === 0;
                          const expandLabel =
                            dateGroup.captures.length > 1
                              ? `${expanded ? "[-]" : `[+${dateGroup.captures.length - 1}]`} ${formatDhakaDateTime(capture.captured_at_utc)}`
                              : formatDhakaDateTime(capture.captured_at_utc);

                          return (
                            <tr className={capture.is_latest ? "latest-capture-row" : "history-capture-row"} key={`${rowKey}-${capture.captured_at_utc}`}>
                              <td className="sticky-col sticky-route-meta route-value">
                                {showDateMeta ? dateGroup.departure_date : ""}
                              </td>
                              <td className="sticky-col sticky-route-meta second route-value">
                                {showDateMeta ? dateGroup.day_label : ""}
                              </td>
                              <td className="sticky-col sticky-route-meta third route-value">
                                {showDateMeta && dateGroup.captures.length > 1 ? (
                                  <button className="history-toggle" onClick={() => toggleRow(rowKey)} type="button">
                                    {expandLabel}
                                  </button>
                                ) : (
                                  expandLabel
                                )}
                              </td>
                              {route.flight_groups.flatMap((flight) => {
                                const theme = themeForAirline(flight.airline);
                                const cell = capture.cells.find((item) => item.flight_group_id === flight.flight_group_id);
                                const summary = summarizeCell(cell);
                                const signal = (cell?.signal ?? "unknown") as SignalKey;
                                return [
                                  <td
                                    className={`report-cell signal-${signal}`}
                                    key={`${capture.captured_at_utc}-${flight.flight_group_id}-min`}
                                    style={{ background: theme.cell, color: theme.text }}
                                  >
                                    {summary.minFare} {signalArrow(signal)}
                                  </td>,
                                  <td
                                    className={`report-cell signal-${signal}`}
                                    key={`${capture.captured_at_utc}-${flight.flight_group_id}-max`}
                                    style={{ background: theme.cell, color: theme.text }}
                                  >
                                    {summary.maxFare}
                                  </td>,
                                  <td
                                    className="report-cell"
                                    key={`${capture.captured_at_utc}-${flight.flight_group_id}-tax`}
                                    style={{ background: theme.cell, color: theme.text }}
                                  >
                                    {summary.tax}
                                  </td>,
                                  <td
                                    className="report-cell"
                                    key={`${capture.captured_at_utc}-${flight.flight_group_id}-seat`}
                                    style={{ background: theme.cell, color: theme.text }}
                                  >
                                    {summary.seats}
                                  </td>,
                                  <td
                                    className="report-cell"
                                    key={`${capture.captured_at_utc}-${flight.flight_group_id}-load`}
                                    style={{ background: theme.cell, color: theme.text }}
                                  >
                                    {summary.load}
                                  </td>
                                ];
                              })}
                            </tr>
                          );
                        });
                      })}
                    </tbody>
                  </table>
                </div>
              </section>
            );
          })
        )}
      </div>
    </div>
  );
}
