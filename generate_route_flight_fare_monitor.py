import argparse
import json
import logging
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path
import re

import pandas as pd
from sqlalchemy import create_engine, text

from db import DATABASE_URL as DEFAULT_DATABASE_URL
from engines.comparison_engine import ComparisonEngine
from engines.excel_comparison_adapter import adapt_comparison_for_excel
from engines.output_writer import OutputWriter
from engines.route_scope import (
    load_airport_countries,
    parse_csv_upper_codes,
    route_matches_scope,
)
from engines.scrape_context import ScrapeContext

LOG = logging.getLogger("route_flight_fare_monitor")


def _normalize_airline_codes(codes):
    out = []
    seen = set()
    for code in codes or []:
        c = str(code or "").strip().upper()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _dominant_scrape_passenger_mix(engine, scrape_id: str):
    q = text(
        """
        SELECT
            COALESCE(frm.adt_count, 1) AS adt_count,
            COALESCE(frm.chd_count, 0) AS chd_count,
            COALESCE(frm.inf_count, 0) AS inf_count,
            COUNT(*) AS row_count
        FROM flight_offers fo
        JOIN flight_offer_raw_meta frm
          ON frm.flight_offer_id = fo.id
        WHERE fo.scrape_id = :scrape_id
        GROUP BY 1,2,3
        ORDER BY row_count DESC, adt_count, chd_count, inf_count
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(q, {"scrape_id": str(scrape_id)}).fetchone()
    if not row:
        return None
    return {
        "adt": int(row[0] or 0),
        "chd": int(row[1] or 0),
        "inf": int(row[2] or 0),
        "rows": int(row[3] or 0),
    }


def _scrape_airline_stats(engine, scrape_id, airline_codes=None):
    airline_codes = _normalize_airline_codes(airline_codes)
    airline_where = ""
    params = {"scrape_id": str(scrape_id)}
    if airline_codes:
        airline_where = " AND fo.airline = ANY(:airline_codes)"
        params["airline_codes"] = airline_codes

    q = text(
        f"""
        SELECT
            fo.airline,
            COUNT(*) AS row_count,
            COUNT(DISTINCT fo.origin || '->' || fo.destination) AS route_count
        FROM flight_offers fo
        WHERE fo.scrape_id = :scrape_id
          {airline_where}
        GROUP BY fo.airline
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, params).mappings().all()
    return {
        str(r["airline"]).upper(): {
            "row_count": int(r["row_count"] or 0),
            "route_count": int(r["route_count"] or 0),
        }
        for r in rows
    }


def _recent_airline_max_stats(engine, lookback=200, airline_codes=None):
    airline_codes = _normalize_airline_codes(airline_codes)
    airline_filter = ""
    params = {"lookback": int(max(2, lookback))}
    if airline_codes:
        airline_filter = "WHERE airline = ANY(:airline_codes)"
        params["airline_codes"] = airline_codes

    q = text(
        f"""
        WITH recent_scrapes AS (
            SELECT scrape_id
            FROM flight_offers
            GROUP BY scrape_id
            ORDER BY MAX(scraped_at) DESC
            LIMIT :lookback
        ),
        per_scrape_airline AS (
            SELECT
                fo.scrape_id,
                fo.airline,
                COUNT(*) AS row_count,
                COUNT(DISTINCT fo.origin || '->' || fo.destination) AS route_count
            FROM flight_offers fo
            JOIN recent_scrapes rs
              ON rs.scrape_id = fo.scrape_id
            {airline_filter}
            GROUP BY fo.scrape_id, fo.airline
        )
        SELECT
            airline,
            MAX(row_count) AS max_row_count,
            MAX(route_count) AS max_route_count
        FROM per_scrape_airline
        GROUP BY airline
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, params).mappings().all()
    return {
        str(r["airline"]).upper(): {
            "max_row_count": int(r["max_row_count"] or 0),
            "max_route_count": int(r["max_route_count"] or 0),
        }
        for r in rows
    }


def _warn_if_partial_scrape_selection(
    engine,
    *,
    current_scrape,
    previous_scrape,
    airline_codes=None,
    scrape_lookback=40,
    min_full_scrape_rows=100,
    min_full_ratio=0.30,
):
    airline_codes = _normalize_airline_codes(airline_codes)
    baseline = _recent_airline_max_stats(
        engine,
        lookback=max(int(scrape_lookback or 0), 200),
        airline_codes=airline_codes,
    )
    if not baseline:
        return

    current_stats = _scrape_airline_stats(engine, current_scrape, airline_codes=airline_codes)
    previous_stats = _scrape_airline_stats(engine, previous_scrape, airline_codes=airline_codes)
    target_airlines = airline_codes or sorted(baseline.keys())

    floor = int(min_full_scrape_rows or 0)
    ratio = float(min_full_ratio or 0.0)

    def _thresholds(max_rows, max_routes):
        row_threshold = 1
        route_threshold = 1
        if max_rows > 0:
            row_threshold = min(max_rows, max(floor, int(max_rows * ratio)))
        if max_routes > 0:
            route_threshold = min(max_routes, max(1, int(math.ceil(max_routes * ratio))))
        return int(row_threshold), int(route_threshold)

    for label, scrape_id, stats in (
        ("current", current_scrape, current_stats),
        ("previous", previous_scrape, previous_stats),
    ):
        for airline_code in target_airlines:
            base = baseline.get(airline_code)
            if not base:
                continue
            max_rows = int(base.get("max_row_count") or 0)
            max_routes = int(base.get("max_route_count") or 0)
            row_threshold, route_threshold = _thresholds(max_rows, max_routes)
            s = stats.get(airline_code, {})
            rows_now = int(s.get("row_count") or 0)
            routes_now = int(s.get("route_count") or 0)
            if rows_now < row_threshold or routes_now < route_threshold:
                LOG.warning(
                    "Selected %s scrape appears partial for airline %s: scrape_id=%s "
                    "rows=%d (threshold=%d, max_recent=%d) routes=%d (threshold=%d, max_recent=%d).",
                    label,
                    airline_code,
                    scrape_id,
                    rows_now,
                    row_threshold,
                    max_rows,
                    routes_now,
                    route_threshold,
                    max_routes,
                )


def _build_run_stamp(timestamp_tz: str):
    if timestamp_tz == "utc":
        now = datetime.now(timezone.utc)
    else:
        now = datetime.now().astimezone()

    ts = now.strftime("%Y%m%d_%H%M%S_%f")
    tz = now.strftime("%z") or "0000"
    if tz.startswith("+"):
        tz_token = f"UTCp{tz[1:]}"
    elif tz.startswith("-"):
        tz_token = f"UTCm{tz[1:]}"
    else:
        tz_token = f"UTC{tz}"
    return ts, tz_token


def _format_capture_label(ts_value):
    if ts_value is None:
        return ""
    try:
        ts = pd.to_datetime(ts_value, utc=True, errors="coerce")
        if pd.isna(ts):
            return str(ts_value)
        local_tz = datetime.now().astimezone().tzinfo
        return ts.tz_convert(local_tz).strftime("%d %b, %H:%M")
    except Exception:
        return str(ts_value)


def _load_execution_plan_payload(output_dir: Path, run_dir: Path):
    """
    Prefer latest computed execution-plan status artifact; fallback to static
    execution_plan in config/schedule.json so the workbook still reflects
    current strategic order when runtime status file is absent.
    """
    candidates = [
        run_dir / "pipeline_execution_plan_latest.json",
        output_dir / "pipeline_execution_plan_latest.json",
        Path("output/reports/pipeline_execution_plan_latest.json"),
    ]
    for p in candidates:
        if not p.exists():
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(obj, dict) and obj:
            payload = dict(obj)
            payload.setdefault("_source", str(p))
            return payload

    schedule_path = Path("config/schedule.json")
    if not schedule_path.exists():
        return None
    try:
        schedule_obj = json.loads(schedule_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(schedule_obj, dict):
        return None
    plan = schedule_obj.get("execution_plan")
    if not isinstance(plan, dict) or not plan:
        return None

    payload = {
        "generated_at_utc": None,
        "ultimate_priority_goal": plan.get("ultimate_priority_goal"),
        "current_phase": plan.get("current_phase"),
        "phase_sequence": plan.get("phase_sequence"),
        "coverage_summary": {},
        "pipeline_rc": None,
        "recommended_next_phase": plan.get("current_phase"),
        "_source": str(schedule_path),
    }
    return payload


def export_macro_xlsm(input_xlsx: Path, output_xlsm: Path | None = None) -> Path:
    script_path = Path(__file__).resolve().parent / "tools" / "export_route_monitor_xlsm.ps1"
    if not script_path.exists():
        raise RuntimeError(f"Macro export script not found: {script_path}")

    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script_path),
        "-InputXlsx",
        str(input_xlsx),
    ]
    if output_xlsm:
        cmd.extend(["-OutputXlsm", str(output_xlsm)])

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        stderr_tail = (proc.stderr or "").strip()[-1000:]
        stdout_tail = (proc.stdout or "").strip()[-1000:]
        raise RuntimeError(
            "Macro export failed. "
            "If VBA injection is blocked, enable: Excel Trust Center > Macro Settings > "
            "Trust access to the VBA project object model.\n"
            f"stdout_tail:\n{stdout_tail}\n"
            f"stderr_tail:\n{stderr_tail}"
        )

    exported_path = None
    for line in (proc.stdout or "").splitlines():
        if line.strip().lower().startswith("xlsm_exported="):
            exported_path = line.split("=", 1)[1].strip()
            break
    if exported_path:
        out = Path(exported_path)
    else:
        out = output_xlsm if output_xlsm else input_xlsx.with_suffix(".xlsm")

    if not out.exists():
        raise RuntimeError(f"Macro export reported success but output file not found: {out}")
    return out


def _filter_df(
    df: pd.DataFrame,
    airline=None,
    origin=None,
    destination=None,
    cabin=None,
    route_scope: str = "all",
    market_country: str = "BD",
):
    out = df.copy()
    airport_countries = load_airport_countries()
    airline_codes = parse_csv_upper_codes(airline)

    if airline_codes and "airline" in out.columns:
        out = out[out["airline"].astype(str).str.upper().isin(set(airline_codes))]
    if origin and "origin" in out.columns:
        out = out[out["origin"].astype(str).str.upper() == str(origin).upper()]
    if destination and "destination" in out.columns:
        out = out[out["destination"].astype(str).str.upper() == str(destination).upper()]
    if cabin and "cabin" in out.columns:
        out = out[out["cabin"].astype(str) == str(cabin)]
    if route_scope != "all" and {"origin", "destination"}.issubset(set(out.columns)):
        out = out[
            out.apply(
                lambda r: route_matches_scope(
                    r.get("origin"),
                    r.get("destination"),
                    scope=route_scope,
                    airport_countries=airport_countries,
                    market_country=market_country,
                ),
                axis=1,
            )
        ]

    return out


def _prepare_for_writer(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Use flight-level aggregated aircraft label when row-level aircraft is missing.
    if "aircraft_label" in out.columns:
        if "aircraft" not in out.columns:
            out["aircraft"] = pd.NA
        aircraft_blank = out["aircraft"].isna() | (out["aircraft"].astype(str).str.strip() == "")
        out.loc[aircraft_blank, "aircraft"] = out.loc[aircraft_blank, "aircraft_label"]

    numeric_defaults = {
        "seat_delta": 0,
        "min_fare_delta": 0,
        "max_fare_delta": 0,
        "tax_delta": 0,
        "load_delta": 0,
    }
    for col, default in numeric_defaults.items():
        if col not in out.columns:
            out[col] = default
        out[col] = out[col].fillna(default)

    nullable_numeric_cols = ["min_seats", "max_seats", "load_pct", "current_tax", "min_rbd_seats", "max_rbd_seats"]
    for col in nullable_numeric_cols:
        if col not in out.columns:
            out[col] = pd.NA

    string_defaults = {
        "min_rbd": "",
        "max_rbd": "",
        "status": "NORMAL",
        "aircraft": "Aircraft NA",
    }
    for col, default in string_defaults.items():
        if col not in out.columns:
            out[col] = default
        out[col] = out[col].fillna(default)

    return out


def generate_route_flight_fare_monitor(
    output_dir="output/reports",
    run_dir=None,
    timestamp_tz="local",
    db_url=DEFAULT_DATABASE_URL,
    style="compact",
    airline=None,
    origin=None,
    destination=None,
    cabin=None,
    current_scrape_id=None,
    previous_scrape_id=None,
    auto_skip_tiny=True,
    scrape_lookback=40,
    min_full_scrape_rows=100,
    min_full_ratio=0.30,
    route_scope="all",
    market_country="BD",
):
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    scrape_ctx = ScrapeContext(engine)
    selection_airline_codes = parse_csv_upper_codes(airline)

    if current_scrape_id and previous_scrape_id:
        current_scrape = current_scrape_id
        previous_scrape = previous_scrape_id
    else:
        if auto_skip_tiny:
            current_scrape, previous_scrape = scrape_ctx.get_latest_two_full_scrapes(
                lookback=scrape_lookback,
                min_rows_floor=min_full_scrape_rows,
                min_full_ratio=min_full_ratio,
                airline_codes=selection_airline_codes,
            )
        else:
            current_scrape, previous_scrape = scrape_ctx.get_latest_two_scrapes(
                airline_codes=selection_airline_codes,
            )

    current_mix = _dominant_scrape_passenger_mix(engine, current_scrape)
    previous_mix = _dominant_scrape_passenger_mix(engine, previous_scrape)
    if current_mix and previous_mix:
        curr_sig = (current_mix["adt"], current_mix["chd"], current_mix["inf"])
        prev_sig = (previous_mix["adt"], previous_mix["chd"], previous_mix["inf"])
        if curr_sig != prev_sig:
            LOG.warning(
                "Passenger-mix mismatch between compared scrapes: current=%s previous=%s. "
                "Route monitor comparisons should use same ADT/CHD/INF basis.",
                current_mix,
                previous_mix,
            )
    _warn_if_partial_scrape_selection(
        engine,
        current_scrape=current_scrape,
        previous_scrape=previous_scrape,
        airline_codes=selection_airline_codes,
        scrape_lookback=scrape_lookback,
        min_full_scrape_rows=min_full_scrape_rows,
        min_full_ratio=min_full_ratio,
    )

    cmp_engine = ComparisonEngine(engine)
    comparison_df = cmp_engine.compare_scrapes(
        current_scrape=current_scrape,
        previous_scrape=previous_scrape,
    )
    scrape_time_map = scrape_ctx.get_scrape_time_map([current_scrape, previous_scrape])
    current_capture_label = _format_capture_label(scrape_time_map.get(current_scrape))
    previous_capture_label = _format_capture_label(scrape_time_map.get(previous_scrape))
    final_df = adapt_comparison_for_excel(comparison_df)
    final_df = _filter_df(
        final_df,
        airline=airline,
        origin=origin,
        destination=destination,
        cabin=cabin,
        route_scope=route_scope,
        market_country=market_country,
    )
    final_df = _prepare_for_writer(final_df)
    final_df["current_capture_label"] = current_capture_label or "Current snapshot"
    final_df["previous_capture_label"] = previous_capture_label or "Previous snapshot"

    if final_df.empty:
        raise RuntimeError("No rows available for route_flight_fare_monitor after filters.")

    base_output = Path(output_dir)
    if run_dir:
        target_dir = Path(run_dir)
        m = re.match(r"run_(\d{8}_\d{6}(?:_\d{6})?)_(UTC[pm]\d{4}|UTC\d{4})$", target_dir.name)
        if m:
            ts = m.group(1)
            tz_token = m.group(2)
        else:
            ts, tz_token = _build_run_stamp(timestamp_tz)
    else:
        ts, tz_token = _build_run_stamp(timestamp_tz)
        target_dir = base_output / f"run_{ts}_{tz_token}"

    target_dir.mkdir(parents=True, exist_ok=True)
    execution_plan_payload = _load_execution_plan_payload(base_output, target_dir)

    output_path = target_dir / f"route_flight_fare_monitor_{ts}_{tz_token}.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        OutputWriter(style=style).write_route_flight_fare_monitor(
            writer,
            final_df,
            execution_plan_status=execution_plan_payload,
        )

    return output_path, len(final_df), current_scrape, previous_scrape


def parse_args():
    parser = argparse.ArgumentParser(description="Generate route_flight_fare_monitor workbook")
    parser.add_argument("--output-dir", default="output/reports")
    parser.add_argument("--run-dir", help="Optional existing run folder to write into")
    parser.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    parser.add_argument("--style", choices=["compact", "presentation"], default="compact")
    parser.add_argument("--db-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--airline")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--cabin")
    parser.add_argument("--route-scope", choices=["all", "domestic", "international"], default="all")
    parser.add_argument("--market-country", default="BD")
    parser.add_argument("--current-scrape-id")
    parser.add_argument("--previous-scrape-id")
    parser.add_argument(
        "--no-auto-skip-tiny",
        action="store_true",
        help="Disable auto-skip logic for tiny test scrapes; use raw latest two scrape IDs.",
    )
    parser.add_argument(
        "--scrape-lookback",
        type=int,
        default=40,
        help="How many recent scrapes to inspect when auto-selecting a full pair (default: 40).",
    )
    parser.add_argument(
        "--min-full-scrape-rows",
        type=int,
        default=100,
        help="Minimum rows for a scrape to be considered full in auto-selection (default: 100).",
    )
    parser.add_argument(
        "--min-full-ratio",
        type=float,
        default=0.30,
        help="Adaptive full threshold ratio vs max rows in lookback (default: 0.30).",
    )
    parser.add_argument(
        "--export-macro-xlsm",
        action="store_true",
        help="Also export a macro-enabled .xlsm workbook with airline/signal filter controls.",
    )
    parser.add_argument(
        "--macro-xlsm-path",
        help="Optional explicit output path for the macro-enabled workbook.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_path, row_count, current_scrape, previous_scrape = generate_route_flight_fare_monitor(
        output_dir=args.output_dir,
        run_dir=args.run_dir,
        timestamp_tz=args.timestamp_tz,
        db_url=args.db_url,
        style=args.style,
        airline=args.airline,
        origin=args.origin,
        destination=args.destination,
        cabin=args.cabin,
        route_scope=args.route_scope,
        market_country=args.market_country,
        current_scrape_id=args.current_scrape_id,
        previous_scrape_id=args.previous_scrape_id,
        auto_skip_tiny=not args.no_auto_skip_tiny,
        scrape_lookback=args.scrape_lookback,
        min_full_scrape_rows=args.min_full_scrape_rows,
        min_full_ratio=args.min_full_ratio,
    )
    msg = (
        "route_flight_fare_monitor: "
        f"rows={row_count} current_scrape={current_scrape} previous_scrape={previous_scrape} -> {output_path}"
    )
    if args.export_macro_xlsm:
        macro_path = export_macro_xlsm(
            output_path,
            Path(args.macro_xlsm_path) if args.macro_xlsm_path else None,
        )
        msg = f"{msg}\nroute_flight_fare_monitor_macro: {macro_path}"
    print(msg)


if __name__ == "__main__":
    main()
