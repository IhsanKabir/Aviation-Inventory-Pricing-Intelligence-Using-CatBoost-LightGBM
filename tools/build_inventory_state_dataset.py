import argparse
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.runtime_config import get_database_url


DEFAULT_SCHEMA_VERSION = "inventory_state_v1"

# Airline-specific inventory semantics. Extend as coverage grows.
AIRLINE_INVENTORY_SEMANTICS: dict[str, dict[str, Any]] = {
    "VQ": {"seat_semantics_mode": "non_additive", "bucket_seat_cap": 9},
    "BG": {"seat_semantics_mode": "unknown", "bucket_seat_cap": None},
    "BS": {"seat_semantics_mode": "unknown", "bucket_seat_cap": None},
    "2A": {"seat_semantics_mode": "unknown", "bucket_seat_cap": None},
}


def parse_args():
    p = argparse.ArgumentParser(
        description="Build compact inventory-state ML dataset (features + same-pax next-observation labels)"
    )
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument(
        "--schema-version",
        choices=["inventory_state_v1", "inventory_state_v2"],
        default=DEFAULT_SCHEMA_VERSION,
        help="Dataset schema version (v2 adds route-level party_gap_profile features)",
    )
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--format", choices=["csv", "parquet", "both"], default="csv")
    p.add_argument("--lookback-days", type=int, default=30)
    p.add_argument(
        "--start-scraped-at",
        "--start-observed-at",
        dest="start_scraped_at",
        help="Inclusive accumulation timestamp/date filter (ISO string). Legacy alias: --start-scraped-at",
    )
    p.add_argument(
        "--end-scraped-at",
        "--end-observed-at",
        dest="end_scraped_at",
        help="Inclusive accumulation timestamp/date filter (ISO string). Legacy alias: --end-scraped-at",
    )
    p.add_argument("--airline", help="Comma-separated airline codes")
    p.add_argument("--origin")
    p.add_argument("--destination")
    p.add_argument("--cabin")
    p.add_argument("--probe-group-id", help="Filter to a specific linked probe session id")
    p.add_argument("--adt", type=int, help="Filter by ADT count")
    p.add_argument("--chd", type=int, help="Filter by CHD count")
    p.add_argument("--inf", type=int, help="Filter by INF count")
    p.add_argument("--limit-rows", type=int, help="Limit raw joined rows for debugging")
    p.add_argument("--no-probe-features", action="store_true", help="Skip ADT 1/2/4 probe feature joins")
    return p.parse_args()


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
    return now, ts, tz_token


def _csv_upper_codes(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [v.strip().upper() for v in str(value).split(",") if v.strip()]


def _to_bool_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False)
    return s.map(lambda x: bool(x) if pd.notna(x) else False).fillna(False)


def _first_non_null(values: Iterable[Any]) -> Any:
    for v in values:
        if pd.notna(v):
            return v
    return None


def _safe_float(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _parse_dt_like(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def _build_where_clause(args) -> tuple[str, dict[str, Any]]:
    clauses = []
    params: dict[str, Any] = {}

    start_dt = _parse_dt_like(args.start_scraped_at)
    end_dt = _parse_dt_like(args.end_scraped_at)
    if start_dt is None and end_dt is None and args.lookback_days is not None:
        start_dt = datetime.now() - timedelta(days=max(1, int(args.lookback_days)))

    if start_dt is not None:
        clauses.append("fo.scraped_at >= :start_scraped_at")
        params["start_scraped_at"] = start_dt
    if end_dt is not None:
        clauses.append("fo.scraped_at <= :end_scraped_at")
        params["end_scraped_at"] = end_dt

    airlines = _csv_upper_codes(args.airline)
    if airlines:
        clauses.append("UPPER(fo.airline) = ANY(:airlines)")
        params["airlines"] = airlines
    if args.origin:
        clauses.append("UPPER(fo.origin) = :origin")
        params["origin"] = str(args.origin).upper()
    if args.destination:
        clauses.append("UPPER(fo.destination) = :destination")
        params["destination"] = str(args.destination).upper()
    if args.cabin:
        clauses.append("fo.cabin = :cabin")
        params["cabin"] = str(args.cabin)
    if args.probe_group_id:
        clauses.append("frm.probe_group_id = :probe_group_id")
        params["probe_group_id"] = str(args.probe_group_id).strip()

    if args.adt is not None:
        clauses.append("COALESCE(frm.adt_count, 1) = :adt")
        params["adt"] = int(args.adt)
    if args.chd is not None:
        clauses.append("COALESCE(frm.chd_count, 0) = :chd")
        params["chd"] = int(args.chd)
    if args.inf is not None:
        clauses.append("COALESCE(frm.inf_count, 0) = :inf")
        params["inf"] = int(args.inf)

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)
    return where_sql, params


def _fetch_joined_rows(engine, args) -> pd.DataFrame:
    where_sql, params = _build_where_clause(args)
    limit_sql = ""
    if args.limit_rows:
        limit_sql = "LIMIT :limit_rows"
        params["limit_rows"] = int(args.limit_rows)

    sql = text(
        f"""
        SELECT
            fo.id AS flight_offer_id,
            fo.scrape_id,
            fo.scraped_at,
            fo.airline,
            fo.flight_number,
            fo.origin,
            fo.destination,
            fo.departure,
            fo.cabin,
            fo.brand,
            fo.price_total_bdt,
            fo.fare_basis,
            fo.seat_capacity,
            fo.seat_available,
            frm.tax_amount,
            frm.aircraft,
            frm.duration_min,
            frm.arrival,
            frm.inventory_confidence,
            frm.booking_class,
            frm.soldout,
            COALESCE(frm.adt_count, 1) AS adt_count,
            COALESCE(frm.chd_count, 0) AS chd_count,
            COALESCE(frm.inf_count, 0) AS inf_count
            ,frm.probe_group_id
        FROM flight_offers fo
        LEFT JOIN flight_offer_raw_meta frm
          ON frm.flight_offer_id = fo.id
        {where_sql}
        ORDER BY fo.scraped_at, fo.id
        {limit_sql}
        """
    )
    return pd.read_sql_query(sql, engine, params=params)


def _prepare_joined_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    for col in ["scraped_at", "departure", "arrival"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["price_total_bdt", "tax_amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["seat_capacity", "seat_available", "duration_min", "adt_count", "chd_count", "inf_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "soldout" in df.columns:
        df["soldout"] = _to_bool_series(df["soldout"])
    else:
        df["soldout"] = False

    for col in ["scrape_id", "airline", "flight_number", "origin", "destination", "cabin", "brand", "fare_basis", "booking_class", "aircraft"]:
        if col in df.columns:
            df[col] = df[col].astype(object).where(pd.notna(df[col]), None)
    if "probe_group_id" in df.columns:
        df["probe_group_id"] = df["probe_group_id"].astype(object).where(pd.notna(df["probe_group_id"]), None)

    df["scrape_id"] = df["scrape_id"].astype(str)
    df["airline"] = df["airline"].astype(str).str.upper()
    df["origin"] = df["origin"].astype(str).str.upper()
    df["destination"] = df["destination"].astype(str).str.upper()
    df["cabin"] = df["cabin"].astype(str).replace({"None": None})

    # Bucket code preference order.
    bucket_code = (
        df["fare_basis"].fillna("").astype(str).str.strip()
        .mask(lambda s: s == "", df.get("booking_class", pd.Series(index=df.index)).fillna("").astype(str).str.strip())
        .mask(lambda s: s == "", df.get("brand", pd.Series(index=df.index)).fillna("").astype(str).str.strip())
    )
    df["bucket_code"] = bucket_code.fillna("").replace("", "UNK")

    df["route_key"] = df["origin"] + "-" + df["destination"]
    df["departure_date"] = df["departure"].dt.date
    df["dep_weekday"] = df["departure"].dt.weekday
    df["dep_month"] = df["departure"].dt.month
    df["dep_time_min"] = df["departure"].dt.hour * 60 + df["departure"].dt.minute
    df["search_hour"] = df["scraped_at"].dt.hour
    dt_delta = df["departure"] - df["scraped_at"]
    df["hours_to_departure"] = dt_delta.dt.total_seconds() / 3600.0
    df["days_to_departure"] = (df["hours_to_departure"] / 24.0).apply(
        lambda x: int(math.floor(x)) if pd.notna(x) else None
    ).astype("Int64")
    return df


def _group_row_key_cols() -> list[str]:
    return [
        "scrape_id",
        "probe_group_id",
        "scraped_at",
        "airline",
        "origin",
        "destination",
        "route_key",
        "flight_number",
        "departure",
        "departure_date",
        "cabin",
        "adt_count",
        "chd_count",
        "inf_count",
    ]


def _aggregate_snapshot_group(g: pd.DataFrame) -> Dict[str, Any]:
    g = g.sort_values(["price_total_bdt", "bucket_code"], na_position="last").copy()
    airline = str(g["airline"].iloc[0]).upper()
    sem = AIRLINE_INVENTORY_SEMANTICS.get(airline, {"seat_semantics_mode": "unknown", "bucket_seat_cap": None})
    seat_cap = _safe_int(g["seat_capacity"].dropna().max()) if g["seat_capacity"].notna().any() else None
    bucket_cap = _safe_int(sem.get("bucket_seat_cap"))
    seat_semantics_mode = sem.get("seat_semantics_mode") or "unknown"

    open_mask = (~g["soldout"]) & g["price_total_bdt"].notna()
    g_open = g[open_mask].copy()
    if g_open.empty:
        g_open = g[g["price_total_bdt"].notna()].copy()

    # Deduplicate by bucket to reduce duplicate rows if any parser duplication slips in.
    if not g_open.empty:
        g_open = (
            g_open.sort_values(["price_total_bdt", "seat_available"], ascending=[True, False], na_position="last")
            .drop_duplicates(subset=["bucket_code"], keep="first")
        )

    all_priced = g[g["price_total_bdt"].notna()].copy()
    if not all_priced.empty:
        all_priced = all_priced.sort_values(["price_total_bdt", "bucket_code"], na_position="last")

    is_available = bool(not all_priced.empty and (~all_priced["soldout"]).any())
    open_bucket_count = int(g_open["bucket_code"].nunique()) if not g_open.empty else 0
    priced_bucket_count = int(all_priced["bucket_code"].nunique()) if not all_priced.empty else 0

    lowest_row = g_open.iloc[0] if not g_open.empty else (all_priced.iloc[0] if not all_priced.empty else None)
    highest_row = g_open.iloc[-1] if not g_open.empty else (all_priced.iloc[-1] if not all_priced.empty else None)

    lowest_fare = _safe_float(lowest_row["price_total_bdt"]) if lowest_row is not None else None
    highest_fare = _safe_float(highest_row["price_total_bdt"]) if highest_row is not None else None
    lowest_bucket_code = str(lowest_row["bucket_code"]) if lowest_row is not None else None
    highest_bucket_code = str(highest_row["bucket_code"]) if highest_row is not None else None
    lowest_bucket_seat = _safe_int(lowest_row["seat_available"]) if lowest_row is not None else None

    seat_source = g_open if not g_open.empty else all_priced
    seat_vals = [int(v) for v in seat_source["seat_available"].dropna().tolist()] if not seat_source.empty else []
    positive_seat_vals = [v for v in seat_vals if v > 0]
    max_bucket_seat = max(positive_seat_vals) if positive_seat_vals else (max(seat_vals) if seat_vals else None)
    open_seat_sum = sum(positive_seat_vals) if positive_seat_vals else None
    has_bucket_seat_info = bool(len(seat_vals) > 0)

    lowest_censored = bool(bucket_cap and lowest_bucket_seat is not None and lowest_bucket_seat >= bucket_cap)
    max_censored = bool(bucket_cap and max_bucket_seat is not None and max_bucket_seat >= bucket_cap)

    if not has_bucket_seat_info:
        proxy_quality = "missing"
    else:
        any_missing = bool(seat_source["seat_available"].isna().any()) if not seat_source.empty else False
        any_censored = False
        if bucket_cap:
            any_censored = any((v is not None and int(v) >= bucket_cap) for v in seat_vals)
        if any_missing:
            proxy_quality = "mixed"
        elif any_censored:
            proxy_quality = "censored"
        else:
            proxy_quality = "good"

    open_cap_ratio = None
    inv_press_pct = None
    if seat_cap and open_seat_sum is not None and seat_cap > 0:
        open_cap_ratio = float(open_seat_sum) / float(seat_cap)
        inv_press_pct = (1.0 - min(1.0, max(0.0, open_cap_ratio))) * 100.0

    fare_spread_abs = None
    fare_spread_pct = None
    if lowest_fare is not None and highest_fare is not None:
        fare_spread_abs = highest_fare - lowest_fare
        if lowest_fare > 0:
            fare_spread_pct = (fare_spread_abs / lowest_fare) * 100.0

    aircraft_type = _first_non_null(g["aircraft"])
    duration_min = _first_non_null(g["duration_min"])
    arrival = _first_non_null(g["arrival"])
    tax_vals = [_safe_float(v) for v in g_open["tax_amount"].tolist()] if not g_open.empty else []
    tax_vals = [v for v in tax_vals if v is not None]
    lowest_tax_amount = _safe_float(lowest_row["tax_amount"]) if lowest_row is not None else None
    has_tax_info = bool(len(tax_vals) > 0)
    inventory_conf_values = {str(v).strip().lower() for v in g["inventory_confidence"].dropna().tolist() if str(v).strip()}
    inv_conf_summary = "mixed" if len(inventory_conf_values) > 1 else (_first_non_null(g["inventory_confidence"]) or None)
    soldout_bucket_count = int(g[g["soldout"]]["bucket_code"].nunique()) if "soldout" in g.columns else 0
    dep_weekday = _safe_int(_first_non_null(g["dep_weekday"])) if "dep_weekday" in g.columns else None
    dep_month = _safe_int(_first_non_null(g["dep_month"])) if "dep_month" in g.columns else None
    dep_time_min = _safe_int(_first_non_null(g["dep_time_min"])) if "dep_time_min" in g.columns else None
    search_hour = _safe_int(_first_non_null(g["search_hour"])) if "search_hour" in g.columns else None
    days_to_departure = _safe_int(_first_non_null(g["days_to_departure"])) if "days_to_departure" in g.columns else None
    hours_to_departure = _safe_float(_first_non_null(g["hours_to_departure"])) if "hours_to_departure" in g.columns else None
    probe_group_id = _first_non_null(g["probe_group_id"]) if "probe_group_id" in g.columns else None

    return {
        "probe_group_id": probe_group_id,
        "aircraft_type": aircraft_type,
        "duration_min": _safe_int(duration_min),
        "arrival": arrival,
        "capacity_physical": seat_cap,
        "seat_semantics_mode": seat_semantics_mode,
        "bucket_seat_cap": bucket_cap,
        "is_available": is_available,
        "priced_bucket_count": priced_bucket_count,
        "open_bucket_count": open_bucket_count,
        "soldout_bucket_count": soldout_bucket_count,
        "lowest_open_bucket_code": lowest_bucket_code,
        "highest_open_bucket_code": highest_bucket_code,
        "lowest_open_fare": lowest_fare,
        "highest_open_fare": highest_fare,
        "lowest_open_tax_amount": lowest_tax_amount,
        "fare_spread_abs": fare_spread_abs,
        "fare_spread_pct": fare_spread_pct,
        "lowest_bucket_seat_proxy": lowest_bucket_seat,
        "lowest_bucket_seat_censored": lowest_censored,
        "max_bucket_seat_proxy": max_bucket_seat,
        "max_bucket_seat_censored": max_censored,
        "open_seat_sum": open_seat_sum,
        "open_cap_ratio": open_cap_ratio,
        "open_cap_clamped_ratio": min(1.0, max(0.0, open_cap_ratio)) if open_cap_ratio is not None else None,
        "inv_press_pct": inv_press_pct,
        "has_tax_info": has_tax_info,
        "has_bucket_seat_info": has_bucket_seat_info,
        "inventory_proxy_quality": proxy_quality,
        "inventory_confidence_summary": inv_conf_summary,
        "dep_weekday": dep_weekday,
        "dep_month": dep_month,
        "dep_time_min": dep_time_min,
        "search_hour": search_hour,
        "days_to_departure": days_to_departure,
        "hours_to_departure": hours_to_departure,
    }


def _aggregate_inventory_states(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = _group_row_key_cols()
    rows: list[dict[str, Any]] = []
    for key, g in df.groupby(group_cols, dropna=False, sort=False):
        base = {col: val for col, val in zip(group_cols, key)}
        agg = _aggregate_snapshot_group(g)
        rows.append({**base, **agg})

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["scrape_id"] = out["scrape_id"].astype(str)
    if "probe_group_id" in out.columns:
        out["probe_group_id"] = out["probe_group_id"].astype(object).where(pd.notna(out["probe_group_id"]), None)
    out["observed_at_utc"] = pd.to_datetime(out["scraped_at"], errors="coerce")
    out["probe_join_id"] = out["probe_group_id"].where(out["probe_group_id"].notna(), "SCRAPE:" + out["scrape_id"].astype(str))
    out["flight_key"] = (
        out["airline"].astype(str)
        + "|"
        + out["flight_number"].astype(str)
        + "|"
        + out["origin"].astype(str)
        + "|"
        + out["destination"].astype(str)
        + "|"
        + out["departure"].dt.strftime("%Y-%m-%d %H:%M:%S").astype(str)
    )

    out["is_weekend"] = out["dep_weekday"].isin([5, 6])
    # Normalize numeric dtypes.
    numeric_cols = [
        "capacity_physical",
        "duration_min",
        "priced_bucket_count",
        "open_bucket_count",
        "soldout_bucket_count",
        "lowest_open_fare",
        "highest_open_fare",
        "lowest_open_tax_amount",
        "fare_spread_abs",
        "fare_spread_pct",
        "lowest_bucket_seat_proxy",
        "max_bucket_seat_proxy",
        "open_seat_sum",
        "open_cap_ratio",
        "open_cap_clamped_ratio",
        "inv_press_pct",
        "hours_to_departure",
        "search_hour",
        "dep_time_min",
        "dep_weekday",
        "dep_month",
        "bucket_seat_cap",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ["adt_count", "chd_count", "inf_count", "days_to_departure"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    return out


def _add_next_search_labels(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    gcols = [
        "airline",
        "origin",
        "destination",
        "flight_number",
        "departure",
        "cabin",
        "adt_count",
        "chd_count",
        "inf_count",
    ]
    out = df.sort_values(["observed_at_utc", "scrape_id"]).copy()
    out["_group_sort_key"] = out["scrape_id"].astype(str)

    def _shift_col(col: str):
        return out.groupby(gcols, dropna=False, sort=False)[col].shift(-1)

    out["y_next_search_exists"] = _shift_col("scrape_id").notna()
    out["y_next_search_scrape_id"] = _shift_col("scrape_id")
    out["y_next_search_observed_at_utc"] = _shift_col("observed_at_utc")
    out["y_next_search_lowest_fare"] = _shift_col("lowest_open_fare")
    out["y_next_search_open_bucket_count"] = _shift_col("open_bucket_count")
    out["y_next_search_inv_press_pct"] = _shift_col("inv_press_pct")
    out["y_next_search_is_available"] = _shift_col("is_available")
    out["y_next_search_lowest_bucket_code"] = _shift_col("lowest_open_bucket_code")

    out["y_next_search_lowest_fare_delta"] = out["y_next_search_lowest_fare"] - out["lowest_open_fare"]
    out["y_next_search_lowest_fare_pct_delta"] = (
        (out["y_next_search_lowest_fare"] - out["lowest_open_fare"]) / out["lowest_open_fare"] * 100.0
    )
    invalid_pct = out["lowest_open_fare"].isna() | (pd.to_numeric(out["lowest_open_fare"], errors="coerce") <= 0)
    out.loc[invalid_pct, "y_next_search_lowest_fare_pct_delta"] = pd.NA

    next_dt = pd.to_datetime(out["y_next_search_observed_at_utc"], errors="coerce")
    cur_dt = pd.to_datetime(out["observed_at_utc"], errors="coerce")
    out["y_next_search_gap_hours"] = (next_dt - cur_dt).dt.total_seconds() / 3600.0

    def _move_class(row):
        if not bool(row.get("y_next_search_exists")):
            return None
        delta = row.get("y_next_search_lowest_fare_delta")
        if pd.isna(delta):
            return None
        if float(delta) > 1e-9:
            return "up"
        if float(delta) < -1e-9:
            return "down"
        return "same"

    out["y_next_search_price_move_class"] = out.apply(_move_class, axis=1)

    # Light multi-step label: fare increase within next 3 searches (same pax basis).
    group_obj = out.groupby(gcols, dropna=False, sort=False)["lowest_open_fare"]
    s1 = group_obj.shift(-1)
    s2 = group_obj.shift(-2)
    s3 = group_obj.shift(-3)
    future_max = pd.concat([s1, s2, s3], axis=1).max(axis=1, skipna=True)
    out["y_fare_increase_within_3_searches"] = (
        future_max.notna() & out["lowest_open_fare"].notna() & (future_max > out["lowest_open_fare"])
    )

    out.drop(columns=["_group_sort_key"], inplace=True, errors="ignore")
    return out


def _add_probe_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    base_key = [
        "probe_join_id",
        "airline",
        "origin",
        "destination",
        "flight_number",
        "departure",
        "cabin",
        "chd_count",
        "inf_count",
    ]
    probe_src = df[base_key + ["adt_count", "lowest_open_fare", "is_available", "lowest_open_bucket_code"]].copy()
    probe_src["adt_count"] = pd.to_numeric(probe_src["adt_count"], errors="coerce").astype("Int64")
    probe_src = probe_src[probe_src["adt_count"].isin([1, 2, 3, 4])]

    if probe_src.empty:
        for col in [
            "probe_has_adt2",
            "probe_has_adt3",
            "probe_has_adt4",
            "fare_adt1",
            "fare_adt2",
            "fare_adt3",
            "fare_adt4",
            "fare_gap_1_to_2",
            "fare_gap_1_to_3",
            "fare_gap_2_to_3",
            "fare_gap_2_to_4",
            "fare_gap_3_to_4",
            "fare_gap_1_to_4",
            "availability_adt1",
            "availability_adt2",
            "availability_adt3",
            "availability_adt4",
            "lowest_bucket_code_adt1",
            "lowest_bucket_code_adt2",
            "lowest_bucket_code_adt3",
            "lowest_bucket_code_adt4",
            "party_breakpoint_est",
        ]:
            if col not in df.columns:
                df[col] = pd.NA
        return df

    fare_pivot = probe_src.pivot_table(index=base_key, columns="adt_count", values="lowest_open_fare", aggfunc="first")
    avail_pivot = (
        probe_src.assign(is_available_num=probe_src["is_available"].astype(int))
        .pivot_table(index=base_key, columns="adt_count", values="is_available_num", aggfunc="first")
    )
    bucket_pivot = probe_src.pivot_table(index=base_key, columns="adt_count", values="lowest_open_bucket_code", aggfunc="first")

    for pivot_df, prefix in [(fare_pivot, "fare"), (avail_pivot, "availability"), (bucket_pivot, "lowest_bucket_code")]:
        pivot_df.columns = [f"{prefix}_adt{int(c)}" for c in pivot_df.columns]
        pivot_df.reset_index(inplace=True)
        df = df.merge(pivot_df, on=base_key, how="left")

    for col in ["availability_adt1", "availability_adt2", "availability_adt3", "availability_adt4"]:
        if col in df.columns:
            df[col] = df[col].map(lambda x: bool(int(x)) if pd.notna(x) else pd.NA)

    df["probe_has_adt2"] = df["fare_adt2"].notna() if "fare_adt2" in df.columns else False
    df["probe_has_adt3"] = df["fare_adt3"].notna() if "fare_adt3" in df.columns else False
    df["probe_has_adt4"] = df["fare_adt4"].notna() if "fare_adt4" in df.columns else False
    if "fare_adt1" in df.columns and "fare_adt2" in df.columns:
        df["fare_gap_1_to_2"] = df["fare_adt2"] - df["fare_adt1"]
    if "fare_adt1" in df.columns and "fare_adt3" in df.columns:
        df["fare_gap_1_to_3"] = df["fare_adt3"] - df["fare_adt1"]
    if "fare_adt2" in df.columns and "fare_adt3" in df.columns:
        df["fare_gap_2_to_3"] = df["fare_adt3"] - df["fare_adt2"]
    if "fare_adt2" in df.columns and "fare_adt4" in df.columns:
        df["fare_gap_2_to_4"] = df["fare_adt4"] - df["fare_adt2"]
    if "fare_adt3" in df.columns and "fare_adt4" in df.columns:
        df["fare_gap_3_to_4"] = df["fare_adt4"] - df["fare_adt3"]
    if "fare_adt1" in df.columns and "fare_adt4" in df.columns:
        df["fare_gap_1_to_4"] = df["fare_adt4"] - df["fare_adt1"]

    def _party_breakpoint(row):
        # Smallest observed ADT among {2,3,4} that changes fare vs ADT1 or becomes unavailable.
        if pd.isna(row.get("fare_adt1")) and pd.isna(row.get("availability_adt1")):
            return None
        base_fare = row.get("fare_adt1")
        base_avail = row.get("availability_adt1")
        for adt in (2, 3, 4):
            avail = row.get(f"availability_adt{adt}")
            fare = row.get(f"fare_adt{adt}")
            if avail is pd.NA and pd.isna(fare):
                continue
            if base_avail is True and avail is False:
                return adt
            if pd.notna(base_fare) and pd.notna(fare) and float(fare) > float(base_fare):
                return adt
        return None

    df["party_breakpoint_est"] = df.apply(_party_breakpoint, axis=1)
    return df


def _add_party_gap_profile_features(df: pd.DataFrame) -> pd.DataFrame:
    """Route-level probe sensitivity profile (aggregated over lookback/probe scope).

    First v2 implementation: aggregates observed probe behavior by (airline, origin, destination, cabin)
    and merges the profile back onto all rows for reuse in modeling/reporting.
    """
    if df.empty:
        return df

    required = {"airline", "origin", "destination", "cabin"}
    if not required.issubset(df.columns):
        return df

    # If probe features are unavailable, create empty columns and return.
    needed_probe_cols = {
        "probe_join_id",
        "adt_count",
        "fare_gap_1_to_2",
        "fare_gap_1_to_3",
        "fare_gap_2_to_3",
        "availability_adt1",
        "availability_adt2",
        "party_breakpoint_est",
    }
    if not needed_probe_cols.intersection(df.columns):
        return df

    key_cols = ["airline", "origin", "destination", "cabin"]
    probe_base_cols = key_cols + ["probe_join_id"]
    for c in probe_base_cols:
        if c not in df.columns:
            return df

    work = df.copy()
    # Use ADT=1 rows as canonical representatives to avoid duplicate weighting across ADT rows.
    if "adt_count" in work.columns:
        work["adt_count_num"] = pd.to_numeric(work["adt_count"], errors="coerce")
        reps = work[work["adt_count_num"] == 1].copy()
        if reps.empty:
            reps = work.copy()
    else:
        reps = work.copy()

    # Deduplicate one record per probe_join_id + route/cabin.
    reps = reps.sort_values(
        [c for c in ["observed_at_utc", "departure", "flight_number"] if c in reps.columns],
        na_position="last",
    ).drop_duplicates(subset=probe_base_cols, keep="first")

    # Pairing and gap availability masks.
    if "fare_gap_1_to_2" in reps.columns:
        reps["pair_1_2"] = reps["fare_gap_1_to_2"].notna()
        reps["jump_1_2"] = reps["fare_gap_1_to_2"].map(lambda x: pd.notna(x) and float(x) > 1e-9)
    else:
        reps["pair_1_2"] = False
        reps["jump_1_2"] = False

    if {"availability_adt1", "availability_adt2"}.issubset(reps.columns):
        reps["avail_drop_1_2"] = (
            reps["availability_adt1"].map(lambda x: x is True)
            & reps["availability_adt2"].map(lambda x: x is False)
        )
    else:
        reps["avail_drop_1_2"] = False

    if "party_breakpoint_est" in reps.columns:
        reps["bp2"] = reps["party_breakpoint_est"] == 2
        reps["bp3"] = reps["party_breakpoint_est"] == 3
        reps["bp4"] = reps["party_breakpoint_est"] == 4
        reps["bp_any"] = reps["party_breakpoint_est"].notna()
    else:
        reps["bp2"] = False
        reps["bp3"] = False
        reps["bp4"] = False
        reps["bp_any"] = False

    def _safe_median(s: pd.Series):
        s = pd.to_numeric(s, errors="coerce")
        return float(s.median()) if s.notna().any() else pd.NA

    def _safe_mean(s: pd.Series):
        s = pd.to_numeric(s, errors="coerce")
        return float(s.mean()) if s.notna().any() else pd.NA

    def _rate(num: pd.Series, den: pd.Series):
        den_count = int(pd.to_numeric(den, errors="coerce").fillna(0).sum())
        if den_count <= 0:
            return pd.NA
        num_count = int(pd.to_numeric(num, errors="coerce").fillna(0).sum())
        return float(num_count / den_count)

    grouped_rows = []
    for keys, g in reps.groupby(key_cols, dropna=False, sort=False):
        row = {k: v for k, v in zip(key_cols, keys)}
        pair_1_2_mask = g["pair_1_2"].fillna(False)
        pair_1_3_mask = g["fare_gap_1_to_3"].notna() if "fare_gap_1_to_3" in g.columns else pd.Series(False, index=g.index)
        pair_2_3_mask = g["fare_gap_2_to_3"].notna() if "fare_gap_2_to_3" in g.columns else pd.Series(False, index=g.index)
        bp_any_mask = g["bp_any"].fillna(False)

        row["party_gap_profile_obs_count"] = int(len(g))
        row["party_gap_profile_probe_session_count"] = int(g["probe_join_id"].nunique(dropna=True))
        row["party_gap_profile_paired_1_2_count"] = int(pair_1_2_mask.sum())
        row["party_gap_profile_paired_1_3_count"] = int(pair_1_3_mask.sum())
        row["party_gap_profile_paired_2_3_count"] = int(pair_2_3_mask.sum())

        row["party_gap_profile_median_gap_1_2"] = _safe_median(g.loc[pair_1_2_mask, "fare_gap_1_to_2"]) if "fare_gap_1_to_2" in g.columns else pd.NA
        row["party_gap_profile_median_gap_1_3"] = _safe_median(g.loc[pair_1_3_mask, "fare_gap_1_to_3"]) if "fare_gap_1_to_3" in g.columns else pd.NA
        row["party_gap_profile_median_gap_2_3"] = _safe_median(g.loc[pair_2_3_mask, "fare_gap_2_to_3"]) if "fare_gap_2_to_3" in g.columns else pd.NA
        row["party_gap_profile_mean_gap_1_2"] = _safe_mean(g.loc[pair_1_2_mask, "fare_gap_1_to_2"]) if "fare_gap_1_to_2" in g.columns else pd.NA
        row["party_gap_profile_mean_gap_1_3"] = _safe_mean(g.loc[pair_1_3_mask, "fare_gap_1_to_3"]) if "fare_gap_1_to_3" in g.columns else pd.NA
        row["party_gap_profile_mean_gap_2_3"] = _safe_mean(g.loc[pair_2_3_mask, "fare_gap_2_to_3"]) if "fare_gap_2_to_3" in g.columns else pd.NA

        row["party_gap_profile_jump_1_2_rate"] = _rate(g["jump_1_2"].astype(int), pair_1_2_mask.astype(int))
        row["party_gap_profile_avail_drop_1_2_rate"] = _rate(g["avail_drop_1_2"].astype(int), pair_1_2_mask.astype(int))
        row["party_gap_profile_breakpoint2_rate"] = _rate(g["bp2"].astype(int), bp_any_mask.astype(int))
        row["party_gap_profile_breakpoint3_rate"] = _rate(g["bp3"].astype(int), bp_any_mask.astype(int))
        row["party_gap_profile_breakpoint4_rate"] = _rate(g["bp4"].astype(int), bp_any_mask.astype(int))

        # Compact categorical profile string (human-readable + reusable for grouping).
        med12 = row["party_gap_profile_median_gap_1_2"]
        med13 = row["party_gap_profile_median_gap_1_3"]
        bp2r = row["party_gap_profile_breakpoint2_rate"]
        jump = row["party_gap_profile_jump_1_2_rate"]
        avail = row["party_gap_profile_avail_drop_1_2_rate"]
        if pd.isna(med12) and pd.isna(med13):
            row["party_gap_profile"] = "insufficient_probe_data"
        else:
            row["party_gap_profile"] = (
                f"bp2r={('NA' if pd.isna(bp2r) else f'{bp2r:.2f}')}"
                f"|j12={('NA' if pd.isna(jump) else f'{jump:.2f}')}"
                f"|a12={('NA' if pd.isna(avail) else f'{avail:.2f}')}"
                f"|g12={('NA' if pd.isna(med12) else int(round(float(med12))))}"
                f"|g13={('NA' if pd.isna(med13) else int(round(float(med13))))}"
                f"|n={row['party_gap_profile_obs_count']}"
            )
        grouped_rows.append(row)

    profile_df = pd.DataFrame(grouped_rows)
    if profile_df.empty:
        return df

    return df.merge(profile_df, on=key_cols, how="left")


def _add_route_specific_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add v2 engineered features, with DAC-SPD-specific masked variants.

    These features are safe for the trainer because they use current-state observables only
    (no future labels). DAC-SPD-specific variants are masked by route flag so a single
    trainer can learn route-specific effects while remaining usable on broader route groups.
    """
    if df.empty:
        return df

    out = df.copy()
    if "route_key" not in out.columns:
        return out

    route_key = out["route_key"].astype(str).str.upper()
    out["route_is_dac_spd"] = route_key.eq("DAC-SPD")
    route_mask = out["route_is_dac_spd"].fillna(False)
    route_mask_num = route_mask.astype(int)

    # Time-of-day bins (departure/search). Kept as categorical features for the trainer.
    def _tod_bin(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        return pd.cut(
            s,
            bins=[-1, 299, 419, 599, 719, 899, 1079, 1439, 10_000],
            labels=[
                "late_night",   # 00:00-04:59
                "early_morning",# 05:00-06:59
                "morning_peak", # 07:00-09:59
                "late_morning", # 10:00-11:59
                "afternoon",    # 12:00-14:59
                "evening",      # 15:00-17:59
                "night",        # 18:00-23:59
                "unknown",
            ],
            include_lowest=True,
        ).astype(object)

    if "dep_time_min" in out.columns:
        out["dep_tod_bin"] = _tod_bin(out["dep_time_min"])
        out["dep_tod_bin"] = out["dep_tod_bin"].where(out["dep_tod_bin"].notna(), "unknown")
        out["dep_tod_bin_dac_spd"] = np.where(route_mask, out["dep_tod_bin"], "non_dac_spd")

    if "search_hour" in out.columns:
        search_min = pd.to_numeric(out["search_hour"], errors="coerce") * 60.0
        out["search_tod_bin"] = _tod_bin(search_min)
        out["search_tod_bin"] = out["search_tod_bin"].where(out["search_tod_bin"].notna(), "unknown")
        out["search_tod_bin_dac_spd"] = np.where(route_mask, out["search_tod_bin"], "non_dac_spd")

    # Days-to-departure buckets and DOW interactions.
    dtd = pd.to_numeric(out.get("days_to_departure"), errors="coerce")
    dtd = dtd.clip(lower=0) if dtd is not None else dtd
    dow = pd.to_numeric(out.get("dep_weekday"), errors="coerce")
    is_weekend_num = out.get("is_weekend")
    if is_weekend_num is not None:
        is_weekend_num = out["is_weekend"].map(lambda x: 1.0 if bool(x) else 0.0)
    else:
        is_weekend_num = pd.Series(np.nan, index=out.index)

    if dtd is not None:
        out["dtd_bucket"] = pd.cut(
            dtd,
            bins=[-1, 0, 1, 2, 3, 7, 14, 30, 10_000],
            labels=["D0", "D1", "D2", "D3", "D4_7", "D8_14", "D15_30", "D31p"],
            include_lowest=True,
        ).astype(object)
        out["dtd_bucket"] = out["dtd_bucket"].where(out["dtd_bucket"].notna(), "unknown")
        out["dac_spd_dtd_bucket"] = np.where(route_mask, out["dtd_bucket"], "non_dac_spd")

    if dtd is not None and dow is not None:
        out["dtd_x_dep_weekday"] = dtd * dow
        out["dtd_x_is_weekend"] = dtd * is_weekend_num
        out["dac_spd_dtd_x_dep_weekday"] = (dtd * dow * route_mask_num).astype(float)
        out["dac_spd_dtd_x_is_weekend"] = (dtd * is_weekend_num * route_mask_num).astype(float)
        out["dac_spd_days_to_departure"] = (dtd * route_mask_num).astype(float)
        out["dac_spd_dep_weekday"] = (dow * route_mask_num).astype(float)
        # Categorical interaction key for route-specific temporal regimes.
        dow_key = dow.map(lambda x: f"dow{int(x)}" if pd.notna(x) else "dowNA")
        dtd_key = out["dac_spd_dtd_bucket"] if "dac_spd_dtd_bucket" in out.columns else "non_dac_spd"
        out["dac_spd_dow_dtd_key"] = np.where(route_mask, dow_key.astype(str) + "|" + pd.Series(dtd_key, index=out.index).astype(str), "non_dac_spd")

    # Fare ladder shape ratios (current-state only).
    low = pd.to_numeric(out.get("lowest_open_fare"), errors="coerce")
    high = pd.to_numeric(out.get("highest_open_fare"), errors="coerce")
    spread_abs = pd.to_numeric(out.get("fare_spread_abs"), errors="coerce")
    open_buckets = pd.to_numeric(out.get("open_bucket_count"), errors="coerce")
    priced_buckets = pd.to_numeric(out.get("priced_bucket_count"), errors="coerce")
    tax_low = pd.to_numeric(out.get("lowest_open_tax_amount"), errors="coerce")
    inv_press = pd.to_numeric(out.get("inv_press_pct"), errors="coerce")
    open_cap = pd.to_numeric(out.get("open_cap_ratio"), errors="coerce")

    valid_low = low.notna() & (low > 0)
    valid_high = high.notna() & (high > 0)
    out["fare_ladder_high_low_ratio"] = np.where(valid_low & valid_high, high / low, np.nan)
    out["fare_ladder_spread_to_low_ratio"] = np.where(valid_low, spread_abs / low, np.nan)
    out["fare_ladder_tax_share_lowest"] = np.where(valid_low & tax_low.notna(), tax_low / low, np.nan)
    out["fare_ladder_open_bucket_density"] = np.where(
        priced_buckets.notna() & (priced_buckets > 0) & open_buckets.notna(),
        open_buckets / priced_buckets,
        np.nan,
    )
    out["fare_ladder_spread_per_open_bucket"] = np.where(
        open_buckets.notna() & (open_buckets > 0) & spread_abs.notna(),
        spread_abs / open_buckets,
        np.nan,
    )
    out["fare_ladder_spread_per_step"] = np.where(
        open_buckets.notna() & (open_buckets > 1) & spread_abs.notna(),
        spread_abs / (open_buckets - 1.0),
        np.nan,
    )

    # Route-specific masked variants for DAC-SPD.
    for src_col, dst_col in [
        ("inv_press_pct", "dac_spd_inv_press_pct"),
        ("open_cap_ratio", "dac_spd_open_cap_ratio"),
        ("lowest_open_fare", "dac_spd_lowest_open_fare"),
        ("fare_spread_abs", "dac_spd_fare_spread_abs"),
        ("fare_spread_pct", "dac_spd_fare_spread_pct"),
        ("fare_ladder_high_low_ratio", "dac_spd_fare_ladder_high_low_ratio"),
        ("fare_ladder_spread_to_low_ratio", "dac_spd_fare_ladder_spread_to_low_ratio"),
        ("fare_ladder_open_bucket_density", "dac_spd_fare_ladder_open_bucket_density"),
        ("party_gap_profile_median_gap_1_2", "dac_spd_party_gap_prior_g12"),
        ("party_gap_profile_jump_1_2_rate", "dac_spd_party_gap_prior_jump12"),
        ("party_gap_profile_breakpoint2_rate", "dac_spd_party_gap_prior_bp2"),
    ]:
        if src_col in out.columns:
            src = pd.to_numeric(out[src_col], errors="coerce")
            out[dst_col] = np.where(route_mask, src, 0.0)

    # Additional route-specific interactions that often matter for two-stage gating and moved-row delta size.
    if dtd is not None:
        if "dac_spd_inv_press_pct" in out.columns:
            out["dac_spd_inv_press_x_dtd"] = out["dac_spd_inv_press_pct"] * dtd.fillna(0)
        if "dac_spd_open_cap_ratio" in out.columns:
            out["dac_spd_open_cap_x_dtd"] = out["dac_spd_open_cap_ratio"] * dtd.fillna(0)
        if "dac_spd_fare_spread_pct" in out.columns:
            out["dac_spd_spread_pct_x_dtd"] = out["dac_spd_fare_spread_pct"] * dtd.fillna(0)

    return out


def _write_outputs(df: pd.DataFrame, meta: dict[str, Any], args, schema_version: str) -> list[str]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _, ts, tz_token = _build_run_stamp(args.timestamp_tz)
    base_name = f"{schema_version}_{ts}_{tz_token}"
    written: list[str] = []

    csv_path = out_dir / f"{base_name}.csv"
    parquet_path = out_dir / f"{base_name}.parquet"
    meta_path = out_dir / f"{base_name}.json"

    if args.format in {"csv", "both"}:
        df.to_csv(csv_path, index=False)
        (out_dir / f"{schema_version}_latest.csv").write_bytes(csv_path.read_bytes())
        written.append(str(csv_path))

    if args.format in {"parquet", "both"}:
        try:
            df.to_parquet(parquet_path, index=False)
            (out_dir / f"{schema_version}_latest.parquet").write_bytes(parquet_path.read_bytes())
            written.append(str(parquet_path))
            meta["parquet_written"] = True
        except Exception as exc:
            meta["parquet_written"] = False
            meta["parquet_error"] = str(exc)

    meta_path.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    (out_dir / f"{schema_version}_latest.json").write_bytes(meta_path.read_bytes())
    written.append(str(meta_path))
    return written


def main():
    args = parse_args()
    schema_version = args.schema_version or DEFAULT_SCHEMA_VERSION
    engine = create_engine(args.db_url, pool_pre_ping=True, future=True)

    raw_df = _fetch_joined_rows(engine, args)
    raw_df = _prepare_joined_rows(raw_df)
    if raw_df.empty:
        raise SystemExit("No rows found for requested filters; dataset not generated.")

    state_df = _aggregate_inventory_states(raw_df)
    state_df = _add_next_search_labels(state_df)
    if not args.no_probe_features:
        state_df = _add_probe_features(state_df)
    if schema_version == "inventory_state_v2":
        state_df = _add_party_gap_profile_features(state_df)
        state_df = _add_route_specific_engineered_features(state_df)

    # Stable sort for exports.
    sort_cols = [c for c in ["observed_at_utc", "airline", "origin", "destination", "departure", "flight_number", "cabin", "adt_count", "chd_count", "inf_count"] if c in state_df.columns]
    state_df = state_df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

    meta = {
        "schema_version": schema_version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_url_redacted": str(args.db_url).split("@")[-1] if "@" in str(args.db_url) else str(args.db_url),
        "filters": {
            "lookback_days": args.lookback_days,
            "start_scraped_at": args.start_scraped_at,
            "end_scraped_at": args.end_scraped_at,
            "airline": args.airline,
            "origin": args.origin,
            "destination": args.destination,
            "cabin": args.cabin,
            "probe_group_id": args.probe_group_id,
            "adt": args.adt,
            "chd": args.chd,
            "inf": args.inf,
            "limit_rows": args.limit_rows,
        },
        "raw_joined_row_count": int(len(raw_df)),
        "dataset_row_count": int(len(state_df)),
        "columns": list(state_df.columns),
        "label_columns": [c for c in state_df.columns if c.startswith("y_")],
        "probe_feature_columns": [
            c
            for c in state_df.columns
            if c.startswith(("fare_adt", "availability_adt", "probe_", "party_breakpoint_", "party_gap_profile"))
        ],
        "same_pax_label_rule": "Labels are generated using next-search rows grouped by identical airline/route/flight/departure/cabin/ADT/CHD/INF.",
    }

    written = _write_outputs(state_df, meta, args, schema_version)
    print(
        f"{schema_version}: raw_rows={len(raw_df)} dataset_rows={len(state_df)} "
        f"labels={len(meta['label_columns'])} -> " + ", ".join(written)
    )


if __name__ == "__main__":
    main()
