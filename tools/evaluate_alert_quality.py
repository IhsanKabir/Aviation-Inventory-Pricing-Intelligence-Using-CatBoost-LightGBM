"""
Evaluate alert quality (P2-C):
- Precision/Recall/F1 for spike and sellout alerts
- False-alarm and missed-event cost tracking
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import DATABASE_URL as DEFAULT_DATABASE_URL


GROUP_COLS = ["airline", "origin", "destination", "cabin"]


def parse_args():
    p = argparse.ArgumentParser(description="Alert quality evaluation")
    p.add_argument("--start-date", help="YYYY-MM-DD")
    p.add_argument("--end-date", help="YYYY-MM-DD")
    p.add_argument("--airline")
    p.add_argument("--origin")
    p.add_argument("--destination")
    p.add_argument("--cabin")

    p.add_argument("--lookback-days", type=int, default=7)

    p.add_argument("--spike-threshold", type=float, default=250.0, help="Actual/pred threshold on total_change_events")
    p.add_argument("--sellout-threshold", type=float, default=1.0, help="Actual/pred threshold on soldout_flag_changes")

    p.add_argument("--spike-false-alarm-cost", type=float, default=1.0)
    p.add_argument("--spike-missed-cost", type=float, default=3.0)
    p.add_argument("--sellout-false-alarm-cost", type=float, default=2.0)
    p.add_argument("--sellout-missed-cost", type=float, default=8.0)

    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    return p.parse_args()


def _build_where(args):
    clauses = []
    params = {}
    if args.start_date:
        clauses.append("rs.report_day >= :start_date")
        params["start_date"] = args.start_date
    if args.end_date:
        clauses.append("rs.report_day <= :end_date")
        params["end_date"] = args.end_date
    if args.airline:
        clauses.append("rs.airline = :airline")
        params["airline"] = args.airline.upper()
    if args.origin:
        clauses.append("rs.origin = :origin")
        params["origin"] = args.origin.upper()
    if args.destination:
        clauses.append("rs.destination = :destination")
        params["destination"] = args.destination.upper()
    if args.cabin:
        clauses.append("rs.cabin = :cabin")
        params["cabin"] = args.cabin
    where = ""
    if clauses:
        where = " WHERE " + " AND ".join(clauses)
    return where, params


def load_daily(args):
    where_sql, params = _build_where(args)
    sql = text(
        f"""
        SELECT
            rs.report_day,
            rs.airline,
            rs.origin,
            rs.destination,
            rs.cabin,
            COALESCE(rs.total_change_events, 0) AS total_change_events,
            COALESCE(rs.price_events, 0) AS price_events,
            COALESCE(rs.availability_events, 0) AS availability_events,
            COALESCE(av.soldout_flag_changes, 0) AS soldout_flag_changes,
            COALESCE(av.row_removed_events, 0) AS row_removed_events,
            COALESCE(av.row_added_events, 0) AS row_added_events
        FROM airline_intel.vw_route_airline_summary rs
        LEFT JOIN airline_intel.vw_availability_changes_daily av
          ON rs.report_day = av.report_day
         AND rs.airline = av.airline
         AND rs.origin = av.origin
         AND rs.destination = av.destination
         AND rs.cabin = av.cabin
        {where_sql}
        ORDER BY rs.airline, rs.origin, rs.destination, rs.cabin, rs.report_day
        """
    )
    engine = create_engine(args.db_url, pool_pre_ping=True, future=True)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        return df
    df["report_day"] = pd.to_datetime(df["report_day"], errors="coerce").dt.date
    return df


def _classify_alerts(df: pd.DataFrame, args):
    out = df.copy()
    lookback = max(int(args.lookback_days), 1)
    out = out.sort_values(GROUP_COLS + ["report_day"]).reset_index(drop=True)

    # Prediction proxy: previous rolling mean (search-independent deterministic baseline)
    out["pred_spike_score"] = (
        out.groupby(GROUP_COLS)["total_change_events"]
        .transform(lambda s: s.shift(1).rolling(lookback, min_periods=1).mean())
    )
    out["pred_sellout_score"] = (
        out.groupby(GROUP_COLS)["soldout_flag_changes"]
        .transform(lambda s: s.shift(1).rolling(lookback, min_periods=1).mean())
    )

    out["actual_spike_alert"] = out["total_change_events"] >= float(args.spike_threshold)
    out["pred_spike_alert"] = out["pred_spike_score"] >= float(args.spike_threshold)

    # sellout actual includes soldout flips or row removals
    out["actual_sellout_alert"] = (
        (out["soldout_flag_changes"] >= float(args.sellout_threshold))
        | (out["row_removed_events"] > 0)
    )
    out["pred_sellout_alert"] = out["pred_sellout_score"] >= float(args.sellout_threshold)
    return out


def _binary_metrics(actual: pd.Series, pred: pd.Series):
    a = actual.fillna(False).astype(bool)
    p = pred.fillna(False).astype(bool)
    tp = int(((a == True) & (p == True)).sum())  # noqa: E712
    fp = int(((a == False) & (p == True)).sum())  # noqa: E712
    fn = int(((a == True) & (p == False)).sum())  # noqa: E712
    tn = int(((a == False) & (p == False)).sum())  # noqa: E712
    precision = tp / (tp + fp) if (tp + fp) else None
    recall = tp / (tp + fn) if (tp + fn) else None
    f1 = (2 * precision * recall / (precision + recall)) if (precision is not None and recall is not None and (precision + recall) > 0) else None
    accuracy = (tp + tn) / (tp + fp + fn + tn) if (tp + fp + fn + tn) else None
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
        "support": int((a == True).sum()),  # noqa: E712
    }


def _cost(fp: int, fn: int, false_alarm_cost: float, missed_cost: float):
    false_alarm = float(fp) * float(false_alarm_cost)
    missed = float(fn) * float(missed_cost)
    return false_alarm, missed, false_alarm + missed


def summarize(df_eval: pd.DataFrame, args):
    rows_overall = []
    rows_route = []

    alert_defs = [
        (
            "spike",
            "actual_spike_alert",
            "pred_spike_alert",
            float(args.spike_false_alarm_cost),
            float(args.spike_missed_cost),
        ),
        (
            "sellout",
            "actual_sellout_alert",
            "pred_sellout_alert",
            float(args.sellout_false_alarm_cost),
            float(args.sellout_missed_cost),
        ),
    ]

    for alert_name, actual_col, pred_col, fa_cost, miss_cost in alert_defs:
        m = _binary_metrics(df_eval[actual_col], df_eval[pred_col])
        c_fa, c_miss, c_total = _cost(m["fp"], m["fn"], fa_cost, miss_cost)
        rows_overall.append(
            {
                "alert_type": alert_name,
                **m,
                "false_alarm_cost_total": c_fa,
                "missed_event_cost_total": c_miss,
                "total_cost": c_total,
                "false_alarm_unit_cost": fa_cost,
                "missed_event_unit_cost": miss_cost,
            }
        )

    for group_key, part in df_eval.groupby(GROUP_COLS, dropna=False):
        for alert_name, actual_col, pred_col, fa_cost, miss_cost in alert_defs:
            m = _binary_metrics(part[actual_col], part[pred_col])
            c_fa, c_miss, c_total = _cost(m["fp"], m["fn"], fa_cost, miss_cost)
            rows_route.append(
                {
                    "airline": group_key[0],
                    "origin": group_key[1],
                    "destination": group_key[2],
                    "cabin": group_key[3],
                    "alert_type": alert_name,
                    **m,
                    "false_alarm_cost_total": c_fa,
                    "missed_event_cost_total": c_miss,
                    "total_cost": c_total,
                    "false_alarm_unit_cost": fa_cost,
                    "missed_event_unit_cost": miss_cost,
                }
            )

    return pd.DataFrame(rows_overall), pd.DataFrame(rows_route)


def _run_stamp(timestamp_tz: str):
    if timestamp_tz == "utc":
        now = datetime.now(timezone.utc)
    else:
        now = datetime.now().astimezone()
    ts = now.strftime("%Y%m%d_%H%M%S")
    return ts


def main():
    args = parse_args()
    df = load_daily(args)
    if df.empty:
        print("No rows found for alert evaluation scope.")
        return 0

    df_eval = _classify_alerts(df, args)
    overall, by_route = summarize(df_eval, args)

    ts = _run_stamp(args.timestamp_tz)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    daily_path = out_dir / f"alert_quality_daily_{ts}.csv"
    overall_path = out_dir / f"alert_quality_overall_{ts}.csv"
    route_path = out_dir / f"alert_quality_by_route_{ts}.csv"

    keep_cols = [
        "report_day",
        "airline",
        "origin",
        "destination",
        "cabin",
        "total_change_events",
        "price_events",
        "soldout_flag_changes",
        "row_removed_events",
        "pred_spike_score",
        "pred_sellout_score",
        "actual_spike_alert",
        "pred_spike_alert",
        "actual_sellout_alert",
        "pred_sellout_alert",
    ]
    df_eval[keep_cols].to_csv(daily_path, index=False)
    overall.to_csv(overall_path, index=False)
    by_route.to_csv(route_path, index=False)

    print(f"daily_rows={len(df_eval)} -> {daily_path}")
    print(f"overall_rows={len(overall)} -> {overall_path}")
    print(f"by_route_rows={len(by_route)} -> {route_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
