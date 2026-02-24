from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    p = argparse.ArgumentParser(description="Analyze a linked passenger-size probe group")
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--probe-group-id", help="Probe group id (defaults to latest probe_group_run_latest.json)")
    p.add_argument("--probe-run-summary", default="output/reports/probe_group_run_latest.json")

    p.add_argument("--dataset-csv", default="output/reports/inventory_state_v1_latest.csv")
    p.add_argument("--auto-build-dataset", action="store_true", help="Rebuild a probe-scoped inventory dataset before analysis")
    p.add_argument("--dataset-lookback-days", type=int, default=7)
    p.add_argument("--dataset-output-dir", default="output/reports")
    p.add_argument("--dataset-format", choices=["csv", "parquet", "both"], default="csv")

    # Optional explicit filters (used mainly for auto-build)
    p.add_argument("--airline")
    p.add_argument("--origin")
    p.add_argument("--destination")
    p.add_argument("--cabin")
    p.add_argument("--chd", type=int)
    p.add_argument("--inf", type=int)

    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--top-n", type=int, default=15, help="Top-N rows to include in markdown tables")
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


def _slug(s: str) -> str:
    out = []
    for ch in str(s):
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        else:
            out.append("_")
    slug = "".join(out).strip("_")
    return slug[:80] if slug else "probe"


def _read_json(path: Path) -> dict | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _resolve_probe_context(args):
    summary = _read_json(Path(args.probe_run_summary)) or {}
    probe_group_id = args.probe_group_id or summary.get("probe_group_id")
    if not probe_group_id:
        raise SystemExit("probe_group_id is required (or provide a valid probe_group_run_latest.json)")

    ctx = {
        "probe_group_id": str(probe_group_id),
        "airline": args.airline or summary.get("airline"),
        "origin": args.origin or (summary.get("route") or {}).get("origin"),
        "destination": args.destination or (summary.get("route") or {}).get("destination"),
        "cabin": args.cabin or summary.get("cabin"),
        "chd": args.chd if args.chd is not None else summary.get("chd"),
        "inf": args.inf if args.inf is not None else summary.get("inf"),
        "probe_adts": summary.get("probe_adts"),
    }
    return ctx, summary


def _auto_build_dataset(args, ctx: dict):
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "tools" / "build_inventory_state_dataset.py"),
        "--probe-group-id",
        str(ctx["probe_group_id"]),
        "--format",
        args.dataset_format,
        "--output-dir",
        args.dataset_output_dir,
        "--lookback-days",
        str(int(args.dataset_lookback_days)),
    ]
    for flag, val in [
        ("--airline", ctx.get("airline")),
        ("--origin", ctx.get("origin")),
        ("--destination", ctx.get("destination")),
        ("--cabin", ctx.get("cabin")),
        ("--chd", ctx.get("chd")),
        ("--inf", ctx.get("inf")),
    ]:
        if val is not None:
            cmd.extend([flag, str(val)])
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    if proc.returncode != 0:
        raise SystemExit(
            "Dataset auto-build failed\n"
            f"cmd={subprocess.list2cmdline(cmd)}\n"
            f"rc={proc.returncode}\n"
            f"stdout_tail={(proc.stdout or '')[-2000:]}\n"
            f"stderr_tail={(proc.stderr or '')[-2000:]}"
        )
    return subprocess.list2cmdline(cmd), (proc.stdout or "").strip()


def _load_dataset(dataset_csv: Path, probe_group_id: str) -> pd.DataFrame:
    if not dataset_csv.exists():
        raise SystemExit(f"Dataset CSV not found: {dataset_csv}")
    df = pd.read_csv(dataset_csv, low_memory=False)
    if "probe_group_id" not in df.columns:
        raise SystemExit("Dataset does not contain probe_group_id. Rebuild with updated tools/build_inventory_state_dataset.py")
    sub = df[df["probe_group_id"].astype(str) == str(probe_group_id)].copy()
    if sub.empty:
        raise SystemExit(f"No rows found for probe_group_id={probe_group_id} in {dataset_csv}")
    return sub


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["departure", "scraped_at", "observed_at_utc", "y_next_search_observed_at_utc"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in [
        "adt_count", "chd_count", "inf_count", "capacity_physical", "open_bucket_count",
        "open_seat_sum", "days_to_departure", "dep_time_min", "duration_min",
        "party_breakpoint_est"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in [
        "lowest_open_fare", "inv_press_pct", "fare_adt1", "fare_adt2", "fare_adt4",
        "fare_gap_1_to_2", "fare_gap_1_to_3", "fare_gap_2_to_3", "fare_gap_2_to_4", "fare_gap_3_to_4", "fare_gap_1_to_4"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _build_flight_sensitivity_table(df: pd.DataFrame) -> pd.DataFrame:
    # One row per flight/departure in the probe group; keep lowest ADT row as canonical.
    key_cols = [
        "probe_group_id", "probe_join_id", "airline", "origin", "destination", "route_key",
        "flight_number", "departure", "cabin", "chd_count", "inf_count"
    ]
    sort_cols = [c for c in ["adt_count", "scraped_at", "scrape_id"] if c in df.columns]
    work = df.sort_values(sort_cols, na_position="last").copy()
    table = work.drop_duplicates(subset=key_cols, keep="first").copy()

    # Derived probe sensitivity metrics
    if "fare_gap_1_to_2" not in table.columns and {"fare_adt1", "fare_adt2"}.issubset(table.columns):
        table["fare_gap_1_to_2"] = table["fare_adt2"] - table["fare_adt1"]
    if {"fare_adt1", "fare_adt2"}.issubset(table.columns):
        table["fare_gap_1_to_2_pct"] = (table["fare_adt2"] - table["fare_adt1"]) / table["fare_adt1"] * 100.0
        bad = table["fare_adt1"].isna() | (table["fare_adt1"] <= 0)
        table.loc[bad, "fare_gap_1_to_2_pct"] = pd.NA

    if {"availability_adt1", "availability_adt2"}.issubset(table.columns):
        a1 = table["availability_adt1"].map(lambda x: bool(x) if pd.notna(x) else pd.NA)
        a2 = table["availability_adt2"].map(lambda x: bool(x) if pd.notna(x) else pd.NA)
        table["availability_drop_1_to_2"] = (a1 == True) & (a2 == False)  # noqa: E712
    else:
        table["availability_drop_1_to_2"] = pd.NA

    def _sensitivity_class(row):
        gap = row.get("fare_gap_1_to_2")
        drop = row.get("availability_drop_1_to_2")
        if pd.notna(drop) and bool(drop):
            return "availability_drop"
        if pd.notna(gap):
            if float(gap) > 0:
                return "price_jump"
            if float(gap) == 0:
                return "stable"
            return "price_drop"
        return "insufficient_pair"

    table["sensitivity_class"] = table.apply(_sensitivity_class, axis=1)

    cols_order = [
        "probe_group_id", "probe_join_id", "airline", "route_key", "origin", "destination",
        "flight_number", "departure", "cabin", "aircraft_type", "capacity_physical",
        "fare_adt1", "fare_adt2", "fare_adt4",
        "fare_gap_1_to_2", "fare_gap_1_to_2_pct", "fare_gap_2_to_4", "fare_gap_1_to_4",
        "availability_adt1", "availability_adt2", "availability_adt4",
        "lowest_bucket_code_adt1", "lowest_bucket_code_adt2", "lowest_bucket_code_adt4",
        "party_breakpoint_est",
        "open_bucket_count", "open_seat_sum", "inv_press_pct",
        "days_to_departure", "dep_time_min",
        "sensitivity_class",
    ]
    cols_order = [c for c in cols_order if c in table.columns] + [c for c in table.columns if c not in cols_order]
    table = table[cols_order].copy()
    return table.sort_values(
        [c for c in ["route_key", "departure", "flight_number"] if c in table.columns],
        na_position="last"
    ).reset_index(drop=True)


def _safe_median(s: pd.Series) -> float | None:
    if s is None:
        return None
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.median())


def _safe_mean(s: pd.Series) -> float | None:
    if s is None:
        return None
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())


def _to_markdown_table(df: pd.DataFrame, max_rows: int = 10) -> str:
    if df is None or df.empty:
        return "_No rows_"
    head = df.head(max_rows).copy()
    try:
        return head.to_markdown(index=False)
    except Exception:
        # Fallback without optional `tabulate` dependency.
        tmp = head.copy()
        for c in tmp.columns:
            tmp[c] = tmp[c].map(lambda v: "" if pd.isna(v) else str(v))
        headers = [str(c) for c in tmp.columns]
        rows = [headers] + tmp.values.tolist()
        widths = [max(len(str(row[i])) for row in rows) for i in range(len(headers))]

        def fmt_row(row_vals):
            return "| " + " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row_vals)) + " |"

        sep = "| " + " | ".join("-" * widths[i] for i in range(len(widths))) + " |"
        lines = [fmt_row(headers), sep]
        for row in tmp.values.tolist():
            lines.append(fmt_row(row))
        return "\n".join(lines)


def _build_summary_payload(ctx: dict, flight_df: pd.DataFrame, raw_df: pd.DataFrame) -> dict:
    total_flights = int(len(flight_df))
    paired_1_2 = flight_df[flight_df.get("fare_adt1").notna() & flight_df.get("fare_adt2").notna()] if {"fare_adt1", "fare_adt2"}.issubset(flight_df.columns) else flight_df.iloc[0:0]
    paired_1_3 = flight_df[flight_df.get("fare_adt1").notna() & flight_df.get("fare_adt3").notna()] if {"fare_adt1", "fare_adt3"}.issubset(flight_df.columns) else flight_df.iloc[0:0]
    paired_2_3 = flight_df[flight_df.get("fare_adt2").notna() & flight_df.get("fare_adt3").notna()] if {"fare_adt2", "fare_adt3"}.issubset(flight_df.columns) else flight_df.iloc[0:0]
    availability_pairs = (
        flight_df[flight_df.get("availability_adt1").notna() & flight_df.get("availability_adt2").notna()]
        if {"availability_adt1", "availability_adt2"}.issubset(flight_df.columns)
        else flight_df.iloc[0:0]
    )
    price_jump_pairs = paired_1_2[pd.to_numeric(paired_1_2.get("fare_gap_1_to_2"), errors="coerce") > 0] if not paired_1_2.empty else paired_1_2
    stable_pairs = paired_1_2[pd.to_numeric(paired_1_2.get("fare_gap_1_to_2"), errors="coerce") == 0] if not paired_1_2.empty else paired_1_2
    availability_drops = (
        availability_pairs[availability_pairs["availability_drop_1_to_2"] == True]  # noqa: E712
        if "availability_drop_1_to_2" in availability_pairs.columns and not availability_pairs.empty
        else availability_pairs.iloc[0:0]
    )
    bp = pd.to_numeric(flight_df.get("party_breakpoint_est"), errors="coerce") if "party_breakpoint_est" in flight_df.columns else pd.Series(dtype=float)

    breakpoint_counts = {}
    if not bp.dropna().empty:
        vc = bp.dropna().astype(int).value_counts().sort_index()
        breakpoint_counts = {int(k): int(v) for k, v in vc.items()}

    return {
        "probe_group_id": ctx["probe_group_id"],
        "airline": ctx.get("airline"),
        "origin": ctx.get("origin"),
        "destination": ctx.get("destination"),
        "cabin": ctx.get("cabin"),
        "probe_adts": ctx.get("probe_adts"),
        "dataset_rows_raw": int(len(raw_df)),
        "flight_rows_unique": total_flights,
        "paired_adt1_adt2_flights": int(len(paired_1_2)),
        "paired_adt1_adt2_pct": (float(len(paired_1_2)) / float(total_flights) * 100.0) if total_flights else None,
        "paired_adt1_adt3_flights": int(len(paired_1_3)),
        "paired_adt1_adt3_pct": (float(len(paired_1_3)) / float(total_flights) * 100.0) if total_flights else None,
        "paired_adt2_adt3_flights": int(len(paired_2_3)),
        "paired_adt2_adt3_pct": (float(len(paired_2_3)) / float(total_flights) * 100.0) if total_flights else None,
        "price_jump_1_to_2_count": int(len(price_jump_pairs)),
        "price_jump_1_to_2_pct_of_pairs": (float(len(price_jump_pairs)) / float(len(paired_1_2)) * 100.0) if len(paired_1_2) else None,
        "stable_price_1_to_2_count": int(len(stable_pairs)),
        "availability_drop_1_to_2_count": int(len(availability_drops)),
        "availability_drop_1_to_2_pct_of_pairs": (float(len(availability_drops)) / float(len(availability_pairs)) * 100.0) if len(availability_pairs) else None,
        "median_fare_gap_1_to_2": _safe_median(flight_df.get("fare_gap_1_to_2")),
        "mean_fare_gap_1_to_2": _safe_mean(flight_df.get("fare_gap_1_to_2")),
        "median_fare_gap_1_to_2_pct": _safe_median(flight_df.get("fare_gap_1_to_2_pct")),
        "median_fare_gap_1_to_3": _safe_median(flight_df.get("fare_gap_1_to_3")),
        "mean_fare_gap_1_to_3": _safe_mean(flight_df.get("fare_gap_1_to_3")),
        "median_fare_gap_2_to_3": _safe_median(flight_df.get("fare_gap_2_to_3")),
        "mean_fare_gap_2_to_3": _safe_mean(flight_df.get("fare_gap_2_to_3")),
        "breakpoint_counts": breakpoint_counts,
        "breakpoint_2_count": int(breakpoint_counts.get(2, 0)),
        "breakpoint_3_count": int(breakpoint_counts.get(3, 0)),
        "breakpoint_4_count": int(breakpoint_counts.get(4, 0)),
    }


def _build_markdown(summary: dict, flight_df: pd.DataFrame, top_n: int, dataset_path: str):
    top_jumps = flight_df.copy()
    if "fare_gap_1_to_2" in top_jumps.columns:
        top_jumps = top_jumps[pd.to_numeric(top_jumps["fare_gap_1_to_2"], errors="coerce").notna()]
        top_jumps = top_jumps.sort_values("fare_gap_1_to_2", ascending=False)

    top_drops = flight_df.copy()
    if "availability_drop_1_to_2" in top_drops.columns:
        top_drops = top_drops[top_drops["availability_drop_1_to_2"] == True]  # noqa: E712
        if "fare_gap_1_to_2" in top_drops.columns:
            top_drops = top_drops.sort_values("fare_gap_1_to_2", ascending=False, na_position="last")

    keep_cols = [
        "route_key", "flight_number", "departure", "cabin",
        "fare_adt1", "fare_adt2", "fare_adt3", "fare_adt4",
        "fare_gap_1_to_2", "fare_gap_1_to_2_pct", "fare_gap_1_to_3", "fare_gap_2_to_3", "fare_gap_2_to_4",
        "availability_adt1", "availability_adt2", "availability_adt3", "availability_adt4", "party_breakpoint_est",
        "lowest_bucket_code_adt1", "lowest_bucket_code_adt2", "lowest_bucket_code_adt3", "lowest_bucket_code_adt4", "sensitivity_class",
    ]
    keep_cols = [c for c in keep_cols if c in flight_df.columns]

    lines = []
    lines.append(f"# Probe Group Analysis: `{summary.get('probe_group_id')}`")
    lines.append("")
    lines.append("## Scope")
    lines.append("")
    lines.append(f"- Airline: `{summary.get('airline')}`")
    if summary.get("origin") and summary.get("destination"):
        lines.append(f"- Route: `{summary.get('origin')}` -> `{summary.get('destination')}`")
    if summary.get("cabin"):
        lines.append(f"- Cabin: `{summary.get('cabin')}`")
    if summary.get("probe_adts"):
        lines.append(f"- Probe ADTs: `{','.join(str(x) for x in summary.get('probe_adts') or [])}`")
    lines.append(f"- Dataset source: `{dataset_path}`")
    lines.append("")
    lines.append("## Summary Metrics")
    lines.append("")
    lines.append(f"- Unique flight rows analyzed: `{summary.get('flight_rows_unique')}`")
    lines.append(f"- ADT1/ADT2 paired flights: `{summary.get('paired_adt1_adt2_flights')}` ({_fmt_pct(summary.get('paired_adt1_adt2_pct'))})")
    if summary.get("paired_adt1_adt3_flights"):
        lines.append(f"- ADT1/ADT3 paired flights: `{summary.get('paired_adt1_adt3_flights')}` ({_fmt_pct(summary.get('paired_adt1_adt3_pct'))})")
    if summary.get("paired_adt2_adt3_flights"):
        lines.append(f"- ADT2/ADT3 paired flights: `{summary.get('paired_adt2_adt3_flights')}` ({_fmt_pct(summary.get('paired_adt2_adt3_pct'))})")
    lines.append(f"- Price jumps (ADT1 -> ADT2): `{summary.get('price_jump_1_to_2_count')}` ({_fmt_pct(summary.get('price_jump_1_to_2_pct_of_pairs'))} of paired)")
    lines.append(f"- Availability drops (ADT1 -> ADT2): `{summary.get('availability_drop_1_to_2_count')}` ({_fmt_pct(summary.get('availability_drop_1_to_2_pct_of_pairs'))} of paired)")
    lines.append(f"- Median fare gap (ADT1 -> ADT2): `{_fmt_num(summary.get('median_fare_gap_1_to_2'))}` BDT")
    lines.append(f"- Median fare gap % (ADT1 -> ADT2): `{_fmt_pct(summary.get('median_fare_gap_1_to_2_pct'))}`")
    if summary.get("median_fare_gap_1_to_3") is not None:
        lines.append(f"- Median fare gap (ADT1 -> ADT3): `{_fmt_num(summary.get('median_fare_gap_1_to_3'))}` BDT")
    if summary.get("median_fare_gap_2_to_3") is not None:
        lines.append(f"- Median fare gap (ADT2 -> ADT3): `{_fmt_num(summary.get('median_fare_gap_2_to_3'))}` BDT")
    if summary.get("breakpoint_counts"):
        bp_text = ", ".join([f"`{k}`: {v}" for k, v in summary["breakpoint_counts"].items()])
        lines.append(f"- Party breakpoint distribution: {bp_text}")
        lines.append(
            f"- Breakpoint counts (explicit): `2={summary.get('breakpoint_2_count', 0)}`, "
            f"`3={summary.get('breakpoint_3_count', 0)}`, `4={summary.get('breakpoint_4_count', 0)}`"
        )
    lines.append("")
    lines.append("## Top Fare Gaps (ADT1 -> ADT2)")
    lines.append("")
    lines.append(_to_markdown_table(top_jumps[keep_cols], max_rows=top_n))
    lines.append("")
    lines.append("## Availability Drops (ADT1 -> ADT2)")
    lines.append("")
    lines.append(_to_markdown_table(top_drops[keep_cols], max_rows=top_n))
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `fare_gap_1_to_2` / `fare_gap_1_to_3` / `fare_gap_2_to_3` and `party_breakpoint_est` require linked probe rows (same `probe_group_id`).")
    lines.append("- These are observable commercial inventory signals, not exact remaining seat counts.")
    return "\n".join(lines) + "\n"


def _fmt_pct(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "NA"
    return f"{float(v):.2f}%"


def _fmt_num(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "NA"
    return f"{float(v):,.2f}"


def _write_outputs(args, probe_group_id: str, flight_df: pd.DataFrame, summary: dict, md_text: str):
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _, ts, tz_token = _build_run_stamp(args.timestamp_tz)
    slug = _slug(probe_group_id)

    csv_path = out_dir / f"probe_group_flight_sensitivity_{slug}_{ts}_{tz_token}.csv"
    md_path = out_dir / f"probe_group_analysis_{slug}_{ts}_{tz_token}.md"
    json_path = out_dir / f"probe_group_analysis_{slug}_{ts}_{tz_token}.json"

    flight_df.to_csv(csv_path, index=False)
    md_path.write_text(md_text, encoding="utf-8")
    json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    # Latest pointers (global and group-specific)
    (out_dir / "probe_group_flight_sensitivity_latest.csv").write_bytes(csv_path.read_bytes())
    (out_dir / "probe_group_analysis_latest.md").write_bytes(md_path.read_bytes())
    (out_dir / "probe_group_analysis_latest.json").write_bytes(json_path.read_bytes())
    (out_dir / f"probe_group_flight_sensitivity_{slug}_latest.csv").write_bytes(csv_path.read_bytes())
    (out_dir / f"probe_group_analysis_{slug}_latest.md").write_bytes(md_path.read_bytes())
    (out_dir / f"probe_group_analysis_{slug}_latest.json").write_bytes(json_path.read_bytes())

    return csv_path, md_path, json_path


def main():
    args = parse_args()
    ctx, summary_doc = _resolve_probe_context(args)

    auto_build_cmd = None
    auto_build_stdout = None
    if args.auto_build_dataset:
        auto_build_cmd, auto_build_stdout = _auto_build_dataset(args, ctx)

    dataset_csv = Path(args.dataset_csv)
    raw_df = _load_dataset(dataset_csv, ctx["probe_group_id"])
    raw_df = _coerce_types(raw_df)
    flight_df = _build_flight_sensitivity_table(raw_df)
    summary = _build_summary_payload(ctx, flight_df, raw_df)
    if auto_build_cmd:
        summary["auto_build_dataset_cmd"] = auto_build_cmd
        summary["auto_build_dataset_stdout"] = auto_build_stdout
    if summary_doc:
        summary["probe_run_summary_source"] = str(Path(args.probe_run_summary))

    md_text = _build_markdown(summary, flight_df, top_n=max(1, int(args.top_n)), dataset_path=str(dataset_csv))
    csv_path, md_path, json_path = _write_outputs(args, ctx["probe_group_id"], flight_df, summary, md_text)

    print(
        f"probe_group_analysis probe_group_id={ctx['probe_group_id']} "
        f"raw_rows={len(raw_df)} flight_rows={len(flight_df)} -> {csv_path}, {md_path}, {json_path}"
    )


if __name__ == "__main__":
    main()
