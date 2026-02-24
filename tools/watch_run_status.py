import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Watch run_all accumulation heartbeat status JSON")
    p.add_argument(
        "--status-file",
        default="output/reports/run_all_status_latest.json",
        help="Path to run_all accumulation heartbeat status JSON",
    )
    p.add_argument("--interval-seconds", type=float, default=2.0, help="Poll interval")
    p.add_argument("--once", action="store_true", help="Print one snapshot and exit")
    p.add_argument("--show-json", action="store_true", help="Also print raw JSON payload")
    p.add_argument(
        "--stale-threshold-seconds",
        type=float,
        default=180.0,
        help="Mark heartbeat as STALE when age exceeds this threshold",
    )
    return p.parse_args()


def _parse_iso(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _heartbeat_age_seconds(payload: dict) -> float | None:
    ts = (
        _parse_iso(payload.get("accumulation_written_at_utc"))
        or _parse_iso(payload.get("written_at_utc"))
        or _parse_iso(payload.get("accumulation_last_query_at_utc"))
        or _parse_iso(payload.get("last_query_at_utc"))
    )
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds())


def _fmt_route(payload: dict) -> str:
    a = payload.get("current_airline") or "?"
    o = payload.get("current_origin") or "?"
    d = payload.get("current_destination") or "?"
    date = payload.get("current_date") or "?"
    cabin = payload.get("current_cabin") or "?"
    return f"{a} {o}->{d} {date} {cabin}"


def _fmt_pax(payload: dict) -> str:
    pax = payload.get("search_passengers")
    if not isinstance(pax, dict):
        return "pax=?/?/?"
    try:
        adt = int(pax.get("adt") or 0)
        chd = int(pax.get("chd") or 0)
        inf = int(pax.get("inf") or 0)
        return f"pax={adt}/{chd}/{inf}"
    except Exception:
        return "pax=?/?/?"


def _fmt_probe_group(payload: dict) -> str:
    pg = payload.get("probe_group_id")
    if pg is None or str(pg).strip() == "":
        return "probe=-"
    return f"probe={str(pg)}"


def _fmt_progress(payload: dict) -> str:
    done = payload.get("overall_query_completed")
    total = payload.get("overall_query_total")
    if done is None and total is None:
        return "queries=?/?"
    try:
        done_i = int(done or 0)
    except Exception:
        done_i = 0
    try:
        total_i = int(total or 0)
    except Exception:
        total_i = 0
    pct = (100.0 * done_i / total_i) if total_i > 0 else 0.0
    return f"queries={done_i}/{total_i} ({pct:.1f}%)"


def _fmt_elapsed(payload: dict) -> str:
    v = payload.get("last_query_elapsed_sec")
    if v is None:
        return "last=?s"
    try:
        return f"last={float(v):.2f}s"
    except Exception:
        return f"last={v}s"


def _compact_line(payload: dict, age_sec: float | None, stale_threshold_seconds: float) -> str:
    state = str(payload.get("state") or "?")
    phase = str(payload.get("phase") or "?")
    accumulation_run_id = str(payload.get("accumulation_run_id") or payload.get("scrape_id") or "?")
    rows = payload.get("last_query_rows")
    rows_txt = "rows=?"
    if rows is not None:
        try:
            rows_txt = f"rows={int(rows)}"
        except Exception:
            rows_txt = f"rows={rows}"
    age_txt = "age=?s" if age_sec is None else f"age={age_sec:.1f}s"
    stale_tag = ""
    if age_sec is not None and float(age_sec) > float(stale_threshold_seconds):
        stale_tag = " STALE"
    return (
        f"state={state} phase={phase}{stale_tag} { _fmt_progress(payload) } "
        f"{_fmt_route(payload)} {_fmt_pax(payload)} {_fmt_probe_group(payload)} {_fmt_elapsed(payload)} {rows_txt} {age_txt} "
        f"accumulation={accumulation_run_id}"
    )


def _read_json(path: Path):
    try:
        if not path.exists():
            return None, f"missing: {path}"
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None, f"invalid json object: {path}"
        return data, None
    except Exception as exc:
        return None, f"read error: {exc}"


def main():
    args = parse_args()
    path = Path(args.status_file)
    interval = max(0.2, float(args.interval_seconds or 2.0))
    last_signature = None

    while True:
        payload, err = _read_json(path)
        now_local = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        if err:
            line = f"[{now_local}] status={err}"
        else:
            age_sec = _heartbeat_age_seconds(payload)
            line = f"[{now_local}] {_compact_line(payload, age_sec, args.stale_threshold_seconds)}"
            if args.show_json:
                line += "\n" + json.dumps(payload, indent=2, ensure_ascii=False, default=str)

        signature = line
        if signature != last_signature or args.once:
            print(line, flush=True)
            last_signature = signature

        if args.once:
            break
        time.sleep(interval)


if __name__ == "__main__":
    main()
