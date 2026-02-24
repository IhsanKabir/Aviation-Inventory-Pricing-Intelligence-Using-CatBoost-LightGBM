"""
Run a linked multi-passenger probe group (e.g., ADT=1,2,4) and export a probe-scoped
inventory-state dataset using a shared `probe_group_id`.

Default behavior is conservative:
- runs accumulation only (`run_pipeline.py --skip-reports`) for each ADT in sequence
- reuses one `probe_group_id`
- exports a compact dataset filtered to that probe group
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    p = argparse.ArgumentParser(description="Run linked passenger-size probe accumulation runs + dataset export")
    p.add_argument("--python-exe", default=sys.executable, help="Python executable to run child scripts")
    p.add_argument("--airline", required=True, help="Airline code, e.g. VQ or BG")
    p.add_argument("--origin", help="Origin airport")
    p.add_argument("--destination", help="Destination airport")
    p.add_argument("--cabin", help="Cabin filter (e.g. Economy)")

    # Date selectors (same as pipeline/common subset)
    p.add_argument("--date", help="Single departure date YYYY-MM-DD")
    p.add_argument("--date-start", help="Inclusive start departure date YYYY-MM-DD")
    p.add_argument("--date-end", help="Inclusive end departure date YYYY-MM-DD")
    p.add_argument("--dates", help="Comma-separated departure dates YYYY-MM-DD")
    p.add_argument("--date-offsets", help="Comma-separated day offsets from today")
    p.add_argument("--dates-file")
    p.add_argument("--schedule-file")

    p.add_argument("--route-scope", choices=["all", "domestic", "international"], default="all")
    p.add_argument("--market-country", default="BD")
    p.add_argument("--limit-routes", type=int)
    p.add_argument("--limit-dates", type=int)
    p.add_argument("--strict-route-audit", action="store_true")

    # Probe dimensions
    p.add_argument("--probe-adts", default="1,2", help="Comma-separated ADT probe set (default: 1,2)")
    p.add_argument("--chd", type=int, default=0)
    p.add_argument("--inf", type=int, default=0)
    p.add_argument("--probe-group-id", help="Optional explicit probe group id; auto-generated if omitted")
    p.add_argument("--sleep-between-probes-seconds", type=float, default=0.0)

    # Pipeline/report behavior
    p.add_argument(
        "--with-reports",
        action="store_true",
        help="Run report generation for each probe accumulation run (default: skip reports for speed)",
    )
    p.add_argument("--route-monitor", action="store_true", help="Pass route-monitor to run_pipeline when --with-reports")
    p.add_argument("--report-output-dir", default="output/reports")
    p.add_argument("--report-timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--report-format", choices=["csv", "xlsx", "both"], default="both")

    # Dataset export
    p.add_argument("--no-dataset-export", action="store_true", help="Skip dataset export step")
    p.add_argument("--dataset-format", choices=["csv", "parquet", "both"], default="csv")
    p.add_argument("--dataset-output-dir", default="output/reports")
    p.add_argument("--dataset-lookback-days", type=int, default=7, help="Fallback lookback if probe_group_id filter is not used")
    p.add_argument("--dataset-no-probe-features", action="store_true")

    # Probe analysis (uses probe_group_id-linked dataset rows)
    p.add_argument(
        "--no-analysis",
        action="store_true",
        help="Skip analyze_probe_group step (default: run analysis after dataset export)",
    )
    p.add_argument("--analysis-output-dir", default="output/reports")
    p.add_argument("--analysis-top-n", type=int, default=15)
    p.add_argument("--analysis-timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument(
        "--analysis-auto-build-dataset",
        action="store_true",
        help="Pass --auto-build-dataset to analyze_probe_group.py (usually not needed because this runner already exports a dataset)",
    )

    # Ops behavior
    p.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    p.add_argument("--fail-fast", action="store_true", help="Stop immediately on first probe failure")
    return p.parse_args()


def _parse_adts(raw: str) -> list[int]:
    vals: list[int] = []
    seen = set()
    for part in str(raw or "").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            v = int(s)
        except Exception:
            continue
        if v < 1:
            continue
        if v not in seen:
            seen.add(v)
            vals.append(v)
    return vals or [1, 2]


def _default_probe_group_id(args, adts: list[int]) -> str:
    now = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    route = f"{(args.origin or 'ALL').upper()}-{(args.destination or 'ALL').upper()}"
    adt_tag = "a" + "-".join(str(x) for x in adts)
    airline = str(args.airline or "UNK").upper()
    return f"probe_{airline}_{route}_{adt_tag}_{now}"


def _add_arg(cmd: list[str], flag: str, value):
    if value is None:
        return
    cmd.extend([flag, str(value)])


def _build_probe_scrape_cmd(args, adt: int, probe_group_id: str) -> list[str]:
    cmd = [args.python_exe, str(REPO_ROOT / "run_pipeline.py")]
    cmd.append("--airline")
    cmd.append(str(args.airline).upper())

    # Accumulation-only by default (faster/cleaner for probe collections)
    if not args.with_reports:
        cmd.append("--skip-reports")
    else:
        cmd.extend(["--report-output-dir", args.report_output_dir])
        cmd.extend(["--report-timestamp-tz", args.report_timestamp_tz])
        cmd.extend(["--report-format", args.report_format])
        if args.route_monitor:
            cmd.append("--route-monitor")

    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--date", args.date)
    _add_arg(cmd, "--date-start", args.date_start)
    _add_arg(cmd, "--date-end", args.date_end)
    _add_arg(cmd, "--dates", args.dates)
    _add_arg(cmd, "--date-offsets", args.date_offsets)
    _add_arg(cmd, "--dates-file", args.dates_file)
    _add_arg(cmd, "--schedule-file", args.schedule_file)
    _add_arg(cmd, "--route-scope", args.route_scope)
    _add_arg(cmd, "--market-country", args.market_country)
    _add_arg(cmd, "--limit-routes", args.limit_routes)
    _add_arg(cmd, "--limit-dates", args.limit_dates)
    if args.strict_route_audit:
        cmd.append("--strict-route-audit")

    cmd.extend(["--adt", str(int(adt))])
    cmd.extend(["--chd", str(max(0, int(args.chd or 0)))])
    cmd.extend(["--inf", str(max(0, int(args.inf or 0)))])
    cmd.extend(["--probe-group-id", probe_group_id])
    return cmd


def _build_dataset_cmd(args, probe_group_id: str) -> list[str]:
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "tools" / "build_inventory_state_dataset.py"),
        "--airline",
        str(args.airline).upper(),
        "--probe-group-id",
        probe_group_id,
        "--format",
        args.dataset_format,
        "--output-dir",
        args.dataset_output_dir,
        "--lookback-days",
        str(int(args.dataset_lookback_days)),
    ]
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--chd", max(0, int(args.chd or 0)))
    _add_arg(cmd, "--inf", max(0, int(args.inf or 0)))
    if args.dataset_no_probe_features:
        cmd.append("--no-probe-features")
    return cmd


def _build_analyze_cmd(args, probe_group_id: str) -> list[str]:
    dataset_csv = Path(args.dataset_output_dir) / "inventory_state_v1_latest.csv"
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "tools" / "analyze_probe_group.py"),
        "--probe-group-id",
        probe_group_id,
        "--dataset-csv",
        str(dataset_csv),
        "--output-dir",
        args.analysis_output_dir,
        "--timestamp-tz",
        args.analysis_timestamp_tz,
        "--top-n",
        str(int(args.analysis_top_n)),
    ]
    # Keep analyzer context explicit for better metadata/debugging.
    _add_arg(cmd, "--airline", str(args.airline).upper() if args.airline else None)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--chd", max(0, int(args.chd or 0)))
    _add_arg(cmd, "--inf", max(0, int(args.inf or 0)))
    if args.analysis_auto_build_dataset:
        cmd.append("--auto-build-dataset")
        cmd.extend(["--dataset-output-dir", args.dataset_output_dir])
        cmd.extend(["--dataset-format", args.dataset_format])
        cmd.extend(["--dataset-lookback-days", str(int(args.dataset_lookback_days))])
    return cmd


def _run_cmd(cmd: list[str], dry_run: bool) -> dict:
    started = datetime.now(timezone.utc)
    cmdline = subprocess.list2cmdline(cmd)
    if dry_run:
        return {
            "cmd": cmdline,
            "rc": None,
            "duration_sec": 0.0,
            "started_at_utc": started.isoformat(),
            "ended_at_utc": started.isoformat(),
            "stdout_tail": "",
            "stderr_tail": "",
            "dry_run": True,
        }
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    ended = datetime.now(timezone.utc)
    return {
        "cmd": cmdline,
        "rc": int(proc.returncode),
        "duration_sec": (ended - started).total_seconds(),
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "stdout_tail": (proc.stdout or "")[-4000:],
        "stderr_tail": (proc.stderr or "")[-4000:],
        "dry_run": False,
    }


def _write_summary(output_dir: Path, payload: dict):
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")
    latest = output_dir / "probe_group_run_latest.json"
    run = output_dir / f"probe_group_run_{ts}.json"
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    latest.write_text(text, encoding="utf-8")
    run.write_text(text, encoding="utf-8")
    return latest, run


def main():
    args = parse_args()
    adts = _parse_adts(args.probe_adts)
    probe_group_id = str(args.probe_group_id).strip() if args.probe_group_id else _default_probe_group_id(args, adts)

    steps: list[dict] = []
    failed = False

    # Probe scrapes
    for idx, adt in enumerate(adts, start=1):
        cmd = _build_probe_scrape_cmd(args, adt=adt, probe_group_id=probe_group_id)
        print(f"[probe {idx}/{len(adts)}] ADT={adt} cmd={subprocess.list2cmdline(cmd)}", flush=True)
        result = _run_cmd(cmd, args.dry_run)
        result["step"] = f"probe_adt_{adt}"
        result["adt"] = adt
        steps.append(result)
        if (result["rc"] or 0) != 0 and not args.dry_run:
            failed = True
            print(f"Probe ADT={adt} failed rc={result['rc']}", flush=True)
            if args.fail_fast:
                break
        if idx < len(adts) and (args.sleep_between_probes_seconds or 0) > 0 and not args.dry_run:
            time.sleep(max(0.0, float(args.sleep_between_probes_seconds)))

    dataset_attempted = False
    dataset_ok = False
    analysis_attempted = False

    # Dataset export
    if not args.no_dataset_export and not (failed and args.fail_fast):
        dataset_attempted = True
        dcmd = _build_dataset_cmd(args, probe_group_id=probe_group_id)
        print(f"[dataset] cmd={subprocess.list2cmdline(dcmd)}", flush=True)
        dres = _run_cmd(dcmd, args.dry_run)
        dres["step"] = "dataset_export"
        steps.append(dres)
        if (dres["rc"] or 0) != 0 and not args.dry_run:
            failed = True
        else:
            dataset_ok = True

    # Probe analysis (default-on, opt-out)
    should_run_analysis = not bool(args.no_analysis)
    if should_run_analysis and not (failed and args.fail_fast):
        # By default, analysis expects a dataset CSV. If dataset export was skipped, allow the
        # caller to rely on an existing latest dataset (or analyzer auto-build).
        if (not args.no_dataset_export and not dataset_ok and not args.dry_run) and not args.analysis_auto_build_dataset:
            print("[analysis] skipped because dataset export failed", flush=True)
        else:
            analysis_attempted = True
            acmd = _build_analyze_cmd(args, probe_group_id=probe_group_id)
            print(f"[analysis] cmd={subprocess.list2cmdline(acmd)}", flush=True)
            ares = _run_cmd(acmd, args.dry_run)
            ares["step"] = "analyze_probe_group"
            steps.append(ares)
            if (ares["rc"] or 0) != 0 and not args.dry_run:
                failed = True

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "probe_group_id": probe_group_id,
        "airline": str(args.airline).upper(),
        "route": {"origin": args.origin, "destination": args.destination},
        "cabin": args.cabin,
        "probe_adts": adts,
        "chd": int(max(0, int(args.chd or 0))),
        "inf": int(max(0, int(args.inf or 0))),
        "dry_run": bool(args.dry_run),
        "with_reports": bool(args.with_reports),
        "dataset_export": not bool(args.no_dataset_export),
        "analysis": not bool(args.no_analysis),
        "dataset_attempted": bool(dataset_attempted),
        "dataset_ok": bool(dataset_ok),
        "analysis_attempted": bool(analysis_attempted),
        "failed": bool(failed),
        "steps": steps,
    }
    latest, run = _write_summary(Path(args.dataset_output_dir), payload)
    print(f"probe_group_id={probe_group_id}", flush=True)
    print(f"failed={failed}", flush=True)
    print(f"summary_latest={latest}", flush=True)
    print(f"summary_run={run}", flush=True)

    if failed and not args.dry_run:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
