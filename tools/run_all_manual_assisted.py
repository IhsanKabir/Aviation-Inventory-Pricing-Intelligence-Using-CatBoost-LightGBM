from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = REPO_ROOT / "tools"
BUILDER_TOOL = TOOLS_DIR / "build_manual_capture_queues.py"
BS2A_BATCH_TOOL = TOOLS_DIR / "bs_2a_manual_capture_batch_runner.py"
Q2_BATCH_TOOL = TOOLS_DIR / "maldivian_plnext_capture_batch_runner.py"
DEFAULT_QUEUE_OUTPUT_DIR = REPO_ROOT / "output" / "manual_sessions" / "queues"
DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"
DEFAULT_CDP_URL = "http://127.0.0.1:9222"


@dataclass
class RunnerSpec:
    name: str
    manifest_key: str
    tool_path: Path


# Extension point for future manual-assisted airline families.
# Add a new RunnerSpec and a command builder function in _build_family_cmd().
RUNNER_ORDER: tuple[str, ...] = ("bs2a", "q2")
RUNNER_SPECS: dict[str, RunnerSpec] = {
    "bs2a": RunnerSpec(name="bs2a", manifest_key="bs2a_queue_file", tool_path=BS2A_BATCH_TOOL),
    "q2": RunnerSpec(name="q2", manifest_key="q2_queue_file", tool_path=Q2_BATCH_TOOL),
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _now_tag() -> str:
    return _now_utc().strftime("%Y%m%d_%H%M%S_UTC")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _detect_python(preferred: str | None) -> str:
    if preferred:
        return preferred
    venv_python = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable or "python"


def _append_opt(cmd: list[str], flag: str, value: Any | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def _append_bool_opt(cmd: list[str], flag_on: str, enabled: bool, flag_off: str | None = None) -> None:
    if enabled:
        cmd.append(flag_on)
    elif flag_off:
        cmd.append(flag_off)


def _count_queue_jobs(queue_file: Path) -> int:
    if not queue_file.exists():
        return 0
    if queue_file.suffix.lower() == ".json":
        raw = _json_load(queue_file)
        if isinstance(raw, list):
            return len([r for r in raw if isinstance(r, dict)])
        if isinstance(raw, dict) and isinstance(raw.get("jobs"), list):
            return len([r for r in raw["jobs"] if isinstance(r, dict)])
        return 0

    with queue_file.open("r", encoding="utf-8-sig", newline="") as f:
        filtered = [line for line in f if line.strip() and not line.lstrip().startswith("#")]
    if not filtered:
        return 0
    return sum(1 for _ in csv.DictReader(filtered))


def _build_queues(args: argparse.Namespace, pyexe: str, run_tag: str) -> tuple[int, Path]:
    queue_output_dir = Path(args.queue_output_dir)
    queue_output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = queue_output_dir / f"manual_capture_queue_manifest_{run_tag}.json"

    cmd = [
        pyexe,
        str(BUILDER_TOOL),
        "--result-out",
        str(manifest_path),
        "--output-dir",
        str(queue_output_dir),
    ]
    _append_opt(cmd, "--schedule-file", args.schedule_file)
    _append_opt(cmd, "--routes-file", args.routes_file)
    _append_opt(cmd, "--date", args.date)
    _append_opt(cmd, "--dates", args.dates)
    _append_opt(cmd, "--date-start", args.date_start)
    _append_opt(cmd, "--date-end", args.date_end)
    _append_opt(cmd, "--date-offsets", args.date_offsets)
    _append_opt(cmd, "--limit-dates", args.limit_dates)
    _append_opt(cmd, "--cabin", args.cabin)
    if args.quick:
        cmd.append("--quick")
    if args.no_bs2a:
        cmd.append("--no-bs2a")
    if args.no_q2:
        cmd.append("--no-q2")
    if args.build_dry_run:
        cmd.append("--dry-run")

    print("[run-all] Building queue files...")
    print("[run-all] " + subprocess.list2cmdline(cmd))
    rc = subprocess.run(cmd, cwd=str(REPO_ROOT)).returncode
    return int(rc or 0), manifest_path


def _build_family_cmd(
    family: str,
    *,
    args: argparse.Namespace,
    pyexe: str,
    queue_file: Path,
    result_out: Path,
    retry_out: Path,
) -> list[str]:
    if family == "bs2a":
        cmd: list[str] = [pyexe, str(BS2A_BATCH_TOOL)]
        _append_opt(cmd, "--queue-file", queue_file)
        _append_opt(cmd, "--cdp-url", args.cdp_url)
        _append_opt(cmd, "--proxy-server", args.proxy_server)
        _append_opt(cmd, "--user-data-dir", args.user_data_dir)
        _append_opt(cmd, "--session-root", args.session_root)
        _append_opt(cmd, "--cabin", args.cabin)
        _append_opt(cmd, "--adt", args.adt)
        _append_opt(cmd, "--chd", args.chd)
        _append_opt(cmd, "--inf", args.inf)
        _append_opt(cmd, "--max-search-attempts", args.max_search_attempts)
        _append_opt(cmd, "--timeout-ms", args.timeout_ms)
        _append_opt(cmd, "--settle-ms", args.settle_ms)
        _append_opt(cmd, "--session-bundle-in", args.session_bundle_in)
        _append_opt(cmd, "--session-bundle-out", args.session_bundle_out)
        _append_opt(cmd, "--sleep-sec", args.sleep_sec)
        _append_opt(cmd, "--result-out", result_out)
        _append_opt(cmd, "--retry-queue-out", retry_out)
        _append_bool_opt(cmd, "--ingest", bool(args.ingest), "--no-ingest")
        if args.ingest_dry_run:
            cmd.append("--ingest-dry-run")
        if args.ingest_allow_mismatch:
            cmd.append("--ingest-allow-mismatch")
        if args.launch_cdp_browser:
            cmd.append("--launch-cdp-browser")
        if args.chrome_path:
            _append_opt(cmd, "--chrome-path", args.chrome_path)
        if args.non_interactive:
            cmd.append("--non-interactive")
        if args.stop_on_error:
            cmd.append("--stop-on-error")
        if args.dry_run_queue:
            cmd.append("--dry-run-queue")
        if args.keep_browser_open:
            cmd.append("--keep-browser-open")
        if args.print_command:
            cmd.append("--print-command")
        return cmd

    if family == "q2":
        cmd = [pyexe, str(Q2_BATCH_TOOL)]
        _append_opt(cmd, "--queue-file", queue_file)
        _append_opt(cmd, "--cdp-url", args.cdp_url)
        _append_opt(cmd, "--proxy-server", args.proxy_server)
        _append_opt(cmd, "--user-data-dir", args.user_data_dir)
        _append_opt(cmd, "--session-root", args.session_root)
        _append_opt(cmd, "--cabin", args.cabin)
        _append_opt(cmd, "--adt", args.adt)
        _append_opt(cmd, "--chd", args.chd)
        _append_opt(cmd, "--inf", args.inf)
        _append_opt(cmd, "--timeout-s", args.timeout_s)
        _append_opt(cmd, "--poll-ms", args.poll_ms)
        _append_opt(cmd, "--sleep-sec", args.sleep_sec)
        _append_opt(cmd, "--result-out", result_out)
        _append_opt(cmd, "--retry-queue-out", retry_out)
        _append_bool_opt(cmd, "--ingest", bool(args.ingest), "--no-ingest")
        if args.ingest_dry_run:
            cmd.append("--ingest-dry-run")
        if args.launch_cdp_browser:
            cmd.append("--launch-cdp-browser")
        if args.chrome_path:
            _append_opt(cmd, "--chrome-path", args.chrome_path)
        _append_bool_opt(cmd, "--open-home", bool(args.open_home), "--no-open-home")
        if args.open_index:
            cmd.append("--open-index")
        if args.stop_on_error:
            cmd.append("--stop-on-error")
        if args.dry_run_queue:
            cmd.append("--dry-run-queue")
        if args.keep_browser_open:
            cmd.append("--keep-browser-open")
        if args.print_command:
            cmd.append("--print-command")
        return cmd

    raise SystemExit(f"No command builder implemented for runner family: {family}")


def _parse_families(raw: str | None) -> list[str]:
    if not raw:
        return list(RUNNER_ORDER)
    out: list[str] = []
    for part in str(raw).split(","):
        p = part.strip().lower()
        if not p:
            continue
        if p not in RUNNER_SPECS:
            raise SystemExit(f"Unknown family '{p}'. Known: {', '.join(sorted(RUNNER_SPECS.keys()))}")
        out.append(p)
    deduped: list[str] = []
    seen = set()
    for f in out:
        if f in seen:
            continue
        seen.add(f)
        deduped.append(f)
    return deduped


def main() -> int:
    p = argparse.ArgumentParser(
        description=(
            "One-command orchestrator for manual-assisted captures. "
            "Builds queue files from routes/schedule, then runs BS/2A and Q2 batch runners sequentially."
        )
    )

    p.add_argument("--python", help="Python executable (default: .venv\\Scripts\\python.exe or current interpreter)")
    p.add_argument("--run-tag", help="Optional fixed run tag (default: UTC timestamp)")
    p.add_argument("--result-out", help="Aggregate run summary JSON path")

    p.add_argument("--manifest-in", help="Use existing queue manifest JSON and skip queue build")
    p.add_argument("--build-only", action="store_true", help="Only build queues and exit")
    p.add_argument("--build-dry-run", action="store_true", help="Pass --dry-run to queue builder")

    # Queue builder inputs
    p.add_argument("--schedule-file", default=str(REPO_ROOT / "config" / "schedule.json"))
    p.add_argument("--routes-file", default=str(REPO_ROOT / "config" / "routes.json"))
    p.add_argument("--queue-output-dir", default=str(DEFAULT_QUEUE_OUTPUT_DIR))
    p.add_argument("--date")
    p.add_argument("--dates")
    p.add_argument("--date-start")
    p.add_argument("--date-end")
    p.add_argument("--date-offsets")
    p.add_argument("--quick", action="store_true")
    p.add_argument("--limit-dates", type=int)
    p.add_argument("--cabin", default="Economy")
    p.add_argument("--no-bs2a", action="store_true", help="Do not generate/run BS/2A queue")
    p.add_argument("--no-q2", action="store_true", help="Do not generate/run Q2 queue")

    # Runner controls
    p.add_argument("--families", help="Comma-separated families to run (default: bs2a,q2)")
    p.add_argument("--session-root", default=str(DEFAULT_SESSION_ROOT))
    p.add_argument("--cdp-url", default=DEFAULT_CDP_URL)
    p.add_argument("--launch-cdp-browser", action="store_true")
    p.add_argument("--chrome-path")
    p.add_argument("--proxy-server")
    p.add_argument("--user-data-dir")

    p.add_argument("--adt", type=int, default=1)
    p.add_argument("--chd", type=int, default=0)
    p.add_argument("--inf", type=int, default=0)

    p.add_argument("--ingest", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--ingest-dry-run", action="store_true")
    p.add_argument("--ingest-allow-mismatch", action="store_true", help="BS/2A only")

    p.add_argument("--non-interactive", action="store_true", help="BS/2A only")
    p.add_argument("--max-search-attempts", type=int, default=1, help="BS/2A only")
    p.add_argument("--timeout-ms", type=int, default=120000, help="BS/2A only")
    p.add_argument("--settle-ms", type=int, default=3000, help="BS/2A only")
    p.add_argument("--session-bundle-in", help="BS/2A only")
    p.add_argument("--session-bundle-out", help="BS/2A only")

    p.add_argument("--open-home", action=argparse.BooleanOptionalAction, default=True, help="Q2 only")
    p.add_argument("--open-index", action="store_true", help="Q2 only")
    p.add_argument("--timeout-s", type=int, default=300, help="Q2 only")
    p.add_argument("--poll-ms", type=int, default=500, help="Q2 only")

    p.add_argument("--stop-on-error", action="store_true")
    p.add_argument("--sleep-sec", type=float, default=0.0)
    p.add_argument("--dry-run-queue", action="store_true")
    p.add_argument("--keep-browser-open", action="store_true")
    p.add_argument("--print-command", action="store_true")

    args = p.parse_args()

    if args.ingest_dry_run and not args.ingest:
        p.error("--ingest-dry-run requires --ingest")
    if args.build_dry_run and not args.build_only and not args.manifest_in:
        p.error("--build-dry-run requires --build-only (or use --manifest-in to run an existing manifest)")

    pyexe = _detect_python(args.python)
    run_tag = args.run_tag or _now_tag()
    session_root = Path(args.session_root)
    queue_runs_dir = session_root / "queue_runs"
    queue_runs_dir.mkdir(parents=True, exist_ok=True)

    aggregate_out = Path(args.result_out) if args.result_out else (queue_runs_dir / f"run_all_manual_assisted_{run_tag}.json")

    if args.manifest_in:
        manifest_path = Path(args.manifest_in)
        if not manifest_path.exists():
            raise SystemExit(f"--manifest-in not found: {manifest_path}")
        build_rc = 0
    else:
        build_rc, manifest_path = _build_queues(args, pyexe, run_tag)
        if build_rc != 0:
            print(f"[run-all] Queue build failed (rc={build_rc}).")
            summary = {
                "ok": False,
                "started_at_utc": _now_iso(),
                "ended_at_utc": _now_iso(),
                "run_tag": run_tag,
                "python": pyexe,
                "build_rc": build_rc,
                "manifest_path": str(manifest_path),
                "results": [],
            }
            aggregate_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"[run-all] Wrote aggregate summary: {aggregate_out}")
            return 3

    if not manifest_path.exists():
        raise SystemExit(f"Queue manifest not found: {manifest_path}")

    manifest = _json_load(manifest_path)
    if not isinstance(manifest, dict):
        raise SystemExit(f"Queue manifest is not a JSON object: {manifest_path}")

    selected_families = _parse_families(args.families)
    if args.no_bs2a and "bs2a" in selected_families:
        selected_families.remove("bs2a")
    if args.no_q2 and "q2" in selected_families:
        selected_families.remove("q2")

    summary: dict[str, Any] = {
        "ok": True,
        "started_at_utc": _now_iso(),
        "run_tag": run_tag,
        "repo_root": str(REPO_ROOT),
        "python": pyexe,
        "manifest_path": str(manifest_path),
        "selected_families": selected_families,
        "results": [],
    }

    if args.build_only:
        summary["ended_at_utc"] = _now_iso()
        summary["build_only"] = True
        aggregate_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print("[run-all] build-only complete.")
        print(f"[run-all] Manifest: {manifest_path}")
        print(f"[run-all] Aggregate summary: {aggregate_out}")
        return 0

    overall_failed = False

    for family in selected_families:
        spec = RUNNER_SPECS[family]
        queue_file_raw = manifest.get(spec.manifest_key)
        queue_file = Path(str(queue_file_raw)) if queue_file_raw else None

        item: dict[str, Any] = {
            "family": family,
            "manifest_key": spec.manifest_key,
            "queue_file": str(queue_file) if queue_file else None,
        }

        if not queue_file or not queue_file.exists():
            item.update({"status": "skipped", "reason": "queue_file_missing"})
            summary["results"].append(item)
            print(f"[run-all] SKIP {family}: queue file missing ({queue_file})")
            continue

        queue_jobs = _count_queue_jobs(queue_file)
        item["queue_jobs"] = queue_jobs
        if queue_jobs <= 0:
            item.update({"status": "skipped", "reason": "queue_empty"})
            summary["results"].append(item)
            print(f"[run-all] SKIP {family}: queue is empty ({queue_file})")
            continue

        result_out = queue_runs_dir / f"{family}_queue_run_{run_tag}.json"
        retry_out = queue_runs_dir / f"{family}_queue_retry_{run_tag}.json"
        cmd = _build_family_cmd(
            family,
            args=args,
            pyexe=pyexe,
            queue_file=queue_file,
            result_out=result_out,
            retry_out=retry_out,
        )

        item["started_at_utc"] = _now_iso()
        item["command"] = subprocess.list2cmdline(cmd)
        print("[run-all] Running:")
        print("[run-all] " + item["command"])
        rc = int(subprocess.run(cmd, cwd=str(REPO_ROOT)).returncode or 0)
        item["ended_at_utc"] = _now_iso()
        item["return_code"] = rc
        item["result_out"] = str(result_out)
        item["retry_queue_out"] = str(retry_out)
        item["status"] = "ok" if rc == 0 else "failed"
        summary["results"].append(item)

        if rc != 0:
            overall_failed = True
            print(f"[run-all] FAILED {family} rc={rc}")
            if args.stop_on_error:
                print("[run-all] stop-on-error set; aborting remaining families.")
                break
        else:
            print(f"[run-all] OK {family}")

    summary["ended_at_utc"] = _now_iso()
    summary["ok"] = not overall_failed
    aggregate_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("")
    print("[run-all] Summary")
    print(json.dumps(
        {
            "ok": summary["ok"],
            "manifest_path": str(manifest_path),
            "aggregate_summary": str(aggregate_out),
            "families_total": len(selected_families),
            "families_run": len([r for r in summary["results"] if r.get("status") in {"ok", "failed"}]),
            "families_failed": len([r for r in summary["results"] if r.get("status") == "failed"]),
        },
        indent=2,
    ))

    return 0 if summary["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
