from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
CAPTURE_TOOL = REPO_ROOT / "tools" / "ttinteractive_browser_assisted_search.py"
MANUAL_INGEST_TOOL = REPO_ROOT / "tools" / "ttinteractive_manual_ingest.py"
DEFAULT_CDP_URL = "http://127.0.0.1:9222"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except Exception:
        return None


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _parse_cdp_port(cdp_url: str) -> int:
    parsed = urlparse(cdp_url)
    if not parsed.scheme or not parsed.hostname or not parsed.port:
        raise SystemExit(f"Invalid --cdp-url: {cdp_url}")
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise SystemExit("--launch-cdp-browser only supports localhost/127.0.0.1 --cdp-url")
    return int(parsed.port)


def _cdp_ready(cdp_url: str, timeout_s: float = 2.0) -> bool:
    url = cdp_url.rstrip("/") + "/json/version"
    try:
        with urlopen(url, timeout=timeout_s) as resp:
            return 200 <= int(resp.status) < 300
    except Exception:
        return False


def _wait_for_cdp(cdp_url: str, timeout_s: float = 20.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if _cdp_ready(cdp_url):
            return True
        time.sleep(0.5)
    return False


def _default_browser_candidates() -> list[Path]:
    candidates = [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    ]
    return [p for p in candidates if p.exists()]


def _pick_browser_exe(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.exists():
            raise SystemExit(f"--chrome-path not found: {p}")
        return p
    candidates = _default_browser_candidates()
    if not candidates:
        raise SystemExit("Could not find Chrome/Edge automatically. Pass --chrome-path.")
    return candidates[0]


def _launch_cdp_browser(
    *,
    cdp_url: str,
    chrome_path: str | None,
    user_data_dir: Path,
    proxy_server: str | None,
) -> None:
    if _cdp_ready(cdp_url):
        print(f"[runner] CDP endpoint already available at {cdp_url}; reusing existing browser.")
        return

    port = _parse_cdp_port(cdp_url)
    exe = _pick_browser_exe(chrome_path)
    user_data_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(exe),
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "about:blank",
    ]
    if proxy_server:
        cmd.insert(-1, f"--proxy-server={proxy_server}")

    print(f"[runner] Launching browser for CDP attach: {exe}")
    subprocess.Popen(cmd, cwd=str(REPO_ROOT))
    if not _wait_for_cdp(cdp_url):
        raise SystemExit(f"CDP endpoint did not become ready: {cdp_url}")
    print(f"[runner] CDP endpoint is ready: {cdp_url}")


def _add_arg(cmd: list[str], flag: str, value: Any | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def _build_run_dir(session_root: Path, args: argparse.Namespace) -> Path:
    carrier = args.carrier.lower()
    if args.capture_only:
        label = f"{carrier}_capture_only_{_now_tag()}"
    else:
        o = str(args.origin or "").upper()
        d = str(args.destination or "").upper()
        date = str(args.date or "")
        label = f"{carrier}_{o}_{d}_{date}_{_now_tag()}"
    return session_root / "runs" / label


def _pick_ingest_python(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.exists():
            raise SystemExit(f"--ingest-python not found: {p}")
        return p
    venv_py = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
    if venv_py.exists():
        return venv_py
    return Path(sys.executable)


def _determine_success(summary: dict[str, Any] | None, capture_only: bool, tool_rc: int) -> tuple[bool, str]:
    if capture_only:
        if tool_rc == 0:
            return True, "capture-only completed"
        return False, f"capture-only failed (rc={tool_rc})"

    if not summary:
        return False, "missing response summary"

    parsed_count = _int_or_zero(summary.get("parsed_selected_days_rows_count"))
    if parsed_count > 0:
        return True, f"parsed_selected_days_rows_count={parsed_count}"

    results_flow = bool(summary.get("reached_search_result_url")) or bool(summary.get("reached_flexibleflightliststatic_url"))
    if bool(summary.get("ok")) and results_flow:
        return True, "TTInteractive results flow reached"

    status = summary.get("status")
    if summary.get("datadome_blocked"):
        return False, f"DataDome blocked (status={status})"
    return False, f"no parsed fare rows (status={status})"


def _print_summary(summary: dict[str, Any] | None, response_path: Path, run_dir: Path) -> None:
    print("")
    print("[runner] Artifacts")
    print(f"  run_dir: {run_dir}")
    print(f"  summary: {response_path}")
    if not summary:
        return
    print("")
    print("[runner] Result summary")
    print(json.dumps(
        {
            "status": summary.get("status"),
            "ok": summary.get("ok"),
            "datadome_blocked": summary.get("datadome_blocked"),
            "reached_search_result_url": summary.get("reached_search_result_url"),
            "reached_flexibleflightliststatic_url": summary.get("reached_flexibleflightliststatic_url"),
            "parsed_selected_days_rows_count": summary.get("parsed_selected_days_rows_count"),
            "search_attempts": summary.get("search_attempts"),
        },
        indent=2,
    ))
    mismatch = summary.get("parsed_selected_days_input_mismatch")
    if mismatch:
        print("")
        print("[runner][warn] Parsed fares do not match the CLI inputs.")
        print(json.dumps(mismatch, indent=2))


def _run_auto_ingest(
    *,
    args: argparse.Namespace,
    run_dir: Path,
    response_out: Path,
    carrier_slug: str,
) -> tuple[int, dict[str, Any] | None]:
    ingest_python = _pick_ingest_python(args.ingest_python)
    ingest_cmd: list[str] = [
        str(ingest_python),
        str(MANUAL_INGEST_TOOL),
        "--summary",
        str(response_out),
        "--carrier",
        args.carrier,
    ]
    if args.ingest_allow_mismatch:
        ingest_cmd.append("--allow-mismatch")
    if args.ingest_dry_run:
        ingest_cmd.append("--dry-run")

    print("")
    print(f"[runner] Starting auto-ingest step using {ingest_python} ...")
    if args.print_command:
        print("[runner] Ingest command:")
        print("  " + subprocess.list2cmdline(ingest_cmd))

    proc = subprocess.run(ingest_cmd, cwd=str(REPO_ROOT))
    ingest_manifest_path = run_dir / f"{carrier_slug}_manual_ingest_result.json"
    ingest_manifest = _load_json(ingest_manifest_path) if ingest_manifest_path.exists() else None
    if ingest_manifest:
        print("")
        print("[runner] Ingest summary")
        print(json.dumps(
            {
                "dry_run": ingest_manifest.get("dry_run"),
                "scrape_id": ingest_manifest.get("scrape_id"),
                "rows_parsed_total": ingest_manifest.get("rows_parsed_total"),
                "rows_valid_for_core": ingest_manifest.get("rows_valid_for_core"),
                "rows_deduped_for_core": ingest_manifest.get("rows_deduped_for_core"),
                "rows_inserted": ingest_manifest.get("rows_inserted"),
            },
            indent=2,
        ))
    return proc.returncode, ingest_manifest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Operator runner for manual-assisted BS/2A TTInteractive capture via CDP attach.",
        epilog=(
            "Typical flow:\n"
            "  1) Launch Chrome/Edge with --remote-debugging-port=9222 (or use --launch-cdp-browser)\n"
            "  2) Run this tool\n"
            "  3) Solve captcha only when prompted; use manual UI fallback only if the tool says so"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--carrier", required=True, choices=["BS", "2A"])
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--capture-only", action="store_true", help="Only refresh cookies/session artifacts")
    parser.add_argument("--ingest", action="store_true", help="After successful capture, auto-run ttinteractive_manual_ingest.py for this run")
    parser.add_argument("--ingest-dry-run", action="store_true", help="With --ingest, validate parse only and skip DB writes")
    parser.add_argument("--ingest-allow-mismatch", action="store_true", help="With --ingest, allow parsed route/date mismatch")
    parser.add_argument("--ingest-python", help="Python executable to use for the ingest step (default: repo .venv if present)")
    parser.add_argument("--cdp-url", default=DEFAULT_CDP_URL, help=f"DevTools endpoint (default: {DEFAULT_CDP_URL})")
    parser.add_argument("--launch-cdp-browser", action="store_true", help="Launch Chrome/Edge with remote debugging if not already running")
    parser.add_argument("--chrome-path", help="Optional Chrome/Edge executable (used for --launch-cdp-browser)")
    parser.add_argument("--proxy-server", help="Proxy for launched browser and capture tool metadata, e.g. http://host:port")
    parser.add_argument("--user-data-dir", help="Browser profile dir for launched CDP browser (default under output/manual_sessions)")
    parser.add_argument("--session-root", default=str(REPO_ROOT / "output" / "manual_sessions"))
    parser.add_argument("--session-bundle-in", help="Override session bundle input JSON path")
    parser.add_argument("--session-bundle-out", help="Override session bundle output JSON path")
    parser.add_argument("--max-search-attempts", type=int, default=1, help="Automated same-browser search retries before manual UI fallback")
    parser.add_argument("--timeout-ms", type=int, default=120000)
    parser.add_argument("--settle-ms", type=int, default=3000)
    parser.add_argument("--non-interactive", action="store_true", help="Do not block on capture-tool input() prompts")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--keep-browser-open", action="store_true")
    parser.add_argument("--print-command", action="store_true", help="Print the underlying capture command before execution")
    args = parser.parse_args()

    if not args.capture_only and not (args.origin and args.destination and args.date):
        parser.error("--origin, --destination, and --date are required unless --capture-only is used")
    if args.capture_only and args.ingest:
        parser.error("--ingest cannot be used with --capture-only")

    if not CAPTURE_TOOL.exists():
        raise SystemExit(f"Capture tool not found: {CAPTURE_TOOL}")
    if args.ingest and not MANUAL_INGEST_TOOL.exists():
        raise SystemExit(f"Manual ingest tool not found: {MANUAL_INGEST_TOOL}")

    session_root = Path(args.session_root)
    session_root.mkdir(parents=True, exist_ok=True)
    run_dir = _build_run_dir(session_root, args)
    run_dir.mkdir(parents=True, exist_ok=True)

    carrier_slug = args.carrier.lower()
    default_bundle = session_root / f"{carrier_slug}_session_bundle.json"
    session_bundle_out = Path(args.session_bundle_out) if args.session_bundle_out else default_bundle
    if args.session_bundle_in:
        session_bundle_in = Path(args.session_bundle_in)
    else:
        session_bundle_in = default_bundle if default_bundle.exists() else None

    cookies_out = session_root / f"{carrier_slug}_cookies.json"
    cookies_full_out = session_root / f"{carrier_slug}_cookies_full.json"
    storage_state_out = session_root / f"{carrier_slug}_storage_state.json"
    user_data_dir = Path(args.user_data_dir) if args.user_data_dir else (session_root / f"{carrier_slug}_cdp_profile")

    response_out = run_dir / f"{carrier_slug}_probe_response.json"
    result_page_out = run_dir / f"{carrier_slug}_searchresult_page.html"
    bootstrap_config_out = run_dir / f"{carrier_slug}_bootstrap_config.json"
    network_dir = run_dir / f"{carrier_slug}_network_json"

    if args.launch_cdp_browser:
        _launch_cdp_browser(
            cdp_url=args.cdp_url,
            chrome_path=args.chrome_path,
            user_data_dir=user_data_dir,
            proxy_server=args.proxy_server,
        )

    cmd: list[str] = [sys.executable, str(CAPTURE_TOOL)]
    _add_arg(cmd, "--carrier", args.carrier)
    _add_arg(cmd, "--cdp-url", args.cdp_url)
    _add_arg(cmd, "--proxy-server", args.proxy_server)
    if args.user_data_dir or args.launch_cdp_browser:
        _add_arg(cmd, "--user-data-dir", user_data_dir)
    _add_arg(cmd, "--session-bundle-out", session_bundle_out)
    if session_bundle_in and session_bundle_in.exists():
        _add_arg(cmd, "--session-bundle-in", session_bundle_in)
    _add_arg(cmd, "--cookies-out", cookies_out)
    _add_arg(cmd, "--cookies-full-out", cookies_full_out)
    _add_arg(cmd, "--storage-state-out", storage_state_out)
    _add_arg(cmd, "--bootstrap-config-out", bootstrap_config_out)
    _add_arg(cmd, "--response-out", response_out)
    _add_arg(cmd, "--result-page-out", result_page_out)
    _add_arg(cmd, "--network-json-dir", network_dir)
    _add_arg(cmd, "--timeout-ms", args.timeout_ms)
    _add_arg(cmd, "--settle-ms", args.settle_ms)
    _add_arg(cmd, "--max-search-attempts", args.max_search_attempts)
    if args.non_interactive:
        cmd.append("--non-interactive")
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--adt", args.adt)
    _add_arg(cmd, "--chd", args.chd)
    _add_arg(cmd, "--inf", args.inf)
    if args.capture_only:
        cmd.append("--capture-only")
    else:
        _add_arg(cmd, "--origin", args.origin)
        _add_arg(cmd, "--destination", args.destination)
        _add_arg(cmd, "--date", args.date)
    if args.keep_browser_open:
        cmd.append("--keep-browser-open")

    print(f"[runner] Run directory: {run_dir}")
    if args.print_command:
        print("[runner] Command:")
        print("  " + subprocess.list2cmdline(cmd))

    tool_proc = subprocess.run(cmd, cwd=str(REPO_ROOT))
    summary = _load_json(response_out) if response_out.exists() else None
    _print_summary(summary, response_out, run_dir)
    ok, reason = _determine_success(summary, args.capture_only, tool_proc.returncode)

    print("")
    if ok:
        print(f"[runner] Capture step success: {reason}")
        if args.ingest:
            ingest_rc, ingest_manifest = _run_auto_ingest(
                args=args,
                run_dir=run_dir,
                response_out=response_out,
                carrier_slug=carrier_slug,
            )
            if ingest_rc != 0:
                print(f"[runner] FAILED: auto-ingest step failed (rc={ingest_rc})")
                return ingest_rc
            rows_inserted = (ingest_manifest or {}).get("rows_inserted")
            if rows_inserted is not None:
                print(f"[runner] SUCCESS: capture + ingest completed (rows_inserted={rows_inserted})")
            else:
                print("[runner] SUCCESS: capture + ingest completed")
            return 0
        print(f"[runner] SUCCESS: {reason}")
        return 0

    print(f"[runner] FAILED: {reason}")
    return tool_proc.returncode or 2


if __name__ == "__main__":
    raise SystemExit(main())
