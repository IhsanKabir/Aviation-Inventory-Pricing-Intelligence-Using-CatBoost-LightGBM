from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_ALL_TOOL = REPO_ROOT / "tools" / "run_all_manual_assisted.py"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json_load(raw: bytes) -> dict[str, Any]:
    if not raw:
        return {}
    obj = json.loads(raw.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("JSON body must be an object")
    return obj


@dataclass
class JobRecord:
    id: str
    status: str
    created_at_utc: str
    started_at_utc: str | None
    ended_at_utc: str | None
    return_code: int | None
    command: list[str]
    log_path: str
    summary_out: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "created_at_utc": self.created_at_utc,
            "started_at_utc": self.started_at_utc,
            "ended_at_utc": self.ended_at_utc,
            "return_code": self.return_code,
            "command": self.command,
            "log_path": self.log_path,
            "summary_out": self.summary_out,
        }


class JobStore:
    def __init__(self, root: Path):
        self.root = root
        self.jobs_dir = root / "jobs"
        self.logs_dir = root / "logs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._jobs: dict[str, JobRecord] = {}

    def put(self, rec: JobRecord) -> None:
        with self._lock:
            self._jobs[rec.id] = rec
        self._persist(rec)

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_recent(self, limit: int = 20) -> list[JobRecord]:
        with self._lock:
            rows = list(self._jobs.values())
        rows.sort(key=lambda r: r.created_at_utc, reverse=True)
        return rows[:limit]

    def update(self, job_id: str, **updates: Any) -> JobRecord | None:
        with self._lock:
            rec = self._jobs.get(job_id)
            if not rec:
                return None
            for k, v in updates.items():
                setattr(rec, k, v)
        self._persist(rec)
        return rec

    def _persist(self, rec: JobRecord) -> None:
        path = self.jobs_dir / f"{rec.id}.json"
        path.write_text(json.dumps(rec.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")


class WorkerServer:
    def __init__(self, *, repo_root: Path, python_exe: str, store: JobStore):
        self.repo_root = repo_root
        self.python_exe = python_exe
        self.store = store

    def launch_job(self, cli_args: list[str]) -> JobRecord:
        job_id = uuid.uuid4().hex[:12]
        summary_out = self.store.root / f"run_all_manual_assisted_{job_id}.json"
        log_path = self.store.logs_dir / f"{job_id}.log"

        cmd = [self.python_exe, str(RUN_ALL_TOOL), "--result-out", str(summary_out), *cli_args]
        rec = JobRecord(
            id=job_id,
            status="queued",
            created_at_utc=_utc_now_iso(),
            started_at_utc=None,
            ended_at_utc=None,
            return_code=None,
            command=cmd,
            log_path=str(log_path),
            summary_out=str(summary_out),
        )
        self.store.put(rec)

        thread = threading.Thread(target=self._run_job, args=(job_id,), daemon=True)
        thread.start()
        return rec

    def _run_job(self, job_id: str) -> None:
        rec = self.store.get(job_id)
        if not rec:
            return
        self.store.update(job_id, status="running", started_at_utc=_utc_now_iso())

        log_file = Path(rec.log_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("w", encoding="utf-8") as lf:
            lf.write(f"[worker] job_id={job_id} started_at_utc={_utc_now_iso()}\n")
            lf.write(f"[worker] command={subprocess.list2cmdline(rec.command)}\n\n")
            lf.flush()
            proc = subprocess.Popen(
                rec.command,
                cwd=str(self.repo_root),
                stdout=lf,
                stderr=subprocess.STDOUT,
                text=True,
            )
            rc = proc.wait()
            lf.write(f"\n[worker] job_id={job_id} ended_at_utc={_utc_now_iso()} rc={rc}\n")

        status = "succeeded" if rc == 0 else "failed"
        self.store.update(
            job_id,
            status=status,
            return_code=int(rc),
            ended_at_utc=_utc_now_iso(),
        )


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _build_handler(server_obj: WorkerServer, token: str | None):
    class Handler(BaseHTTPRequestHandler):
        def _auth_ok(self) -> bool:
            if not token:
                return True
            hdr = self.headers.get("Authorization", "")
            return hdr == f"Bearer {token}"

        def _read_json(self) -> dict[str, Any]:
            try:
                ln = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                ln = 0
            raw = self.rfile.read(ln) if ln > 0 else b""
            return _safe_json_load(raw)

        def do_GET(self) -> None:
            if not self._auth_ok():
                _write_json(self, 401, {"ok": False, "error": "unauthorized"})
                return

            if self.path == "/health":
                _write_json(self, 200, {"ok": True, "ts_utc": _utc_now_iso()})
                return

            if self.path == "/jobs":
                rows = [r.to_dict() for r in server_obj.store.list_recent()]
                _write_json(self, 200, {"ok": True, "jobs": rows})
                return

            if self.path.startswith("/jobs/"):
                job_id = self.path.split("/", 2)[2].strip()
                rec = server_obj.store.get(job_id)
                if not rec:
                    _write_json(self, 404, {"ok": False, "error": "job_not_found", "job_id": job_id})
                    return
                _write_json(self, 200, {"ok": True, "job": rec.to_dict()})
                return

            _write_json(self, 404, {"ok": False, "error": "not_found"})

        def do_POST(self) -> None:
            if not self._auth_ok():
                _write_json(self, 401, {"ok": False, "error": "unauthorized"})
                return

            if self.path != "/run-all-manual-assisted":
                _write_json(self, 404, {"ok": False, "error": "not_found"})
                return

            try:
                body = self._read_json()
            except Exception as exc:
                _write_json(self, 400, {"ok": False, "error": "invalid_json", "detail": str(exc)})
                return

            cli_args = body.get("cli_args", [])
            if not isinstance(cli_args, list) or not all(isinstance(x, str) for x in cli_args):
                _write_json(self, 400, {"ok": False, "error": "cli_args_must_be_string_list"})
                return

            rec = server_obj.launch_job(cli_args)
            _write_json(
                self,
                202,
                {
                    "ok": True,
                    "accepted": True,
                    "job_id": rec.id,
                    "status_url": f"/jobs/{rec.id}",
                    "job": rec.to_dict(),
                },
            )

        def log_message(self, fmt: str, *args: Any) -> None:
            # Keep console output clean for operators.
            return

    return Handler


def _detect_python(preferred: str | None) -> str:
    if preferred:
        return preferred
    venv = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
    if venv.exists():
        return str(venv)
    return sys.executable


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Local webhook worker for n8n/PAD to trigger run_all_manual_assisted.py without executeCommand nodes."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--python", help="Python executable used to run run_all_manual_assisted.py")
    parser.add_argument("--token", help="Optional bearer token for all requests")
    parser.add_argument(
        "--state-root",
        default=str(REPO_ROOT / "output" / "manual_sessions" / "webhook_worker"),
        help="Directory for worker job status + logs + run summaries",
    )
    args = parser.parse_args()

    python_exe = _detect_python(args.python)
    state_root = Path(args.state_root)
    state_root.mkdir(parents=True, exist_ok=True)

    store = JobStore(state_root)
    worker = WorkerServer(repo_root=REPO_ROOT, python_exe=python_exe, store=store)
    handler = _build_handler(worker, args.token)

    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"[worker] listening on http://{args.host}:{args.port}")
    print(f"[worker] python={python_exe}")
    print(f"[worker] state_root={state_root}")
    if args.token:
        print("[worker] auth=Bearer token required")
    else:
        print("[worker] auth=disabled (local trust mode)")

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\n[worker] stopping")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
