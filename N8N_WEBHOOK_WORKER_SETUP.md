# n8n Replacement for `executeCommand` (Webhook + Local Worker)

Last updated: 2026-02-27

## Why

Your n8n environment does not support `n8n-nodes-base.executeCommand`, so direct local shell execution from n8n is not available.

This setup replaces it with:

1. n8n HTTP nodes
2. local Python webhook worker
3. existing orchestrator `tools/run_all_manual_assisted.py`

## 1) Start local worker

```powershell
Set-Location C:\Users\TLL-90134\Documents\airline_scraper_full_clone
tools\manual_assisted_webhook_worker.bat --host 127.0.0.1 --port 8787 --token REPLACE_ME
```

Health check:

```powershell
iwr http://127.0.0.1:8787/health -UseBasicParsing | Select-Object -Expand Content
```

## 2) Import n8n workflow

Import file: `n8n workflow.txt`

Then edit in `Config` node:

- `workerBaseUrl` = `http://127.0.0.1:8787`
- `workerToken` = same token used in worker launch
- `cliArgs` = CLI args for `run_all_manual_assisted.py`

Example:

```json
["--limit-dates","1","--ingest","--non-interactive","--stop-on-error"]
```

## 3) Trigger run from n8n

Flow behavior:

1. `Start Run (POST)` submits job
2. Worker returns `job_id`
3. n8n waits and polls `/jobs/{job_id}`
4. On `succeeded` or `failed`, flow exits with final paths

## 4) Where artifacts are stored

Worker files:

- `output/manual_sessions/webhook_worker/jobs/<job_id>.json`
- `output/manual_sessions/webhook_worker/logs/<job_id>.log`
- `output/manual_sessions/webhook_worker/run_all_manual_assisted_<job_id>.json`

Normal capture artifacts remain under:

- `output/manual_sessions/queues/`
- `output/manual_sessions/queue_runs/`
- `output/manual_sessions/runs/`

## 5) PAD usage with same worker (optional)

If you prefer PAD for scheduling:

1. PAD step runs the worker (once per machine boot or operator session)
2. PAD uses `Invoke-WebRequest` to POST `/run-all-manual-assisted`
3. PAD polls `/jobs/<job_id>` until done

No direct Chrome automation steps are required in PAD for orchestration itself.

## 6) PAD direct runner (no worker, optional alternative)

Use:

`tools\pad_run_manual_assisted.ps1`

Example:

```powershell
powershell -ExecutionPolicy Bypass -File tools\pad_run_manual_assisted.ps1 `
  -RepoRoot "C:\Users\TLL-90134\Documents\airline_scraper_full_clone" `
  -RunAllArgs @("--limit-dates","1","--ingest","--non-interactive","--stop-on-error")
```

This is useful when PAD is your only scheduler and you do not want a persistent webhook worker process.
