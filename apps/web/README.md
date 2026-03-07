# Aero Pulse Web Application Plan

This directory now contains the first Next.js shell for the operational monitor.

## Stack

- Next.js
- React
- TypeScript
- API-first data access through the FastAPI reporting layer

## Local Run

From the repository root:

```powershell
cd apps\web
npm install
npm run dev
```

Default API target:

- `http://127.0.0.1:8000`

Override with:

- `API_BASE_URL`
- `NEXT_PUBLIC_API_BASE_URL`

See:

- [.env.example](.env.example)

## Current Pages

- `/`
  Executive shell with API health, latest cycle, airline, and route cards.

- `/routes`
  Live route monitor with API-backed filters for route, airline, and cabin scope.

- `/penalties`
  Penalty comparison screen against the reporting API.

- `/taxes`
  Tax comparison screen against the reporting API.

- `/changes`
  Change-event browser plan surface.

- `/forecasting`
  Warehouse-backed ML/DL forecast and backtest review surface.

## Why Vercel May Help

Vercel is useful for the Next.js frontend only. It is not required to build the shell locally.

Recommended split later:

- Vercel:
  deploy the Next.js frontend

- separate backend host:
  deploy FastAPI

- BigQuery + Looker Studio:
  analytics and dashboards
