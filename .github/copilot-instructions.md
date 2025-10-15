# Copilot instructions for this repo

Purpose: help AI coding agents be productive immediately in this Python + dbt health data stack (WHOOP + Quest) by knowing the architecture, workflows, and repo-specific patterns.

## Big picture
 Stack: Python ingestion (requests, psycopg2) -> Postgres raw schemas -> dbt staging -> marts (final). Optional Prefect orchestration.
 Schemas (schema.sql): meta, whoop_raw, quest_raw, staging (dbt), unified (legacy), marts (dbt final).
- Data flow:
  1) Ingest WHOOP/Quest into whoop_raw/quest_raw via upserts (idempotent) in `db.py` and adapters under `health_data/sources/*`.
 dbt: `dbt_project.yml`, models in `dbt/models/{staging,unified}` (built into `marts`), macro `dbt/macros/incremental_time_filter.sql`.
  3) Tests defined in `dbt/models/unified/unified.yml` enforce keys and NOT NULLs.

## Where things live (examples)
- CLI entry: `health_data/cli/main.py` (commands: bootstrap, whoop auth/ingest, ingest-pdf, unified-info).
- Legacy direct script: `whoop_ingest.py` (OAuth flow, pagination, rate-limit retry, Json upserts). Prefer the CLI for new work.
- DB helpers (raw): `db.py` uses psycopg2 + ON CONFLICT and stores full payloads as JSONB.
- DB helpers (unified): `health_data/db/unified.py` (was used before dbt; keep read-only reference).
- Orchestration: `orchestration/flows.py` (Prefect 2.0 flows call CLI/dbt).
- dbt: `dbt_project.yml`, models in `dbt/models/{staging,unified}`, macro `dbt/macros/incremental_time_filter.sql`.

## Critical workflows (PowerShell)
- Bootstrap DB and deps:
  ```powershell
  docker compose up -d
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  pip install -r requirements.txt
  python -m health_data.cli.main bootstrap
  ```
- WHOOP OAuth + ingest raw:
  ```powershell
  python -m health_data.cli.main whoop auth
  python -m health_data.cli.main whoop ingest --daily-refresh   # previous UTC day
  # or bounded range
  python -m health_data.cli.main whoop ingest --since 2025-09-01T00:00:00Z --until 2025-09-05T00:00:00Z
  ```
- dbt profile and build/test:
  ```powershell
  New-Item -ItemType Directory -Force $env:USERPROFILE\.dbt | Out-Null
  Copy-Item dbt\profiles.example.yml $env:USERPROFILE\.dbt\profiles.yml
  dbt debug
  dbt run && dbt test
  ```
- VS Code task: run “dbt run unified models” (or `dbt run --select unified_sleep_sessions unified_workouts unified_vitals`).

## Repo-specific conventions
- Raw layer upserts: store the full source JSON in a `raw` column and denormalize key metrics; use `ON CONFLICT` for idempotency (see upsert_* in `db.py`).
- Incremental modeling:
  - dbt unified models are `materialized: incremental` with stable `unique_key` (e.g., `whoop_sleep_id`, `vital_key`, `lab_result_key`).
  - Time-based filters gate incrementals (see `unified_*` models and macro `macros/incremental_time_filter.sql`).
- Daily refresh strategy: delete previous day window in raw (`delete_activity_range` in `db.py`) then re-fetch (`whoop ingest --daily-refresh`).
- Staging models: select/rename from raw; example `dbt/models/staging/stg_whoop_sleeps.sql` defines `raw_id`, `start_time`, minutes from millis, etc.
- Tests live in model YAML (`dbt/models/unified/unified.yml`) using built-in `not_null` and `unique`.

## Integrations & config
- WHOOP OAuth env in `.env`: `WHOOP_CLIENT_ID`, `WHOOP_CLIENT_SECRET`, optional `WHOOP_REDIRECT_URI` (default `http://localhost:8765/callback`).
- Postgres env: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` (default DB_NAME=health_data; docker-compose defaults match schema.sql).
- dbt profiles: copy `dbt/profiles.example.yml` to `%USERPROFILE%\.dbt\profiles.yml` or set `$env:DBT_PROFILES_DIR = "$(Get-Location)\dbt"`.
- Prefect flows (optional): `python -m orchestration.flows run-full-refresh` or `run-daily-update`.

## Do/Don’t for agents
- Do: prefer dbt for new transformations in unified; keep raw upsert shape stable.
- Do: add tests in the model YAML when adding columns/keys.
- Don’t: commit `target/`, `logs/`, tokens (`.token_store.json`), `.env`, or `dbt_internal_packages/`.

Feedback welcome: If parts of the flow (e.g., Quest ingestion adapter details) are unclear or missing, tell me what you need and I’ll refine these instructions.