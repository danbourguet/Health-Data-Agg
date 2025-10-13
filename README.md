# Health Data Aggregator (WHOOP + Future Sources)

This project began as a WHOOP-only ingestor and is evolving into a modular personal health data aggregator (planned: WHOOP, Epic/MyChart via FHIR, Quest labs). Current production-ready portion remains the WHOOP pipeline; a new package scaffold (`health_data/`) and unified CLI are being introduced incrementally.

Ingest your WHOOP data (profile, body measurements, cycles, sleeps, recoveries, workouts) into a local Postgres database running in Docker. Canonical cross-source tables and additional adapters will arrive in subsequent milestones.

> What does "canonical" mean here?  
> A canonical table is a normalized, source-agnostic representation (e.g. `sleep_sessions`) populated from multiple raw source formats (WHOOP now, FHIR later). Raw WHOOP tables stay untouched as the authoritative ingestion store; transformation jobs *append* into canonical tables so analytics can query one unified schema.

## Features (Current WHOOP Capabilities)
- OAuth2 Authorization Code Flow (opens local browser, stores tokens in `.token_store.json`)
- Automatic token refresh
- Pagination handling for collection endpoints
- Retry and rate limit (429) backoff
- Idempotent upserts into Postgres

## Prerequisites
- Docker & Docker Compose
- Python 3.11+
- A WHOOP Developer application (client id & secret) with scopes:
  `read:profile read:body_measurement read:cycles read:sleep read:recovery read:workout`

## Setup
1. Copy environment file:
```powershell
Copy-Item .env.example .env
```
2. Edit `.env` and set:
```
WHOOP_CLIENT_ID=your_client_id
WHOOP_CLIENT_SECRET=your_client_secret
WHOOP_REDIRECT_URI=http://localhost:8765/callback
```
3. Start Postgres:
```powershell
docker compose up -d
```
4. Create virtual environment & install dependencies:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### One-shot bootstrap & ingest
Alternatively run the helper script (creates venv, installs deps, performs OAuth if needed, runs full ingestion):
```powershell
./run_all.ps1
```
Force re-auth and restrict to a date range:
```powershell
./run_all.ps1 -ForceAuth -Start 2025-09-01T00:00:00Z -End 2025-10-01T00:00:00Z
```

## Authenticate
Open browser and complete WHOOP OAuth once (tokens cached afterwards):
```powershell
python whoop_ingest.py --auth-only
```

Show command help (supports Windows style `-?`):
```powershell
python whoop_ingest.py -?
```

## Ingest Data
Fetch everything:
```powershell
python whoop_ingest.py
```
Or simply:
```powershell
./run_all.ps1
```
Specify resource subset:
```powershell
python whoop_ingest.py --resources profile body
```
Filter by time window (ISO 8601). Start/end apply to collection endpoints only:
```powershell
python whoop_ingest.py --start 2024-12-01T00:00:00Z --end 2025-01-01T00:00:00Z --resources cycles sleeps workouts recoveries
```

Daily refresh (replace previous full UTC day only):
```powershell
python whoop_ingest.py --daily-refresh
```
This deletes any existing activity rows whose start is within yesterday's UTC 00:00:00 to today 00:00:00 and reloads just that range.

### Reset / Truncate Tables
Clear only activity data (cycles, sleeps, recoveries, workouts) before reloading:
```powershell
python whoop_ingest.py --reset
```
Or wipe everything including profile & body measurement:
```powershell
python whoop_ingest.py --reset-all
```
Combine with date filters:
```powershell
python whoop_ingest.py --reset --start 2025-09-01T00:00:00Z --resources cycles sleeps
```
Via one-shot script:
```powershell
./run_all.ps1; python whoop_ingest.py --reset
```

## New Unified CLI (Transitional)
You can now use the experimental unified CLI (currently WHOOP only):
```powershell
python -m health_data.cli.main migrate      # apply new canonical & metadata migrations
python -m health_data.cli.main whoop auth   # perform WHOOP OAuth
python -m health_data.cli.main whoop ingest --resources cycles sleeps --since 2025-09-01T00:00:00Z --until 2025-09-07T00:00:00Z
```
The legacy script (`whoop_ingest.py`) still works and is the stable path; both will coexist until the new architecture fully replaces direct scripts.

## Data Model Overview

### Raw WHOOP Layer
Tables now prefixed with `whoop_raw_` to make source + layer explicit:
- `whoop_raw_user_basic_profile`
- `whoop_raw_user_body_measurement`
- `whoop_raw_cycles`
- `whoop_raw_sleeps`
- `whoop_raw_recoveries`
- `whoop_raw_workouts`

Compatibility views (`cycles`, `sleeps`, etc.) are created by migration `20251013_02_whoop_raw_rename.sql` so legacy code and ad‑hoc queries still function. New development should target the prefixed tables.

### Canonical Layer (Source-Agnostic)
- `sleep_sessions`
- `workouts_canonical`
- `biometrics_vitals`
- (Future) `lab_results`, `encounters`, `medications`, `conditions`

Each canonical table stores `source_system` + `raw_source_id` to trace lineage back to the raw record.

## Roadmap Snapshot
Planned major milestones:
1. Refactor (DONE initial scaffold) – package layout & migration system.
2. Canonical tables population for WHOOP (sleep/workouts/vitals) – IN PROGRESS.
3. Epic/MyChart (SMART on FHIR) adapter – Patient, Observation (vitals, labs), Encounter, Condition, Medication.
4. Lab results canonical mapping & optional Quest ingestion strategies (PDF/FHIR export parsing).
5. Scheduling & watermarks – automated daily incrementals.
6. Security hardening – encrypted token store, role-based DB access.
7. Documentation & testing expansion.

## Extending (Legacy WHOOP-Specific Path)
Add a new WHOOP endpoint (legacy method):
1. Create fetch function in `whoop_ingest.py` using `api_request` or `fetch_paginated`.
2. Add to `RESOURCE_MAP` and implement an upsert in `db.py` + schema update.

Future (modular) extension will involve adding a new adapter under `health_data/sources/<source_name>/` implementing the `SourceAdapter` interface.

## Token Store Security
The `.token_store.json` file contains sensitive tokens; keep it out of version control (add to `.gitignore`).

## Troubleshooting
- 401 Unauthorized repeatedly: delete `.token_store.json` and re-run `--auth-only`.
- Rate limiting: script auto-backs off; large historical ranges may take time.
- SSL or network errors: script will abort—re-run; upserts are idempotent.

## License
Provided as-is for personal data aggregation.
