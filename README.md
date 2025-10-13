# Health Data Aggregator (Modular, Multi-Source Ready)

Modular personal health data platform starting with WHOOP. Designed from day one with a clear separation between:
1. Raw source-specific ingestion tables (`whoop_raw` schema)
2. Canonical, source-agnostic analytical tables (`canonical` schema)
3. Meta / operational tables (`meta` schema)

Planned future integrations: Epic/MyChart (FHIR), Quest / other lab sources.

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

## Unified CLI (Primary Interface)
All functionality is exposed through the modular CLI (legacy scripts removed).

Authenticate (performs OAuth if needed):
```powershell
python -m health_data.cli.main whoop auth
```
Run migrations (canonical + meta tables):
```powershell
python -m health_data.cli.main migrate
```
Ingest all WHOOP resources (raw only):
```powershell
python -m health_data.cli.main whoop ingest
```
Ingest a subset for a time window:
```powershell
python -m health_data.cli.main whoop ingest cycles sleeps --since 2025-09-01T00:00:00Z --until 2025-09-02T00:00:00Z
```
Ingest + populate canonical tables simultaneously:
```powershell
python -m health_data.cli.main whoop ingest --canonical
```
Daily refresh (drops previous UTC day in raw activity tables then reloads + transforms):
```powershell
python -m health_data.cli.main whoop ingest --daily-refresh --canonical
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

### Schemas
- `meta`: operational metadata (tokens, future run logs)
- `whoop_raw`: raw WHOOP ingestion tables (structure mirrors API concepts)
- `canonical`: normalized analytical tables (multi-source ready)

### whoop_raw Tables
- `whoop_raw.user_basic_profile`
- `whoop_raw.user_body_measurement`
- `whoop_raw.cycles`
- `whoop_raw.sleeps`
- `whoop_raw.recoveries`
- `whoop_raw.workouts`

### canonical Tables (current)
- `canonical.user_identity`
- `canonical.sleep_sessions`
- `canonical.workouts`
- `canonical.biometrics_vitals`

Each canonical row includes `source_system` and `raw_source_id` for lineage.

## Roadmap Snapshot
Planned major milestones:
1. Refactor (DONE initial scaffold) – package layout & migration system.
2. Canonical tables population for WHOOP (sleep/workouts/vitals) – IN PROGRESS.
3. Epic/MyChart (SMART on FHIR) adapter – Patient, Observation (vitals, labs), Encounter, Condition, Medication.
4. Lab results canonical mapping & optional Quest ingestion strategies (PDF/FHIR export parsing).
5. Scheduling & watermarks – automated daily incrementals.
6. Security hardening – encrypted token store, role-based DB access.
7. Documentation & testing expansion.

## Extending
Add a new WHOOP resource (example outline):
1. Implement fetch function in `health_data/sources/whoop/resources.py`.
2. Add it to `RESOURCE_MAP`.
3. Add a raw upsert (if new table needed) + schema change.
4. Optionally add a canonical transform in `health_data/db/canonical.py` and reference it in `TRANSFORM_DISPATCH`.

Future sources (FHIR, labs) will implement their own adapter under `health_data/sources/<source_name>/` and reuse canonical insert helpers.

## Token Store Security
The `.token_store.json` file contains sensitive tokens; keep it out of version control (add to `.gitignore`).

## Troubleshooting
- 401 Unauthorized repeatedly: delete `.token_store.json` and re-run `--auth-only`.
- Rate limiting: script auto-backs off; large historical ranges may take time.
- SSL or network errors: script will abort—re-run; upserts are idempotent.

## License
Provided as-is for personal data aggregation.
