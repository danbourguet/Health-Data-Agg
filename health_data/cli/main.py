"""Unified CLI.

Simplified operational flow:
    1. bootstrap           -> apply schema.sql (idempotent)
    2. whoop auth          -> obtain/refresh WHOOP OAuth tokens
    3. whoop ingest        -> pull raw WHOOP data into whoop_raw.* tables
    4. unified rebuild   -> transform raw -> unified.* tables

Migration runner retained for future incremental changes but not required for basic use.
"""
from __future__ import annotations
import click
from typing import Optional
from health_data.sources.whoop.adapter import WhoopAdapter
from health_data.sources.quest.adapter import QuestAdapter
from health_data.db.migration_runner import run_migrations
from db import delete_activity_range  # reuse existing helper for now
from pathlib import Path
import psycopg2, os
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('.') / '.env', override=False)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_USER = os.getenv('DB_USER', 'whoop')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'whoop_password')
DB_NAME = os.getenv('DB_NAME', 'whoop')
DSN = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

@click.group()
def cli():
    """Health Data Aggregator CLI."""

@cli.command()
@click.option('--dry-run', is_flag=True, help='List pending migrations without applying.')
def migrate(dry_run: bool):
    """Apply database migrations."""
    run_migrations(dry_run=dry_run)

@cli.command()
def bootstrap():
    """Apply schema.sql directly (idempotent bootstrap)."""
    schema_path = Path('schema.sql')
    if not schema_path.exists():
        raise click.ClickException('schema.sql not found at project root.')
    sql_text = schema_path.read_text(encoding='utf-8')
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
    click.echo('Bootstrap complete: schemas/tables ensured.')

@cli.group()
def whoop():
    """WHOOP source commands."""

@whoop.command('auth')
def whoop_auth():
    adapter = WhoopAdapter()
    adapter.authenticate()
    click.echo('WHOOP authentication completed.')

@whoop.command('ingest')
@click.argument('resource_args', nargs=-1)
@click.option('--resources', help='Comma or space separated subset of resources (default all).')
@click.option('--since', type=str, help='Start ISO timestamp for collection resources.')
@click.option('--until', type=str, help='End ISO timestamp for collection resources.')
@click.option('--daily-refresh', is_flag=True, help='Refresh previous UTC day window (deletes that window then re-fetches).')
def whoop_ingest(resource_args, resources, since: Optional[str], until: Optional[str], daily_refresh: bool):
    from datetime import datetime, timezone, timedelta
    adapter = WhoopAdapter()
    adapter.authenticate()
    available = adapter.list_resources()
    # Build resource list from precedence: --resources option > positional args > all
    if resources:
        res_list = resources.replace(',', ' ').split()
    elif resource_args:
        res_list = list(resource_args)
    else:
        res_list = available
    for r in res_list:
        if r not in available:
            raise click.UsageError(f'Unknown WHOOP resource: {r}')

    if daily_refresh:
        now_utc = datetime.now(timezone.utc)
        today_start = datetime(year=now_utc.year, month=now_utc.month, day=now_utc.day, tzinfo=timezone.utc)
        prev_start = today_start - timedelta(days=1)
        prev_end = today_start
        delete_activity_range(prev_start.isoformat(), prev_end.isoformat())
        since = prev_start.isoformat()
        until = prev_end.isoformat()
        click.echo(f'Refreshing WHOOP data for previous UTC day: {since} to {until}')

    # Ingest raw only (canonical transformation separated into its own command)
    for result in adapter.ingest(res_list, since=since, until=until, canonical=False):
        click.echo(f'{result.resource}: fetched={result.records_fetched} stored={result.records_loaded} status={result.status}')
        if result.error:
            click.echo(f'  Error: {result.error}', err=True)

@cli.group()
def unified():  # pragma: no cover - simple group
    """Unified layer operations."""

@unified.command('rebuild')
@click.option('--truncate', is_flag=True, help='Truncate canonical tables before rebuilding (default: True).', default=True)
def unified_rebuild(truncate: bool):
    """Rebuild unified tables from raw WHOOP data."""
    import psycopg2
    from health_data.db.canonical import transform_record
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            if truncate:
                cur.execute('TRUNCATE unified.sleep_sessions, unified.workouts, unified.biometrics_vitals RESTART IDENTITY CASCADE;')
                # Keep user_identity (upsert semantics) - do not truncate
        conn.commit()
    # Stream raw rows and transform
    resources_processed = { 'profile':0, 'sleeps':0, 'workouts':0, 'recoveries':0 }
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            # profile (single row)
            cur.execute('SELECT raw FROM whoop_raw.user_basic_profile')
            for (raw,) in cur.fetchall():
                transform_record('profile', raw)
                resources_processed['profile'] += 1
            # sleeps
            cur.execute('SELECT raw FROM whoop_raw.sleeps')
            for (raw,) in cur.fetchall():
                transform_record('sleeps', raw)
                resources_processed['sleeps'] += 1
            # workouts
            cur.execute('SELECT raw FROM whoop_raw.workouts')
            for (raw,) in cur.fetchall():
                transform_record('workouts', raw)
                resources_processed['workouts'] += 1
            # recoveries
            cur.execute('SELECT raw FROM whoop_raw.recoveries')
            for (raw,) in cur.fetchall():
                transform_record('recoveries', raw)
                resources_processed['recoveries'] += 1
    click.echo('Unified rebuild complete:')
    for k,v in resources_processed.items():
        click.echo(f'  {k}: {v} records processed')

@cli.group()
def quest():  # pragma: no cover
    """Quest diagnostics ingestion (FHIR file-based initial)."""

@quest.command('ingest')
@click.option('--path', 'path_', required=True, help='Path to Quest PDF/JSON/NDJSON file or directory of such files.')
@click.option('--patient-id', help='Override patient id (if not derivable)')
@click.option('--resources', help='Comma/space list (patient,observations) default all.')
@click.option('--since', type=str, help='Observation collected >= since (ISO).')
@click.option('--until', type=str, help='Observation collected < until (ISO).')
@click.option('--unified', 'unified_flag', is_flag=True, help='Also load into unified.lab_results (observations only).')
def quest_ingest(path_, patient_id, resources, since, until, unified_flag):
    if unified_flag:
        # Fail fast if lab_results missing; instruct user to run bootstrap/setup.
        import psycopg2
        with psycopg2.connect(DSN) as _conn:
            with _conn.cursor() as _cur:
                _cur.execute("SELECT to_regclass('unified.lab_results')")
                if not _cur.fetchone()[0]:
                    raise click.ClickException("unified.lab_results not found. Run: python -m health_data.cli.main bootstrap (or setup_db.py) before ingesting with --unified.")
    adapter = QuestAdapter(path_=path_, patient_id=patient_id)
    adapter.authenticate()
    available = adapter.list_resources()
    if resources:
        res_list = resources.replace(',', ' ').split()
    else:
        res_list = available
    for r in res_list:
        if r not in available:
            raise click.UsageError(f'Unknown Quest resource: {r}')
    for result in adapter.ingest(res_list, since=since, until=until, canonical=unified_flag):
        click.echo(f'{result.resource}: fetched={result.records_fetched} stored={result.records_loaded} status={result.status}')
        if result.error:
            click.echo(f'  Error: {result.error}', err=True)

if __name__ == '__main__':
    cli()
