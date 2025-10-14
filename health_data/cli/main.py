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

@cli.command('unified-info')
def unified_info():
    """Explain how to build the unified layer (dbt primary)."""
    click.echo('Unified layer is now built via dbt models. Run:')
    click.echo('  dbt run && dbt test')
    click.echo('See README for details.')

@cli.command('ingest-pdf')
@click.option('--path', 'path_', required=True, help='Path to Quest PDF file or directory of such files.')
@click.option('--patient-id', help='Override patient id (if not derivable)')
def ingest_pdf(path_, patient_id):
    """Ingest Quest PDF lab results only (no API/FHIR)."""
    adapter = QuestAdapter(path_=path_, patient_id=patient_id)
    adapter.authenticate()
    for result in adapter.ingest(['observations'], since=None, until=None, canonical=False):
        click.echo(f'{result.resource}: fetched={result.records_fetched} stored={result.records_loaded} status={result.status}')
        if result.error:
            click.echo(f'  Error: {result.error}', err=True)

if __name__ == '__main__':
    cli()
