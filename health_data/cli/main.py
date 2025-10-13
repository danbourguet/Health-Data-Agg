"""Unified CLI (initial WHOOP only)."""
from __future__ import annotations
import click
from typing import Optional
from health_data.sources.whoop.adapter import WhoopAdapter
from health_data.db.migration_runner import run_migrations
from db import delete_activity_range  # reuse existing helper for now

@click.group()
def cli():
    """Health Data Aggregator CLI."""

@cli.command()
@click.option('--dry-run', is_flag=True, help='List pending migrations without applying.')
def migrate(dry_run: bool):
    """Apply database migrations."""
    run_migrations(dry_run=dry_run)

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
@click.option('--canonical', is_flag=True, help='Also transform into canonical tables (future).')
@click.option('--daily-refresh', is_flag=True, help='Refresh previous UTC day window.')
def whoop_ingest(resource_args, resources, since: Optional[str], until: Optional[str], canonical: bool, daily_refresh: bool):
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

    for result in adapter.ingest(res_list, since=since, until=until, canonical=canonical):
        click.echo(f'{result.resource}: fetched={result.records_fetched} stored={result.records_loaded} status={result.status}')
        if result.error:
            click.echo(f'  Error: {result.error}', err=True)

if __name__ == '__main__':
    cli()
