"""Prefect flows for end-to-end orchestration.

Flows:
  full_refresh: bootstrap -> whoop ingest -> quest ingest (optional) -> unified rebuild
  daily_update: whoop daily refresh -> quest ingest (new files) -> unified rebuild

To run locally (ephemeral):
  python -m orchestration.flows run-full-refresh

To register with Prefect server/cloud later, you can wrap these flows with deployments.
"""
from __future__ import annotations
import subprocess, os
from datetime import datetime, timedelta, timezone
from prefect import flow, task

PY = ['python', '-m', 'health_data.cli.main']
DBT = ['dbt']

@task
def bootstrap_db():
    subprocess.run(PY + ['bootstrap'], check=True)

@task
def whoop_ingest_all():
    subprocess.run(PY + ['whoop', 'ingest'], check=True)

@task
def whoop_daily_refresh():
    subprocess.run(PY + ['whoop', 'ingest', '--daily-refresh'], check=True)

@task
def quest_ingest_path(path: str | None = None, unified: bool = True):
    if not path:
        return
    args = PY + ['quest', 'ingest', '--path', path]
    if unified:
        args.append('--unified')
    subprocess.run(args, check=True)

@task
def dbt_run():
    subprocess.run(DBT + ['run'], check=True)

@task
def dbt_test():
    subprocess.run(DBT + ['test'], check=True)

@flow(name='full_refresh')
def full_refresh(quest_path: str | None = None):
    bootstrap_db()
    whoop_ingest_all()
    quest_ingest_path(quest_path)
    dbt_run()
    dbt_test()

@flow(name='daily_update')
def daily_update(quest_path: str | None = None):
    whoop_daily_refresh()
    quest_ingest_path(quest_path)
    dbt_run()
    dbt_test()

if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('command', choices=['run-full-refresh','run-daily-update'])
    ap.add_argument('--quest-path')
    args = ap.parse_args()
    if args.command == 'run-full-refresh':
        full_refresh(quest_path=args.quest_path)
    else:
        daily_update(quest_path=args.quest_path)
