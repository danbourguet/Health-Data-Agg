"""Simple migration runner applying SQL files in order.

Usage: from cli, call run_migrations()
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import List
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('.') / '.env', override=False)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_USER = os.getenv('DB_USER', 'whoop')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'whoop_password')
DB_NAME = os.getenv('DB_NAME', 'whoop')
DSN = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

MIGRATIONS_DIR = Path(__file__).parent / 'migrations'

def _ensure_migrations_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id SERIAL PRIMARY KEY,
                migration_id TEXT NOT NULL UNIQUE,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    conn.commit()

def list_sql_files() -> List[Path]:
    return sorted([p for p in MIGRATIONS_DIR.glob('*.sql') if p.is_file()])

def applied_migration_ids(conn) -> set[str]:
    _ensure_migrations_table(conn)
    with conn.cursor() as cur:
        cur.execute('SELECT migration_id FROM schema_migrations')
        rows = cur.fetchall()
    return {r[0] for r in rows}

def apply_migration(conn, path: Path):
    migration_id = path.name
    sql_text = path.read_text(encoding='utf-8')
    with conn.cursor() as cur:
        cur.execute(sql_text)
        cur.execute('INSERT INTO schema_migrations (migration_id) VALUES (%s)', (migration_id,))
    conn.commit()
    print(f"Applied migration {migration_id}")

def run_migrations(dry_run: bool = False):
    with psycopg2.connect(DSN) as conn:
        done = applied_migration_ids(conn)
        files = list_sql_files()
        for f in files:
            if f.name in done:
                continue
            if dry_run:
                print(f"PENDING {f.name}")
            else:
                apply_migration(conn, f)
        if dry_run:
            print('Dry run complete.')

if __name__ == '__main__':
    run_migrations()
