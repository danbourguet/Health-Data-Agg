"""One-shot database setup script.

Usage (after virtualenv + requirements install):
  python scripts/setup_db.py

Reads schema.sql and applies it. Idempotent: uses IF NOT EXISTS in DDL.
Exits non-zero on failure.
"""
from __future__ import annotations
import os, sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv(Path('.') / '.env')

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_USER = os.getenv('DB_USER', 'whoop')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'whoop_password')
DB_NAME = os.getenv('DB_NAME', 'health_data')
DSN = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

SCHEMA_FILE = Path('schema.sql')

def main():
    if not SCHEMA_FILE.exists():
        print('schema.sql not found.', file=sys.stderr)
        sys.exit(1)
    sql_text = SCHEMA_FILE.read_text(encoding='utf-8')
    try:
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
            conn.commit()
        print('Database schemas/tables ensured.')
    except Exception as e:  # noqa: BLE001
        print(f'Error applying schema: {e}', file=sys.stderr)
        sys.exit(2)

if __name__ == '__main__':
    main()
