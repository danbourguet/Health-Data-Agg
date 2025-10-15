import os
import sys
import json
import time
import threading
import logging
import argparse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Iterable, Optional, List

import requests
from dotenv import load_dotenv

from db import (
    run_schema,
    upsert_user_basic_profile,
    upsert_user_body_measurement,
    upsert_cycle,
    upsert_sleep,
    upsert_recovery,
    upsert_workout,
    truncate_activity_tables,
    truncate_all_tables,
    delete_activity_range,
)

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

API_BASE = 'https://api.prod.whoop.com/developer'
AUTH_BASE = 'https://api.prod.whoop.com/oauth/oauth2'
TOKEN_URL = f'{AUTH_BASE}/token'
AUTH_URL = f'{AUTH_BASE}/auth'

load_dotenv(dotenv_path=Path('.') / '.env', override=False)

CLIENT_ID = os.getenv('WHOOP_CLIENT_ID')
CLIENT_SECRET = os.getenv('WHOOP_CLIENT_SECRET')
REDIRECT_URI = os.getenv('WHOOP_REDIRECT_URI', 'http://localhost:8765/callback')
SCOPES = os.getenv('WHOOP_SCOPES', 'read:profile read:body_measurement read:cycles read:sleep read:recovery read:workout')
PAGE_LIMIT = int(os.getenv('REQUEST_PAGE_LIMIT', '50'))
TOKEN_STORE = Path('.token_store.json')
from db import get_conn

if not CLIENT_ID or not CLIENT_SECRET:
    logger.error('Missing WHOOP_CLIENT_ID or WHOOP_CLIENT_SECRET. Set them in .env or environment variables.')

class TokenManager:
    def __init__(self):
        self._lock = threading.Lock()
        self.tokens = self._load_tokens()

    def _load_tokens(self):
        # Prefer DB token storage
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT access_token, refresh_token, scope, token_type, expires_at FROM meta.oauth_tokens ORDER BY created_at DESC LIMIT 1')
                    row = cur.fetchone()
                    if row:
                        return {
                            'access_token': row[0],
                            'refresh_token': row[1],
                            'scope': row[2],
                            'token_type': row[3],
                            'expires_at': row[4].isoformat() if hasattr(row[4], 'isoformat') else row[4],
                        }
        except Exception:
            pass
        if TOKEN_STORE.exists():
            try:
                return json.loads(TOKEN_STORE.read_text())
            except Exception:
                return None
        return None

    def _save_tokens(self, data: dict):
        # Save to DB and fall back to file
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO meta.oauth_tokens (access_token, refresh_token, scope, token_type, expires_at) VALUES (%s,%s,%s,%s,%s)',
                        (data.get('access_token'), data.get('refresh_token'), data.get('scope') or '', data.get('token_type') or 'bearer', data.get('expires_at'))
                    )
                conn.commit()
        except Exception:
            TOKEN_STORE.write_text(json.dumps(data, indent=2))

    def have_valid_access(self) -> bool:
        if not self.tokens:
            return False
        exp = datetime.fromisoformat(self.tokens['expires_at'])
        return exp > datetime.now(timezone.utc) + timedelta(seconds=30)

    def get_access_token(self) -> str:
        with self._lock:
            if self.have_valid_access():
                return self.tokens['access_token']
            if self.tokens and 'refresh_token' in self.tokens:
                self.refresh()
                return self.tokens['access_token']
            self.authorize_flow()
            return self.tokens['access_token']

    def authorize_flow(self):
        logger.info('Starting authorization code flow...')
        state = os.urandom(16).hex()
        params = {
            'response_type': 'code',
            'client_id': CLIENT_ID,
            'redirect_uri': REDIRECT_URI,
            'scope': SCOPES,
            'state': state,
        }
        url = f"{AUTH_URL}?{urlencode(params)}"
        logger.info(f'Open this URL if browser does not open automatically:\n{url}')
        webbrowser.open(url)

        code_holder = {}

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self_inner):  # noqa: N802
                parsed = urlparse(self_inner.path)
                if parsed.path != '/callback':
                    self_inner.send_response(404)
                    self_inner.end_headers()
                    return
                qs = parse_qs(parsed.query)
                if qs.get('state', [''])[0] != state:
                    self_inner.send_response(400)
                    self_inner.end_headers()
                    self_inner.wfile.write(b'State mismatch')
                    return
                if 'code' not in qs:
                    self_inner.send_response(400)
                    self_inner.end_headers()
                    self_inner.wfile.write(b'Missing code')
                    return
                code_holder['code'] = qs['code'][0]
                self_inner.send_response(200)
                self_inner.end_headers()
                self_inner.wfile.write(b'Authorization complete. You can close this window.')

            def log_message(self_inner, format, *args):  # noqa: A003
                return  # suppress

        server = HTTPServer(('localhost', 8765), Handler)
        # Run server in thread
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

        timeout = time.time() + 300
        while 'code' not in code_holder and time.time() < timeout:
            time.sleep(0.2)
        server.shutdown()
        if 'code' not in code_holder:
            raise RuntimeError('Did not receive authorization code in time.')
        code = code_holder['code']
        logger.info('Received authorization code, exchanging for tokens...')
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': REDIRECT_URI,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
        }
        resp = requests.post(TOKEN_URL, data=data, timeout=30)
        resp.raise_for_status()
        token_json = resp.json()
        expires_in = token_json.get('expires_in', 3600)
        token_json['expires_at'] = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
        self.tokens = token_json
        self._save_tokens(token_json)
        logger.info('Token exchange complete.')

    def refresh(self):
        logger.info('Refreshing access token...')
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.tokens['refresh_token'],
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
        }
        resp = requests.post(TOKEN_URL, data=data, timeout=30)
        if resp.status_code != 200:
            logger.warning('Refresh failed, starting full auth flow.')
            self.authorize_flow()
            return
        token_json = resp.json()
        expires_in = token_json.get('expires_in', 3600)
        token_json['expires_at'] = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
        # keep original refresh if not provided
        token_json.setdefault('refresh_token', self.tokens.get('refresh_token'))
        self.tokens = token_json
        self._save_tokens(token_json)
        logger.info('Refresh complete.')

TOKEN_MANAGER = TokenManager()

# HTTP request helper with retry & rate limit handling

def api_request(method: str, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    backoff = 1
    for attempt in range(8):
        token = TOKEN_MANAGER.get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        resp = requests.request(method, url, params=params, headers=headers, timeout=60)
        if resp.status_code == 401 and attempt < 7:
            logger.info('401 Unauthorized, refreshing token...')
            TOKEN_MANAGER.refresh()
            continue
        if resp.status_code == 429 and attempt < 7:
            retry_after = int(resp.headers.get('Retry-After', backoff))
            logger.warning(f'Rate limited (429). Sleeping {retry_after}s...')
            time.sleep(retry_after)
            backoff = min(backoff * 2, 60)
            continue
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {}
    raise RuntimeError(f'Failed request {method} {path} after retries')

# Pagination helpers

def fetch_paginated(path: str, limit: int = PAGE_LIMIT, start: Optional[str] = None, end: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    # WHOOP API maximum 'limit' per spec is 25 for these collection endpoints
    limit = min(limit, 25)
    params: Dict[str, Any] = {'limit': limit}
    if start:
        params['start'] = start
    if end:
        params['end'] = end
    next_token: Optional[str] = None
    while True:
        if next_token:
            params['nextToken'] = next_token
        data = api_request('GET', path, params=params)
        records = data.get('records', [])
        for r in records:
            yield r
        next_token = data.get('next_token') or data.get('nextToken')
        if not next_token:
            break

# Resource fetchers

def fetch_profile():
    return api_request('GET', '/v2/user/profile/basic')

def fetch_body_measurement():
    return api_request('GET', '/v2/user/measurement/body')

def fetch_cycles(start=None, end=None):
    yield from fetch_paginated('/v2/cycle', start=start, end=end)

def fetch_sleeps(start=None, end=None):
    yield from fetch_paginated('/v2/activity/sleep', start=start, end=end)

def fetch_recoveries(start=None, end=None):
    yield from fetch_paginated('/v2/recovery', start=start, end=end)

def fetch_workouts(start=None, end=None):
    yield from fetch_paginated('/v2/activity/workout', start=start, end=end)

RESOURCE_MAP = {
    'profile': fetch_profile,
    'body': fetch_body_measurement,
    'cycles': fetch_cycles,
    'sleeps': fetch_sleeps,
    'recoveries': fetch_recoveries,
    'workouts': fetch_workouts,
}

# Storage dispatch (targets whoop_raw_* tables via db.py upserts after rename migration)

def store_record(resource: str, record: Dict[str, Any]):
    if resource == 'profile':
        upsert_user_basic_profile(record)
    elif resource == 'body':
        upsert_user_body_measurement(record)
    elif resource == 'cycles':
        upsert_cycle(record)
    elif resource == 'sleeps':
        upsert_sleep(record)
    elif resource == 'recoveries':
        upsert_recovery(record)
    elif resource == 'workouts':
        upsert_workout(record)
    else:
        logger.warning(f'No storage handler for resource {resource}')


def iso_or_none(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    # accept date or datetime
    try:
        if len(s) == 10:
            return datetime.fromisoformat(s).date().isoformat()  # type: ignore
        return datetime.fromisoformat(s).isoformat()
    except Exception:
        raise argparse.ArgumentTypeError(f'Invalid ISO date/datetime: {s}')


def ingest(resources: List[str], start: Optional[str], end: Optional[str]):
    # schema migration
    run_schema()

    for res in resources:
        fetcher = RESOURCE_MAP[res]
        if res in {'profile', 'body'}:
            logger.info(f'Fetching single resource {res}...')
            data = fetcher()
            store_record(res, data)
            logger.info(f'Stored {res}.')
        else:
            count = 0
            logger.info(f'Fetching collection {res}...')
            for rec in fetcher(start=start, end=end):
                store_record(res, rec)
                count += 1
                if count % 50 == 0:
                    logger.info(f'{res}: stored {count} records...')
            logger.info(f'{res}: stored {count} total records.')


def parse_args(argv: List[str]):
    # Support Windows-style -? help
    if '-?' in argv:
        # Replace with --help for argparse to handle
        argv = [('--help' if a == '-?' else a) for a in argv]
    parser = argparse.ArgumentParser(description='WHOOP data ingestion', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--resources', nargs='+', default=['profile', 'body', 'cycles', 'sleeps', 'recoveries', 'workouts'],
                        choices=list(RESOURCE_MAP.keys()), help='Resources to ingest')
    parser.add_argument('--start', type=str, help='ISO datetime start filter')
    parser.add_argument('--end', type=str, help='ISO datetime end filter')
    parser.add_argument('--auth-only', action='store_true', help='Perform OAuth flow and exit')
    parser.add_argument('--reset', action='store_true', help='Truncate activity tables before ingest (cycles, sleeps, recoveries, workouts)')
    parser.add_argument('--reset-all', action='store_true', help='Truncate ALL tables including user profile/measurements before ingest')
    parser.add_argument('--daily-refresh', action='store_true', help='Replace data for the previous full UTC day only (ignores provided --start/--end)')
    return parser.parse_args(argv)


def main(argv: List[str]):
    args = parse_args(argv)
    if args.auth_only:
        TOKEN_MANAGER.get_access_token()
        print('Authentication complete. Token stored in Postgres (meta.oauth_tokens).')
        return
    # Always ensure schema exists before any truncate operations (user may have dropped tables)
    try:
        run_schema()
    except Exception as e:
        logger.warning(f'Schema initialization attempt before reset flags failed (continuing): {e}')
    if args.reset_all:
        logger.warning('Truncating ALL tables (including user profile & body measurement)...')
        truncate_all_tables()
    elif args.reset:
        logger.warning('Truncating activity tables (cycles, sleeps, recoveries, workouts)...')
        truncate_activity_tables()
    if args.daily_refresh:
        # Previous UTC day boundaries
        now_utc = datetime.now(timezone.utc)
        today_start = datetime(year=now_utc.year, month=now_utc.month, day=now_utc.day, tzinfo=timezone.utc)
        prev_start = today_start - timedelta(days=1)
        prev_end = today_start
        # Delete existing activity rows in that window
        logger.info(f'Refreshing previous UTC day window {prev_start.isoformat()} to {prev_end.isoformat()} (exclusive)')
        delete_activity_range(prev_start.isoformat(), prev_end.isoformat())
        ingest(args.resources, prev_start.isoformat(), prev_end.isoformat())
    else:
        ingest(args.resources, args.start, args.end)

if __name__ == '__main__':
    main(sys.argv[1:])
