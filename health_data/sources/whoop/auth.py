"""WHOOP OAuth2 token management (authorization code + refresh)."""
from __future__ import annotations
import os, json, threading, time, webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs
from typing import Optional
import requests
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv(Path('.') / '.env')

API_BASE = 'https://api.prod.whoop.com/developer'
AUTH_BASE = 'https://api.prod.whoop.com/oauth/oauth2'
TOKEN_URL = f'{AUTH_BASE}/token'
AUTH_URL = f'{AUTH_BASE}/auth'

CLIENT_ID = os.getenv('WHOOP_CLIENT_ID')
CLIENT_SECRET = os.getenv('WHOOP_CLIENT_SECRET')
REDIRECT_URI = os.getenv('WHOOP_REDIRECT_URI', 'http://localhost:8765/callback')
SCOPES = os.getenv('WHOOP_SCOPES', 'read:profile read:body_measurement read:cycles read:sleep read:recovery read:workout')
TOKEN_STORE = Path('.token_store.json')  # legacy fallback
from db import get_conn  # use DB persistence in meta.oauth_tokens

class TokenManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._persisted = False  # True if tokens were loaded from DB or saved successfully
        self.tokens = self._load()

    def _load(self):
        # Prefer DB token storage
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT access_token, refresh_token, scope, token_type, expires_at FROM meta.oauth_tokens ORDER BY created_at DESC LIMIT 1')
                    row = cur.fetchone()
                    if row:
                        self._persisted = True
                        return {
                            'access_token': row[0],
                            'refresh_token': row[1],
                            'scope': row[2],
                            'token_type': row[3],
                            'expires_at': row[4].isoformat() if hasattr(row[4], 'isoformat') else row[4],
                        }
        except Exception:
            pass
        # Fallback to local file
        if TOKEN_STORE.exists():
            try:
                tk = json.loads(TOKEN_STORE.read_text())
                self._persisted = False
                return tk
            except Exception:
                return None
        return None

    def _save(self, data: dict):
        # Save to DB (normalize fields to match meta.oauth_tokens columns)
        try:
            # Normalize/derive values
            access_token = data.get('access_token')
            # refresh_token may be omitted on some responses; persist previous one or empty string
            refresh_token = data.get('refresh_token') or (self.tokens or {}).get('refresh_token') or ''
            # WHOOP returns 'scope' as a space-delimited string; if missing, default to empty
            scope_val = data.get('scope') or data.get('scopes') or ''
            token_type = data.get('token_type') or 'bearer'
            expires_at_val = data.get('expires_at')
            if not expires_at_val:
                # Derive from expires_in if available; default to 1 hour
                exp_secs = data.get('expires_in', 3600)
                expires_at_dt = datetime.now(timezone.utc) + timedelta(seconds=exp_secs)
            else:
                # Accept string or datetime; convert to datetime for psycopg2
                if isinstance(expires_at_val, str):
                    try:
                        expires_at_dt = datetime.fromisoformat(expires_at_val)
                    except Exception:
                        # Fallback: treat as UTC naive or strip Z
                        expires_at_dt = datetime.now(timezone.utc) + timedelta(seconds=int(data.get('expires_in', 3600)))
                else:
                    expires_at_dt = expires_at_val
            if not access_token:
                raise ValueError('Missing access_token in token payload')
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO meta.oauth_tokens (access_token, refresh_token, scope, token_type, expires_at) VALUES (%s,%s,%s,%s,%s)',
                        (access_token, refresh_token, scope_val, token_type, expires_at_dt)
                    )
                conn.commit()
            self._persisted = True
        except Exception as e:
            logger.warning('Failed to persist WHOOP token to DB; falling back to local file: %s', e)
            # Fallback to local file if DB write fails
            # Ensure local file has a consistent shape as well
            file_data = {
                'access_token': access_token or data.get('access_token'),
                'refresh_token': refresh_token,
                'scope': scope_val,
                'token_type': token_type,
                'expires_at': (expires_at_dt.isoformat() if 'expires_at_dt' in locals() else data.get('expires_at'))
                               or (datetime.now(timezone.utc) + timedelta(seconds=int(data.get('expires_in', 3600)))).isoformat(),
            }
            TOKEN_STORE.write_text(json.dumps(file_data, indent=2))

    def _valid(self) -> bool:
        if not self.tokens:
            return False
        try:
            exp = datetime.fromisoformat(self.tokens['expires_at'])
        except Exception:
            return False
        return exp > datetime.now(timezone.utc) + timedelta(seconds=30)

    def get_access_token(self) -> str:
        with self._lock:
            if self._valid():
                # If tokens were loaded from file and not yet persisted, write them to DB now
                if not self._persisted:
                    self._save(self.tokens)
                return self.tokens['access_token']
            if self.tokens and self.tokens.get('refresh_token'):
                self.refresh()
                return self.tokens['access_token']
            self.authorize_flow()
            return self.tokens['access_token']

    def authorize_flow(self):
        if not CLIENT_ID or not CLIENT_SECRET:
            raise RuntimeError('Missing WHOOP_CLIENT_ID / WHOOP_CLIENT_SECRET')
        state = os.urandom(16).hex()
        params = {
            'response_type': 'code',
            'client_id': CLIENT_ID,
            'redirect_uri': REDIRECT_URI,
            'scope': SCOPES,
            'state': state,
        }
        url = f"{AUTH_URL}?{urlencode(params)}"
        logger.info('Launching browser for WHOOP OAuth authorization...')
        logger.info(url)
        webbrowser.open(url)
        code_holder: dict[str,str] = {}

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self_inner):  # noqa: N802
                parsed = urlparse(self_inner.path)
                if parsed.path != '/callback':
                    self_inner.send_response(404); self_inner.end_headers(); return
                qs = parse_qs(parsed.query)
                if qs.get('state', [''])[0] != state:
                    self_inner.send_response(400); self_inner.end_headers(); self_inner.wfile.write(b'State mismatch'); return
                if 'code' not in qs:
                    self_inner.send_response(400); self_inner.end_headers(); self_inner.wfile.write(b'Missing code'); return
                code_holder['code'] = qs['code'][0]
                self_inner.send_response(200); self_inner.end_headers(); self_inner.wfile.write(b'Authorization complete. You may close this tab.')
            def log_message(self_inner, format, *args):
                return

        server = HTTPServer(('localhost', 8765), Handler)
        t = threading.Thread(target=server.serve_forever, daemon=True); t.start()
        timeout = time.time() + 300
        while 'code' not in code_holder and time.time() < timeout:
            time.sleep(0.2)
        server.shutdown()
        if 'code' not in code_holder:
            raise RuntimeError('Authorization timed out')
        data = {
            'grant_type': 'authorization_code',
            'code': code_holder['code'],
            'redirect_uri': REDIRECT_URI,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
        }
        resp = requests.post(TOKEN_URL, data=data, timeout=30); resp.raise_for_status()
        tk = resp.json(); exp = tk.get('expires_in', 3600)
        # Normalize token fields
        tk['expires_at'] = (datetime.now(timezone.utc) + timedelta(seconds=exp)).isoformat()
        tk['token_type'] = tk.get('token_type') or 'bearer'
        tk['scope'] = tk.get('scope') or tk.get('scopes') or ''
        # refresh_token should be present for auth_code grant, but guard anyway
        if not tk.get('refresh_token') and self.tokens:
            rt = (self.tokens or {}).get('refresh_token')
            if rt:
                tk['refresh_token'] = rt
        self.tokens = tk; self._save(tk)
        logger.info('WHOOP token stored.')

    def refresh(self):
        if not self.tokens or 'refresh_token' not in self.tokens:
            self.authorize_flow(); return
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.tokens['refresh_token'],
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
        }
        resp = requests.post(TOKEN_URL, data=data, timeout=30)
        if resp.status_code != 200:
            logger.warning('Refresh failed; starting auth flow')
            self.authorize_flow(); return
        tk = resp.json(); exp = tk.get('expires_in', 3600)
        # Normalize token fields
        tk['expires_at'] = (datetime.now(timezone.utc) + timedelta(seconds=exp)).isoformat()
        tk['token_type'] = tk.get('token_type') or 'bearer'
        tk['scope'] = tk.get('scope') or tk.get('scopes') or ''
        tk.setdefault('refresh_token', self.tokens.get('refresh_token'))
        self.tokens = tk; self._save(tk)
        logger.info('WHOOP token refreshed.')

TOKEN_MANAGER = TokenManager()

def get_access_token() -> str:
    return TOKEN_MANAGER.get_access_token()
