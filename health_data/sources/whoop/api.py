"""Low-level WHOOP API helpers (request + pagination)."""
from __future__ import annotations
import logging, time
from typing import Dict, Any, Iterable, Optional
import requests
from .auth import get_access_token

logger = logging.getLogger(__name__)
API_BASE = 'https://api.prod.whoop.com/developer'

def api_request(method: str, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    backoff = 1
    for attempt in range(8):
        token = get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        resp = requests.request(method, url, params=params, headers=headers, timeout=60)
        if resp.status_code == 401 and attempt < 7:
            logger.info('401 Unauthorized; retrying after token refresh...')
            continue
        if resp.status_code == 429 and attempt < 7:
            retry_after = int(resp.headers.get('Retry-After', backoff))
            logger.warning(f'429 Rate limited; sleeping {retry_after}s (attempt {attempt+1})')
            time.sleep(retry_after)
            backoff = min(backoff * 2, 60)
            continue
        resp.raise_for_status()
        return resp.json() if resp.content else {}
    raise RuntimeError(f'Failed request {method} {path} after retries')

def fetch_paginated(path: str, limit: int = 25, start: Optional[str] = None, end: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    limit = min(limit, 25)
    params: Dict[str, Any] = {'limit': limit}
    if start: params['start'] = start
    if end: params['end'] = end
    next_token: Optional[str] = None
    while True:
        if next_token:
            params['nextToken'] = next_token
        data = api_request('GET', path, params=params)
        for r in data.get('records', []):
            yield r
        next_token = data.get('next_token') or data.get('nextToken')
        if not next_token:
            break
