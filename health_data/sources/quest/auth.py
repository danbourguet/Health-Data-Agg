"""Quest FHIR API authentication utilities.

Supports two modes:
1. Pre-provided bearer token via env var QUEST_ACCESS_TOKEN (simplest for personal use).
2. OAuth2 Client Credentials (if/when client id/secret available):
   Requires QUEST_CLIENT_ID, QUEST_CLIENT_SECRET, QUEST_TOKEN_URL.

Token caching is in-memory only for now.
"""
from __future__ import annotations
import os, time, requests
from dataclasses import dataclass
from typing import Optional

@dataclass
class QuestToken:
    access_token: str
    expires_at: float  # epoch seconds

_TOKEN: Optional[QuestToken] = None

def get_access_token() -> str:
    # 1. Manual override
    manual = os.getenv('QUEST_ACCESS_TOKEN')
    if manual:
        return manual
    global _TOKEN
    if _TOKEN and _TOKEN.expires_at - 30 > time.time():
        return _TOKEN.access_token
    client_id = os.getenv('QUEST_CLIENT_ID')
    client_secret = os.getenv('QUEST_CLIENT_SECRET')
    token_url = os.getenv('QUEST_TOKEN_URL')
    if not (client_id and client_secret and token_url):
        raise RuntimeError('Provide QUEST_ACCESS_TOKEN or QUEST_CLIENT_ID/QUEST_CLIENT_SECRET/QUEST_TOKEN_URL for Quest API access.')
    resp = requests.post(token_url, data={
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': os.getenv('QUEST_SCOPE', 'openid profile')
    }, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f'Quest token request failed {resp.status_code}: {resp.text[:200]}')
    data = resp.json()
    expires_in = data.get('expires_in', 3600)
    _TOKEN = QuestToken(access_token=data['access_token'], expires_at=time.time() + expires_in)
    return _TOKEN.access_token
