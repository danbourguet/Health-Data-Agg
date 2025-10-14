"""Quest FHIR API request + pagination helpers."""
from __future__ import annotations
import os, time, requests
from typing import Dict, Any, Iterable, Optional
from .auth import get_access_token

DEFAULT_BASE = 'https://api.quest.example.com/fhir/R4'  # placeholder; override via QUEST_FHIR_BASE_URL

class QuestAPIError(RuntimeError):
    pass

def api_request(method: str, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    base = os.getenv('QUEST_FHIR_BASE_URL', DEFAULT_BASE).rstrip('/')
    url = f"{base}/{path.lstrip('/')}"
    headers = {
        'Authorization': f'Bearer {get_access_token()}',
        'Accept': 'application/fhir+json'
    }
    for attempt in range(5):
        resp = requests.request(method, url, params=params, headers=headers, timeout=60)
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            time.sleep(2 ** attempt)
            continue
        if resp.status_code != 200:
            raise QuestAPIError(f"Quest API {method} {url} -> {resp.status_code} {resp.text[:200]}")
        return resp.json()
    raise QuestAPIError(f"Quest API {method} {url} failed after retries")

def fetch_patient(patient_id: str) -> Dict[str, Any]:
    return api_request('GET', f'Patient/{patient_id}')

def fetch_observations(patient_id: str, since: Optional[str] = None, until: Optional[str] = None, page_size: int = 100) -> Iterable[Dict[str, Any]]:
    # Build initial search params
    params = {
        'patient': patient_id,
        '_count': page_size
    }
    # FHIR _lastUpdated uses prefixes (ge, le). We'll approximate with date/time if provided.
    # If the API expects different query parameters (e.g., date=ge...), adapt here.
    if since:
        params['_lastUpdated'] = f'ge{since}'
    if until:
        # FHIR supports combined date filters; for stricter logic we'd chain, but keep simple
        existing = params.get('_lastUpdated')
        # Can't supply two _lastUpdated; real pattern is & _lastUpdated=ge... & _lastUpdated=lt...
        # So instead we will store separately and add manually below.
    base = os.getenv('QUEST_FHIR_BASE_URL', DEFAULT_BASE).rstrip('/')
    next_url = f"{base}/Observation"
    while next_url:
        headers = {
            'Authorization': f'Bearer {get_access_token()}',
            'Accept': 'application/fhir+json'
        }
        resp = requests.get(next_url, params=params, headers=headers, timeout=60)
        params = None  # only first call uses params
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            time.sleep(1)
            continue
        if resp.status_code != 200:
            raise QuestAPIError(f"Quest API GET {next_url} -> {resp.status_code} {resp.text[:200]}")
        bundle = resp.json()
        for entry in bundle.get('entry', []):
            res = entry.get('resource')
            if res and res.get('resourceType') == 'Observation':
                # Apply until filter manually if provided
                if until:
                    eff = res.get('effectiveDateTime') or res.get('issued')
                    if eff and eff >= until:
                        continue
                yield res
        # Find next link
        next_link = None
        for link in bundle.get('link', []):
            if link.get('relation') == 'next':
                next_link = link.get('url')
                break
        next_url = next_link
