"""WHOOP resource fetch functions mapping."""
from __future__ import annotations
from typing import Iterable, Optional, Dict, Any
from .api import api_request, fetch_paginated

def fetch_profile():
    return api_request('GET', '/v2/user/profile/basic')

def fetch_body_measurement():
    return api_request('GET', '/v2/user/measurement/body')

def fetch_cycles(start: Optional[str] = None, end: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    yield from fetch_paginated('/v2/cycle', start=start, end=end)

def fetch_sleeps(start: Optional[str] = None, end: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    yield from fetch_paginated('/v2/activity/sleep', start=start, end=end)

def fetch_recoveries(start: Optional[str] = None, end: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    yield from fetch_paginated('/v2/recovery', start=start, end=end)

def fetch_workouts(start: Optional[str] = None, end: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    yield from fetch_paginated('/v2/activity/workout', start=start, end=end)

RESOURCE_MAP = {
    'profile': fetch_profile,
    'body': fetch_body_measurement,
    'cycles': fetch_cycles,
    'sleeps': fetch_sleeps,
    'recoveries': fetch_recoveries,
    'workouts': fetch_workouts,
}
