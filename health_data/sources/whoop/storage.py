"""WHOOP raw storage dispatch using existing db upsert helpers."""
from __future__ import annotations
from typing import Dict, Any
from db import (
    upsert_user_basic_profile,
    upsert_user_body_measurement,
    upsert_cycle,
    upsert_sleep,
    upsert_recovery,
    upsert_workout,
)

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
