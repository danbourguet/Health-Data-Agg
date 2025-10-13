"""Canonical layer DB helper functions for WHOOP transformations."""
from __future__ import annotations
import os
from datetime import datetime
from typing import Optional
import psycopg2
from psycopg2.extras import Json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('.') / '.env', override=False)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_USER = os.getenv('DB_USER', 'whoop')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'whoop_password')
DB_NAME = os.getenv('DB_NAME', 'whoop')
DSN = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

SOURCE_SYSTEM = 'whoop'

# Connection helper

def get_conn():  # noqa: D401
    return psycopg2.connect(DSN)

# User identity

def get_or_create_internal_user(conn, source_user_id: str | int, email: Optional[str] = None, first_name: Optional[str] = None, last_name: Optional[str] = None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            'SELECT internal_user_id FROM canonical.user_identity WHERE source_system=%s AND source_user_id=%s',
            (SOURCE_SYSTEM, str(source_user_id))
        )
        row = cur.fetchone()
        if row:
            internal_id = row[0]
            # Optionally update last_seen and metadata if newly provided
            cur.execute('UPDATE canonical.user_identity SET last_seen=NOW(), email=COALESCE(%s,email), first_name=COALESCE(%s,first_name), last_name=COALESCE(%s,last_name) WHERE internal_user_id=%s',
                        (email, first_name, last_name, internal_id))
            return internal_id
        cur.execute(
            'INSERT INTO canonical.user_identity (source_system, source_user_id, email, first_name, last_name) VALUES (%s,%s,%s,%s,%s) RETURNING internal_user_id',
            (SOURCE_SYSTEM, str(source_user_id), email, first_name, last_name)
        )
        return cur.fetchone()[0]

# Insert helpers (idempotent via unique indexes added in later migration)

def insert_sleep_session(conn, internal_user_id: int, raw_id: str, start_time: str, end_time: Optional[str], duration_minutes: Optional[int], efficiency_pct: Optional[float], rem_minutes: Optional[int], deep_minutes: Optional[int], light_minutes: Optional[int], awake_minutes: Optional[int], respiratory_rate: Optional[float], raw: dict):
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO canonical.sleep_sessions (internal_user_id,start_time,end_time,duration_minutes,efficiency_pct,rem_minutes,deep_minutes,light_minutes,awake_minutes,respiratory_rate,source_system,raw_source_id,raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (internal_user_id, start_time, end_time, duration_minutes, efficiency_pct, rem_minutes, deep_minutes, light_minutes, awake_minutes, respiratory_rate, SOURCE_SYSTEM, raw_id, Json(raw))
        )


def insert_workout(conn, internal_user_id: int, raw_id: str, start_time: str, end_time: Optional[str], sport: Optional[str], avg_hr: Optional[int], max_hr: Optional[int], strain: Optional[float], energy_kj: Optional[float], distance_m: Optional[float], altitude_gain_m: Optional[float], altitude_change_m: Optional[float], raw: dict):
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO canonical.workouts (internal_user_id,start_time,end_time,sport,average_hr,max_hr,strain,energy_kj,distance_m,altitude_gain_m,altitude_change_m,source_system,raw_source_id,raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (internal_user_id, start_time, end_time, sport, avg_hr, max_hr, strain, energy_kj, distance_m, altitude_gain_m, altitude_change_m, SOURCE_SYSTEM, raw_id, Json(raw))
        )


def insert_vital(conn, internal_user_id: int, recorded_at: str, vital_type: str, value_num: Optional[float], unit: Optional[str], raw_source_id: Optional[str], raw: dict):
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO canonical.biometrics_vitals (internal_user_id, recorded_at, type, value_num, unit, source_system, raw_source_id, raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (internal_user_id, recorded_at, vital_type, value_num, unit, SOURCE_SYSTEM, raw_source_id, Json(raw))
        )

# Transformation functions

def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    # Replace trailing Z with +00:00 for Python fromisoformat compatibility
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def millis_to_minutes(ms: Optional[int]) -> Optional[int]:
    if ms is None:
        return None
    return int(round(ms / 60000))


def transform_sleep(conn, record: dict):
    user_id = record.get('user_id')
    internal_id = get_or_create_internal_user(conn, user_id)
    start = record.get('start')
    end = record.get('end')
    start_dt = parse_iso(start)
    end_dt = parse_iso(end)
    duration_minutes = None
    if start_dt and end_dt:
        duration_minutes = int((end_dt - start_dt).total_seconds() / 60)
    score = record.get('score') or {}
    stage = score.get('stage_summary') or {}
    efficiency = score.get('sleep_efficiency_percentage')
    respiratory_rate = score.get('respiratory_rate')
    rem_minutes = millis_to_minutes(stage.get('total_rem_sleep_time_milli'))
    light_minutes = millis_to_minutes(stage.get('total_light_sleep_time_milli'))
    deep_minutes = millis_to_minutes(stage.get('total_slow_wave_sleep_time_milli'))
    awake_minutes = millis_to_minutes(stage.get('total_awake_time_milli'))
    insert_sleep_session(
        conn,
        internal_user_id=internal_id,
        raw_id=record['id'],
        start_time=start,
        end_time=end,
        duration_minutes=duration_minutes,
        efficiency_pct=efficiency,
        rem_minutes=rem_minutes,
        deep_minutes=deep_minutes,
        light_minutes=light_minutes,
        awake_minutes=awake_minutes,
        respiratory_rate=respiratory_rate,
        raw=record,
    )
    # Also record respiratory rate as vital (if present)
    if respiratory_rate and start:
        insert_vital(conn, internal_id, start, 'respiratory_rate', float(respiratory_rate), 'breaths/min', record['id'], record)


def transform_workout(conn, record: dict):
    user_id = record.get('user_id')
    internal_id = get_or_create_internal_user(conn, user_id)
    score = record.get('score') or {}
    insert_workout(
        conn,
        internal_user_id=internal_id,
        raw_id=record['id'],
        start_time=record.get('start'),
        end_time=record.get('end'),
        sport=record.get('sport_name'),
        avg_hr=score.get('average_heart_rate'),
        max_hr=score.get('max_heart_rate'),
        strain=score.get('strain'),
        energy_kj=score.get('kilojoule'),
        distance_m=score.get('distance_meter'),
        altitude_gain_m=score.get('altitude_gain_meter'),
        altitude_change_m=score.get('altitude_change_meter'),
        raw=record,
    )


def transform_recovery(conn, record: dict):
    user_id = record.get('user_id')
    internal_id = get_or_create_internal_user(conn, user_id)
    score = record.get('score') or {}
    # Resting HR, HRV, recovery score, SPO2, skin temp
    recorded_at = None
    # Use associated cycle start time if present for timestamp anchor
    # fallback to NOW() is avoided; if no time we skip
    # (cycles table already has start; record has cycle_id, but not timestamp here)
    # We'll store using sleep_id link is not convenient; skip timestamp if not available.
    # WHOOP recovery object may not include explicit timestamp fields in v2; leaving None safe.
    resting = score.get('resting_heart_rate')
    if resting is not None:
        insert_vital(conn, internal_id, record.get('created_at') or record.get('updated_at') or datetime.utcnow().isoformat(), 'resting_hr', float(resting), 'bpm', str(record.get('cycle_id')), record)
    hrv = score.get('hrv_rmssd_milli')
    if hrv is not None:
        insert_vital(conn, internal_id, record.get('created_at') or record.get('updated_at') or datetime.utcnow().isoformat(), 'hrv_rmssd', float(hrv), 'ms', str(record.get('cycle_id')), record)
    spo2 = score.get('spo2_percentage')
    if spo2 is not None:
        insert_vital(conn, internal_id, record.get('created_at') or record.get('updated_at') or datetime.utcnow().isoformat(), 'spo2_pct', float(spo2), 'percent', str(record.get('cycle_id')), record)
    skin_temp = score.get('skin_temp_celsius')
    if skin_temp is not None:
        insert_vital(conn, internal_id, record.get('created_at') or record.get('updated_at') or datetime.utcnow().isoformat(), 'skin_temp_celsius', float(skin_temp), 'C', str(record.get('cycle_id')), record)
    recovery_score = score.get('recovery_score')
    if recovery_score is not None:
        insert_vital(conn, internal_id, record.get('created_at') or record.get('updated_at') or datetime.utcnow().isoformat(), 'recovery_score', float(recovery_score), 'score', str(record.get('cycle_id')), record)


def transform_profile(conn, record: dict):
    # Ensure identity row enriched with profile details
    get_or_create_internal_user(conn, record.get('user_id'), email=record.get('email'), first_name=record.get('first_name'), last_name=record.get('last_name'))

TRANSFORM_DISPATCH = {
    'sleeps': transform_sleep,
    'workouts': transform_workout,
    'recoveries': transform_recovery,
    'profile': transform_profile,
}


def transform_record(resource: str, record: dict):
    func = TRANSFORM_DISPATCH.get(resource)
    if not func:
        return
    with get_conn() as conn:
        func(conn, record)
        conn.commit()
