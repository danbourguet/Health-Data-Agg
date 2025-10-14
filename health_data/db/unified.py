"""Unified layer DB helper functions (formerly canonical)."""
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

def get_conn():
    return psycopg2.connect(DSN)

def get_or_create_internal_user(conn, source_user_id: str | int, email: Optional[str] = None, first_name: Optional[str] = None, last_name: Optional[str] = None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            'SELECT internal_user_id FROM unified.user_identity WHERE source_system=%s AND source_user_id=%s',
            (SOURCE_SYSTEM, str(source_user_id))
        )
        row = cur.fetchone()
        if row:
            internal_id = row[0]
            cur.execute('UPDATE unified.user_identity SET last_seen=NOW(), email=COALESCE(%s,email), first_name=COALESCE(%s,first_name), last_name=COALESCE(%s,last_name) WHERE internal_user_id=%s',
                        (email, first_name, last_name, internal_id))
            return internal_id
        cur.execute(
            'INSERT INTO unified.user_identity (source_system, source_user_id, email, first_name, last_name) VALUES (%s,%s,%s,%s,%s) RETURNING internal_user_id',
            (SOURCE_SYSTEM, str(source_user_id), email, first_name, last_name)
        )
        return cur.fetchone()[0]

def insert_sleep_session(conn, internal_user_id: int, raw_id: str, start_time: str, end_time: Optional[str], duration_minutes: Optional[int], efficiency_pct: Optional[float], rem_minutes: Optional[int], deep_minutes: Optional[int], light_minutes: Optional[int], awake_minutes: Optional[int], respiratory_rate: Optional[float], raw: dict):
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO unified.sleep_sessions (internal_user_id,start_time,end_time,duration_minutes,efficiency_pct,rem_minutes,deep_minutes,light_minutes,awake_minutes,respiratory_rate,source_system,raw_source_id,raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (internal_user_id, start_time, end_time, duration_minutes, efficiency_pct, rem_minutes, deep_minutes, light_minutes, awake_minutes, respiratory_rate, SOURCE_SYSTEM, raw_id, Json(raw))
        )

def insert_workout(conn, internal_user_id: int, raw_id: str, start_time: str, end_time: Optional[str], sport: Optional[str], avg_hr: Optional[int], max_hr: Optional[int], strain: Optional[float], energy_kj: Optional[float], distance_m: Optional[float], altitude_gain_m: Optional[float], altitude_change_m: Optional[float], raw: dict):
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO unified.workouts (internal_user_id,start_time,end_time,sport,average_hr,max_hr,strain,energy_kj,distance_m,altitude_gain_m,altitude_change_m,source_system,raw_source_id,raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (internal_user_id, start_time, end_time, sport, avg_hr, max_hr, strain, energy_kj, distance_m, altitude_gain_m, altitude_change_m, SOURCE_SYSTEM, raw_id, Json(raw))
        )

def insert_vital(conn, internal_user_id: int, recorded_at: str, vital_type: str, value_num: Optional[float], unit: Optional[str], raw_source_id: Optional[str], raw: dict):
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO unified.biometrics_vitals (internal_user_id, recorded_at, type, value_num, unit, source_system, raw_source_id, raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (internal_user_id, recorded_at, vital_type, value_num, unit, SOURCE_SYSTEM, raw_source_id, Json(raw))
        )

def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
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
    start = record.get('start'); end = record.get('end')
    start_dt = parse_iso(start); end_dt = parse_iso(end)
    duration_minutes = int((end_dt - start_dt).total_seconds() / 60) if (start_dt and end_dt) else None
    score = record.get('score') or {}
    stage = score.get('stage_summary') or {}
    efficiency = score.get('sleep_efficiency_percentage')
    respiratory_rate = score.get('respiratory_rate')
    rem_minutes = millis_to_minutes(stage.get('total_rem_sleep_time_milli'))
    light_minutes = millis_to_minutes(stage.get('total_light_sleep_time_milli'))
    deep_minutes = millis_to_minutes(stage.get('total_slow_wave_sleep_time_milli'))
    awake_minutes = millis_to_minutes(stage.get('total_awake_time_milli'))
    insert_sleep_session(conn, internal_id, record['id'], start, end, duration_minutes, efficiency, rem_minutes, deep_minutes, light_minutes, awake_minutes, respiratory_rate, record)
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
    ts = record.get('created_at') or record.get('updated_at') or datetime.utcnow().isoformat()
    def vital(name, value, unit):
        if value is not None:
            insert_vital(conn, internal_id, ts, name, float(value), unit, str(record.get('cycle_id')), record)
    vital('resting_hr', score.get('resting_heart_rate'), 'bpm')
    vital('hrv_rmssd', score.get('hrv_rmssd_milli'), 'ms')
    vital('spo2_pct', score.get('spo2_percentage'), 'percent')
    vital('skin_temp_celsius', score.get('skin_temp_celsius'), 'C')
    vital('recovery_score', score.get('recovery_score'), 'score')

def transform_profile(conn, record: dict):
    get_or_create_internal_user(conn, record.get('user_id'), email=record.get('email'), first_name=record.get('first_name'), last_name=record.get('last_name'))

def insert_lab_result(conn, internal_user_id: int, raw_id: str, loinc_code: str | None, test_name: str | None, collected_at: str | None,
                      value_num: float | None, value_text: str | None, unit: str | None, ref_low: float | None, ref_high: float | None, abnormal_flag: str | None, raw: dict):
    with conn.cursor() as cur:
        cur.execute(
            'INSERT INTO unified.lab_results (internal_user_id, loinc_code, test_name, collected_at, value_num, value_text, unit, reference_low, reference_high, abnormal_flag, source_system, raw_source_id, raw) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (internal_user_id, loinc_code, test_name, collected_at, value_num, value_text, unit, ref_low, ref_high, abnormal_flag, 'quest', raw_id, Json(raw))
        )

def transform_quest_observation(conn, record: dict):
    obs_id = record.get('id')
    patient_ref = (record.get('subject') or {}).get('reference') if isinstance(record.get('subject'), dict) else None
    patient_id = None
    if patient_ref and patient_ref.startswith('Patient/'):
        patient_id = patient_ref.split('/',1)[1]
    if patient_id is None:
        return
    internal_id = get_or_create_internal_user(conn, patient_id)
    loinc_code = None; test_name = None
    coding = ((record.get('code') or {}).get('coding') or [])
    if coding:
        c0 = coding[0]
        loinc_code = c0.get('code'); test_name = c0.get('display') or (record.get('code') or {}).get('text')
    collected_at = record.get('effectiveDateTime') or record.get('issued')
    value_num = None; value_text = None; unit = None
    if 'valueQuantity' in record and record['valueQuantity']:
        vq = record['valueQuantity']; value_num = vq.get('value'); unit = vq.get('unit')
    elif 'valueString' in record:
        value_text = record.get('valueString')
    elif 'valueCodeableConcept' in record:
        value_text = (record['valueCodeableConcept'].get('text') or '')
    ref_low = None; ref_high = None; abnormal_flag = None
    rr = record.get('referenceRange') or []
    if rr:
        r0 = rr[0]; low = r0.get('low') or {}; high = r0.get('high') or {}
        ref_low = (low.get('value')) if low else None
        ref_high = (high.get('value')) if high else None
    interp = record.get('interpretation') or {}
    interp_coding = interp.get('coding') if isinstance(interp, dict) else None
    if interp_coding:
        abnormal_flag = interp_coding[0].get('code')
    insert_lab_result(conn, internal_id, obs_id, loinc_code, test_name, collected_at, value_num, value_text, unit, ref_low, ref_high, abnormal_flag, record)

TRANSFORM_DISPATCH = {
    'sleeps': transform_sleep,
    'workouts': transform_workout,
    'recoveries': transform_recovery,
    'profile': transform_profile,
    'quest_observation': transform_quest_observation,
}

def transform_record(resource: str, record: dict):
    func = TRANSFORM_DISPATCH.get(resource)
    if not func:
        return
    with get_conn() as conn:
        func(conn, record)
        conn.commit()
