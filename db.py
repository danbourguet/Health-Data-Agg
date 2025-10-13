import os
import psycopg2
from contextlib import contextmanager
from psycopg2.extras import Json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('.') / '.env', override=False)

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_USER = os.getenv('DB_USER', 'whoop')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'whoop_password')
DB_NAME = os.getenv('DB_NAME', 'whoop')

DSN = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"

@contextmanager
def get_conn():
    conn = psycopg2.connect(DSN)
    try:
        yield conn
    finally:
        conn.close()

def run_schema():
    schema_path = Path('schema.sql')
    sql = schema_path.read_text(encoding='utf-8')
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

# Upsert helpers

def upsert_user_basic_profile(data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''INSERT INTO user_basic_profile (user_id,email,first_name,last_name,raw,updated_at)
                   VALUES (%s,%s,%s,%s,%s,NOW())
                   ON CONFLICT (user_id) DO UPDATE SET email=EXCLUDED.email, first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, raw=EXCLUDED.raw, updated_at=NOW()''',
                (data['user_id'], data['email'], data.get('first_name'), data.get('last_name'), Json(data))
            )
        conn.commit()

def upsert_user_body_measurement(data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''INSERT INTO user_body_measurement (id,height_meter,weight_kilogram,max_heart_rate,raw,updated_at)
                   VALUES (TRUE,%s,%s,%s,%s,NOW())
                   ON CONFLICT (id) DO UPDATE SET height_meter=EXCLUDED.height_meter, weight_kilogram=EXCLUDED.weight_kilogram, max_heart_rate=EXCLUDED.max_heart_rate, raw=EXCLUDED.raw, updated_at=NOW()''',
                (data.get('height_meter'), data.get('weight_kilogram'), data.get('max_heart_rate'), Json(data))
            )
        conn.commit()

def upsert_cycle(data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            score = data.get('score') or {}
            cur.execute(
                '''INSERT INTO cycles (id,user_id,start,"end",score_state,score,cycle_strain,cycle_kilojoule,cycle_average_heart_rate,cycle_max_heart_rate,raw,created_at,updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                   ON CONFLICT (id) DO UPDATE SET user_id=EXCLUDED.user_id, start=EXCLUDED.start, "end"=EXCLUDED.end, score_state=EXCLUDED.score_state, score=EXCLUDED.score, cycle_strain=EXCLUDED.cycle_strain, cycle_kilojoule=EXCLUDED.cycle_kilojoule, cycle_average_heart_rate=EXCLUDED.cycle_average_heart_rate, cycle_max_heart_rate=EXCLUDED.cycle_max_heart_rate, raw=EXCLUDED.raw, updated_at=NOW()''',
                (data['id'], data['user_id'], data.get('start'), data.get('end'), data.get('score_state'), Json(score) if score else None,
                 score.get('strain'), score.get('kilojoule'), score.get('average_heart_rate'), score.get('max_heart_rate'), Json(data))
            )
        conn.commit()

def upsert_sleep(data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            score = data.get('score') or {}
            stage = score.get('stage_summary') or {}
            needed = score.get('sleep_needed') or {}
            cur.execute(
                '''INSERT INTO sleeps (id,cycle_id,user_id,start,"end",nap,score_state,score,
                   sleep_respiratory_rate,sleep_efficiency_percentage,sleep_consistency_percentage,sleep_performance_percentage,
                   sleep_needed_baseline_milli,sleep_needed_need_from_sleep_debt_milli,sleep_needed_need_from_recent_strain_milli,sleep_needed_need_from_recent_nap_milli,
                   sleep_stage_disturbance_count,sleep_stage_sleep_cycle_count,sleep_stage_total_awake_time_milli,sleep_stage_total_in_bed_time_milli,sleep_stage_total_no_data_time_milli,sleep_stage_total_rem_sleep_time_milli,sleep_stage_total_light_sleep_time_milli,sleep_stage_total_slow_wave_sleep_time_milli,
                   raw,created_at,updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,
                           %s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,%s,
                           %s,NOW(),NOW())
                   ON CONFLICT (id) DO UPDATE SET cycle_id=EXCLUDED.cycle_id, user_id=EXCLUDED.user_id, start=EXCLUDED.start, "end"=EXCLUDED.end, nap=EXCLUDED.nap, score_state=EXCLUDED.score_state, score=EXCLUDED.score,
                       sleep_respiratory_rate=EXCLUDED.sleep_respiratory_rate, sleep_efficiency_percentage=EXCLUDED.sleep_efficiency_percentage, sleep_consistency_percentage=EXCLUDED.sleep_consistency_percentage, sleep_performance_percentage=EXCLUDED.sleep_performance_percentage,
                       sleep_needed_baseline_milli=EXCLUDED.sleep_needed_baseline_milli, sleep_needed_need_from_sleep_debt_milli=EXCLUDED.sleep_needed_need_from_sleep_debt_milli, sleep_needed_need_from_recent_strain_milli=EXCLUDED.sleep_needed_need_from_recent_strain_milli, sleep_needed_need_from_recent_nap_milli=EXCLUDED.sleep_needed_need_from_recent_nap_milli,
                       sleep_stage_disturbance_count=EXCLUDED.sleep_stage_disturbance_count, sleep_stage_sleep_cycle_count=EXCLUDED.sleep_stage_sleep_cycle_count, sleep_stage_total_awake_time_milli=EXCLUDED.sleep_stage_total_awake_time_milli, sleep_stage_total_in_bed_time_milli=EXCLUDED.sleep_stage_total_in_bed_time_milli, sleep_stage_total_no_data_time_milli=EXCLUDED.sleep_stage_total_no_data_time_milli, sleep_stage_total_rem_sleep_time_milli=EXCLUDED.sleep_stage_total_rem_sleep_time_milli, sleep_stage_total_light_sleep_time_milli=EXCLUDED.sleep_stage_total_light_sleep_time_milli, sleep_stage_total_slow_wave_sleep_time_milli=EXCLUDED.sleep_stage_total_slow_wave_sleep_time_milli,
                       raw=EXCLUDED.raw, updated_at=NOW()''',
                (
                    data['id'], data.get('cycle_id'), data.get('user_id'), data.get('start'), data.get('end'), data.get('nap'), data.get('score_state'), Json(score) if score else None,
                    score.get('respiratory_rate'), score.get('sleep_efficiency_percentage'), score.get('sleep_consistency_percentage'), score.get('sleep_performance_percentage'),
                    needed.get('baseline_milli'), needed.get('need_from_sleep_debt_milli'), needed.get('need_from_recent_strain_milli'), needed.get('need_from_recent_nap_milli'),
                    stage.get('disturbance_count'), stage.get('sleep_cycle_count'), stage.get('total_awake_time_milli'), stage.get('total_in_bed_time_milli'), stage.get('total_no_data_time_milli'), stage.get('total_rem_sleep_time_milli'), stage.get('total_light_sleep_time_milli'), stage.get('total_slow_wave_sleep_time_milli'),
                    Json(data)
                )
            )
        conn.commit()

def upsert_recovery(data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            score = data.get('score') or {}
            cur.execute(
                '''INSERT INTO recoveries (cycle_id,sleep_id,user_id,score_state,score,
                   recovery_score_value,recovery_resting_heart_rate,recovery_hrv_rmssd_milli,recovery_spo2_percentage,recovery_skin_temp_celsius,recovery_user_calibrating,
                   raw,created_at,updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                   ON CONFLICT (cycle_id) DO UPDATE SET sleep_id=EXCLUDED.sleep_id, user_id=EXCLUDED.user_id, score_state=EXCLUDED.score_state, score=EXCLUDED.score,
                       recovery_score_value=EXCLUDED.recovery_score_value, recovery_resting_heart_rate=EXCLUDED.recovery_resting_heart_rate, recovery_hrv_rmssd_milli=EXCLUDED.recovery_hrv_rmssd_milli, recovery_spo2_percentage=EXCLUDED.recovery_spo2_percentage, recovery_skin_temp_celsius=EXCLUDED.recovery_skin_temp_celsius, recovery_user_calibrating=EXCLUDED.recovery_user_calibrating,
                       raw=EXCLUDED.raw, updated_at=NOW()''',
                (data['cycle_id'], data.get('sleep_id'), data.get('user_id'), data.get('score_state'), Json(score) if score else None,
                 score.get('recovery_score'), score.get('resting_heart_rate'), score.get('hrv_rmssd_milli'), score.get('spo2_percentage'), score.get('skin_temp_celsius'), score.get('user_calibrating'),
                 Json(data))
            )
        conn.commit()

def upsert_workout(data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            score = data.get('score') or {}
            zones = score.get('zone_durations') or {}
            cur.execute(
                '''INSERT INTO workouts (id,v1_id,user_id,sport_name,start,"end",score_state,score,
                   workout_strain,workout_kilojoule,workout_average_heart_rate,workout_max_heart_rate,workout_percent_recorded,workout_distance_meter,workout_altitude_gain_meter,workout_altitude_change_meter,
                   zone_zero_milli,zone_one_milli,zone_two_milli,zone_three_milli,zone_four_milli,zone_five_milli,
                   raw,created_at,updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,
                           %s,NOW(),NOW())
                   ON CONFLICT (id) DO UPDATE SET v1_id=EXCLUDED.v1_id, user_id=EXCLUDED.user_id, sport_name=EXCLUDED.sport_name, start=EXCLUDED.start, "end"=EXCLUDED.end, score_state=EXCLUDED.score_state, score=EXCLUDED.score,
                       workout_strain=EXCLUDED.workout_strain, workout_kilojoule=EXCLUDED.workout_kilojoule, workout_average_heart_rate=EXCLUDED.workout_average_heart_rate, workout_max_heart_rate=EXCLUDED.workout_max_heart_rate, workout_percent_recorded=EXCLUDED.workout_percent_recorded, workout_distance_meter=EXCLUDED.workout_distance_meter, workout_altitude_gain_meter=EXCLUDED.workout_altitude_gain_meter, workout_altitude_change_meter=EXCLUDED.workout_altitude_change_meter,
                       zone_zero_milli=EXCLUDED.zone_zero_milli, zone_one_milli=EXCLUDED.zone_one_milli, zone_two_milli=EXCLUDED.zone_two_milli, zone_three_milli=EXCLUDED.zone_three_milli, zone_four_milli=EXCLUDED.zone_four_milli, zone_five_milli=EXCLUDED.zone_five_milli,
                       raw=EXCLUDED.raw, updated_at=NOW()''',
                (data['id'], data.get('v1_id'), data.get('user_id'), data.get('sport_name'), data.get('start'), data.get('end'), data.get('score_state'), Json(score) if score else None,
                 score.get('strain'), score.get('kilojoule'), score.get('average_heart_rate'), score.get('max_heart_rate'), score.get('percent_recorded'), score.get('distance_meter'), score.get('altitude_gain_meter'), score.get('altitude_change_meter'),
                 zones.get('zone_zero_milli'), zones.get('zone_one_milli'), zones.get('zone_two_milli'), zones.get('zone_three_milli'), zones.get('zone_four_milli'), zones.get('zone_five_milli'),
                 Json(data))
            )
        conn.commit()

# Reset helpers

ACTIVITY_TABLES = [
    'workouts',
    'recoveries',
    'sleeps',
    'cycles'
]

USER_TABLES = [
    'user_body_measurement',
    'user_basic_profile'
]

def truncate_activity_tables():
    # Ensure schema exists before attempting truncate
    try:
        run_schema()
    except Exception:
        pass
    with get_conn() as conn:
        with conn.cursor() as cur:
            for tbl in ACTIVITY_TABLES:
                try:
                    cur.execute(f'TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE;')
                except Exception:
                    # Skip if table truly does not exist
                    conn.rollback()
                    continue
        conn.commit()

def truncate_all_tables():
    # Ensure schema exists first
    try:
        run_schema()
    except Exception:
        pass
    with get_conn() as conn:
        with conn.cursor() as cur:
            for tbl in ACTIVITY_TABLES + USER_TABLES:
                try:
                    cur.execute(f'TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE;')
                except Exception:
                    conn.rollback()
                    continue
        conn.commit()

def delete_activity_range(start_iso: str, end_iso: str):
    """Delete activity records whose start timestamp falls within [start_iso, end_iso)."""
    queries = {
        'cycles': 'DELETE FROM cycles WHERE start >= %s AND start < %s',
        'sleeps': 'DELETE FROM sleeps WHERE start >= %s AND start < %s',
        'recoveries': 'DELETE FROM recoveries WHERE cycle_id IN (SELECT id FROM cycles WHERE start >= %s AND start < %s)',
        'workouts': 'DELETE FROM workouts WHERE start >= %s AND start < %s'
    }
    with get_conn() as conn:
        with conn.cursor() as cur:
            # order: recoveries depends on cycles (via cycle_id), so delete recoveries first
            for table, sql in [('recoveries', queries['recoveries']), ('sleeps', queries['sleeps']), ('workouts', queries['workouts']), ('cycles', queries['cycles'])]:
                cur.execute(sql, (start_iso, end_iso))
        conn.commit()
