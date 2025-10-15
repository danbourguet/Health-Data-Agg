-- Schema bootstrap (idempotent)
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS whoop_raw;
CREATE SCHEMA IF NOT EXISTS unified;
CREATE SCHEMA IF NOT EXISTS quest_raw;
CREATE SCHEMA IF NOT EXISTS staging; -- used by dbt staging models

-- Meta / tokens
CREATE TABLE IF NOT EXISTS meta.oauth_tokens (
    id SERIAL PRIMARY KEY,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    scope TEXT NOT NULL,
    token_type TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_oauth_tokens_expires_at ON meta.oauth_tokens(expires_at);

-- Basic User Profile
CREATE TABLE IF NOT EXISTS whoop_raw.user_basic_profile (
    user_id BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    raw JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Body measurement (keep only latest)
CREATE TABLE IF NOT EXISTS whoop_raw.user_body_measurement (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id), -- enforce single row
    height_meter DOUBLE PRECISION,
    weight_kilogram DOUBLE PRECISION,
    max_heart_rate INT,
    raw JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Cycles
CREATE TABLE IF NOT EXISTS whoop_raw.cycles (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    start TIMESTAMPTZ,
    "end" TIMESTAMPTZ,
    score_state TEXT,
    score JSONB,
    -- Denormalized cycle score metrics
    cycle_strain DOUBLE PRECISION,
    cycle_kilojoule DOUBLE PRECISION,
    cycle_average_heart_rate INT,
    cycle_max_heart_rate INT,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_whoop_raw_cycles_start ON whoop_raw.cycles(start);

-- Sleeps
CREATE TABLE IF NOT EXISTS whoop_raw.sleeps (
    id UUID PRIMARY KEY,
    cycle_id BIGINT,
    user_id BIGINT,
    start TIMESTAMPTZ,
    "end" TIMESTAMPTZ,
    nap BOOLEAN,
    score_state TEXT,
    score JSONB,
    -- Sleep score (top-level)
    sleep_respiratory_rate DOUBLE PRECISION,
    sleep_efficiency_percentage DOUBLE PRECISION,
    sleep_consistency_percentage DOUBLE PRECISION,
    sleep_performance_percentage DOUBLE PRECISION,
    -- Sleep needed
    sleep_needed_baseline_milli BIGINT,
    sleep_needed_need_from_sleep_debt_milli BIGINT,
    sleep_needed_need_from_recent_strain_milli BIGINT,
    sleep_needed_need_from_recent_nap_milli BIGINT,
    -- Stage summary
    sleep_stage_disturbance_count INT,
    sleep_stage_sleep_cycle_count INT,
    sleep_stage_total_awake_time_milli BIGINT,
    sleep_stage_total_in_bed_time_milli BIGINT,
    sleep_stage_total_no_data_time_milli BIGINT,
    sleep_stage_total_rem_sleep_time_milli BIGINT,
    sleep_stage_total_light_sleep_time_milli BIGINT,
    sleep_stage_total_slow_wave_sleep_time_milli BIGINT,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_whoop_raw_sleeps_start ON whoop_raw.sleeps(start);

-- Recoveries
CREATE TABLE IF NOT EXISTS whoop_raw.recoveries (
    cycle_id BIGINT PRIMARY KEY,
    sleep_id UUID,
    user_id BIGINT,
    score_state TEXT,
    score JSONB,
    recovery_score_value DOUBLE PRECISION,
    recovery_resting_heart_rate DOUBLE PRECISION,
    recovery_hrv_rmssd_milli DOUBLE PRECISION,
    recovery_spo2_percentage DOUBLE PRECISION,
    recovery_skin_temp_celsius DOUBLE PRECISION,
    recovery_user_calibrating BOOLEAN,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Workouts
CREATE TABLE IF NOT EXISTS whoop_raw.workouts (
    id UUID PRIMARY KEY,
    v1_id BIGINT,
    user_id BIGINT,
    sport_name TEXT,
    start TIMESTAMPTZ,
    "end" TIMESTAMPTZ,
    score_state TEXT,
    score JSONB,
    workout_strain DOUBLE PRECISION,
    workout_kilojoule DOUBLE PRECISION,
    workout_average_heart_rate INT,
    workout_max_heart_rate INT,
    workout_percent_recorded DOUBLE PRECISION,
    workout_distance_meter DOUBLE PRECISION,
    workout_altitude_gain_meter DOUBLE PRECISION,
    workout_altitude_change_meter DOUBLE PRECISION,
    -- Zone durations (denormalized)
    zone_zero_milli BIGINT,
    zone_one_milli BIGINT,
    zone_two_milli BIGINT,
    zone_three_milli BIGINT,
    zone_four_milli BIGINT,
    zone_five_milli BIGINT,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_whoop_raw_workouts_start ON whoop_raw.workouts(start);

-- Unified tables (minimal set currently in use)
CREATE TABLE IF NOT EXISTS unified.user_identity (
    internal_user_id SERIAL PRIMARY KEY,
    source_system TEXT NOT NULL,
    source_user_id TEXT NOT NULL,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_system, source_user_id)
);

CREATE TABLE IF NOT EXISTS unified.sleep_sessions (
    id BIGSERIAL PRIMARY KEY,
    internal_user_id INT REFERENCES unified.user_identity(internal_user_id),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    duration_minutes INT,
    efficiency_pct DOUBLE PRECISION,
    rem_minutes INT,
    deep_minutes INT,
    light_minutes INT,
    awake_minutes INT,
    respiratory_rate DOUBLE PRECISION,
    source_system TEXT NOT NULL,
    raw_source_id TEXT,
    raw JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sleep_sessions_user_time ON unified.sleep_sessions(internal_user_id, start_time);
CREATE UNIQUE INDEX IF NOT EXISTS uix_sleep_sessions_source ON unified.sleep_sessions(source_system, raw_source_id);

CREATE TABLE IF NOT EXISTS unified.workouts (
    id BIGSERIAL PRIMARY KEY,
    internal_user_id INT REFERENCES unified.user_identity(internal_user_id),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    sport TEXT,
    average_hr INT,
    max_hr INT,
    strain DOUBLE PRECISION,
    energy_kj DOUBLE PRECISION,
    distance_m DOUBLE PRECISION,
    altitude_gain_m DOUBLE PRECISION,
    altitude_change_m DOUBLE PRECISION,
    source_system TEXT NOT NULL,
    raw_source_id TEXT,
    raw JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_workouts_user_time ON unified.workouts(internal_user_id, start_time);
CREATE UNIQUE INDEX IF NOT EXISTS uix_workouts_source ON unified.workouts(source_system, raw_source_id);

CREATE TABLE IF NOT EXISTS unified.biometrics_vitals (
    id BIGSERIAL PRIMARY KEY,
    internal_user_id INT REFERENCES unified.user_identity(internal_user_id),
    recorded_at TIMESTAMPTZ NOT NULL,
    type TEXT NOT NULL,
    value_num DOUBLE PRECISION,
    unit TEXT,
    value_text TEXT,
    source_system TEXT NOT NULL,
    raw_source_id TEXT,
    quality_flags TEXT[],
    raw JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_biometrics_vitals_user_time ON unified.biometrics_vitals (internal_user_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_biometrics_vitals_type ON unified.biometrics_vitals (type);

-- Quest raw tables (FHIR-based ingestion)
CREATE TABLE IF NOT EXISTS quest_raw.patient (
    id TEXT PRIMARY KEY,
    raw JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quest_raw.observations (
    id TEXT PRIMARY KEY,
    patient_id TEXT,
    code TEXT,
    code_system TEXT,
    code_display TEXT,
    effective_datetime TIMESTAMPTZ,
    value_num DOUBLE PRECISION,
    value_text TEXT,
    unit TEXT,
    reference_low DOUBLE PRECISION,
    reference_high DOUBLE PRECISION,
    abnormal_flag TEXT,
    raw JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_quest_observations_patient_time ON quest_raw.observations (patient_id, effective_datetime);
CREATE INDEX IF NOT EXISTS idx_quest_observations_code ON quest_raw.observations (code);

-- Unified lab results (multi-source, currently Quest)
CREATE TABLE IF NOT EXISTS unified.lab_results (
    id BIGSERIAL PRIMARY KEY,
    internal_user_id INT REFERENCES unified.user_identity(internal_user_id),
    loinc_code TEXT,
    test_name TEXT,
    collected_at TIMESTAMPTZ,
    value_num DOUBLE PRECISION,
    value_text TEXT,
    unit TEXT,
    reference_low DOUBLE PRECISION,
    reference_high DOUBLE PRECISION,
    abnormal_flag TEXT,
    source_system TEXT NOT NULL,
    raw_source_id TEXT,
    raw JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_lab_results_user_time ON unified.lab_results (internal_user_id, collected_at);
CREATE INDEX IF NOT EXISTS idx_lab_results_code ON unified.lab_results (loinc_code);
CREATE UNIQUE INDEX IF NOT EXISTS uix_lab_results_source ON unified.lab_results (source_system, raw_source_id);
