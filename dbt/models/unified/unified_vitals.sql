{{ config(materialized='incremental', unique_key='vital_key') }}

with base as (
  select
    r.user_id,
    -- pick a timestamp from raw json if present; fall back to current_timestamp
    coalesce(
      nullif(r.raw->>'created_at','')::timestamptz,
      nullif(r.raw->>'updated_at','')::timestamptz,
      current_timestamp
    ) as measure_time,
    r.recovery_score as recovery_score,
    r.resting_hr as resting_hr,
    r.hrv_rmssd as hrv_rmssd,
    r.spo2_pct as spo2_pct,
    r.skin_temp_celsius as skin_temp_celsius
  from {{ ref('stg_whoop_recoveries') }} r
), unnested as (
  select user_id, measure_time, 'resting_hr'::text as metric, resting_hr::numeric as value, 'bpm'::text as unit, 'whoop'::text as source from base where resting_hr is not null
  union all
  select user_id, measure_time, 'hrv_rmssd'::text, hrv_rmssd::numeric, 'ms'::text, 'whoop'::text from base where hrv_rmssd is not null
  union all
  select user_id, measure_time, 'spo2_pct'::text, spo2_pct::numeric, 'percent'::text, 'whoop'::text from base where spo2_pct is not null
  union all
  select user_id, measure_time, 'skin_temp_celsius'::text, skin_temp_celsius::numeric, 'C'::text, 'whoop'::text from base where skin_temp_celsius is not null
  union all
  select user_id, measure_time, 'recovery_score'::text, recovery_score::numeric, 'score'::text, 'whoop'::text from base where recovery_score is not null
)
select
  md5(concat(user_id::text,'-',metric,'-',coalesce(measure_time::text,'NULL'))) as vital_key,
  user_id,
  measure_time,
  metric,
  value,
  unit,
  source
from unnested

{% if is_incremental() %}
where measure_time > (select coalesce(max(measure_time), '1970-01-01'::timestamptz) from {{ this }})
{% endif %}