select
  s.id as raw_id,
  s.user_id,
  s.start as start_time,
  s."end" as end_time,
  s.sleep_efficiency_percentage as efficiency_pct,
  s.sleep_respiratory_rate as respiratory_rate,
  (s.sleep_stage_total_rem_sleep_time_milli/60000)::int as rem_minutes,
  (s.sleep_stage_total_slow_wave_sleep_time_milli/60000)::int as deep_minutes,
  (s.sleep_stage_total_light_sleep_time_milli/60000)::int as light_minutes,
  (s.sleep_stage_total_awake_time_milli/60000)::int as awake_minutes,
  s.raw
from {{ source('whoop_raw','sleeps') }} s