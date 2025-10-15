select
  r.cycle_id as raw_id,
  r.user_id,
  (r.score->>'recovery_score')::numeric as recovery_score,
  (r.score->>'resting_heart_rate')::numeric as resting_hr,
  (r.score->>'hrv_rmssd_milli')::numeric as hrv_rmssd,
  (r.score->>'spo2_percentage')::numeric as spo2_pct,
  (r.score->>'skin_temp_celsius')::numeric as skin_temp_celsius,
  r.raw
from {{ source('whoop_raw','recoveries') }} r