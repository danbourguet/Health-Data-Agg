select
  w.id as raw_id,
  w.user_id,
  w.start as start_time,
  w."end" as end_time,
  w.sport_name as sport,
  (w.score->>'average_heart_rate')::int as average_hr,
  (w.score->>'max_heart_rate')::int as max_hr,
  (w.score->>'strain')::numeric as strain,
  (w.score->>'kilojoule')::numeric as energy_kj,
  (w.score->>'distance_meter')::numeric as distance_m,
  (w.score->>'altitude_gain_meter')::numeric as altitude_gain_m,
  (w.score->>'altitude_change_meter')::numeric as altitude_change_m,
  w.raw
from {{ source('whoop_raw','workouts') }} w