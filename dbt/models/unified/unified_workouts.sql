{{ config(materialized='incremental', unique_key='whoop_workout_id') }}

select
  w.raw_id as whoop_workout_id,
  w.user_id,
  w.start_time,
  w.end_time,
  w.sport,
  -- approximate kcal from energy_kj (1 kcal ≈ 4.184 kJ)
  (w.energy_kj / 4.184)::numeric as calories,
  w.average_hr as avg_heart_rate,
  w.max_hr as max_heart_rate,
  w.raw as score
from {{ ref('stg_whoop_workouts') }} w

{% if is_incremental() %}
  where w.start_time > (select coalesce(max(start_time), '1970-01-01'::timestamp) from {{ this }})
{% endif %}