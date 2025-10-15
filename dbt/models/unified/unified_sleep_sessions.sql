{{ config(materialized='incremental', unique_key='whoop_sleep_id') }}

-- Derive unified sleep sessions from staging WHOOP sleeps (align with staging columns)
select
  s.raw_id as whoop_sleep_id,
  s.user_id,
  s.start_time,
  s.end_time,
  -- derive duration in minutes if both timestamps present
  case
    when s.end_time is not null and s.start_time is not null then
      extract(epoch from (s.end_time::timestamptz - s.start_time::timestamptz))::int / 60
    else null
  end as duration_minutes,
  s.efficiency_pct,
  s.respiratory_rate,
  s.rem_minutes,
  s.deep_minutes,
  s.light_minutes,
  s.awake_minutes,
  s.raw
from {{ ref('stg_whoop_sleeps') }} s

{% if is_incremental() %}
where s.start_time > (select coalesce(max(start_time), '1970-01-01'::timestamp) from {{ this }})
{% endif %}