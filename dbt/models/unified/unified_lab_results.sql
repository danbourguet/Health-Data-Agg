{{ config(materialized='incremental', unique_key='lab_result_key') }}

select
  md5(concat(o.patient_id,'-',coalesce(o.collected_at::text,'na'),'-',coalesce(o.code,'na'))) as lab_result_key,
  o.patient_id as user_id,
  o.collected_at,
  o.code,
  o.test_name,
  coalesce(o.value_num::text, o.value_text) as value_text,
  o.unit,
  o.reference_low,
  o.reference_high,
  o.abnormal_flag
from {{ ref('stg_quest_observations') }} o

{% if is_incremental() %}
  where o.collected_at > (select coalesce(max(collected_at), '1970-01-01') from {{ this }})
{% endif %}