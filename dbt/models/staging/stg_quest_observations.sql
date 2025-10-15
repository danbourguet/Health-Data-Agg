select
  o.id as raw_id,
  o.patient_id,
  o.code as code,
  o.code_display as test_name,
  o.effective_datetime as collected_at,
  o.value_num,
  o.value_text,
  o.unit,
  o.reference_low,
  o.reference_high,
  o.abnormal_flag,
  o.raw
from {{ source('quest_raw','observations') }} o