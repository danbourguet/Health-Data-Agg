{% macro incremental_time_filter(column_name) %}
{# Applies a standard incremental filter using max(column) from this relation #}
{% if is_incremental() %}
where {{ column_name }} > (select coalesce(max({{ column_name }}), '1970-01-01'::timestamp) from {{ this }})
{% endif %}
{% endmacro %}