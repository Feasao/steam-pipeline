
-- STRING -> INT64 for whole numbers that may be written as floats ('999.0').

{% macro to_int(col) -%}
    safe_cast(safe_cast({{ col }} as numeric) as int64)
{%- endmacro %}