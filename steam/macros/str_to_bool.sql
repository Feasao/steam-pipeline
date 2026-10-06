{#
    STRING -> BOOL for the spellings MMG uses,
#}
{% macro to_bool(col) -%}
    case lower(trim({{ col }}))
        when 'true'  then true
        when '1'     then true
        when 'false' then false
        when '0'     then false
    end
{%- endmacro %}