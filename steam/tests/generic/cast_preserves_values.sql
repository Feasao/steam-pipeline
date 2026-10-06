-- Fails if a staging cast turned non-NULL source values into NULLs.

{% test cast_preserves_values(model, column_name, source_relation, source_column) %}

with raw as (
    select countif({{ source_column }} is not null and trim({{ source_column }}) != '') as n
    from {{ source_relation }}
),

staged as (
    select countif({{ column_name }} is not null) as n
    from {{ model }}
)

select
    raw.n    as raw_non_null,
    staged.n as staged_non_null
from raw, staged
where raw.n != staged.n

{% endtest %}