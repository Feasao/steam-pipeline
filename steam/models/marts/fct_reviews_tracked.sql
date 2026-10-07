-- Incremental fct for reviews for the tracked appids, because clustering saves nothing
-- since the appids are a wide range

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'reviewed_at', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by = ['appid'],
    on_schema_change = 'fail'
) }}

select
    r.*,
    s.source as app_source
from {{ ref('fct_reviews') }} as r
inner join {{ ref('app_ids') }} as s
    on r.appid = s.appid

{% if is_incremental() %}
where r.reviewed_at >= timestamp_trunc(_dbt_max_partition, month)
{% endif %}