-- Grain: one row per review with key recommendationId.
-- MMG is a static dump. The reviews_as_of var simulates arrival.

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'reviewed_at', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by=['appid'],
    on_schema_change='fail'
) }}

select
    recommendationid,
    appid,
    reviewed_at,
    review_updated_at,
    voted_up,
    language,
    votes_up,
    votes_funny,
    weighted_vote_score,
    comment_count,
    received_for_free,
    written_during_early_access,
    author_steamid,
    author_playtime_at_review_min

from {{ ref('stg_mmg_reviews') }}
where reviewed_at < timestamp('{{ var("reviews_as_of", "9999-12-31") }}')

{% if is_incremental() %}
  -- re-read the whole latest month to not rewrite over entries
  and reviewed_at >= timestamp_trunc(_dbt_max_partition, month)
{% endif %}