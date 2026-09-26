with raw as (
    select collected_at, source_file, response_json
    from {{ source('steam_raw', 'raw_charts') }}
),

ranked as (
    select
        raw.collected_at,
        raw.source_file,
        -- the charts count the previous day and not the days peak
        date(timestamp_seconds(
            cast(json_value(raw.response_json, '$.response.rollup_date') as int64)
        )) as rollup_date,
        r as rank_json
    from raw,
        unnest(json_query_array(raw.response_json, '$.response.ranks')) as r
)

select
    cast(json_value(rank_json, '$.appid') as int64)          as appid,
    rollup_date,
    collected_at,
    source_file,
    cast(json_value(rank_json, '$.rank') as int64)           as chart_rank,
    cast(json_value(rank_json, '$.last_week_rank') as int64) as last_week_rank,
    cast(json_value(rank_json, '$.peak_in_game') as int64)   as peak_players
from ranked