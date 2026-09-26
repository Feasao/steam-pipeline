-- one row per app per day
-- A rollup day should be published once
--the first file that reported the day wins
--steam on the 16th published the 14 again with wrong values and that is what necessitated this guard and the rollup check

select * except(rn)
from (
    select
        appid, rollup_date, chart_rank, last_week_rank, peak_players, collected_at,
        row_number() over (
            partition by appid, rollup_date
            order by collected_at asc, source_file asc
        ) as rn
    from {{ ref('stg_charts') }}
)
where rn = 1