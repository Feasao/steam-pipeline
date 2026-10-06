-- A rollup day should be published once
--the first file that reported the day wins
--steam on the 16th published the 14 again with wrong values and that is what necessitated this guard and the rollup check

with files as (
    select distinct rollup_date, source_file, collected_at
    from {{ ref('stg_charts') }}
),

first_file as (
    select rollup_date, source_file
    from (
        select
            rollup_date,
            source_file,
            row_number() over (
                partition by rollup_date
                order by collected_at, source_file
            ) as rn
        from files
    )
    where rn = 1
)

select
    c.appid,
    c.rollup_date,
    c.chart_rank,
    c.last_week_rank,
    c.peak_players,
    c.collected_at
from {{ ref('stg_charts') }} c
join first_file f
    using (rollup_date, source_file)