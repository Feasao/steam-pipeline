-- Returns rollup days whose ranking differs between files.
{{ config(severity = 'warn') }}

with per_file as (
    select
        rollup_date,
        source_file,
        string_agg(format('%d:%d', appid, peak_players), ',' order by chart_rank) as fingerprint
    from {{ ref('stg_charts') }}
    group by rollup_date, source_file
)

select rollup_date,
       count(*)                    as files,
       count(distinct fingerprint) as versions
from per_file
group by rollup_date
having count(distinct fingerprint) > 1