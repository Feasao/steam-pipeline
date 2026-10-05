with daily as (
    select appid, date(collected_at) as day, discount_percent,
           row_number() over (partition by appid, date(collected_at)
                              order by collected_at desc) as rn
    from {{ ref('stg_prices') }}
    where price_status = 'priced'
)
select
    a.source,
    d.day,
    count(*)                                                 as priced_apps,
    countif(d.discount_percent > 0)                          as discounted,
    round(100 * countif(d.discount_percent > 0) / count(*), 1) as pct_discounted,
    round(avg(if(d.discount_percent > 0, d.discount_percent, null)), 1) as avg_discount
from daily d
join {{ ref('app_ids') }} a using (appid)
where d.rn = 1
  and d.day in ('2026-09-28', '2026-10-02')
group by a.source, d.day
order by a.source, d.day;