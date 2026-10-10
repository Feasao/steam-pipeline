with daily as (
    select appid, date(collected_at) as day, discount_percent,
           row_number() over (partition by appid, date(collected_at)
                              order by collected_at desc) as rn
    from {{ ref('stg_prices') }}
    where price_status = 'priced'
)
select day,
       count(*)                                                     as priced_apps,
       countif(discount_percent > 0)                                as discounted,
       round(100 * countif(discount_percent > 0) / count(*), 1)     as pct_discounted,
       round(avg(if(discount_percent > 0, discount_percent, null)), 1) as avg_discount
from daily
where rn = 1 and day >= '2026-09-20'
group by day
order by day;