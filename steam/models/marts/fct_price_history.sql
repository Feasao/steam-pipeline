with changes as (
    select
        appid,
        collected_at,
        price_status,
        list_price,
        current_price,
        discount_percent,
        lag(current_price)    over w as prev_price,
        lag(discount_percent) over w as prev_discount,
        lag(price_status)     over w as prev_status
    from {{ ref('stg_prices') }}
    window w as (partition by appid order by collected_at)
),

versions as (
    select *
    from changes
    where prev_price is distinct from current_price
       or prev_discount is distinct from discount_percent
       or prev_status is distinct from price_status
)

select
    appid,
    price_status,
    list_price,
    current_price,
    discount_percent,
    collected_at as valid_from,
    lead(collected_at) over (partition by appid order by collected_at) as valid_to
from versions