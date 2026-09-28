-- one row per tracked app
with apps as (
    select appid, source, added_at
    from {{ ref('app_ids') }}
),

current_price as (
    select appid, app_name, app_type, price_status, current_price, collected_at
    from {{ ref('stg_prices_current') }}
)

select
    a.appid,
    a.source,
    a.added_at,
    c.app_name,
    c.app_type,
    c.price_status,
    c.current_price,
    c.collected_at   as last_ok_at,
    c.appid is null  as never_collected
from apps a
left join current_price c
    on a.appid = c.appid