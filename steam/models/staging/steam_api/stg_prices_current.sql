select * except(rn)
from (
    select
        *,
        row_number() over (
            partition by appid
            order by collected_at desc, source_file desc
        ) as rn
    from {{ ref('stg_prices') }}
    where price_status != 'fetch_failed'
)
where rn = 1