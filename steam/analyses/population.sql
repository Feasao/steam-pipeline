select
    coalesce(source, 'ALL')                     as source,
    count(*)                                    as apps,
    countif(never_collected)                    as never_collected,
    countif(price_status = 'priced')            as priced,
    countif(price_status = 'free')              as free,
    countif(price_status = 'unreleased')        as unreleased,
    countif(price_status = 'unpriced')          as unpriced
from {{ ref('dim_apps') }}
group by rollup (source)
order by apps desc