with raw as (
    select
        appid,
        collected_at,
        source_file,
        response_json
    from {{ source('steam_raw', 'raw_prices') }}

),

parsed as (

    select
        appid,
        collected_at,
        source_file,

        json_value(response_json, '$.success') = 'true' as fetch_ok,
        json_value(response_json, '$.data.name') as app_name,
        json_value(response_json, '$.data.type') as app_type,
        json_value(response_json, '$.data.is_free') = 'true' as is_free,
        json_value(response_json, '$.data.release_date.coming_soon') = 'true' as coming_soon,
        json_value(response_json, '$.data.release_date.date') as release_date_raw,

        cast(json_value(response_json, '$.data.price_overview.initial') as int64) as initial_cents,
        cast(json_value(response_json, '$.data.price_overview.final') as int64) as final_cents,
        cast(json_value(response_json, '$.data.price_overview.discount_percent') as int64) as discount_percent,
        json_value(response_json, '$.data.price_overview.currency') as currency

    from raw

)

select
    appid,
    collected_at,
    source_file,
    app_name,
    app_type,

    case
        when not fetch_ok then 'fetch_failed'
        when coming_soon then 'unreleased'
        when is_free then 'free'
        when final_cents is not null then 'priced'
        else 'unpriced'
    end as price_status,

    initial_cents / 100.0 as list_price,
    final_cents / 100.0 as current_price,
    coalesce(discount_percent, 0) as discount_percent,
    currency,
    release_date_raw

from parsed