-- Grain: one row per app with key appid. 239,664 rows.

with source as (
    select * from {{ source('mmg_raw', 'applications') }}
),

parsed as (
    select
        *,
        coalesce(
            safe.parse_date('%b %d, %Y', release_date),
            safe.parse_date('%d %b, %Y', release_date),
            safe.parse_date('%Y-%m-%d',  release_date)
        ) as release_date_parsed
    from source
)

select
    appid,
    name                                                        as app_name,
    type                                                        as app_type,
    {{ to_bool('is_free') }}                                    as is_free,

    release_date                                                as release_date_raw,
    case
        when extract(year from release_date_parsed) between 1997 and 2027
        then release_date_parsed
    end                                                         as release_date,

    safe_cast(regexp_extract(required_age, r'^\d+') as int64)   as required_age,
    {{ to_int('metacritic_score') }}                            as metacritic_score,
    {{ to_int('recommendations_total') }}                       as recommendations_total,

    {{ to_int('mat_initial_price') }} / 100.0                   as list_price,
    {{ to_int('mat_final_price') }} / 100.0                     as current_price,
    {{ to_int('mat_discount_percent') }}                        as discount_percent,
    mat_currency                                                as currency,

    {{ to_bool('mat_supports_windows') }}                       as supports_windows,
    {{ to_bool('mat_supports_mac') }}                           as supports_mac,
    {{ to_bool('mat_supports_linux') }}                         as supports_linux,
    {{ to_int('mat_achievement_count') }}                       as achievement_count,

    short_description,
    supported_languages

from parsed