-- one row per review  with key recommendationid. 1,048,148 rows.


with source as (
    select * from {{ source('mmg_raw', 'reviews') }}
)

select
    recommendationid,
    appid,
    timestamp_created                                               as reviewed_at,
    timestamp_seconds(safe_cast(timestamp_updated as int64))        as review_updated_at,

    {{ to_bool('voted_up') }}                                       as voted_up,
    language,
    review_text,

    safe_cast(votes_up as int64)                                    as votes_up,
    nullif(safe_cast(votes_funny as int64), 4294967295)             as votes_funny,
    safe_cast(weighted_vote_score as float64)                       as weighted_vote_score,
    safe_cast(comment_count as int64)                               as comment_count,

    {{ to_bool('received_for_free') }}                              as received_for_free,
    {{ to_bool('written_during_early_access') }}                    as written_during_early_access,

    author_steamid,
    safe_cast(author_num_games_owned as int64)                      as author_num_games_owned,
    safe_cast(author_num_reviews as int64)                          as author_num_reviews,
    safe_cast(author_playtime_forever as int64)                     as author_playtime_forever_min,
    safe_cast(author_playtime_last_two_weeks as int64)              as author_playtime_last_two_weeks_min,
    safe_cast(author_playtime_at_review as int64)                   as author_playtime_at_review_min,
    timestamp_seconds(nullif(safe_cast(author_last_played as int64), 0)) as author_last_played_at

from source