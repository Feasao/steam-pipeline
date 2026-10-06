-- If top 100 player peaks is 100 a day

select rollup_date, count(*) as `rows`
from {{ ref('fct_player_peaks') }}
group by rollup_date
having count(*) != 100