# Steam Price Observatory

A daily ELT pipeline that builds price and player count history for about 430 Steam
titles from an API that only returns present prices.

Steam's Web API tells you what a game costs currently. It has no endpoint for
price history, and the public bulk datasets are snapshots. This
project collects daily observations and models them as slowly-changing dimensions,
producing a dataset that cannot be obtained any other way.

**Stack:** Python · BigQuery · dbt Core · S3 · Airflow · GitHub Actions

## Architecture

<!-- todo: diagram once I add S3 -->

| Layer             | What it does                                        |
| ----------------- | --------------------------------------------------- |
| `collection.py`   | Polls Steam for the working set, writes raw JSONL   |
| `loadbq.py`       | Loads JSONL into BigQuery, full-reload (idempotent) |
| `models/staging/` | Parses JSON, casts types, derives `price_status`    |
| `snapshots/`      | SCD Type 2 history of price changes                 |
| `models/marts/`   | Analytics-ready tables                              |

## Data sources

| Source                     | Role                  | Notes                            |
| -------------------------- | --------------------- | -------------------------------- |
| Steam `appdetails`         | Daily price snapshots | Public API                       |
| Steam `GetMostPlayedGames` | Daily concurrents     | Top 100 only                     |
| Steam Dataset 2025         | Historical backfill   | 239,664 apps - 1,048,148 reviews |


<!-- ### Working set selection -->

<!-- todo -->

## Data quality findings

Notes from exploring the sources before modelling. Several of these changed the
design.

### The historical dataset covers 56% of Steam's catalogue

Steam's app list contains far more than games, like DLC, demos, soundtracks, videos
and playtests that all have IDs. The upstream collection called `appdetails` on every
one of them, and about 44% returned nothing, mostly because the content was
delisted or region-restricted.

Delisted titles tend to be older ones, which means release 
counts by year undercount the past and any pricing trend only reflects games that survived,
so there is caution to be had against historical count analysis.

### Steam prices are not currency-convertible

The historical data contains 32 currencies. Valve uses **regional pricing tiers**
rather than exchange-rate conversion. That means that a title at $19.99 in the US may be ₹999 (~$10.5) in
India, cross-currency aggregation is therefore invalid.
This pipeline filters to `USD` (because of a 98.6% coverage on the historical dataset) and collects
with `cc=us` pinned, so both sources share one currency.

### Review volume spikes 9x in 2025

Reviews average ~5,750/month in 2024 and ~54,000/month in 2025. The upstream collector used the review endpoint's default
most-recent-first ordering with a per-app cap, so recent reviews are heavily
over-sampled. **Consequence:** no marts that calculate review volume over time have merit from
this source. The `steam_purchase` flag is also constant `true` across all
1,048,148 rows, all reviews come from verified purchases only.

### The value `price_status`

A null price does not mean one thing. Observed causes:

| Status         | Cause                                                            |
| -------------- | ---------------------------------------------------------------- |
| `priced`       | Normal paid title                                                |
| `free`         | `is_free: true`, no price block in the responce                  |
| `unreleased`   | `coming_soon: true` — not yet purchasable                        |
| `fetch_failed` | API returned `success: false`                                    |
| `unpriced`     | Free-to-play without the flag, delisted, non-game listings       |


Failed fetches are **stored as rows** for transparency.

The status `unpriced` is a mixed bucket:  
Rocket League has moved to Epic and delisted, Shogun 2's appid has been superseded by another, Call of Duty has a button redirect, FiveM is an advertisement.

### Other source defects of the historical source

- `required_age` contains `17+` and `javascript:ToggleCheckbox(...)` alongside integers
- `votes_funny` maxes at 4,294,967,295 (2<sup>32</sup>−1), an overflow
- `release_date` includes `1969-12-31` (epoch-zero parse failures), `9998` and `6969`
- The Hugging Face comparison set encodes missing values as `""` rather than `NULL`,
  so null-rate tests are meaningless against it
- `tags` and `movies` are empty arrays throughout that set

### Retired endpoints

A big limitation is that useful public endpoints that can be used to query the steam api are
not maintained or retired, for example `ISteamApps/GetAppList/v2`
that directs users to `IStoreService/GetAppList` that requires a steam API key, but
has a useful `last_modified` for
change-data-capture.


## Design decisions

### Full reload, not incremental

The loader `loadbq.py` truncates and rewrites on every run. This is made to be **idempotent** so that any failed
run is recovered by running it again, with no possibility of duplicates. 
At ~13,000 rows/month it does not pose a significant load on the implementation. The
crossover to incremental loading is deliberately deferred until volume justifies it.

### Raw JSON stored whole

In `collection.py` the complete API response is stored as a string
and parsed in dbt. Extraction mistakes are unrecoverable for daily snapshots and
a field not collected is not reconstructed.

### `response_json` as STRING, not BigQuery's JSON type

The `JSON` type rejects malformed values at load time, which would contradict
storing failures as data. `JSON_VALUE()` queries strings equally well.

<!-- todo : partitioning + clustering rationale-->

---

## Known limitations

- **Collection cadence is irregular.**   
The collection happens by a local scheduled task that is subject to errors. It will be updated once Workload Identity Federation with Github Actions is set up.
- **Snapshot timestamps are run-time, not observation-time.**   
dbt snapshots record
  `dbt_valid_from` as when the snapshot ran, not when the price was collected.
- **`source` in `config/app_ids.csv` records first match**.   
Apps in the top-paid list for example are tagged `charts_top100`, by first appear. That means the column cannot be used to answer *"How do top-paid titles behave?"* since some are tagged elsewhere, will change if needed.
- **Working set is frozen at selection time.**   
Games discounted after selection do not enter the sample, the tracked games are fixed when the appids to track are chosen.


<!-- ## Setup


## Running
 -->


## Attribution

The historical data from [Steam Dataset 2025](https://doi.org/10.5281/zenodo.17266922) by Donald Fountain.   
Steam data retrieved via the public [Steam Web API](https://steamcommunity.com/dev).
