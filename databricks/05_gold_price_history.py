# Databricks notebook source
# MAGIC %md
# MAGIC ##Gold Price History
# MAGIC
# MAGIC Recreating the fact price history with one row per price version of an app  
# MAGIC Outputs: steam.gold.fct_price_history, rebuilt from steam.silver.prices on each run   
# MAGIC
# MAGIC A new version starts when current_price, discount_percent or price_status changes.  
# MAGIC Fetch_failed days are dropped since they are not new versions but fails

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE steam.gold.fct_price_history AS
# MAGIC WITH obs AS (
# MAGIC   SELECT appid, price_status, list_price, current_price, discount_percent, collected_at,
# MAGIC          LAG(current_price)    OVER (PARTITION BY appid ORDER BY collected_at) AS prev_price,
# MAGIC          LAG(discount_percent) OVER (PARTITION BY appid ORDER BY collected_at) AS prev_disc,
# MAGIC          LAG(price_status)     OVER (PARTITION BY appid ORDER BY collected_at) AS prev_status
# MAGIC   FROM steam.silver.prices
# MAGIC   WHERE price_status != 'fetch_failed'
# MAGIC ),
# MAGIC flagged AS (
# MAGIC   SELECT *,
# MAGIC          prev_status IS NULL
# MAGIC          OR prev_price  IS DISTINCT FROM current_price
# MAGIC          OR prev_disc   IS DISTINCT FROM discount_percent
# MAGIC          OR prev_status IS DISTINCT FROM price_status AS is_new_version
# MAGIC   FROM obs
# MAGIC )
# MAGIC SELECT appid, price_status, list_price, current_price, discount_percent,
# MAGIC        collected_at AS valid_from,
# MAGIC        LEAD(collected_at) OVER (PARTITION BY appid ORDER BY collected_at) AS valid_to
# MAGIC FROM flagged
# MAGIC WHERE is_new_version;

# COMMAND ----------

gold, silver = "steam.gold.fct_price_history", "steam.silver.prices"

successful_rows = spark.sql(f"SELECT count(*) FROM {silver} WHERE price_status != 'fetch_failed'").first()[0]
apps            = spark.sql(f"SELECT count(DISTINCT appid) FROM {silver} WHERE price_status != 'fetch_failed'").first()[0]

gold_rows        = spark.table(gold).count()
current_versions = spark.sql(f"SELECT count(*) FROM {gold} WHERE valid_to IS NULL").first()[0]

dupes = spark.sql(f"""
    SELECT appid, valid_from FROM {gold}
    GROUP BY appid, valid_from
    HAVING count(*) > 1
""").count()

bad_ranges = spark.sql(f"SELECT count(*) FROM {gold} WHERE valid_from >= valid_to").first()[0]

bad_free = spark.sql(f"""
    SELECT appid FROM {gold}
    WHERE appid IN (
        SELECT appid FROM {silver}
        WHERE price_status != 'fetch_failed'
        GROUP BY appid
        HAVING count_if(price_status != 'free' OR current_price IS NOT NULL) = 0
    )
    GROUP BY appid
    HAVING count(*) > 1
""").count()

failures = []
if dupes != 0:
    failures.append(f"{dupes} duplicate (appid, valid_from) pairs")
if bad_ranges != 0:
    failures.append(f"{bad_ranges} versions with valid_to <= valid_from")
if bad_free != 0:
    failures.append(f"{bad_free} always-free apps with more than one version")
if current_versions != apps:
    failures.append(f"{current_versions} current versions, expected {apps} (one per app)")
if not apps <= gold_rows <= successful_rows:
    failures.append(f"{gold_rows} gold rows, expected between {apps} and {successful_rows}")

if failures:
    raise ValueError("fct_price_history checks failed:\n- " + "\n- ".join(failures))

print(f"OK: {gold_rows:,} versions for {apps:,} apps from {successful_rows:,} successful observations")
print(f"No duplicate keys, no bad ranges, one current version per app, always-free apps have one version")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY steam.gold.fct_price_history;

# COMMAND ----------

# Differences from last and second to last versions
prev = spark.sql("DESCRIBE HISTORY steam.gold.fct_price_history LIMIT 2").collect()[-1]["version"]
changed = spark.sql(f"""
    SELECT * FROM steam.gold.fct_price_history
    EXCEPT
    SELECT * FROM steam.gold.fct_price_history VERSION AS OF {prev}
""").count()
print(f"{changed} rows changed since version {prev} ({changed // 2} apps, if every change closed one version and opened one)")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Reconcilliation with BigQuery
# MAGIC SELECT
# MAGIC   count(*)                                             AS versions,
# MAGIC   count(DISTINCT appid)                                AS apps,
# MAGIC   SUM(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END)    AS current_versions,
# MAGIC   ROUND(SUM(current_price), 2)                         AS sum_current_price,
# MAGIC   MIN(valid_from)                                      AS first_valid_from,
# MAGIC   MAX(valid_from)                                      AS last_valid_from
# MAGIC FROM steam.gold.fct_price_history

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results
# MAGIC **Gold fct_price_history**  
# MAGIC 2,514 versions for 792 apps from 46,371 successful observations in silver.  
# MAGIC Every app has exactly one current version.
# MAGIC
# MAGIC **Quality checks:** No duplicate keys, no version with valid_to <= valid_from,
# MAGIC always-free apps have one version
# MAGIC
# MAGIC **Free with a price**  
# MAGIC 1 app is labeled free in silver while having a price_overview.
# MAGIC 518790, theHunter: Call of the Wild,  has the `is_free` flag true because it is free for theHunter Classic owners,
# MAGIC but it has a regular price and discounts for everyone else. Silver labels it `free` while current_price changes.
# MAGIC It was found by the check that always-free apps have one version, so now that check also excludes apps with a price.
# MAGIC
# MAGIC The order of silver's CASE (is_free before price) matches stg_prices on BigQuery, so it is kept for reconciliation.
# MAGIC