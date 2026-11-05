# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver prices with MERGE
# MAGIC One row per (appid, collected_at)  
# MAGIC Outputs: steam.silver.v_prices_parsed, a parsing view for reusability and cleaner code  
# MAGIC steam.silver.prices table, kept current with MERGE, clustered on appid, with constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW steam.silver.v_prices_parsed AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT
# MAGIC     appid,
# MAGIC     CAST(collected_at AS TIMESTAMP)                          AS collected_at,
# MAGIC     source_file,
# MAGIC     obs_date,
# MAGIC     _ingested_at                                             AS _bronze_ingested_at,
# MAGIC     response_json:success = 'true'                           AS fetch_ok,
# MAGIC     response_json:data.name                                  AS app_name,
# MAGIC     response_json:data.type                                  AS app_type,
# MAGIC     response_json:data.is_free = 'true'                      AS is_free,
# MAGIC     response_json:data.release_date.coming_soon = 'true'     AS coming_soon,
# MAGIC     response_json:data.release_date.date                     AS release_date_raw,
# MAGIC     response_json:data.price_overview.initial::int           AS initial_cents,
# MAGIC     response_json:data.price_overview.final::int             AS final_cents,
# MAGIC     response_json:data.price_overview.discount_percent::int  AS discount_percent,
# MAGIC     response_json:data.price_overview.currency               AS currency
# MAGIC   FROM steam.bronze.raw_prices
# MAGIC )
# MAGIC SELECT
# MAGIC   appid,
# MAGIC   collected_at,
# MAGIC   source_file,
# MAGIC   app_name,
# MAGIC   app_type,
# MAGIC   CASE
# MAGIC     WHEN NOT fetch_ok              THEN 'fetch_failed'
# MAGIC     WHEN coming_soon               THEN 'unreleased'
# MAGIC     WHEN is_free                   THEN 'free'
# MAGIC     WHEN final_cents IS NOT NULL   THEN 'priced'
# MAGIC     ELSE 'unpriced'
# MAGIC   END                                                        AS price_status,
# MAGIC   CAST(initial_cents / 100.0 AS DECIMAL(10,2))               AS list_price,
# MAGIC   CAST(final_cents / 100.0 AS DECIMAL(10,2))                 AS current_price,
# MAGIC   COALESCE(discount_percent, 0)                              AS discount_percent,
# MAGIC   currency,
# MAGIC   release_date_raw,
# MAGIC   obs_date,
# MAGIC   _bronze_ingested_at
# MAGIC FROM parsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS steam.silver.prices (
# MAGIC   appid                BIGINT        NOT NULL,
# MAGIC   collected_at         TIMESTAMP     NOT NULL,
# MAGIC   source_file          STRING,
# MAGIC   app_name             STRING,
# MAGIC   app_type             STRING,
# MAGIC   price_status         STRING        NOT NULL,
# MAGIC   list_price           DECIMAL(10,2),
# MAGIC   current_price        DECIMAL(10,2),
# MAGIC   discount_percent     INT,
# MAGIC   currency             STRING,
# MAGIC   release_date_raw     STRING,
# MAGIC   obs_date             DATE,
# MAGIC   _bronze_ingested_at  TIMESTAMP,
# MAGIC   _silver_merged_at    TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY (appid);

# COMMAND ----------


props = {r.key for r in spark.sql("SHOW TBLPROPERTIES steam.silver.prices").collect()}
if "delta.constraints.valid_price_status" not in props:
    spark.sql("""
        ALTER TABLE steam.silver.prices ADD CONSTRAINT valid_price_status
        CHECK (price_status IN ('priced', 'free', 'unreleased', 'fetch_failed', 'unpriced'))
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Match on (appid, collected_at) to not have duplicates
# MAGIC --incremental on bronze arrival time for efficiency
# MAGIC MERGE INTO steam.silver.prices AS t
# MAGIC USING (
# MAGIC   SELECT appid, collected_at, source_file, app_name, app_type, price_status,
# MAGIC          list_price, current_price, discount_percent, currency, release_date_raw,
# MAGIC          obs_date, _bronze_ingested_at,
# MAGIC          ROW_NUMBER() OVER(PARTITION BY appid,collected_at ORDER BY _bronze_ingested_at DESC) rn
# MAGIC   FROM steam.silver.v_prices_parsed
# MAGIC   WHERE _bronze_ingested_at > coalesce((select max(_bronze_ingested_at) from steam.silver.prices), TIMESTAMP'1900-01-01')
# MAGIC   QUALIFY rn = 1
# MAGIC ) AS s
# MAGIC ON t.appid = s.appid and t.collected_at = s.collected_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   appid, collected_at, source_file, app_name, app_type, price_status,
# MAGIC   list_price, current_price, discount_percent, currency, release_date_raw,
# MAGIC   obs_date, _bronze_ingested_at, _silver_merged_at
# MAGIC ) VALUES (
# MAGIC   s.appid, s.collected_at, s.source_file, s.app_name, s.app_type, s.price_status,
# MAGIC   s.list_price, s.current_price, s.discount_percent, s.currency, s.release_date_raw,
# MAGIC   s.obs_date, s._bronze_ingested_at, current_timestamp()
# MAGIC );

# COMMAND ----------

# No duplicates test 
silver_rows = spark.table("steam.silver.prices").count()
bronze_keys = spark.table("steam.bronze.raw_prices").dropDuplicates(['appid', 'collected_at']).count()
dupes = spark.sql("""
          SELECT appid,collected_at 
          FROM steam.silver.prices 
          GROUP BY appid,collected_at 
          HAVING count(*)>1  
""").count()

if dupes != 0:
    raise ValueError(f"silver.prices has {dupes} duplicate (appid, collected_at) keys")
if silver_rows != bronze_keys:
    raise ValueError(f"silver.prices has {silver_rows} rows, bronze has {bronze_keys} distinct keys")

print(f"OK: silver.prices has {silver_rows} rows and no duplicate keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results
# MAGIC First MERGE inserted 45,343 distinct (appid, collected_at) keys in bronze. A rerun inserted 0.  
# MAGIC Next day's run with real new data inserted 1,568 (2 x 784, the two new days).  
# MAGIC
# MAGIC Quality checks: no duplicate keys, silver rows == distinct bronze keys (46,911).  
# MAGIC Constraints: NOT NULL on the key and price_status has only one of 5 values.  
# MAGIC
# MAGIC ###Tests in 04b
# MAGIC An invalid price_status is rejected  
# MAGIC A record 5 days old arriving now is inserted once  
# MAGIC A duplicate re-delivery inserts 0. The check fails when silver is behind bronze.   
# MAGIC For liquid clustering on appid, OPTIMIZE compacted 3 files (72 KB) into 1 (50 KB), clustering has nothing to do at this size.