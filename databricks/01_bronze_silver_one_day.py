# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze to silver demo day
# MAGIC One row per (appid, collected_at), one collection day.  
# MAGIC Produces steam.bronze.raw_prices, steam.silver.prices_one_day (same logic as stg_prices on BigQuery).

# COMMAND ----------

from pyspark.sql import functions as F

bronze = (spark.read
    .schema("appid BIGINT, collected_at STRING, response_json STRING")
    .json("/Volumes/steam/bronze/s3_raw/steam_api/dt=2026-10-24/prices_*.jsonl")
    .select("*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("_ingested_at")))

bronze.write.mode("overwrite").saveAsTable("steam.bronze.raw_prices")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), count(DISTINCT appid), min(source_file)
# MAGIC FROM steam.bronze.raw_prices;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE steam.silver.prices_one_day AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT
# MAGIC     appid,
# MAGIC     CAST(collected_at AS TIMESTAMP)                          AS collected_at,
# MAGIC     source_file,
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
# MAGIC   release_date_raw
# MAGIC FROM parsed;

# COMMAND ----------

from pyspark.sql import functions as F

def j(path):
    return F.get_json_object("response_json", path)

parsed = spark.table("steam.bronze.raw_prices").select(
    "appid",
    F.col("collected_at").cast("timestamp").alias("collected_at"),
    "source_file",
    (j("$.success") == "true").alias("fetch_ok"),
    j("$.data.name").alias("app_name"),
    j("$.data.type").alias("app_type"),
    (j("$.data.is_free") == "true").alias("is_free"),
    (j("$.data.release_date.coming_soon") == "true").alias("coming_soon"),
    j("$.data.release_date.date").alias("release_date_raw"),
    j("$.data.price_overview.initial").cast("int").alias("initial_cents"),
    j("$.data.price_overview.final").cast("int").alias("final_cents"),
    j("$.data.price_overview.discount_percent").cast("int").alias("discount_percent"),
    j("$.data.price_overview.currency").alias("currency"),
)

py_df = parsed.select(
    "appid", "collected_at", "source_file", "app_name", "app_type",
    F.when(~F.col("fetch_ok"), "fetch_failed")
     .when(F.col("coming_soon"), "unreleased")
     .when(F.col("is_free"), "free")
     .when(F.col("final_cents").isNotNull(), "priced")
     .otherwise("unpriced")
     .alias("price_status"),
    (F.col("initial_cents") / 100).cast("decimal(10,2)").alias("list_price"),
    (F.col("final_cents") / 100).cast("decimal(10,2)").alias("current_price"),
    F.coalesce("discount_percent", F.lit(0)).alias("discount_percent"),
    "currency",
    "release_date_raw",
)

# COMMAND ----------

sql_df = spark.table("steam.silver.prices_one_day")

print(sql_df.dtypes == py_df.dtypes)
print(sql_df.exceptAll(py_df).count(),
      py_df.exceptAll(sql_df).count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT price_status, count(*)
# MAGIC FROM steam.silver.prices_one_day
# MAGIC GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Result
# MAGIC SQL and PySpark identical
# MAGIC price_status counts match BigQuery stg_prices for the same day with  
# MAGIC fetch_failed 2, free 42, priced 723, unpriced 13, unreleased 4 (784 rows)