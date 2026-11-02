# Databricks notebook source
# MAGIC %md
# MAGIC ## Player-count charts
# MAGIC The raws have one row per charts file.  
# MAGIC The silver layer will be one row per (source_file, appid), same as stg_charts.  
# MAGIC **Output:** steam.bronze.raw_charts, steam.silver.charts.

# COMMAND ----------

display(spark.read.text("/Volumes/steam/bronze/s3_raw/steam_api/dt=2026-10-30/charts_*.json"))

# COMMAND ----------

from pyspark.sql import functions as F

CHARTS_SOURCE = "/Volumes/steam/bronze/s3_raw/steam_api/dt=*/charts_*.json"
CHARTS_CHECKPOINT = "/Volumes/steam/bronze/checkpoints/raw_charts"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema("collected_at STRING, response STRING")
    .load(CHARTS_SOURCE)
    .select(
        "collected_at",
        F.col("response").alias("response_json"),
        F.col("_metadata.file_path").alias("source_file"),
        F.to_date(
            F.regexp_extract(F.col("_metadata.file_path"), r"dt=(\d{4}-\d{2}-\d{2})", 1)
        ).alias("obs_date"),
        F.current_timestamp().alias("_ingested_at"),
    )
    .writeStream
    .option("checkpointLocation", CHARTS_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("steam.bronze.raw_charts")
    .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)                 AS files,
# MAGIC        count(DISTINCT obs_date) AS days,
# MAGIC        min(obs_date), max(obs_date),
# MAGIC        sum(CASE WHEN response_json LIKE '%"ranks"%' THEN 0 ELSE 1 END) AS files_without_ranks
# MAGIC FROM steam.bronze.raw_charts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT left(response_json, 80) FROM steam.bronze.raw_charts LIMIT 3;

# COMMAND ----------

RANKS_SCHEMA = """
  response STRUCT<
    rollup_date: BIGINT,
    ranks: ARRAY<STRUCT<rank: INT, appid: BIGINT, last_week_rank: INT, peak_in_game: BIGINT>>
  >
"""

charts = (spark.table("steam.bronze.raw_charts")
    .withColumn("j", F.from_json("response_json", RANKS_SCHEMA))
    .select(
        F.col("collected_at").cast("timestamp").alias("collected_at"),
        "source_file",
        F.to_date(F.timestamp_seconds("j.response.rollup_date")).alias("rollup_date"),
        F.explode("j.response.ranks").alias("r"),
    )
    .select(
        F.col("r.appid").alias("appid"),
        "rollup_date",
        "collected_at",
        "source_file",
        F.col("r.rank").alias("chart_rank"),
        F.col("r.last_week_rank").alias("last_week_rank"),
        F.col("r.peak_in_game").alias("peak_players"),
    ))

charts.write.mode("overwrite").saveAsTable("steam.silver.charts")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)                     AS rows,
# MAGIC        count(DISTINCT source_file)  AS files,
# MAGIC        count(DISTINCT rollup_date)  AS rollup_days,
# MAGIC        min(rollup_date), max(rollup_date)
# MAGIC FROM steam.silver.charts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, count(*) AS n
# MAGIC FROM steam.silver.charts
# MAGIC GROUP BY source_file
# MAGIC HAVING count(*) != 100;
# MAGIC -- must be empty

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH p AS (
# MAGIC   SELECT obs_date, count(DISTINCT source_file) AS price_files
# MAGIC   FROM steam.bronze.raw_prices GROUP BY obs_date
# MAGIC ),
# MAGIC c AS (
# MAGIC   SELECT obs_date, count(*) AS chart_files
# MAGIC   FROM steam.bronze.raw_charts GROUP BY obs_date
# MAGIC )
# MAGIC SELECT coalesce(p.obs_date, c.obs_date) AS obs_date, price_files, chart_files
# MAGIC FROM p FULL OUTER JOIN c ON p.obs_date = c.obs_date
# MAGIC WHERE price_files IS DISTINCT FROM chart_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT obs_date, source_file, left(response_json, 200) AS start_of_response
# MAGIC FROM steam.bronze.raw_charts
# MAGIC WHERE response_json NOT LIKE '%"ranks"%';

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH per_file AS (
# MAGIC   SELECT rollup_date,
# MAGIC          source_file,
# MAGIC          md5(concat_ws(',', sort_array(collect_list(
# MAGIC              concat_ws(':', chart_rank, appid, peak_players))))) AS fingerprint
# MAGIC   FROM steam.silver.charts
# MAGIC   GROUP BY rollup_date, source_file
# MAGIC )
# MAGIC SELECT rollup_date,
# MAGIC        count(*)                    AS files,
# MAGIC        count(DISTINCT fingerprint) AS versions
# MAGIC FROM per_file
# MAGIC WHERE rollup_date IN ('2026-09-06', '2026-09-14', '2026-10-12', '2026-10-20')
# MAGIC GROUP BY rollup_date
# MAGIC ORDER BY rollup_date;

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/steam/bronze/s3_raw/steam_api/dt=2026-10-06/"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Results
# MAGIC **Bronze raw_charts:** 58 files over 54 collection days 2026-09-07 to 2026-10-30 and 1 file without ranks which was a failed fetch kept as data  
# MAGIC
# MAGIC **Silver charts:** 5,700 rows from 57 files  
# MAGIC The failed file is dropped by explode and every other
# MAGIC file has exactly 100 ranks. Explode drops files without ranks the same as UNNEST on BigQuery. A rerun of the bronze stream loaded nothing new.  
# MAGIC
# MAGIC **Failed fetch:** 2026-10-06 has two charts files. The first failed with a NameResolutionError. The pipeline then was running localy and had lost its internet connection, the second
# MAGIC succeeded and covered it.
# MAGIC
# MAGIC **Missing days:** 09-07, 09-15, 10-13, 10-21. 
# MAGIC On the collection day after
# MAGIC each day Steam returned the chart from two days earlier instead of the
# MAGIC new one. Gold keeps the
# MAGIC first file per rollup day.