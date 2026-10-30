# Databricks notebook source
# MAGIC %md
# MAGIC ## All history into bronze, incremental with Auto Loader
# MAGIC One row per (appid, collected_at), one line of a prices_*.jsonl file  
# MAGIC Outputs: steam.bronze.raw_prices with all days, loaded incrementally, steam.bronze.mmg_reviews one-off

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS steam.bronze.checkpoints;

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/steam/bronze/s3_raw/steam_api/"))

# COMMAND ----------

   # RESET - only when starting over. Drop the table and delete the checkpoint together,
   # otherwise a rerun either loads nothing into an empty table or appends duplicates.
   # spark.sql("DROP TABLE IF EXISTS steam.bronze.raw_prices")
   # dbutils.fs.rm(CHECKPOINT, recurse=True)

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE = "/Volumes/steam/bronze/s3_raw/steam_api/dt=*/prices_*.jsonl"
CHECKPOINT = "/Volumes/steam/bronze/checkpoints/raw_prices"

stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema("appid BIGINT, collected_at STRING, response_json STRING")
    .load(SOURCE)
    .select(
        "*",
        F.col("_metadata.file_path").alias("source_file"),
        F.to_date(
            F.regexp_extract(F.col("_metadata.file_path"), r"dt=(\d{4}-\d{2}-\d{2})", 1)
        ).alias("obs_date"),
        F.current_timestamp().alias("_ingested_at"),
    ))

query = (stream.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("steam.bronze.raw_prices"))

query.awaitTermination()
print("done")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)                    AS rows_loaded,
# MAGIC        count(DISTINCT source_file) AS files,
# MAGIC        count(DISTINCT obs_date)    AS days,
# MAGIC        min(obs_date), max(obs_date)
# MAGIC FROM steam.bronze.raw_prices;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT obs_date, count(*) AS n
# MAGIC FROM steam.bronze.raw_prices
# MAGIC WHERE obs_date IN ('2026-09-07', '2026-09-08', '2026-09-25')
# MAGIC GROUP BY obs_date ORDER BY obs_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY steam.bronze.raw_prices;

# COMMAND ----------

display(dbutils.fs.ls(CHECKPOINT))

# COMMAND ----------

reviews = spark.read.parquet("/Volumes/steam/bronze/s3_raw/steam_history/reviews/")
reviews.printSchema()   

# COMMAND ----------

(reviews
 .withColumn("source_file", F.col("_metadata.file_path"))
 .withColumn("_ingested_at", F.current_timestamp())
 .write.mode("overwrite")
 .saveAsTable("steam.bronze.mmg_reviews"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results
# MAGIC **Load choice**: Instead of using copy into I decided to use AutoLoader because it is a Structured Streaming source which is much more scalable for millions of files and it can be used both for batches and continuous streaming, so it is more flexible. The only downside is that the checkpoint and the table must be reset together to avoid duplicates since it is write append.
# MAGIC
# MAGIC AutoLoaded 55 files, 52 days from 2026-09-07 to 2026-10-28 and cross checked validity with BigQuery.  
# MAGIC Reran 2 days later and tested the incremental loading that works as intended appending only the new days.
# MAGIC
# MAGIC 2026-09-07 3 files due to testing  
# MAGIC 2026-09-08 a run before the appids settled with 431 rows  
# MAGIC 2026-09-25 two files because Steam changed its response key that day, I fixed the fetch which marked most titles fetch failed and reran it 
# MAGIC
# MAGIC **Table maintenance:** predictive optimization ran an optimize on the table (history
# MAGIC version 3), compacting the small files written by the streaming batches.
# MAGIC
# MAGIC **Reviews:** recreated steam.bronze.mmg_reviews which has 1,048,148