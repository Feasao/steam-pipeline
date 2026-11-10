# Databricks notebook source
# MAGIC %md
# MAGIC ## MMG reviews, one-off batch load
# MAGIC One row per review from the historical MMG dataset (Parquet), which does not change  
# MAGIC Outputs: steam.bronze.mmg_reviews, overwritten on each run. Not part of the daily job

# COMMAND ----------

from pyspark.sql import functions as F

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
# MAGIC steam.bronze.mmg_reviews has 1,048,148 rows (strings except appid and timestamp_created).