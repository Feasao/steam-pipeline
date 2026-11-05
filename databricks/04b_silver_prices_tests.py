# Databricks notebook source
# MAGIC %md
# MAGIC ## Manual silver prices tests
# MAGIC These cells write test rows into bronze and silver and delete them again at the end.
# MAGIC Run all cells in order. If a cell fails, run the bottom recovery cells.

# COMMAND ----------

silver = "steam.silver.prices"
before = spark.table(silver).count()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW test_src AS
# MAGIC SELECT * FROM steam.bronze.raw_prices
# MAGIC WHERE appid = 440
# MAGIC   AND obs_date = date_sub(current_date(), 5)
# MAGIC   AND source_file NOT LIKE 'manual_%'
# MAGIC ORDER BY collected_at
# MAGIC LIMIT 1;
# MAGIC
# MAGIC INSERT INTO steam.bronze.raw_prices
# MAGIC SELECT appid, CAST(CAST(collected_at AS TIMESTAMP) - INTERVAL 3 HOURS AS STRING),
# MAGIC        response_json, 'manual_late_test', obs_date, current_timestamp()
# MAGIC FROM test_src;
# MAGIC
# MAGIC INSERT INTO steam.bronze.raw_prices
# MAGIC SELECT appid, collected_at,
# MAGIC        response_json, 'manual_dup_test', obs_date, current_timestamp()
# MAGIC FROM test_src;

# COMMAND ----------

# MAGIC %run ./04a_silver_prices

# COMMAND ----------

after = spark.table(silver).count()
late = spark.sql(f"SELECT count(*) FROM {silver} WHERE source_file = 'manual_late_test'").first()[0]
dup  = spark.sql(f"SELECT count(*) FROM {silver} WHERE source_file = 'manual_dup_test'").first()[0]

for table in (silver, "steam.bronze.raw_prices"):
    spark.sql(f"DELETE FROM {table} WHERE source_file IN ('manual_late_test', 'manual_dup_test')")

assert (after - before, late, dup) == (1, 1, 0), \
    f"expected +1 row (late 1, dup 0), got +{after - before} (late {late}, dup {dup})"
print("OK: late record inserted once, redelivered record ignored; test rows removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Recovery if a run stopped before the cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM steam.silver.prices      WHERE source_file IN ('manual_late_test', 'manual_dup_test');
# MAGIC DELETE FROM steam.bronze.raw_prices  WHERE source_file IN ('manual_late_test', 'manual_dup_test');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM steam.bronze.raw_prices WHERE source_file LIKE 'manual%';