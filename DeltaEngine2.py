# Databricks notebook source
# MAGIC %sql
# MAGIC create database tripdb

# COMMAND ----------

trip_df = spark.read.format("csv").option("header","True").option("inferSchema","True").load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")

# COMMAND ----------

display(trip_df)

# COMMAND ----------

trip_df.repartition(500).write.format("delta").partitionBy("vendor_name").saveAsTable("tripdb.trips_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail tripdb.trips_delta

# COMMAND ----------

# MAGIC %fs head dbfs:/user/hive/warehouse/tripdb.db/trips_delta/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tripdb.trips_delta where total_amt = 20

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE tripdb.trips_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history tripdb.trips_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tripdb.trips_delta where total_amt = 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tripdb.trips_delta version as of 0 where total_amt = 20

# COMMAND ----------

trip_df.repartition(200).write.mode("append").format("delta").saveAsTable("tripdb.trips_delta_new1")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail tripdb.trips_delta_new1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tripdb.trips_delta_new1 where passenger_count = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize tripdb.trips_delta_new1 zorder by (passenger_count)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tripdb.trips_delta_new1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail tripdb.trips_delta_new1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history tripdb.trips_delta_new1

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM tripdb.trips_delta_new1 RETAIN 1 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from tripdb.trips_delta_new1 version as of 0