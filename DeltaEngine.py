# Databricks notebook source
# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz

# COMMAND ----------

# MAGIC %sql
# MAGIC create database trip_db

# COMMAND ----------

trip_df = spark.read.format("csv").option("header","True").option("inferSchema","True").load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")

# COMMAND ----------

display(trip_df)

# COMMAND ----------

trip_df.count()

# COMMAND ----------

trip_df.repartition(20).write.format("delta").saveAsTable("trip_db.trips_delta")

# COMMAND ----------

trip_df.repartition(20).write.format("parquet").saveAsTable("trip_db.trips_parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail trip_db.trips_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended trip_db.trips_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(Fare_Amt),max(Fare_Amt) from trip_db.trips_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(Fare_Amt),max(Fare_Amt) from trip_db.trips_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from trip_db.trips_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from trip_db.trips_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_db.trips_delta where Total_Amt = 234

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_db.trips_parquet where Total_Amt = 234

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_db.trips_delta where Total_Amt > 232

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_db.trips_parquet where Total_Amt > 232

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(trip_distance),sum(Total_Amt) from trip_db.trips_delta group by vendor_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(trip_distance),sum(fare_amt) from trip_db.trips_delta group by vendor_name

# COMMAND ----------

# MAGIC %sql
# MAGIC cache
# MAGIC select * from trip_db.trips_delta