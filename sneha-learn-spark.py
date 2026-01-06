# Databricks notebook source
spark

# COMMAND ----------

spark.createDataFrame([{"Google":"Colab","Spark":"Scala"},{"Google":"Dataproc","Spark":"Python"}]).show()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/weather/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/weather/

# COMMAND ----------

weather_csv = spark.read.csv("dbfs:/databricks-datasets/weather/low_temps")

# COMMAND ----------

weather_csv.show()


# COMMAND ----------

customer_df = spark.sql("select * from default.store_customers")

# COMMAND ----------

customer_df.show()

# COMMAND ----------

customer_df.count()

# COMMAND ----------

display(customer_df)

# COMMAND ----------

