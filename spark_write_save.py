# Databricks notebook source
# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/databricks-datasets/Rdatasets/data-001/csv/datasets/Titanic.csv

# COMMAND ----------

titanic_data = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/datasets/Titanic.csv",header = "true",inferSchema="true")

# COMMAND ----------

titanic_data.show()

# COMMAND ----------

survivors = titanic_data.filter(titanic_data["survived"]== 'No')

# COMMAND ----------

survivors.show()

# COMMAND ----------

survivors.write.format("csv").mode("overwrite").save("/titanic/data/survivor.csv")

# COMMAND ----------

survivors.write.format("delta").mode("overwrite").saveAsTable("titanic_survivor")

# COMMAND ----------

