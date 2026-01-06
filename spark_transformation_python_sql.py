# Databricks notebook source
diamonds_df = spark.read.format("csv").option("header","true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

# COMMAND ----------

diamonds_df.show()

# COMMAND ----------

diamonds_df.count()

# COMMAND ----------

filtered_df = diamonds_df.filter((diamonds_df["cut"]== "Ideal") & (diamonds_df["price"]>1000))

# COMMAND ----------

filtered_df.show()

# COMMAND ----------

from pyspark.sql.functions import avg
grouped_df = filtered_df.groupBy("clarity").agg(avg("price"))

# COMMAND ----------

grouped_df.show()

# COMMAND ----------

ordered_df = grouped_df.orderBy(grouped_df["avg(price)"].desc())

# COMMAND ----------

ordered_df.show()

# COMMAND ----------

ordered_df.write.format("parquet").mode("overwrite").saveAsTable("clarity_fx_skills")

# COMMAND ----------

df1 = spark.sql("select * from default.clarity_fx_skills")
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY clarity_fx_skills

# COMMAND ----------

diamonds_df.write.format("delta").mode("overwrite").saveAsTable("clarity_fx_skills_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.clarity_fx_skills_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY clarity_fx_skills_delta

# COMMAND ----------

diamonds_df.write.format("delta").mode("append").saveAsTable("clarity_fx_skills_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY clarity_fx_skills_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC update clarity_fx_skills_delta set table = '61' where cut = "Ideal"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY clarity_fx_skills_delta
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/clarity_fx_skills_delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history clarity_fx_skills_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills_delta
# MAGIC timestamp as of '2023-12-28T16:38:18.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills_delta@v0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills_delta@v1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills_delta@v2

# COMMAND ----------

# MAGIC %sql
# MAGIC restore clarity_fx_skills_delta TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarity_fx_skills_delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history clarity_fx_skills_delta

# COMMAND ----------

