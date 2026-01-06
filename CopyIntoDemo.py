# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists orders(OrderId int, OrderDate string,CustomerId int,TotalAmount long,Status string) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail orders

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /FileStore/data1

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO orders
# MAGIC FROM (select OrderId::int,OrderDate,CustomerId::int,TotalAmount::long,Status from 'dbfs:/FileStore/data1/*')
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true')
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql("select count(*) from orders").show()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/data1")