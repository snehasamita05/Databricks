# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE orders(order_id int, order_date string, customer_id int,order_status string) using DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders values (1,'2013-07-25
# MAGIC 00:00:00.0',11599,'CLOSED'),(2,'2013-07-25
# MAGIC 00:00:00.0',256,'PENDING_PAYMENT'),
# MAGIC (3,'2013-07-25 00:00:00.0',12111,'COMPLETE'),(4,'2013-07-25
# MAGIC 00:00:00.0',8827,'CLOSED'),(5,'2013-07-25 00:00:00.0',11318,'COMPLETE')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("orders",1)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from orders where order_id = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("orders",2)

# COMMAND ----------

# MAGIC %sql
# MAGIC update orders set order_status = 'COMPLETE' where order_id = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('orders',3)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database retaildb

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retaildb.orders_bronze
# MAGIC (
# MAGIC order_id int,
# MAGIC order_date string,
# MAGIC customer_id int,
# MAGIC order_status string,
# MAGIC filename string,
# MAGIC createdon timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "dbfs:/FileStore/data/orders_bronze.delta"
# MAGIC partitioned by (order_status)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retaildb.orders_silver
# MAGIC (
# MAGIC order_id int,
# MAGIC order_date date,
# MAGIC customer_id int,
# MAGIC order_status string,
# MAGIC order_year int GENERATED ALWAYS AS (YEAR(order_date)),
# MAGIC order_month int GENERATED ALWAYS AS (MONTH(order_date)),
# MAGIC createdon timestamp,
# MAGIC modifiedon timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "dbfs:/FileStore/data/orders_silver.delta"
# MAGIC partitioned by (order_status)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retaildb.orders_gold
# MAGIC (
# MAGIC customer_id int,
# MAGIC order_status string,
# MAGIC order_year int,
# MAGIC num_orders int
# MAGIC )
# MAGIC using delta
# MAGIC location "dbfs:/FileStore/data/orders_gold.delta"
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into retaildb.orders_bronze from (
# MAGIC select order_id::int,
# MAGIC order_date::string,
# MAGIC customer_id::int,
# MAGIC order_status::string,
# MAGIC INPUT_FILE_NAME() as filename,
# MAGIC CURRENT_TIMESTAMP() as createdon
# MAGIC FROM 'dbfs:/FileStore/raw'
# MAGIC )
# MAGIC fileformat = CSV
# MAGIC format_options('header' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.orders_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retaildb.orders_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes ('retaildb.orders_bronze',1)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view orders_bronze_changes
# MAGIC as
# MAGIC select * from table_changes('retaildb.orders_bronze',2) where order_id > 0
# MAGIC and customer_id > 0 and order_status in
# MAGIC ('PAYMENT_REVIEW','PROCESSING','CLOSED','SUSPECTED_FRAUD','C
# MAGIC OMPLETE','P
# MAGIC ENDING','CANCELLED','PENDING_PAYMENT')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from orders_bronze_changes

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into retaildb.orders_silver tgt
# MAGIC using orders_bronze_changes src on tgt.order_id = src.order_id
# MAGIC when matched
# MAGIC then
# MAGIC update set tgt.order_status = src.order_status, tgt.customer_id =
# MAGIC src.customer_id,
# MAGIC tgt.modifiedon = CURRENT_TIMESTAMP()
# MAGIC when not matched
# MAGIC then
# MAGIC insert(order_id, order_date, customer_id, order_status, createdon,
# MAGIC modifiedon)
# MAGIC values (order_id, order_date, customer_id, order_status,
# MAGIC CURRENT_TIMESTAMP(),
# MAGIC CURRENT_TIMESTAMP())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite table retaildb.orders_gold
# MAGIC select customer_id, order_status, order_year, count(order_id) as num_orders
# MAGIC from
# MAGIC retaildb.orders_silver group by customer_id, order_status, order_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.orders_gold