# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orders_schema = "order_id long,customer_id long,customer_fname string,customer_lname string,city string,state string,pincode long,line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/Streaming_input/input1")

# COMMAND ----------

orders_df = spark \
.readStream \
.format("json") \
.schema(orders_schema) \
.option("path","dbfs:/FileStore/Streaming_input/input1") \
.load()

# COMMAND ----------

orders_df.createOrReplaceTempView("orders")

# COMMAND ----------

exploded_orders = spark.sql("""select order_id,customer_id,city,state,
pincode,explode(line_items) lines from orders""")

# COMMAND ----------

exploded_orders.createOrReplaceTempView("exploded_orders")

# COMMAND ----------

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
lines.order_item_id as item_id, lines.order_item_product_id as product_id,
lines.order_item_quantity as quantity,lines.order_item_product_price as price,
lines.order_item_subtotal as subtotal from exploded_orders""")

# COMMAND ----------

flattened_orders.createOrReplaceTempView("orders_flattened")

# COMMAND ----------

aggregated_orders = spark.sql("""select customer_id, approx_count_distinct(order_id) as orders_placed, 
count(item_id) as products_purchased,sum(subtotal) as amount_spent 
from orders_flattened group by customer_id""")

# COMMAND ----------

aggregated_orders.createOrReplaceTempView("orders_aggregated")

# COMMAND ----------

streaming_query = aggregated_orders \
    .writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", "checkpointdir101") \
    .toTable("orders_result101")


# COMMAND ----------

spark.sql("select * from orders_result101").show()

# COMMAND ----------

streaming_query.status