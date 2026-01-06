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

def myfunction(orders_result, batch_id):
    print(f"Processing batch: {batch_id}")
    orders_result.createOrReplaceTempView("orders_result")

    merge_statement = """
    MERGE INTO orders_final_result2 t
    USING orders_result s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN
      UPDATE SET t.products_purchased = s.products_purchased,
                 t.orders_placed = s.orders_placed,
                 t.amount_spent = s.amount_spent
    WHEN NOT MATCHED THEN
      INSERT (customer_id, products_purchased, orders_placed, amount_spent)
      VALUES (s.customer_id, s.products_purchased, s.orders_placed, s.amount_spent)
    """

    orders_result.sparkSession.sql(merge_statement)


# COMMAND ----------

streaming_query = aggregated_orders \
.writeStream \
.format("delta") \
.outputMode("update") \
.option("checkpointLocation","checkpointdir110") \
.foreachBatch(myfunction) \
.start()


# COMMAND ----------

streaming_query.status

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Streaming_input/input1",True)

# COMMAND ----------

spark.sql("create table orders_final_result2(customer_id long,orders_placed long,products_purchased long,amount_spent float)")

# COMMAND ----------

spark.sql("select * from orders_final_result2").show()