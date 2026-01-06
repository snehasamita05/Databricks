# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/streaming_input/input1", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/Checkpointlocation111", recurse=True)

# COMMAND ----------

#if table exist you can drop that table using below command

spark.sql("drop table orders_result_final")

# COMMAND ----------

#Also we will delete the directory also
dbutils.fs.rm("dbfs:/user/hive/warehouse/orders_result_final", True)

# COMMAND ----------

# we will create input directory
dbutils.fs.mkdirs("dbfs:/FileStore/streaming_input/input1")

# COMMAND ----------

#target table creation command

spark.sql("create table orders_result_final (customer_id long, orders_placed long, products_purchased long, amount_spent float)")

# COMMAND ----------

schema_json = "order_id long, customer_id long, customer_fname string, customer_lname string,  city string, state string, pincode long, line_iems array<struct<order_item_id:long, order_item_product_id:long, order_item_quantity:long, order_item_subtotal:float, order_item_product_price:float>>"

# COMMAND ----------

order_data = spark.readStream \
.format("json") \
.schema(schema_json) \
.option("path", "dbfs:/FileStore/streaming_input/input1") \
.load()


# COMMAND ----------

order_data.createOrReplaceTempView("orders")

# COMMAND ----------

exploded_orders = spark.sql("select order_id, customer_id, city, state, pincode, explode(line_iems) as lines from orders")

# COMMAND ----------

exploded_orders.createOrReplaceTempView("exploded_orders")

# COMMAND ----------

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
                            lines.order_item_id as item_id,
                            lines.order_item_product_id as product_id, 
                            lines.order_item_quantity as quantity,
                            lines.order_item_product_price as product_price, 
                            lines.order_item_subtotal as subtotal
                            from exploded_orders""")

# COMMAND ----------

def myfunction(flattened_orders, batch_id):

    flattened_orders.createOrReplaceTempView("flattened_orders")

    aggregated_orders = flattened_orders._jdf.sparkSession().sql("""select customer_id, approx_count_distinct(order_id) as orders_placed, count(product_id) as products_purchased, sum(subtotal) 
                              as amount_spent
                              from flattened_orders
                              group By customer_id
                              """)
    

    aggregated_orders.createOrReplaceTempView("orders_result")

    merge_statement = """merge into orders_result_final t using orders_result s 
    on t.customer_id = s.customer_id
    when matched 
    then update set t.orders_placed = t.orders_placed + s.orders_placed, t.products_purchased = t.products_purchased + s.products_purchased, t.amount_spent = t.amount_spent + s.amount_spent
    when not matched 
    then insert * 
    """
    flattened_orders._jdf.sparkSession().sql(merge_statement)


    

# COMMAND ----------

streaming_query = flattened_orders \
    .writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "Checkpointlocation111") \
    .trigger(availableNow=True) \
    .foreachBatch(myfunction) \
    .start()

# COMMAND ----------

spark.sql("select * from orders_result_final").show()

# COMMAND ----------

streaming_query.status