# Databricks notebook source
from pyspark.sql.functions import*

# COMMAND ----------

confluentBootstrapServers = 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'
confluentApiKey = 'KQOKDTUG2UFHJBM3'
confluentSecret = 'N0lfHKIicxbu3fJKerAZ1ftt+cu/Up9KBg+MFF3zlI0doMhKp9t8x7UPkauXLiz5'
confluentTopicName = 'retail-data-new'

# COMMAND ----------

orders_df = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers",confluentBootstrapServers) \
.option("kafka.security.protocol","SASL_SSL") \
.option("kafka.sasl.mechanism","PLAIN") \
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
.option("kafka.ssl.endpoint.identification.algorithm","https") \
.option("subscribe",confluentTopicName) \
.option("startingTimestamp",1) \
.option("maxOffsetPerTrigger",50) \
.load()

# COMMAND ----------

converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")

# COMMAND ----------

display(converted_orders_df)

# COMMAND ----------

orders_schema = "order_id long,customer_id long,customer_fname string,customer_lname string,city string,state string,pincode long,line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"

# COMMAND ----------

parsed_orders_df = converted_orders_df.select("key",from_json("value",orders_schema).alias("value"),"topic","partition","offset","timestamp","timestampType")

# COMMAND ----------

display(parsed_orders_df)

# COMMAND ----------

parsed_orders_df.createOrReplaceTempView("orders")

# COMMAND ----------

exploded_orders = spark.sql("""select key, value.order_id as order_id, value.customer_id as customer_id, value.customer_fname as customer_fname,
          value.customer_lname as customer_lname, value.city as city, value.state as state, value.pincode as pincode, explode(value.line_items) lines from orders""")

# COMMAND ----------

display(exploded_orders)

# COMMAND ----------

exploded_orders.createOrReplaceTempView("exploded_orders")

# COMMAND ----------

flattened_orders = spark.sql("""select order_id, customer_id, customer_fname,customer_lname, city, state, pincode, 
          lines.order_item_id as item_id, lines.order_item_product_id as product_id,
          lines.order_item_quantity as quantity, lines.order_item_product_price as price,
          lines.order_item_subtotal as subtotal from exploded_orders """)

# COMMAND ----------

display(flattened_orders)

# COMMAND ----------

flattened_orders \
.writeStream \
.queryName("ingestionquery") \
.format("delta") \
.outputMode("append") \
.option("checkpointLocation","checkpointdir302") \
.toTable("orderstablenew302")

# COMMAND ----------

spark.sql("select * from orderstablenew302").show()