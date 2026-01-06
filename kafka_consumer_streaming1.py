# Databricks notebook source
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

converted_orders_df \
.writeStream \
.queryName("ingestionquery") \
.format("delta") \
.outputMode("append") \
.option("checkpointLocation","checkpointdir301") \
.toTable("orderstablenew301")

# COMMAND ----------

spark.sql("select * from orderstablenew301").show()