# Databricks notebook source
customer_mini_df = spark.sql("select * from store_customers_mini ")

# COMMAND ----------

customer_mini_df.show()

# COMMAND ----------

transaction_mini_df = spark.sql("select * from store_transactions_mini")

# COMMAND ----------

transaction_mini_df.show()

# COMMAND ----------

customer_df = spark.sql("select * from store_customers")

# COMMAND ----------

customer_df.show()

# COMMAND ----------

transaction_df = spark.sql("select * from store_transactions")

# COMMAND ----------

transaction_df.show()

# COMMAND ----------

transaction_df.count()

# COMMAND ----------

customer_product_df = customer_df.join(transaction_df,customer_df.CustomerID == transaction_df.CustomerID)

# COMMAND ----------

customer_product_df.show()

# COMMAND ----------

customer_product_df.count()

# COMMAND ----------

customer_product_df.groupBy("Country").agg({"Amount":"sum"}).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val CustomerDF = spark.sql("select * from store_customers")
# MAGIC val TransactionDF = spark.sql("select * from store_transactions")

# COMMAND ----------

# MAGIC %scala
# MAGIC val customerProductDF = CustomerDF.join(TransactionDF,CustomerDF("CustomerID")===TransactionDF("CustomerID"))

# COMMAND ----------

# MAGIC %scala
# MAGIC customerProductDF.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val CustomerProductDF = spark.sql("select * from store_customers join store_transactions on store_customers.CustomerID = store_transactions.CustomerID")

# COMMAND ----------

# MAGIC %scala
# MAGIC CustomerProductDF.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{sum}
# MAGIC val CountryAmountDF = customerProductDF.groupBy("Country").agg(sum("Amount"))

# COMMAND ----------

# MAGIC %scala
# MAGIC CountryAmountDF.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val CountryAmountDF = spark.sql("select Country,sum(Amount) as TotalAmount from (select * from store_customers join store_transactions on store_customers.CustomerID = store_transactions.CustomerID) group by Country")

# COMMAND ----------

# MAGIC %scala
# MAGIC CountryAmountDF.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC CustomerDF.createOrReplaceTempView("Customers")
# MAGIC TransactionDF.createOrReplaceTempView("Transactions")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.sql("select * from customers")
# MAGIC df.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val CountryAmountDF = spark.sql("select Country,sum(Amount) as TotalAmount from (select * from customers join Transactions on customers.CustomerID = Transactions.CustomerID) group by Country")

# COMMAND ----------

# MAGIC %scala
# MAGIC CountryAmountDF.show()

# COMMAND ----------

customer_mini_df.show()


# COMMAND ----------

transaction_mini_df.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transaction_mini_df,customer_mini_df.CustomerID ==transaction_mini_df.CustomerID)

# COMMAND ----------

cust_prod_mini.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transaction_mini_df,customer_mini_df.CustomerID ==transaction_mini_df.CustomerID , how = "inner")
cust_prod_mini.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transaction_mini_df,customer_mini_df.CustomerID ==transaction_mini_df.CustomerID , how = "left")
cust_prod_mini.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transaction_mini_df,customer_mini_df.CustomerID ==transaction_mini_df.CustomerID , how = "right")
cust_prod_mini.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transaction_mini_df,customer_mini_df.CustomerID ==transaction_mini_df.CustomerID , how = "full")
cust_prod_mini.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transaction_mini_df,customer_mini_df.CustomerID ==transaction_mini_df.CustomerID , how = "left_semi")
cust_prod_mini.show()

# COMMAND ----------

cust_prod_mini = customer_mini_df.join(transaction_mini_df,customer_mini_df.CustomerID ==transaction_mini_df.CustomerID , how = "left_anti")
cust_prod_mini.show()

# COMMAND ----------

