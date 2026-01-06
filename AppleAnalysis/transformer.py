# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast

# COMMAND ----------


# Initialize Spark
spark = SparkSession.builder.getOrCreate()

class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDF):
        pass

class FirstTransformer(Transformer):
    def transform(self, inputDFs):
        """
        Customers who bought AirPods after buying the iPhone
        """
        # Retrieve Transaction DataFrame
        TransactionInputDF = inputDFs.get("TransactionInputDF")
        
        if TransactionInputDF is not None:
            print("TransactionInputDF in transform")
            TransactionInputDF.show()

            # Define window specification
            windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
            transformedDF = TransactionInputDF.withColumn("next_product_name", lead("product_name").over(windowSpec))

            print("AirPods after buying iPhones")
            transformedDF.orderBy("customer_id", "transaction_date", "product_name").show()

            # Filter customers who bought AirPods after iPhones
            filteredDF = transformedDF.filter((col("product_name") == 'iPhone') & (col("next_product_name") == 'AirPods'))
            filteredDF.orderBy("customer_id", "transaction_date", "product_name").show()

            # Join with customer data
            customerInputDF = inputDFs.get("customerInputDF")
            joinDF = customerInputDF.join(broadcast(filteredDF), "customer_id")

            print("JOINED DF")
            joinDF.show()

            return joinDF.select("customer_id", "customer_name", "location")
        else:
            print("TransactionInputDF is not available or is None.")