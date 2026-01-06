# Databricks notebook source
# MAGIC %md
# MAGIC ### Factory Pattern

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

class DataSource:
    def __init__(self, path):
        self.path = path

    def get_data_frame(self):
        raise ValueError("Not Implemented")

class CSVDataSource(DataSource):
    def get_data_frame(self):
        return spark.read.format("csv").option("header", True).load(self.path)

class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return spark.read.format("parquet").load(self.path)

class DeltaDataSource(DataSource):
    def get_data_frame(self):
        return spark.read.table(self.path)

def get_data_source(data_type, file_path):
    if data_type == "csv":
        return CSVDataSource(file_path)
    elif data_type == "parquet":
        return ParquetDataSource(file_path)
    elif data_type == "delta":
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f"Not Implemented for data_type {data_type}")