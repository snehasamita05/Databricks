# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

class DataSink:
    def __init__(self, df, path, method, params):
        self.df = df
        self.path = path
        self.method = method
        self.params = params

    def load_data_frame(self):
        raise ValueError("Not Implemented")

class LoadToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)

class LoadToDBFSWithPartitionBy(DataSink):
    def load_data_frame(self):
        partitionByColumn = self.params.get("partitionByColumn")
        self.df.write.mode(self.method).partitionBy(*partitionByColumn).save(self.path)

def get_sink_source(sink_type, df, path, method, params=None):
    if sink_type == "dbfs":
        return LoadToDBFS(df, path, method, params)
    elif sink_type == "dbfs_with_partition":
        return LoadToDBFSWithPartitionBy(df, path, method, params)
    else:
        raise ValueError(f"Not implemented for sink type {sink_type}")