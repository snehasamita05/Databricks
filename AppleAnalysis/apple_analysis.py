# Databricks notebook source
# MAGIC %md
# MAGIC ### Factory Pattern

# COMMAND ----------

#%run "/Users/snehasamita05@gmail.com/AppleAnalysis/reader_factory"


# COMMAND ----------

# MAGIC %run "/Users/snehasamita05@gmail.com/AppleAnalysis/transformer"

# COMMAND ----------

# MAGIC %run "/Users/snehasamita05@gmail.com/AppleAnalysis/extractor"

# COMMAND ----------

# MAGIC %run "/Users/snehasamita05@gmail.com/AppleAnalysis/Loader"

# COMMAND ----------

class WorkFlow:
    def __init__(self):
        pass

    def runner(self):
        try:
            # Step 1: Extract data
            print("Step 1: Extracting data...")
            inputDFs = AirpodsAfterIphoneExtractors().extract()

            if not inputDFs:
                raise ValueError("Data extraction failed! No data returned.")

            # Step 2: Transform data
            print("Step 2: Transforming data...")
            firstTransformer = FirstTransformer()
            firstTransformeredDF = firstTransformer.transform(inputDFs)

            if firstTransformeredDF is None:
                raise ValueError("Transformation failed! The DataFrame returned is None.")

            # Step 3: Load transformed data using AirpodsAfterIphoneExtractors with transformed data
            print("Step 3: Loading data...")
            loader = AirpodsAfterIphoneExtractors(firstTransformeredDF)
            loader.sink()

            print("Workflow completed successfully!")

        except ValueError as ve:
            print(f"Error: {ve}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")

# Run the workflow
workflow = WorkFlow().runner()


# COMMAND ----------

# MAGIC %md
# MAGIC ### INITIATE THE SPARK SESSION

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("theBigDataShow.me").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call the dataframe

# COMMAND ----------

input_df = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/Transaction_Updated.csv")
input_df.show()