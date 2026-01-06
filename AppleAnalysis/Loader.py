# Databricks notebook source
# MAGIC %run "/Users/snehasamita05@gmail.com/AppleAnalysis/loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformeredDF):
         if transformeredDF is None:
            raise ValueError("The transformeredDF cannot be None.")
            self.transformeredDF = transformeredDF

    def sink(self):
        raise NotImplementedError("Subclasses should implement this method")


class AirpodsAfterIphoneExtractors(AbstractLoader):
    def sink(self):
        # Correct indentation and function call
        sink_instance = get_sink_source(
            sink_type="dbfs",
            df=self.transformeredDF,
            path="dbfs:/FileStore/tables/AirpodsAfterIphoneExtractors",
            method="overwrite"
        )
        sink_instance.load_data_frame()  # Correct method call



      