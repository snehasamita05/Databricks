# Databricks notebook source
# MAGIC %run "/Users/snehasamita05@gmail.com/AppleAnalysis/reader_factory"

# COMMAND ----------

class Extractor:
    def __init__(self):
        pass
        

    def extract(self):
        raise NotImplementedError("Subclasses must implement this method.")


class AirpodsAfterIphoneExtractors(Extractor):
    def extract(self):
        """
        Extract data from sources with error handling.
        """
        try:
            # Define the file paths
            transaction_file_path = "dbfs:/FileStore/Transaction_Updated.csv"
            customer_table_path = "default.customer_delta_table"
            
            # Try to load the transaction data
            try:
                TransactionInputDF = get_data_source("csv", transaction_file_path).get_data_frame()
                TransactionInputDF.orderBy("customer_id", "transaction_date").show()
            except Exception as e:
                print(f"Error loading transaction data: {e}")
                return {}

            # Try to load the customer data
            try:
                customerInputDF = get_data_source("delta", customer_table_path).get_data_frame()
                customerInputDF.show()
            except Exception as e:
                print(f"Error loading customer data: {e}")
                return {}

            # Return the dataframes if both loads are successful
            return {
                "TransactionInputDF": TransactionInputDF,
                "customerInputDF": customerInputDF
            }

        except Exception as e:
            print(f"Unexpected error occurred while extracting data: {e}")
            return {}


        
      