# Databricks notebook source
# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz

# COMMAND ----------

nytaxi_df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("codec","gzip").load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")

# COMMAND ----------

nytaxi_df.show()

# COMMAND ----------

def concat_string(s1,s2):
    return s1 + "_" + s2


# COMMAND ----------

concat_string("sneha", "samita")

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
concat_udf = udf(concat_string,StringType())

# COMMAND ----------

df1 = nytaxi_df.withColumn("vendor_payment",concat_udf(nytaxi_df.vendor_name,nytaxi_df.Payment_Type))

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.select("vendor_payment").show()

# COMMAND ----------

from pyspark.sql.functions import concat, col
df2 = nytaxi_df.withColumn("vendor_payment",concat(col("vendor_name"),col("Payment_Type")))
df2.select("vendor_payment").show()

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit
df2 = nytaxi_df.withColumn("vendor_payment",concat(col("vendor_name"),lit("_"),col("Payment_Type")))
df2.select("vendor_payment").show()

# COMMAND ----------

df2.withColumn("country",lit("USA")).show()

# COMMAND ----------

df3= df2.select("vendor_payment","Total_Amt").withColumn("country",lit("USA")).show()

# COMMAND ----------

add = lambda x,y: x+y

# COMMAND ----------

add(3,4)

# COMMAND ----------

find_max = lambda x,y : x if x>y else y

# COMMAND ----------

find_max(4,8)

# COMMAND ----------

from pyspark.sql.types import DoubleType
sumtwo = udf(lambda x,y : x+y,DoubleType())

# COMMAND ----------

df3= nytaxi_df.withColumn("fareplussurcharge",sumtwo(nytaxi_df.surcharge,nytaxi_df.Fare_Amt)).show()

# COMMAND ----------

nytaxi_df.show()

# COMMAND ----------

def reverse_string(s):
    return s[::-1]

# COMMAND ----------

reverse_string("sneha")

# COMMAND ----------

reverse_string_udf = udf(reverse_string,StringType())

# COMMAND ----------

df5 = nytaxi_df.withColumn("reversed_vendor_name",reverse_string_udf("vendor_name")).select("vendor_name","reversed_vendor_name").show()

# COMMAND ----------

from pyspark.sql.types import StringType, ArrayType
Split_words_udf = udf(lambda text : text.split(), ArrayType(StringType()))

# COMMAND ----------

df = spark.createDataFrame([(1,"data engineering"), (2,"delta lake lakehouse")], ["id","text"])
df.show()

# COMMAND ----------

df= df.withColumn("words",Split_words_udf(df["text"]))
df.show()

# COMMAND ----------

