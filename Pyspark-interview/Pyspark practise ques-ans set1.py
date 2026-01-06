# Databricks notebook source
# MAGIC %md 
# MAGIC ### Question 1
# MAGIC ### 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# MAGIC You are given two PySpark DataFrames containing customer transaction data from two different sources. Each DataFrame has common columns, but the column names may have conflicts due to different naming conventions in the sources.
# MAGIC
# MAGIC You need to:
# MAGIC Merge these DataFrames using an outer join on the customer_id column.
# MAGIC Resolve column conflicts by renaming duplicate columns with a suffix indicating their source.
# MAGIC Fill any missing values in the resulting DataFrame appropriately.
# MAGIC
# MAGIC
# MAGIC 𝐬𝐜𝐡𝐞𝐦𝐚 
# MAGIC data1 = [ (101, "Alice", 500, "2024-08-01"), (102, "Bob", 300, "2024-08-02"), (103, "Charlie", 450, "2024-08-03") ] 
# MAGIC
# MAGIC columns1 = ["customer_id", "name", "amount_spent", "transaction_date"] 
# MAGIC
# MAGIC df1 = spark.createDataFrame(data1, columns1) 
# MAGIC
# MAGIC Sample Data for df2 data2 = [ (101, "Alice", 520, "2024-08-01"), (104, "David", 600, "2024-08-04"), (105, "Eva", 700, "2024-08-05") ] 
# MAGIC
# MAGIC columns2 = ["customer_id", "name", "total_spent", "transaction_date"] 
# MAGIC
# MAGIC df2 = spark.createDataFrame(data2, columns2) 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("MergeData").getOrCreate()
data1 = [ (101, "Alice", 500, "2024-08-01"), (102, "Bob", 300, "2024-08-02"), (103, "Charlie", 450, "2024-08-03") ]
columns1 = ["customer_id", "name", "amount_spent", "transaction_date"]
data2 = [ (101, "Alice", 520, "2024-08-01"), (104, "David", 600, "2024-08-04"), (105, "Eva", 700, "2024-08-05") ]
columns2 = ["customer_id", "name", "total_spent", "transaction_date"]
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)


# COMMAND ----------

merger_df = df1.alias("A").join(df2.alias("B"), on = "customer_id", how = "outer")
renamed_df = merger_df.select(col("customer_id"),col("A.name").alias("name_A"),col("amount_spent"),col("A.transaction_date").alias("transaction_date_A"),col("A.name").alias("name_A"),col("total_spent"),col("A.transaction_date").alias("transaction_date_A"))

# COMMAND ----------

renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2
# MAGIC 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# MAGIC Given a dataset of monthly sales records with salespeople names and their regions, calculate the month with the highest sales for each region using window functions and the max() function. Ensure that the result includes the region name, month, and sales value. Consider sales fluctuations, and the dataset should contain multiple records for each region to test windowing correctly.
# MAGIC
# MAGIC 𝐬𝐜𝐡𝐞𝐦𝐚 
# MAGIC data = [ ("Amit", "North", "Jan", 12000), ("Rajesh", "North", "Feb", 15000), ("Sunita", "North", "Mar", 11000), ("Meena", "South", "Jan", 17000), 
# MAGIC ("Ravi", "South", "Feb", 20000), ("Priya", "South", "Mar", 18000), 
# MAGIC ("Suresh", "East", "Jan", 10000), ("Vishal", "East", "Feb", 22000), 
# MAGIC ("Akash", "East", "Mar", 21000), ("Anjali", "West", "Jan", 15000), 
# MAGIC ("Deepak", "West", "Feb", 13000), ("Nidhi", "West", "Mar", 17000), ] 
# MAGIC
# MAGIC Step 3: Define schema and create DataFrame columns = ["Salesperson", "Region", "Month", "Sales"]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

spark = SparkSession.builder.appName("MonthlySalesRecord").getOrCreate()
data = [ ("Amit", "North", "Jan", 12000), ("Rajesh", "North", "Feb", 15000), ("Sunita", "North", "Mar", 11000), ("Meena", "South", "Jan", 17000), ("Ravi", "South", "Feb", 20000), ("Priya", "South", "Mar", 18000), ("Suresh", "East", "Jan", 10000), ("Vishal", "East", "Feb", 22000), ("Akash", "East", "Mar", 21000), ("Anjali", "West", "Jan", 15000), ("Deepak", "West", "Feb", 13000), ("Nidhi", "West", "Mar", 17000), ]
columns = ["Salesperson", "Region", "Month", "Sales"]

# COMMAND ----------

df = spark.createDataFrame(data, columns)

# COMMAND ----------

windowspecs = Window.partitionBy("Region").orderBy(col("Sales").desc())
ranked_df = df.withColumn("rank", F.row_number().over(windowspecs))
max_sales_per_region = ranked_df.filter(col("rank")==1).select("Region","Salesperson","Month","Sales")
max_sales_per_region.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3
# MAGIC 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# MAGIC Imagine you're analyzing the monthly sales performance of a company across different regions. You want to calculate:
# MAGIC The cumulative sales for each region over months.
# MAGIC The rank of each month based on sales within the same region.
# MAGIC
# MAGIC 𝐬𝐜𝐡𝐞𝐦𝐚 
# MAGIC data = [ ("East", "Jan", 200), ("East", "Feb", 300), 
# MAGIC ("East", "Mar", 250), ("West", "Jan", 400), 
# MAGIC ("West", "Feb", 350), ("West", "Mar", 450) ]
# MAGIC
# MAGIC  Define schema and create DataFrame 
# MAGIC
# MAGIC columns = ["Region", "Month", "Sales"]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

spark = SparkSession.builder.appName("MonthlySalesPerformance").getOrCreate()
data = [ ("East", "Jan", 200), ("East", "Feb", 300), ("East", "Mar", 250), ("West", "Jan", 400), ("West", "Feb", 350), ("West", "Mar", 450) ]
columns = ["Region", "Month", "Sales"]
df = spark.createDataFrame(data,columns)

# COMMAND ----------

windowspecs= Window.partitionBy("Region").orderBy("Sales")

result_df1 = df.withColumn("Commutative_sales", F.sum("Sales").over(windowspecs)) \
              .withColumn("Rank", F.rank().over(windowspecs))


result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 4
# MAGIC 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# MAGIC In PySpark, StructType and StructField are used for defining schema in DataFrames. Consider a scenario where you have employee data stored in a CSV file with the following structure:
# MAGIC Define an explicit schema for this dataset using StructType and StructField.
# MAGIC Load this data into a PySpark DataFrame using the defined schema.
# MAGIC Extract the employees who belong to the "IT" department and have a salary greater than 70000.
# MAGIC Split the Address column into two separate columns: City and State.
# MAGIC Save the transformed data into a Parquet file.
# MAGIC
# MAGIC 𝐬𝐜𝐡𝐞𝐦𝐚 
# MAGIC
# MAGIC schema = StructType([ StructField("Emp_ID", IntegerType(), True), StructField("Name", StringType(), True), 
# MAGIC StructField("Age", IntegerType(), True), 
# MAGIC StructField("Salary", IntegerType(), True), 
# MAGIC StructField("Department", StringType(), True), 
# MAGIC StructField("Address", StringType(), True) ]) 
# MAGIC
# MAGIC
# MAGIC data = [ (101, "Rajesh", 30, 60000, "IT", "Mumbai, Maharashtra"), 
# MAGIC (102, "Priya", 28, 75000, "HR", "Bengaluru, Karnataka"), 
# MAGIC (103, "Suresh", 35, 50000, "Finance", "Chennai, Tamil Nadu"), 
# MAGIC (104, "Anjali", 25, 80000, "IT", "Pune, Maharashtra"), (105, "Arjun", 40, 90000, "Management", "Hyderabad, Telangana") ]
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col,split


# COMMAND ----------

schema = StructType([ StructField("Emp_ID", IntegerType(), True), 
                      StructField("Name", StringType(), True), 
                      StructField("Age", IntegerType(), True), 
                      StructField("Salary", IntegerType(), True), 
                      StructField("Department", StringType(), True), 
                      StructField("Address", StringType(), True) ])
data = [ (101, "Rajesh", 30, 60000, "IT", "Mumbai, Maharashtra"), 
         (102, "Priya", 28, 75000, "HR", "Bengaluru, Karnataka"), 
         (103, "Suresh", 35, 50000, "Finance", "Chennai, Tamil Nadu"), 
         (104, "Anjali", 25, 80000, "IT", "Pune, Maharashtra"), 
         (105, "Arjun", 40, 90000, "Management", "Hyderabad, Telangana") ]

# COMMAND ----------

df1 = spark.createDataFrame(data, schema=schema)
df1.display()

# COMMAND ----------

filtered_df = df1.filter((col("Department")=="IT")& (col("Salary")>70000))
df_transformed = filtered_df.withColumn("City",split(col("Address"),",")[0]).withColumn("State",split(col("Address"),",")[1]).drop("Address")
df_transformed.show()
df_transformed.write.mode("overwrite").parquet("/tmp/filtered_employee.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 5
# MAGIC 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# MAGIC You have been given a large dataset containing employee salary details. Your goal is to optimize a PySpark job that performs a groupBy operation while minimizing the shuffle.
# MAGIC Task:
# MAGIC Write a PySpark job to calculate the total salary per department.
# MAGIC Optimize the job to reduce shuffle while performing the groupBy operation.
# MAGIC Explain why your optimization reduces shuffle and improves performance.
# MAGIC Approach:
# MAGIC To minimize shuffle during a groupBy operation, we should:
# MAGIC Use repartition() efficiently to avoid unnecessary partitions.
# MAGIC Use reduceByKey() instead of groupByKey(), as it performs local aggregation before shuffling.
# MAGIC If working with a DataFrame, use partitionBy() while writing output.
# MAGIC
# MAGIC 𝐬𝐜𝐡𝐞𝐦𝐚 
# MAGIC data = [ (101, "Rahul", "IT", 90000), (102, "Sita", "HR", 75000), 
# MAGIC (103, "Vikram", "IT", 85000), (104, "Priya", "HR", 72000), 
# MAGIC (105, "Anjali", "IT", 88000), (106, "Manish", "Sales", 67000), 
# MAGIC (107, "Neha", "Sales", 70000) ] 
# MAGIC
# MAGIC columns = ["emp_id", "name", "dept", "salary"]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# COMMAND ----------

spark = SparkSession.builder.appName("OptimizeData").getOrCreate()
data = [  (101, "Rahul", "IT", 90000), 
          (102, "Sita", "HR", 75000), 
          (103, "Vikram", "IT", 85000), 
           (104, "Priya", "HR", 72000),
           (105, "Anjali", "IT", 88000), 
            (106, "Manish", "Sales", 67000), 
            (107, "Neha", "Sales", 70000) ]

columns = ["emp_id", "name", "dept", "salary"]


# COMMAND ----------

df = spark.createDataFrame(data,columns)
rdd = df.rdd.map(lambda x: (x[2], x[3]))
optimized_result = rdd.reduceByKey(lambda x, y: x+y)
optimized_df = optimized_result.toDF(["dept", "total_salary"])
optimized_df.show()


# COMMAND ----------

df_optimized = df.repartition("dept").groupBy("dept").agg(sum("salary").alias("total_salary"))
df_optimized.show()