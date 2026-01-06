// Databricks notebook source
val diamondsDf = spark.read.format("csv").option("header", "true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")


// COMMAND ----------

diamondsDf.show()

// COMMAND ----------

val filteredDf = diamondsDf.filter(diamondsDf("cut") === "Ideal" && diamondsDf("price") > 1000)

// COMMAND ----------

import org.apache.spark.sql.functions.avg
val groupedDf = filteredDf.groupBy("clarity").agg(avg("price"))

// COMMAND ----------

val orderedDf = groupedDf.orderBy(groupedDf("avg(price)").desc)

// COMMAND ----------

val orderedDfNew = orderedDf.withColumnRenamed("avg(price)", "avg_price")

// COMMAND ----------

