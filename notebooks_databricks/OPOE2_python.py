# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

# Credentials set up by running config
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("wasbs://npdfiles@npdblobopoe2.blob.core.windows.net/field_production_monthly.csv")

# COMMAND ----------

# Create new column full of the number 1
df_new = df.withColumn("opoe", lit(1))

# COMMAND ----------

# Create a user defined function
def addone(s):
  return s + 1

# Needed to be used within SparkSQL
sqlContext.udf.register("addOne", addone, IntegerType())

# COMMAND ----------

# Bring in the user defined function already set up
addone_udf = udf(addone, IntegerType())

df_new = df_new.withColumn("opoe2", addone_udf("opoe"))

# COMMAND ----------

display(df_new)

# COMMAND ----------

df_new.repartition(1).write.format('com.databricks.spark.csv').save("wasbs://npdfiles@npdblobopoe2.blob.core.windows.net/new.csv",header = 'true', mode='overwrite')