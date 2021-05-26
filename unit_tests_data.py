# Databricks notebook source
# DBTITLE 1,Install Great Expectations
# MAGIC %pip install great_expectations

# COMMAND ----------

# DBTITLE 1,Import Great Expectations
import great_expectations as ge

# COMMAND ----------

# DBTITLE 1,Retrieve the Titanic dataset
df = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/train.csv")

# COMMAND ----------

# DBTITLE 1,Display the Titanic dataset
display(df)

# COMMAND ----------

# DBTITLE 1,Transform the DataFrame as a GreatExpectations DataFrame
import great_expectations as ge
df_ge = ge.dataset.SparkDFDataset(df)

# COMMAND ----------

# DBTITLE 1,Launch a unit test to chek that the Embarked column is as expected
df_ge.expect_column_distinct_values_to_be_in_set("Embarked", ["S", "Q", "C"])

# COMMAND ----------


