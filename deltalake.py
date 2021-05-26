# Databricks notebook source
# DBTITLE 1,Import DeltaTable to work with DeltaLake
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,OPTIONAL - Save two new versions of employees - Remove comments to execute if needed
# from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# #Version 1
# data = [("James","","Smith","36636","M",3000),
#     ("Michael","Rose","","40288","M",4000),
#     ("Robert","","Williams","42114","M",4000),
#     ("Maria","Anne","Jones","39192","F",4000),
#     ("Jen","Mary","Brown","","F",-1)
#   ]

# schema = StructType([ \
#     StructField("firstname",StringType(),True), \
#     StructField("middlename",StringType(),True), \
#     StructField("lastname",StringType(),True), \
#     StructField("id", StringType(), True), \
#     StructField("gender", StringType(), True), \
#     StructField("salary", IntegerType(), True) \
#   ])
 
# df = spark.createDataFrame(data=data,schema=schema)
# df.write.mode("overwrite").save("/mnt/delta/employee")

# #Version 2
# data = [("Sacha","","Smith","36636","M",3000),
#     ("Dimitri","Rose","","40288","M",4000),
#     ("Raskolnikov","","Williams","42114","M",4000),
#     ("Macha","Anne","Jones","39192","F",4000),
#     ("Anton","Mary","Brown","","F",-1)
#   ]

# schema = StructType([ \
#     StructField("firstname",StringType(),True), \
#     StructField("middlename",StringType(),True), \
#     StructField("lastname",StringType(),True), \
#     StructField("id", StringType(), True), \
#     StructField("gender", StringType(), True), \
#     StructField("salary", IntegerType(), True) \
#   ])
 
# df = spark.createDataFrame(data=data,schema=schema)
# df.write.mode("overwrite").save("/mnt/delta/employee")

# COMMAND ----------

# DBTITLE 1,Load the file as Delta table
employees = spark.read.load("/mnt/delta/employee", format="delta")
employees.show()

# COMMAND ----------

# DBTITLE 1,Display history of the table
employees_for_history = DeltaTable.forPath(spark, "/mnt/delta/employee")
display(employees_for_history.history())

# COMMAND ----------

# DBTITLE 1,Display the first version of data
first_version_of_employees = spark.read.option("versionAsOf", 0).format("delta").load("/mnt/delta/employee")
display(first_version_of_employees)

# COMMAND ----------

# DBTITLE 1,Display the last version of data
second_version_of_employees = spark.read.format("delta").load("/mnt/delta/employee")
display(second_version_of_employees)

# COMMAND ----------

# DBTITLE 1,Display the version of the timestamp 2021-05-03T15:06:07.000+0000
#Be aware that this timestamp could not suit your data
#Choose the timestamp regarding the history of your table
first_version_of_employees = spark.read.option("timestampAsOf", "2021-05-03T15:06:07.000+0000").format("delta").load("/mnt/delta/employee")
display(first_version_of_employees)

# COMMAND ----------


