# Databricks notebook source
# To read a file from a location using dataframe(df)

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/files/ab.csv")

display(df)

or

df=spark.read.csv("/FileStore/files/ab.csv",header="true",inferSchema="true")

# COMMAND ----------

# To read Fire Department Calls for Service File
# Creating a dataframe using dataframe Reader API

df_fire_calls=spark.read.format("csv").option("header","true")\
.option("inferSchema","true").load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")




# COMMAND ----------

# To get first 10 records from the dataframe
df_fire_calls.show(10)

# COMMAND ----------

display(df_fire_calls)

# COMMAND ----------

#To convert our df into a global temporary view ( Like Table on which we can run SQL Queries)

df_fire_calls.createGlobalTempView("fire_view")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM global_temp.fire_view
# MAGIC LIMIT 3
# MAGIC

# COMMAND ----------

# Creating a spark DATABASE TABLE using spark SQL 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo_db
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.fire_calls
# MAGIC (
# MAGIC CallNumber integer,
# MAGIC   UnitID string,
# MAGIC   IncidentNumber integer,
# MAGIC   CallType string,
# MAGIC   CallDate string,
# MAGIC   WatchDate string,
# MAGIC   CallFinalDisposition string,
# MAGIC   AvailableDtTm string,
# MAGIC   Address string,
# MAGIC   City string,
# MAGIC   Zipcode integer,
# MAGIC   Battalion string,
# MAGIC   StationArea string,
# MAGIC   Box string,
# MAGIC   OriginalPriority string,
# MAGIC   Priority string,
# MAGIC   FinalPriority integer,
# MAGIC   ALSUnit boolean,
# MAGIC   CallTypeGroup string,
# MAGIC   NumAlarms integer,
# MAGIC   UnitType string,
# MAGIC   UnitSequenceInCallDispatch integer,
# MAGIC   FirePreventionDistrict string,
# MAGIC   SupervisorDistrict string,
# MAGIC   Neighborhood string
# MAGIC ) using parquet
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO demo_db.fire_calls
# MAGIC VALUES(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
# MAGIC null, null, null, null, null, null)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM demo_db.fire_calls

# COMMAND ----------

# MAGIC %sql
# MAGIC # It is not a better way to load data into spark tables
# MAGIC
# MAGIC # Deleting records of table
# MAGIC
# MAGIC TRUNCATE TABLE demo_db.fire_calls
# MAGIC
# MAGIC # Better way will be
# MAGIC
# MAGIC INSERT INTO demo_db.fire_calls
# MAGIC select * from global_temp.fire_view
# MAGIC
# MAGIC # Now we get all records
# MAGIC
# MAGIC SELECT * FROM demo_db.fire_calls

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.fire
# MAGIC (
# MAGIC    CallNumber integer,
# MAGIC   UnitID string,
# MAGIC   IncidentNumber integer,
# MAGIC   CallType string,
# MAGIC   CallDate string,
# MAGIC   WatchDate string,
# MAGIC   CallFinalDisposition string,
# MAGIC   AvailableDtTm string,
# MAGIC   Address string,
# MAGIC   City string,
# MAGIC   Zipcode integer,
# MAGIC   Battalion string,
# MAGIC   StationArea string,
# MAGIC   Box string,
# MAGIC   OriginalPriority string,
# MAGIC   Priority string,
# MAGIC   FinalPriority integer,
# MAGIC   ALSUnit boolean,
# MAGIC   CallTypeGroup string,
# MAGIC   NumAlarms integer,
# MAGIC   UnitType string,
# MAGIC   UnitSequenceInCallDispatch integer,
# MAGIC   FirePreventionDistrict string,
# MAGIC   SupervisorDistrict string,
# MAGIC   Neighborhood string,
# MAGIC   Location string,
# MAGIC   RowID string,
# MAGIC   Delay float
# MAGIC ) using parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO demo_db.fire
# MAGIC SELECT * FROM global_temp.fire_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_db.fire

# COMMAND ----------


