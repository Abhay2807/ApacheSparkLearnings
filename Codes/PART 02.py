# Databricks notebook source
# To read Fire Department Calls for Service File
# Creating a dataframe using dataframe Reader API

df_fire_calls=spark.read.format("csv").option("header","true")\
.option("inferSchema","true").load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(df_fire_calls)

# COMMAND ----------

# To rename all columns to remove space between their names

new_df=df_fire_calls\
    .withColumnRenamed("Call Number", "CallNumber") \
    .withColumnRenamed("Unit ID", "UnitID") \
    .withColumnRenamed("Incident Number", "IncidentNumber") \
    .withColumnRenamed("Call Date", "CallDate") \
    .withColumnRenamed("Watch Date", "WatchDate") \
    .withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
    .withColumnRenamed("Available DtTm", "AvailableDtTm") \
    .withColumnRenamed("Zipcode of Incident", "Zipcode") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("ALS Unit", "ALSUnit") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict")

display(new_df)

# COMMAND ----------

# To get data type of all columns in a DF

new_df.printSchema()

# COMMAND ----------

# AvailableDtTm This column should be of type : TimeStamp instead of string dataType 
from pyspark.sql.functions import *

new_df_2=new_df.withColumn("AvailableDtTm",to_timestamp("AvailableDtTm","MM/dd/yyyy hh:mm:ss a"))\
    .withColumn("Delay",round("Delay",2))
new_df_2.printSchema()

# COMMAND ----------

display(new_df_2)

# COMMAND ----------



# COMMAND ----------

new_df_2.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC How many distinct types of calls were made to the Fire Department?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct CallType) as distinct_call_type_count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null

# COMMAND ----------

new_df_2.createOrReplaceTempView("ans_view")

# We created temporary view to run sql over a df

sql_df=spark.sql(""" 
select count(distinct CallType) as distinct_call_type_count
from ans_view
where CallType is not null """)

display(sql_df)

# COMMAND ----------

ans_df_1=new_df_2.where("CallType is not NULL")\
    .select("CallType").distinct()

print(ans_df_1.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC What were distinct types of calls made to the Fire Department?
# MAGIC select distinct CallType as distinct_call_types
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null

# COMMAND ----------

ans_df_2=new_df_2.select("CallType").where("CallType is not NULL").distinct()
display(ans_df_2)

# COMMAND ----------

# MAGIC %md
# MAGIC Find out all response for delayed times greater than 5 mins?
# MAGIC select CallNumber, Delay
# MAGIC from fire_service_calls_tbl
# MAGIC where Delay > 5

# COMMAND ----------

ans_df_3=new_df_2.select("Delay","CallNumber").where("Delay>5")
ans_df_3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC What were the most common call types?
# MAGIC select CallType, count(*) as count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null
# MAGIC group by CallType
# MAGIC order by count desc

# COMMAND ----------

ans_df_4=new_df_2.where("CallType is not NULL").select("CallType").groupBy("CallType").count().orderBy("count",Ascending=True)
ans_df_4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC What zip codes accounted for most common calls?
# MAGIC select CallType, ZipCode, count(*) as count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null
# MAGIC group by CallType, Zipcode
# MAGIC order by count desc

# COMMAND ----------

ans_df_4=new_df_2.where("CallType is not NULL").select("CallType",'ZipCode').groupBy("CallType","ZipCode").count().orderBy("count",Ascending=True)
ans_df_4.display()

# COMMAND ----------

What San Francisco neighborhoods are in the zip codes 94102 and 94103
select distinct Neighborhood, Zipcode
from fire_service_calls_tbl
where Zipcode== 94102 or Zipcode == 94103

# COMMAND ----------

df=new_df_2.select("Neighborhood","ZipCode").where("ZipCode IN (94102,94103) ").distinct()
df.display()

# COMMAND ----------

What was the sum of all calls, average, min and max of the response times for calls?
select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
from fire_service_calls_tbl

# COMMAND ----------

new_df=new_df_2.select(sum("NumAlarms"),round(avg("Delay"),4),min("Delay"),max("Delay")).display()

# COMMAND ----------

How many distinct years of data is in the CSV file?
select distinct year(to_timestamp(CallDate, "MM/dd/yyyy")) as year_num
from fire_service_calls_tbl
order by year_num

# COMMAND ----------

new_df=new_df_2.select(year("CallDate")).distinct().orderBy(year("CallDate")).display()


# COMMAND ----------

What week of the year in 2018 had the most fire calls?
select weekofyear(to_timestamp(CallDate, "MM/dd/yyyy")) week_year, count(*) as count
from fire_service_calls_tbl 
where year(to_timestamp(CallDate, "MM/dd/yyyy")) == 2018
group by week_year
order by count desc

# COMMAND ----------

result=new_df_2.filter(year("CallDate")==2018).groupBy(weekofyear("CallDate").alias("Week-year")).agg(count("*").alias("c")).orderBy(desc("c"))
result.display()

# COMMAND ----------

What neighborhoods in San Francisco had the worst response time in 2018?
select Neighborhood, Delay
from fire_service_calls_tbl 
where year(to_timestamp(CallDate, "MM/dd/yyyy")) == 2018

# COMMAND ----------

res=new_df_2.filter(year("CallDate")==2018).select("Neighborhood","Delay").orderBy(desc("Delay")).display()
