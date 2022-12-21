# Databricks notebook source
#File Name: LoadCuratedETActivities
#ADF Pipeline Name: ET_ADL
#SQLDW Table: NA
#Description:
  # Writes ET activities data to curated folder in ADL

# COMMAND ----------

# MAGIC %run "/library/configFile"

# COMMAND ----------

dbutils.widgets.text("runid", "fc5340b9-0ec9-4745-aaa1-da5d64995f61")
runid = dbutils.widgets.get("runid")

# COMMAND ----------

import pytz
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
processTime = datetime.now(pytz.timezone("UTC")).strftime('%Y-%m-%dT%H:%M:%S')

# COMMAND ----------

# Load ET Data
et_activity = spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .option("multiLine","true")\
        .option("delimiter","|")\
        .option("quote", '"')\
        .option("escape",'"')\
        .option("nullValue","null")\
  .load("dbfs:/mnt/foundation/et/activities.txt")

et_activity = et_activity.toDF(*(col.replace('\r', '') for col in et_activity.columns))

for col_name in et_activity.columns:
  et_activity = et_activity.withColumn(col_name, F.regexp_replace(col_name, '^\s+|\s+$|\r//g', ''))
  
et_activity = et_activity.withColumnRenamed('ORIG_ACT_CODE', 'ACTIVITY_CODE')
et_activity = et_activity.withColumnRenamed('NAME', 'ACTIVITY_NAME') #UPPER_NAME ?
et_activity = et_activity.withColumnRenamed('RECORD_STATUS', 'ACTIVITY_RECORD_STATUS')
# et_activity = et_activity.withColumn('SOURCE_SYSTEM_NAME', F.lit("ET").cast(StringType()))
# Adding source for Vaccines and non Vaccines activities
et_activity = et_activity.withColumnRenamed('STREAM', 'SOURCE_SYSTEM_NAME')
et_activity = et_activity.withColumn('ACTIVITY_TYPE', F.lit(None).cast(StringType()))

# display(et_activity)

et_activity = et_activity.select(
  'ACTIVITY_CODE',
  'ACTIVITY_NAME',
  'ACTIVITY_RECORD_STATUS',
  'SOURCE_SYSTEM_NAME',
  'ACTIVITY_TYPE'
  )



# COMMAND ----------

# write to curated

tmp_file_path = 'dbfs:/mnt/raw/' + 'LoadCuratedETActivities-' + runid
curatedPath = 'dbfs:/mnt/curated/et/'

et_activity.coalesce(1).write\
            .option("sep", "|")\
            .option("header", "true")\
            .option("quote",  '"')\
            .option("escape", '"')\
            .option("nullValue", "null")\
        .csv(tmp_file_path)
# copy part-* csv file to curated and rename
dbutils.fs.cp(dbutils.fs.ls(tmp_file_path)[-1][0], curatedPath + 'activity.txt', recurse = True)

# remove temp folders
dbutils.fs.rm(tmp_file_path, recurse = True)