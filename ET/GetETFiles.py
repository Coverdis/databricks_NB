# Databricks notebook source
#File Name:GetETFiles
#ADF Pipeline Name: ET_ADL
#SQLDW Table:NA
#Description:
  # Notebook to get the list of et_user_role_mapping_* files

# COMMAND ----------

# MAGIC %run "/library/configFile"

# COMMAND ----------

import pytz
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from glob import glob
import re

processTime = datetime.now(pytz.timezone("UTC")).strftime('%Y-%m-%dT%H:%M:%S')

dbutils.widgets.text("runid", "222")
runid = dbutils.widgets.get("runid")

# COMMAND ----------

files = [x.split('/')[-1:] for x in glob('/dbfs/mnt/foundation/et/et_user_role_mapping_*.xlsx', recursive=True)]

# COMMAND ----------

print(files)

# COMMAND ----------

schemaString = "Files"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.createDataFrame(files, schema)

if df.count()==0:
  df = spark.createDataFrame([], schema)

# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------

# write to raw
tmp_file_path = 'dbfs:/mnt/raw/timesheet/GetETFiles'
curatedPath = 'dbfs:/mnt/raw/timesheet/'
df.coalesce(1).write\
            .option("sep", "|")\
            .option("header", "true")\
            .option("quote",  '"')\
            .option("escape", '"')\
            .option("nullValue", "null")\
        .csv(tmp_file_path)
# copy part-* csv file to curated and rename
dbutils.fs.cp(dbutils.fs.ls(tmp_file_path)[-1][0], curatedPath + 'etfiles.txt', recurse = True)

# remove temp folders
dbutils.fs.rm(tmp_file_path, recurse = True)