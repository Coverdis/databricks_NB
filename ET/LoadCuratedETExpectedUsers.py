# Databricks notebook source
#File Name: LoadCuratedETProjects
#ADF Pipeline Name: ET_ADL
#SQLDW Table: NA
#Description:
  # Writes ET Expected Users data to curated folder in ADL

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
et_users = spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .option("multiLine","true")\
        .option("delimiter","|")\
        .option("quote", '"')\
        .option("escape",'"')\
        .option("nullValue","null")\
  .load("dbfs:/mnt/foundation/et/et_expected_users.txt")

et_users = et_users.toDF(*(col.replace('\r', '') for col in et_users.columns))

for col_name in et_users.columns:
  et_users = et_users.withColumn(col_name, F.regexp_replace(col_name, '^\s+|\s+$|\r//g', ''))
  


# display(et_projects)

et_users = et_users.select(
  'USER_ID',
  'FTES_COVERED',
  'STANDARD_HOURS',
  'CONTRACTED_HOURS',
  'COST_CENTER_ID'
  )

# display(et_projects)

# COMMAND ----------

# write to curated

tmp_file_path = 'dbfs:/mnt/raw/' + 'LoadCuratedETExpectedUsers-' + runid
curatedPath = 'dbfs:/mnt/curated/et/'

et_users.coalesce(1).write\
            .option("sep", "|")\
            .option("header", "true")\
            .option("quote",  '"')\
            .option("escape", '"')\
            .option("nullValue", "null")\
        .csv(tmp_file_path)
# copy part-* csv file to curated and rename
dbutils.fs.cp(dbutils.fs.ls(tmp_file_path)[-1][0], curatedPath + 'et_expected_users.txt', recurse = True)

# remove temp folders
dbutils.fs.rm(tmp_file_path, recurse = True)