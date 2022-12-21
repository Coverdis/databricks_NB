# Databricks notebook source
#File Name: LoadCuratedETPersonRole
#ADF Pipeline Name: ET_ADL
#SQLDW Table: NA
#Description:
  # Writes ET prson role data to curated folder in ADL

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
df = spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .option("multiLine","true")\
        .option("delimiter","|")\
        .option("quote", '"')\
        .option("escape",'"')\
        .option("nullValue","null")\
  .load("dbfs:/mnt/foundation/et/link_usr_role.txt")

df = df.toDF(*(col.replace('\r', '') for col in df.columns))


  
df = df.withColumnRenamed('USER_ID', 'PERSON_MUD_ID')
df = df.withColumnRenamed('ROLE_ID', 'RBS_ID')
df = df.withColumn('SOURCE', F.lit('ET').cast(StringType()))

df = df.select(
  'PERSON_MUD_ID',
  'RBS_ID',
  'SOURCE'
    )

# COMMAND ----------

# write to curated

tmp_file_path = 'dbfs:/mnt/raw/' + 'LoadCuratedETPersonRole-' + runid
curatedPath = 'dbfs:/mnt/curated/et/'

df.coalesce(1).write\
            .option("sep", "|")\
            .option("header", "true")\
            .option("quote",  '"')\
            .option("escape", '"')\
            .option("nullValue", "null")\
        .csv(tmp_file_path)
# copy part-* csv file to curated and rename
dbutils.fs.cp(dbutils.fs.ls(tmp_file_path)[-1][0], curatedPath + 'person_role.txt', recurse = True)

# remove temp folders
dbutils.fs.rm(tmp_file_path, recurse = True)