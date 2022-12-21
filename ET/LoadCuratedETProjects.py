# Databricks notebook source
#File Name: LoadCuratedETProjects
#ADF Pipeline Name: ET_ADL
#SQLDW Table: NA
#Description:
  # Writes ET projects data to curated folder in ADL

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
et_projects = spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .option("multiLine","true")\
        .option("delimiter","|")\
        .option("quote", '"')\
        .option("escape",'"')\
        .option("nullValue","null")\
  .load("dbfs:/mnt/foundation/et/projects.txt")

et_projects = et_projects.toDF(*(col.replace('\r', '') for col in et_projects.columns))

for col_name in et_projects.columns:
  et_projects = et_projects.withColumn(col_name, F.regexp_replace(col_name, '^\s+|\s+$|\r//g', ''))
  
et_projects = et_projects.withColumnRenamed('ORIG_PROJ_CODE', 'PROJECT_CODE')
et_projects = et_projects.withColumnRenamed('NAME', 'PROJECT_NAME')
et_projects = et_projects.withColumnRenamed('PROJECT_PHASE', 'PROJECT_PHASE')
et_projects = et_projects.withColumnRenamed('THERAPY_AREA', 'THERAPY_AREA')
et_projects = et_projects.withColumnRenamed('PROJECT_STATUS', 'PROJECT_STATUS_DESCRIPTION')
et_projects = et_projects.withColumnRenamed('RECORD_STATUS', 'PROJECT_RECORD_STATUS')
et_projects = et_projects.withColumn('SOURCE_SYSTEM_NAME', F.lit("ET").cast(StringType()))

# display(et_projects)

et_projects = et_projects.select(
  'PROJECT_CODE',
  'PROJECT_NAME',
  'PROJECT_PHASE',
  'THERAPY_AREA',
  'PROJECT_STATUS_DESCRIPTION',
  'PROJECT_RECORD_STATUS',
  'SOURCE_SYSTEM_NAME',
  'STREAM'
  )

# display(et_projects)

# COMMAND ----------

# write to curated

tmp_file_path = 'dbfs:/mnt/raw/' + 'LoadCuratedETProjects-' + runid
curatedPath = 'dbfs:/mnt/curated/et/'

et_projects.coalesce(1).write\
            .option("sep", "|")\
            .option("header", "true")\
            .option("quote",  '"')\
            .option("escape", '"')\
            .option("nullValue", "null")\
        .csv(tmp_file_path)
# copy part-* csv file to curated and rename
dbutils.fs.cp(dbutils.fs.ls(tmp_file_path)[-1][0], curatedPath + 'project.txt', recurse = True)

# remove temp folders
dbutils.fs.rm(tmp_file_path, recurse = True)