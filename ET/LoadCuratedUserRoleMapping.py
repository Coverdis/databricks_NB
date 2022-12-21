# Databricks notebook source
#File Name:LoadCuratedUserRoleMapping
#ADF Pipeline Name: ET_ADL
#SQLDW Table:NA
#Description:
  #Notebook to load ET User / Role data from ADL foundation to curated in ET folder

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pytz
from datetime import datetime, timedelta

import pandas as pd

# COMMAND ----------

# MAGIC %run /library/configFile

# COMMAND ----------

process_time=datetime.now(pytz.timezone("UTC")).strftime('%m/%d/%Y')
dbutils.widgets.text("runid", "1sdw2-we23d-qkgn9-uhbg2-hdj22")
runid = dbutils.widgets.get("runid")
dbutils.widgets.text("filename", "")
staging_filename = dbutils.widgets.get("filename")

# COMMAND ----------

# Function define to read the CSV file from a given ADL Folder
def read_csv_file(path, delimiter):
  
  role = spark.read.format("csv")\
      .option("inferSchema", "false")\
      .option("header","true")\
      .option("multiLine","true")\
      .option("delimiter",delimiter)\
      .option("quote", '"')\
      .option("escape",'"')\
      .option("nullValue","null")\
  .load(path)
  role = role.toDF(*(col.replace('\r', '') for col in role.columns))
  for col_name in role.columns:
    role = role.withColumn(col_name, F.regexp_replace(col_name, '[\\n\\r]', ' '))
    role = role.withColumn(col_name, F.regexp_replace(col_name, '^\s+|\s+$|\r//g', ''))
    role=role.withColumnRenamed(col_name,col_name.replace(' ','_'))    
  return role

# COMMAND ----------

# Function define to read the XLSX file from a given ADL Folder
def read_xlsx_file(path):
  
  file = spark.read.format("com.crealytics.spark.excel")\
                  .option("inferSchema", "true")\
                  .option("header", "true")\
                  .option("sheetName", "Sheet1")\
                  .load(path)
  return file

# COMMAND ----------

# Functiond defined to find the latest file in the Target folder from a given ADL folder
def latest_file_target(file_folder):
  if len(dbutils.fs.ls(file_folder))>0:
    recent_files_year=max([int(x.path.split('/')[-1][-11:-7]) for x in dbutils.fs.ls(file_folder)])
    recent_files_month=max([int(x.path.split('/')[-1][-6:-4].replace('_','')) for x in dbutils.fs.ls(file_folder) if int(x.path.split('/')[-1][-11:-7])==recent_files_year])
    if recent_files_month<10: recent_files_month='0'+str(recent_files_month)
    source=file_folder+'et_user_role_mapping_'+str(recent_files_year)+'_'+str(recent_files_month)+'.txt'
    return source

# COMMAND ----------

staging_file='dbfs:/mnt/foundation/et/'+ staging_filename
curated_path='dbfs:/mnt/curated/et/user_role/'
target_file=curated_path+'et_user_role_mapping_'+staging_file[45:][:7]+'.txt'

# COMMAND ----------

print(staging_file)
print(target_file)

# COMMAND ----------

if not file_exists(target_file):
  latest_file=latest_file_target(curated_path)
  if latest_file and file_exists(latest_file):
    # copy latest file and rename
    dbutils.fs.cp(latest_file, target_file, recurse = True)

# COMMAND ----------

# Loading source file
staging=read_xlsx_file(staging_file)
staging=staging.select(['PERSON_CODE','RBS_LEVEL_1','RBS_LEVEL_2','RBS_LEVEL_3','RBS_LEVEL_4','RBS_LEVEL_5','RBS_LEVEL_6','PERSON_ROLE_CODE'])
staging=staging.filter("PERSON_CODE is not null and PERSON_CODE<>''")
staging=staging.withColumn('PERSON_CODE',F.lower(staging.PERSON_CODE))
staging=staging.withColumnRenamed('RBS_LEVEL_1','RBS_LEVEL_1_upd')
staging=staging.withColumnRenamed('RBS_LEVEL_2','RBS_LEVEL_2_upd')
staging=staging.withColumnRenamed('RBS_LEVEL_3','RBS_LEVEL_3_upd')
staging=staging.withColumnRenamed('RBS_LEVEL_4','RBS_LEVEL_4_upd')
staging=staging.withColumnRenamed('RBS_LEVEL_5','RBS_LEVEL_5_upd')
staging=staging.withColumnRenamed('RBS_LEVEL_6','RBS_LEVEL_6_upd')
staging=staging.withColumnRenamed('PERSON_ROLE_CODE','PERSON_ROLE_CODE_upd')
staging=staging.withColumn('LAST_UPDATE_DATE_upd',F.lit(process_time))
#display(staging)

# COMMAND ----------

# Reading the target file 
if target_file and file_exists(target_file):
  target=read_csv_file(target_file,'|')
  
  # Joining to find the update records for each user id with new column
  staging_df1=target.join(staging,['PERSON_CODE'],how='left')
  
  staging_df1=staging_df1.withColumn('RBS_LEVEL_1_1',F.when(F.coalesce(staging_df1.RBS_LEVEL_1,F.lit('')) != F.coalesce(staging_df1.RBS_LEVEL_1_upd,F.lit('')), staging_df1.RBS_LEVEL_1_upd).otherwise(staging_df1.RBS_LEVEL_1))
  staging_df1=staging_df1.withColumn('RBS_LEVEL_2_1',F.when(F.coalesce(staging_df1.RBS_LEVEL_2,F.lit('')) != F.coalesce(staging_df1.RBS_LEVEL_2_upd,F.lit('')), staging_df1.RBS_LEVEL_2_upd).otherwise(staging_df1.RBS_LEVEL_2))
  staging_df1=staging_df1.withColumn('RBS_LEVEL_3_1',F.when(F.coalesce(staging_df1.RBS_LEVEL_3,F.lit('')) != F.coalesce(staging_df1.RBS_LEVEL_3_upd,F.lit('')), staging_df1.RBS_LEVEL_3_upd).otherwise(staging_df1.RBS_LEVEL_3))
  staging_df1=staging_df1.withColumn('RBS_LEVEL_4_1',F.when(F.coalesce(staging_df1.RBS_LEVEL_4,F.lit('')) != F.coalesce(staging_df1.RBS_LEVEL_4_upd,F.lit('')), staging_df1.RBS_LEVEL_4_upd).otherwise(staging_df1.RBS_LEVEL_4))
  staging_df1=staging_df1.withColumn('RBS_LEVEL_5_1',F.when(F.coalesce(staging_df1.RBS_LEVEL_5,F.lit('')) != F.coalesce(staging_df1.RBS_LEVEL_5_upd,F.lit('')), staging_df1.RBS_LEVEL_5_upd).otherwise(staging_df1.RBS_LEVEL_5))
  staging_df1=staging_df1.withColumn('RBS_LEVEL_6_1',F.when(F.coalesce(staging_df1.RBS_LEVEL_6,F.lit('')) != F.coalesce(staging_df1.RBS_LEVEL_6_upd,F.lit('')), staging_df1.RBS_LEVEL_6_upd).otherwise(staging_df1.RBS_LEVEL_6))
  staging_df1=staging_df1.withColumn('PERSON_ROLE_CODE_1',F.when(F.coalesce(staging_df1.PERSON_ROLE_CODE,F.lit('')) != F.coalesce(staging_df1.PERSON_ROLE_CODE_upd,F.lit('')), staging_df1.PERSON_ROLE_CODE_upd).otherwise(staging_df1.PERSON_ROLE_CODE))
  staging_df1=staging_df1.withColumn('LAST_UPDATE_DATE_1',F.when((staging_df1.RBS_LEVEL_1 != staging_df1.RBS_LEVEL_1_upd) | (staging_df1.RBS_LEVEL_2 != staging_df1.RBS_LEVEL_2_upd) | (staging_df1.RBS_LEVEL_3 != staging_df1.RBS_LEVEL_3_upd) | (staging_df1.RBS_LEVEL_4 != staging_df1.RBS_LEVEL_4_upd) | (staging_df1.RBS_LEVEL_5 != staging_df1.RBS_LEVEL_5_upd) | (staging_df1.RBS_LEVEL_6 != staging_df1.RBS_LEVEL_6_upd) | (staging_df1.PERSON_ROLE_CODE != staging_df1.PERSON_ROLE_CODE_upd), staging_df1.LAST_UPDATE_DATE_upd).otherwise(staging_df1.LAST_UPDATE_DATE))
  staging_df1=staging_df1.select(['PERSON_CODE','RBS_LEVEL_1_1','RBS_LEVEL_2_1','RBS_LEVEL_3_1','RBS_LEVEL_4_1','RBS_LEVEL_5_1','RBS_LEVEL_6_1','PERSON_ROLE_CODE_1','LAST_UPDATE_DATE_1'])
  
  # Renaming the column of staging dataframe against the target column
  staging_df1=staging_df1.withColumnRenamed('RBS_LEVEL_1_1','RBS_LEVEL_1')
  staging_df1=staging_df1.withColumnRenamed('RBS_LEVEL_2_1','RBS_LEVEL_2')
  staging_df1=staging_df1.withColumnRenamed('RBS_LEVEL_3_1','RBS_LEVEL_3')
  staging_df1=staging_df1.withColumnRenamed('RBS_LEVEL_4_1','RBS_LEVEL_4')
  staging_df1=staging_df1.withColumnRenamed('RBS_LEVEL_5_1','RBS_LEVEL_5')
  staging_df1=staging_df1.withColumnRenamed('RBS_LEVEL_6_1','RBS_LEVEL_6')
  staging_df1=staging_df1.withColumnRenamed('PERSON_ROLE_CODE_1','PERSON_ROLE_CODE')
  staging_df1=staging_df1.withColumnRenamed('LAST_UPDATE_DATE_1','LAST_UPDATE_DATE')
  
  # joining the staging with taget to find the new records from the staging and renaming the column to merge with target
  new_records_appending=staging.join(target,['PERSON_CODE'],how='leftanti')
  new_records_appending=new_records_appending.withColumnRenamed('RBS_LEVEL_1_upd','RBS_LEVEL_1')
  new_records_appending=new_records_appending.withColumnRenamed('RBS_LEVEL_2_upd','RBS_LEVEL_2')
  new_records_appending=new_records_appending.withColumnRenamed('RBS_LEVEL_3_upd','RBS_LEVEL_3')
  new_records_appending=new_records_appending.withColumnRenamed('RBS_LEVEL_4_upd','RBS_LEVEL_4')
  new_records_appending=new_records_appending.withColumnRenamed('RBS_LEVEL_5_upd','RBS_LEVEL_5')
  new_records_appending=new_records_appending.withColumnRenamed('RBS_LEVEL_6_upd','RBS_LEVEL_6')
  new_records_appending=new_records_appending.withColumnRenamed('PERSON_ROLE_CODE_upd','PERSON_ROLE_CODE')
  new_records_appending=new_records_appending.withColumnRenamed('LAST_UPDATE_DATE_upd','LAST_UPDATE_DATE')
    
  # Merging the both update and New Records 
  final_df=staging_df1.union(new_records_appending)
  final_df=final_df.withColumn('PERSON_ROLE_CODE',F.when(final_df.PERSON_ROLE_CODE=="",final_df.RBS_LEVEL_6).otherwise(final_df.PERSON_ROLE_CODE))
else:
  # Renaming the column of staging dataframe against the target column
  staging=staging.withColumnRenamed('RBS_LEVEL_1_upd','RBS_LEVEL_1')
  staging=staging.withColumnRenamed('RBS_LEVEL_2_upd','RBS_LEVEL_2')
  staging=staging.withColumnRenamed('RBS_LEVEL_3_upd','RBS_LEVEL_3')
  staging=staging.withColumnRenamed('RBS_LEVEL_4_upd','RBS_LEVEL_4')
  staging=staging.withColumnRenamed('RBS_LEVEL_5_upd','RBS_LEVEL_5')
  staging=staging.withColumnRenamed('RBS_LEVEL_6_upd','RBS_LEVEL_6')
  staging=staging.withColumnRenamed('PERSON_ROLE_CODE_upd','PERSON_ROLE_CODE')
  staging=staging.withColumnRenamed('LAST_UPDATE_DATE_upd','LAST_UPDATE_DATE')
  final_df=staging

# COMMAND ----------

# Writing into the curating layer with Repo
csv_temp='dbfs:/mnt/raw/ET_ROLE'+runid+staging_filename
final_df.coalesce(1).write\
            .option("sep", "|")\
            .option("header", "true")\
            .option("quote",  '"')\
            .option("escape", '"')\
            .option("nullValue", "null")\
        .mode('overwrite')\
      .csv(csv_temp)

# copy part-* csv file to foundation and rename
dbutils.fs.cp(dbutils.fs.ls(csv_temp)[-1][0], target_file, recurse = True)

# remove temp folder
dbutils.fs.rm(csv_temp, recurse = True)