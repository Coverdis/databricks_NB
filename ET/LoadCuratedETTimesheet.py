# Databricks notebook source
#File Name:LoadCuratedETTimesheet
#ADF Pipeline Name: ET_ADL
#SQLDW Table:NA
#Description:
  #Notebook to load Timesheet data from ADL foundation to curated layer in ET folder

# COMMAND ----------

# MAGIC %run "/library/configFile"

# COMMAND ----------

dbutils.widgets.text("runid", "1sdw2-we23d-qkgn9-uhbg2-hdj22")
dbutils.widgets.text("year", "2021")
dbutils.widgets.text("month", "1")
runid = dbutils.widgets.get("runid")
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
file = 'timesheets-' + year + '-' + month + '.txt'
print (year)
print(month)

# COMMAND ----------

import pytz
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
processTime = datetime.now(pytz.timezone("UTC")).strftime('%Y-%m-%dT%H:%M:%S')

foundationPath = 'dbfs:/mnt/foundation/et/timesheet/'
curatedPath='dbfs:/mnt/curated/et/timesheet/'
unifiedPathPersonRole='dbfs:/mnt/unified/resource_management/person_role_mapping/'

# COMMAND ----------

# Functiond defined to find the oldest file from a given ADL folder
def oldest_file_target(file_folder):
  if len(dbutils.fs.ls(file_folder))>0:
    file_year=min([int(x.path.split('/')[-1][-11:-7]) for x in dbutils.fs.ls(file_folder)])
    file_month=min([int(x.path.split('/')[-1][-6:-4].replace('-','')) for x in dbutils.fs.ls(file_folder) if int(x.path.split('/')[-1][-11:-7])==file_year])
    if file_month<10: file_month='0'+str(file_month)
    source=file_folder+'person_role_mapping-'+str(file_year)+'-'+str(file_month)+'.txt'
    return source

# COMMAND ----------

timesheet = spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .option("multiLine","true")\
        .option("delimiter","|")\
        .option("quote", '"')\
        .option("escape",'"')\
        .option("nullValue","null")\
  .load(foundationPath + year + '/' + file)

timesheet = timesheet.toDF(*(col.replace('\r', '') for col in timesheet.columns))

timesheet.createOrReplaceTempView('timesheet')

# COMMAND ----------

role_file = unifiedPathPersonRole+'person_role_mapping-'+year+'-'+month+'.txt'

# Copy user role mapping mmonthly file to the unified layer. For historical months, we peek the oldest one
if not file_exists(role_file):
  role_file = oldest_file_target(unifiedPathPersonRole)
  
role = spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .option("multiLine","true")\
        .option("delimiter","|")\
        .option("quote", '"')\
        .option("escape",'"')\
        .option("nullValue","null")\
  .load(role_file)
  
role.createOrReplaceTempView('role')

# COMMAND ----------

print(role_file)

# COMMAND ----------

query='SELECT PROJECT_ID as PROJECT_CODE,PROJECT_DESC as PROJECT_NAME,PROJECT_CATEGORY_DESC,ACTIVITY_ID as ACTIVITY_CODE,ACTIVITY_DESC as ACTIVITY_NAME,COST_CENTER_ID as COST_CENTER_CODE,UNIT,START_DATE,\
r.RBS_LEVEL_6 as LVL6_RBS_DESC,\
r.PERSON_ROLE_CODE,r.RBS_LEVEL_1,r.RBS_LEVEL_2,r.RBS_LEVEL_3,r.RBS_LEVEL_4,r.RBS_LEVEL_5,r.RBS_LEVEL_6,\
PRO_RECOMMENDED_FLG as PROJECT_RECOMMENDED_FLAG,ACT_RECOMMENDED_FLG as ACTIVITY_RECOMMENDED_FLAG,tm.user_id,tm.USER_NAME,FTES_COVERED,STANDARD_FTES,STANDARD_HOURS,ACTUAL_FTES,ACTUAL_HOURS,ACTIVITY_RESOURCE_ID \
FROM timesheet tm left outer join role r  on (lower(tm.user_id)=lower(r.PERSON_CODE))' 
tm=sqlContext.sql(query)
tm=tm.withColumn('SOURCE', F.lit('ET').cast(StringType()))

# COMMAND ----------

tm=tm.withColumn('RBS_ID',F.when(tm.PERSON_ROLE_CODE.isNotNull(),tm.PERSON_ROLE_CODE).otherwise(F.concat(F.when(tm.RBS_LEVEL_1.isNull(), F.lit('')).otherwise(tm.RBS_LEVEL_1), F.lit('-'), F.when(tm.RBS_LEVEL_2.isNull(), F.lit('')).otherwise(tm.RBS_LEVEL_2), F.lit('-'), F.when(tm.RBS_LEVEL_3.isNull(), F.lit('')).otherwise(tm.RBS_LEVEL_3), F.lit('-'), F.when(tm.RBS_LEVEL_4.isNull(), F.lit('')).otherwise(tm.RBS_LEVEL_4), F.lit('-'), F.when(tm.RBS_LEVEL_5.isNull(), F.lit('')).otherwise(tm.RBS_LEVEL_5), F.lit('-'), F.when(tm.RBS_LEVEL_6.isNull(), F.lit('')).otherwise(tm.RBS_LEVEL_6))))

# COMMAND ----------

tm=tm.drop('PERSON_ROLE_CODE','RBS_LEVEL_1','RBS_LEVEL_2','RBS_LEVEL_3','RBS_LEVEL_4','RBS_LEVEL_5','RBS_LEVEL_6')

# COMMAND ----------

# write to curated
tmp_file_path = 'dbfs:/mnt/raw/timesheet' +year + month + runid
tm.coalesce(1).write\
            .option("sep", "|")\
            .option("header", "true")\
            .option("quote",  '"')\
            .option("escape", '"')\
            .option("nullValue", "null")\
        .csv(tmp_file_path)
# copy part-* csv file to curated and rename
dbutils.fs.cp(dbutils.fs.ls(tmp_file_path)[-1][0], curatedPath + year + '/' + file, recurse = True)

# remove temp folders
dbutils.fs.rm(tmp_file_path, recurse = True)