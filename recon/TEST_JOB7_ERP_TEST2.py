import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
#from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp
#import pandas as pd
#import numpy
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#test
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#reading all erp files from s3

s3_eu= "s3://dev-globalreporting/OPERATIONS/ORACLE_ERP/TRANSACTION_DETAILS_REPORT/EU/TEST/"

s3_eu_df = spark.read.format("csv").option("delimiter",",").option("inferSchema", "true").option("header","true").option("multiLine", "true").option("quote", "\"").option("escape", "\"").load(s3_eu + "*.csv")


#converting all df into table 
#s3_cm_df.createOrReplaceTempView("erp_table1")
s3_eu_df.createOrReplaceTempView("erp_table2")



#query to combine all records from all df's
erp_query= """select * from erp_table2"""
erp_df=spark.sql(erp_query)
#erp_df=erp_df.withColumn('Entered Amount',col('Entered Amount').cast('decimal(18,2)'))
#erp_df=erp_df.withColumn('Prepared date',col('Prepared date').cast(TimestampType()))
#erp_df = erp_df.withColumn('Prepared date', to_timestamp(col('Prepared date'), 'yyyy-MM-dd hh:mm:ss a'))

count_erp_df=erp_df.count()
print(count_erp_df)
df_jan=erp_df.filter(erp_df.Period== 'JAN-24').count()
print(df_jan)

#converting dataframe into DynamicFrame
erp_df = DynamicFrame.fromDF(erp_df, glueContext, "erp_df")


#creating table and loading into snowflake table erp table
SnowflakeSink1 = glueContext.write_dynamic_frame.from_options(
    frame=erp_df,
    connection_type="snowflake",
    connection_options={
      "autopushdown": "on",
        "sfWarehouse": "GLOBAL_REPORTING_WH",
        "dbtable": "ORACLE_ERP_RECON_BACKUP",
        "connectionName": "dev-snowflake-globalreporting",
        "preactions": "truncate table ORACLE_ERP_RECON_BACKUP",
        "sfDatabase": "DEV_GR_STG",
        "sfSchema": "ETL_CONFIG",
        "sfUser": "svcgrpsfrw",
    },
    transformation_ctx="SnowflakeSink1",
)

job.commit()
